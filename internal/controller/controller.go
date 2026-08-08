package controller

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

// PullRequestCreator is the forge surface owned by the pull-request saga.
type PullRequestCreator interface {
	CreatePullRequest(context.Context, forge.Target, forge.CreatePullRequestRequest) (forge.PullRequest, error)
	FindPullRequest(context.Context, forge.Target, string, string, string) (forge.PullRequest, bool, error)
	GetPullRequest(context.Context, forge.Target, int) (forge.PullRequestState, error)
	PullRequestReplyExists(context.Context, forge.Target, forge.ReplyRequest, string) (bool, error)
	ReplyToPullRequest(context.Context, forge.Target, forge.ReplyRequest) error
}

type Controller struct {
	store           *store.Store
	kubernetes      kubernetes.Interface
	config          config.Config
	pullRequests    PullRequestCreator
	logger          *slog.Logger
	locks           *keyedLocks
	providerTimeout time.Duration
}

type attemptResourceSnapshot struct {
	Job         batchv1.Job   `json:"job"`
	Secret      corev1.Secret `json:"secret"`
	ForgeTarget *forge.Target `json:"forge_target,omitempty"`
}

var errResourceCreationBlocked = errors.New("resource creation blocked by current task outcome")

func New(db *store.Store, client kubernetes.Interface, cfg config.Config, pullRequests PullRequestCreator) (*Controller, error) {
	if db == nil {
		return nil, errors.New("controller Store is nil")
	}
	if client == nil {
		return nil, errors.New("controller Kubernetes client is nil")
	}
	if pullRequests == nil {
		return nil, errors.New("controller PullRequestCreator is nil")
	}
	if strings.TrimSpace(cfg.Controller.Namespace) == "" {
		return nil, errors.New("controller namespace is empty")
	}
	if cfg.Controller.Deadline <= 0 {
		return nil, errors.New("controller deadline must be positive")
	}
	seen := make(map[string]struct{}, len(cfg.Repositories))
	for _, repository := range cfg.Repositories {
		if _, err := forgeTarget(cfg, repository); err != nil {
			return nil, err
		}
		if strings.TrimSpace(repository.Name) == "" {
			continue
		}
		if _, exists := seen[repository.Name]; exists {
			return nil, fmt.Errorf("duplicate repository name %q", repository.Name)
		}
		seen[repository.Name] = struct{}{}
	}
	return &Controller{
		store:           db,
		kubernetes:      client,
		config:          cfg,
		pullRequests:    pullRequests,
		logger:          slog.Default(),
		locks:           newKeyedLocks(),
		providerTimeout: 15 * time.Second,
	}, nil
}

func (c *Controller) CreateTask(ctx context.Context, params store.CreateTaskParams) (store.Task, error) {
	if params.IdempotencyKey != "" {
		existing, err := c.store.GetTaskByIdempotencyKey(ctx, params.IdempotencyKey)
		if err == nil {
			return existing, nil
		}
		if !errors.Is(err, store.ErrNotFound) {
			return store.Task{}, fmt.Errorf("load task by idempotency key: %w", err)
		}
	}
	repository, err := c.repository(params.Repository)
	if err != nil {
		return store.Task{}, err
	}
	if err := c.validateAttemptConfig(params, repository); err != nil {
		return store.Task{}, err
	}
	created, inserted, err := c.store.CreateTaskOnce(ctx, params)
	if err != nil {
		return store.Task{}, err
	}
	if !inserted {
		return created, nil
	}
	unlock, err := c.locks.lock(ctx, created.ID)
	if err != nil {
		return created, err
	}
	created, err = c.store.GetTask(ctx, created.ID)
	if err != nil {
		unlock()
		return store.Task{}, err
	}
	defer unlock()
	log := c.logger.With("task", created.ID, "attempt", created.CurrentAttemptID)
	attempt, err := c.store.CurrentAttempt(ctx, created.ID)
	if err != nil {
		return created, err
	}
	if created.CancellationRequested || terminal(created.State) {
		return created, nil
	}
	if _, _, err := c.prepareAttemptSnapshot(ctx, created, attempt, repository); err != nil {
		return created, err
	}
	if stateAtOrAfter(created.State, task.JOB_PENDING) {
		return created, nil
	}
	if created.State == task.RECEIVED {
		if err := c.transition(ctx, created.ID, task.RECEIVED, task.QUEUED, "accepted and queued", "controller"); err != nil {
			return created, err
		}
		created.State = task.QUEUED
	}
	if created.State == task.QUEUED {
		if err := c.transition(ctx, created.ID, task.QUEUED, task.CREATING_JOB, "creating worker Job", "controller"); err != nil {
			return store.Task{}, err
		}
		created.State = task.CREATING_JOB
	}
	if created.State != task.CREATING_JOB {
		return created, fmt.Errorf("%w: new task %q is %q before resource creation", store.ErrConflict, created.ID, created.State)
	}
	if err := c.createAttemptResources(ctx, created, attempt, repository); err != nil {
		latest, getErr := c.store.GetTask(ctx, created.ID)
		if errors.Is(err, errResourceCreationBlocked) || getErr == nil && (latest.CancellationRequested || terminal(latest.State) || stateAtOrAfter(latest.State, task.JOB_PENDING)) {
			return latest, nil
		}
		reason := failureMessage("kubernetes", jobs.Name(created.ID, attempt.Number), "", nil, -1, err)
		if permanentKubernetesError(err) {
			_ = c.store.MarkLogsExhausted(ctx, created.ID, attempt.ID)
			_ = c.transition(ctx, created.ID, task.CREATING_JOB, task.FAILED, reason, "kubernetes")
			failed, _ := c.store.GetTask(ctx, created.ID)
			return failed, err
		}
		_ = c.store.RecordObservation(ctx, created.ID, "transient resource creation; retry pending "+reason, "kubernetes")
		return c.store.GetTask(ctx, created.ID)
	}
	if err := c.completeAttemptResources(ctx, created.ID, attempt.ID, "worker Job created job="+jobs.Name(created.ID, attempt.Number)); err != nil {
		if errors.Is(err, errResourceCreationBlocked) {
			return c.store.GetTask(ctx, created.ID)
		}
		return store.Task{}, err
	}
	log.InfoContext(ctx, "worker Job queued", "job", jobs.Name(created.ID, attempt.Number))
	return c.store.GetTask(ctx, created.ID)
}

func (c *Controller) Retry(ctx context.Context, taskID string) (store.Attempt, error) {
	return c.RetryWithKey(ctx, taskID, "")
}

// RetryWithKey starts one retry transaction and deduplicates a caller-supplied
// intent key across process restarts.
func (c *Controller) RetryWithKey(ctx context.Context, taskID, idempotencyKey string) (store.Attempt, error) {
	unlock, err := c.locks.lock(ctx, taskID)
	if err != nil {
		return store.Attempt{}, err
	}
	if strings.TrimSpace(idempotencyKey) != "" {
		if replayed, err := c.store.GetRetryIntent(ctx, taskID, idempotencyKey); err == nil {
			unlock()
			return replayed, nil
		} else if !errors.Is(err, store.ErrNotFound) {
			unlock()
			return store.Attempt{}, err
		}
	}
	current, err := c.store.GetTask(ctx, taskID)
	if err != nil {
		unlock()
		return store.Attempt{}, err
	}
	repository, err := c.repository(current.Repository)
	if err != nil {
		unlock()
		return store.Attempt{}, err
	}
	if err := c.validateAttemptConfig(store.CreateTaskParams{Repository: current.Repository, Prompt: current.Prompt}, repository); err != nil {
		unlock()
		return store.Attempt{}, err
	}
	plan, planned, err := c.store.PlanRetryAttempt(ctx, taskID, idempotencyKey)
	if err != nil {
		unlock()
		return store.Attempt{}, err
	}
	if !planned {
		unlock()
		return plan.Attempt, nil
	}
	if events, eventErr := c.store.ListForgeEventsByAttempt(ctx, plan.PreviousAttemptID); eventErr == nil && hasRunningForgeEvent(events) {
		previous, getErr := c.store.GetAttempt(ctx, taskID, plan.PreviousAttemptID)
		if getErr == nil {
			getErr = c.verifyAttemptProviderOwnership(ctx, current, previous, "")
		}
		if getErr != nil {
			unlock()
			return store.Attempt{}, getErr
		}
	} else if eventErr != nil {
		unlock()
		return store.Attempt{}, eventErr
	}
	plan.Attempt, _, _, err = c.buildAttemptSnapshot(current, plan.Attempt, repository)
	if err != nil {
		unlock()
		return store.Attempt{}, err
	}
	attempt, inserted, err := c.store.StartPlannedRetryTaskOnce(ctx, taskID, idempotencyKey, plan)
	if err != nil {
		unlock()
		return store.Attempt{}, err
	}
	if !inserted {
		unlock()
		return attempt, nil
	}
	unlock()
	if err := c.startAttempt(ctx, current, attempt, repository); err != nil {
		return store.Attempt{}, err
	}
	attempt, err = c.store.CurrentAttempt(ctx, taskID)
	if err != nil {
		return store.Attempt{}, err
	}
	c.logger.InfoContext(ctx, "retry Job queued", "task", taskID, "attempt", attempt.ID, "job", jobs.Name(taskID, attempt.Number))
	return attempt, nil
}

func (c *Controller) Cancel(ctx context.Context, taskID string) error {
	unlock, err := c.locks.lock(ctx, taskID)
	if err != nil {
		return err
	}
	current, err := c.store.GetTask(ctx, taskID)
	if err != nil {
		unlock()
		return err
	}
	if terminal(current.State) {
		unlock()
		return fmt.Errorf("terminal task state %q cannot be cancelled", current.State)
	}
	attempt, err := c.store.CurrentAttempt(ctx, taskID)
	if err != nil {
		unlock()
		return err
	}
	if err := c.store.RequestCancellation(ctx, taskID); err != nil {
		unlock()
		return err
	}
	defer unlock()
	apiCtx, cancel := context.WithTimeout(ctx, c.providerTimeout)
	defer cancel()
	jobName := jobs.Name(taskID, attempt.Number)
	job, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(apiCtx, jobName, metav1.GetOptions{})
	if err == nil {
		if err := verifyResourceLabels("Job", job.Labels, taskID, attempt); err != nil {
			return err
		}
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get worker Job %s for cancellation: %w", jobName, err)
	}
	if err == nil {
		if err := c.deleteJob(apiCtx, c.config.Controller.Namespace, jobName, job.UID); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cancel worker Job %s: %w", jobName, err)
		}
	}
	c.logger.InfoContext(ctx, "task cancellation requested", "task", taskID, "attempt", attempt.ID, "job", jobName)
	return nil
}

func (c *Controller) startAttempt(ctx context.Context, taskRecord store.Task, attempt store.Attempt, repository config.RepositoryConfig) error {
	unlock, err := c.locks.lock(ctx, taskRecord.ID)
	if err != nil {
		return err
	}
	defer unlock()
	current, err := c.store.GetTask(ctx, taskRecord.ID)
	if err != nil {
		return err
	}
	currentAttempt, err := c.store.CurrentAttempt(ctx, taskRecord.ID)
	if err != nil {
		return err
	}
	if current.CancellationRequested || terminal(current.State) || currentAttempt.ID != attempt.ID {
		return nil
	}
	attempt = currentAttempt
	if current.State == task.QUEUED {
		if err := c.transition(ctx, taskRecord.ID, task.QUEUED, task.CREATING_JOB, "creating retry worker Job", "controller"); err != nil {
			return err
		}
		current.State = task.CREATING_JOB
	}
	if current.State != task.CREATING_JOB {
		return nil
	}
	if _, err := c.ensureAttemptResources(ctx, taskRecord, attempt, repository); err != nil {
		latest, getErr := c.store.GetTask(ctx, taskRecord.ID)
		if errors.Is(err, errResourceCreationBlocked) || getErr == nil && (latest.CancellationRequested || terminal(latest.State) || stateAtOrAfter(latest.State, task.JOB_PENDING)) {
			return nil
		}
		reason := failureMessage("kubernetes", jobs.Name(taskRecord.ID, attempt.Number), "", nil, -1, err)
		if permanentKubernetesError(err) {
			_ = c.store.MarkLogsExhausted(ctx, taskRecord.ID, attempt.ID)
			_ = c.transition(ctx, taskRecord.ID, task.CREATING_JOB, task.FAILED, reason, "kubernetes")
			return err
		}
		return c.store.RecordObservation(ctx, taskRecord.ID, "transient resource creation; retry pending "+reason, "kubernetes")
	}
	return c.completeAttemptResources(ctx, taskRecord.ID, attempt.ID, "retry worker Job created job="+jobs.Name(taskRecord.ID, attempt.Number))
}

func (c *Controller) completeAttemptResources(ctx context.Context, taskID, attemptID, reason string) error {
	current, err := c.store.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if current.CancellationRequested || terminal(current.State) {
		return fmt.Errorf("%w: task %q is %q (cancellation requested=%t)", errResourceCreationBlocked, taskID, current.State, current.CancellationRequested)
	}
	attempt, err := c.store.CurrentAttempt(ctx, taskID)
	if err != nil {
		return err
	}
	if attempt.ID != attemptID {
		return fmt.Errorf("%w: task %q current attempt is %q, expected %q", errResourceCreationBlocked, taskID, attempt.ID, attemptID)
	}
	if stateAtOrAfter(current.State, task.JOB_PENDING) {
		return nil
	}
	if current.State != task.CREATING_JOB {
		return fmt.Errorf("%w: task %q is %q after resource creation", store.ErrConflict, taskID, current.State)
	}
	return c.transition(ctx, taskID, task.CREATING_JOB, task.JOB_PENDING, reason, "kubernetes")
}

func (c *Controller) createAttemptResources(ctx context.Context, taskRecord store.Task, attempt store.Attempt, repository config.RepositoryConfig) error {
	_, err := c.ensureAttemptResources(ctx, taskRecord, attempt, repository)
	return err
}

func (c *Controller) ensureAttemptResources(ctx context.Context, taskRecord store.Task, attempt store.Attempt, repository config.RepositoryConfig) ([]string, error) {
	current, currentAttempt, err := c.currentResourceIntent(ctx, taskRecord.ID, attempt.ID)
	if err != nil {
		return nil, err
	}
	taskRecord, attempt = current, currentAttempt
	job, secret, err := c.prepareAttemptSnapshot(ctx, taskRecord, attempt, repository)
	if err != nil {
		return nil, err
	}
	var recovered []string
	apiCtx, cancel := context.WithTimeout(ctx, c.providerTimeout)
	defer cancel()
	existingSecret, err := c.kubernetes.CoreV1().Secrets(secret.Namespace).Get(apiCtx, secret.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		existingSecret = nil
		if _, err = c.kubernetes.CoreV1().Secrets(secret.Namespace).Create(apiCtx, secret, metav1.CreateOptions{}); apierrors.IsAlreadyExists(err) {
			existingSecret, err = c.kubernetes.CoreV1().Secrets(secret.Namespace).Get(apiCtx, secret.Name, metav1.GetOptions{})
		} else if err == nil {
			recovered = append(recovered, "Secret "+secret.Name)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("ensure task Secret %s: %w", secret.Name, err)
	}
	if existingSecret != nil {
		if err := verifyResourceLabels("Secret", existingSecret.Labels, taskRecord.ID, attempt); err != nil {
			return nil, err
		}
	}
	if err := c.store.RegisterSecretCleanup(ctx, store.SecretCleanup{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, AttemptNumber: attempt.Number,
		Namespace: secret.Namespace, JobName: job.Name, SecretName: secret.Name,
	}); err != nil {
		return nil, err
	}
	if _, _, err := c.currentResourceIntent(ctx, taskRecord.ID, attempt.ID); err != nil {
		return nil, err
	}
	existingJob, err := c.kubernetes.BatchV1().Jobs(job.Namespace).Get(apiCtx, job.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		existingJob = nil
		if _, err = c.kubernetes.BatchV1().Jobs(job.Namespace).Create(apiCtx, job, metav1.CreateOptions{}); apierrors.IsAlreadyExists(err) {
			existingJob, err = c.kubernetes.BatchV1().Jobs(job.Namespace).Get(apiCtx, job.Name, metav1.GetOptions{})
		} else if err == nil {
			recovered = append(recovered, "Job "+job.Name)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("ensure worker Job %s: %w", job.Name, err)
	}
	if existingJob != nil {
		if err := verifyResourceLabels("Job", existingJob.Labels, taskRecord.ID, attempt); err != nil {
			return nil, err
		}
	}
	return recovered, nil
}

func (c *Controller) currentResourceIntent(ctx context.Context, taskID, attemptID string) (store.Task, store.Attempt, error) {
	record, err := c.store.GetTask(ctx, taskID)
	if err != nil {
		return store.Task{}, store.Attempt{}, err
	}
	attempt, err := c.store.CurrentAttempt(ctx, taskID)
	if err != nil {
		return store.Task{}, store.Attempt{}, err
	}
	if record.CancellationRequested || terminal(record.State) || attempt.ID != attemptID {
		return record, attempt, fmt.Errorf("%w: task %q state=%q cancellation_requested=%t current_attempt=%q expected_attempt=%q", errResourceCreationBlocked, taskID, record.State, record.CancellationRequested, attempt.ID, attemptID)
	}
	return record, attempt, nil
}

func (c *Controller) deleteJob(ctx context.Context, namespace, name string, uid types.UID) error {
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{PropagationPolicy: &policy}
	if uid != "" {
		options.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	return c.kubernetes.BatchV1().Jobs(namespace).Delete(ctx, name, options)
}

func (c *Controller) buildAttemptResources(taskRecord store.Task, attempt store.Attempt, repository config.RepositoryConfig) (*batchv1.Job, *corev1.Secret, error) {
	jobConfig, err := c.jobConfig(repository)
	if err != nil {
		return nil, nil, err
	}
	prompt, baseBranch := taskRecord.Prompt, repository.DefaultBranch
	taskBranch := c.branchName(repository, taskRecord.ID, attempt.Number)
	if attempt.Prompt != "" {
		prompt = attempt.Prompt
	}
	if attempt.BaseBranch != "" {
		baseBranch = attempt.BaseBranch
	}
	if attempt.TaskBranch != "" {
		taskBranch = attempt.TaskBranch
	}
	job, secret, err := jobs.Build(jobConfig, jobs.TaskManifest{
		TaskID:             taskRecord.ID,
		Repository:         repository.Name,
		CloneURL:           repository.CloneURL,
		BaseBranch:         baseBranch,
		TaskBranch:         taskBranch,
		Prompt:             prompt,
		OpenCodeCommand:    c.openCodeCommand(repository),
		ValidationCommands: c.validationCommands(repository),
		MaxFixAttempts:     c.maxFixAttempts(repository),
	}, jobs.Attempt{ID: attempt.ID, Number: attempt.Number})
	if err != nil {
		return nil, nil, err
	}
	for _, labels := range []map[string]string{job.Labels, job.Spec.Template.Labels, secret.Labels} {
		labels["simpleswe.dev/attempt-id"] = attempt.ID
	}
	return job, secret, nil
}

func (c *Controller) prepareAttemptSnapshot(ctx context.Context, taskRecord store.Task, attempt store.Attempt, repository config.RepositoryConfig) (*batchv1.Job, *corev1.Secret, error) {
	if len(attempt.ResourceSnapshot) > 0 {
		if len(attempt.ManifestJSON) == 0 || strings.TrimSpace(attempt.ConfigDigest) == "" {
			return nil, nil, forge.MarkPermanent(fmt.Errorf("%w: attempt %q has an incomplete immutable snapshot", store.ErrConflict, attempt.ID))
		}
		var snapshot attemptResourceSnapshot
		if err := json.Unmarshal(attempt.ResourceSnapshot, &snapshot); err != nil {
			return nil, nil, forge.MarkPermanent(fmt.Errorf("decode attempt resource snapshot: %w", err))
		}
		if snapshot.Job.Name == "" || snapshot.Secret.Name == "" {
			return nil, nil, forge.MarkPermanent(fmt.Errorf("decode attempt resource snapshot: Job and Secret names are required"))
		}
		return snapshot.Job.DeepCopy(), snapshot.Secret.DeepCopy(), nil
	}
	if events, err := c.store.ListForgeEventsByAttempt(ctx, attempt.ID); err == nil && len(events) > 0 {
		return nil, nil, forge.MarkPermanent(fmt.Errorf("%w: forge follow-up attempt %q has no immutable snapshot", store.ErrConflict, attempt.ID))
	} else if err != nil {
		return nil, nil, err
	}
	snapshotted, job, secret, err := c.buildAttemptSnapshot(taskRecord, attempt, repository)
	if err != nil {
		return nil, nil, err
	}
	if err := c.store.SaveAttemptSnapshot(ctx, taskRecord.ID, attempt.ID, snapshotted.ManifestJSON, snapshotted.ResourceSnapshot, snapshotted.ConfigDigest); err != nil {
		return nil, nil, err
	}
	return job, secret, nil
}

func hasRunningForgeEvent(events []store.ForgeEvent) bool {
	for _, event := range events {
		if event.Status == store.ForgeEventRunning {
			return true
		}
	}
	return false
}

func (c *Controller) buildAttemptSnapshot(taskRecord store.Task, attempt store.Attempt, repository config.RepositoryConfig) (store.Attempt, *batchv1.Job, *corev1.Secret, error) {
	job, secret, err := c.buildAttemptResources(taskRecord, attempt, repository)
	if err != nil {
		return store.Attempt{}, nil, nil, err
	}
	target, err := forgeTarget(c.config, repository)
	if err != nil {
		return store.Attempt{}, nil, nil, err
	}
	resources, err := json.Marshal(attemptResourceSnapshot{Job: *job, Secret: *secret, ForgeTarget: &target})
	if err != nil {
		return store.Attempt{}, nil, nil, fmt.Errorf("marshal attempt resource snapshot: %w", err)
	}
	attempt.ManifestJSON = append([]byte(nil), secret.Data["task.json"]...)
	attempt.ResourceSnapshot = resources
	attempt.ConfigDigest = fmt.Sprintf("sha256:%x", sha256.Sum256(resources))
	return attempt, job, secret, nil
}

func (c *Controller) validateAttemptConfig(params store.CreateTaskParams, repository config.RepositoryConfig) error {
	_, _, err := c.buildAttemptResources(store.Task{ID: "swe-validation", Prompt: params.Prompt}, store.Attempt{ID: "swe-attempt-validation", Number: 1}, repository)
	if err != nil {
		return fmt.Errorf("validate worker manifest before task acceptance: %w", err)
	}
	return nil
}

func permanentKubernetesError(err error) bool {
	return errors.Is(err, store.ErrConflict) || apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) || apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsMethodNotSupported(err)
}

func verifyResourceLabels(kind string, labels map[string]string, taskID string, attempt store.Attempt) error {
	want := map[string]string{
		"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskID,
		"simpleswe.dev/attempt": strconv.Itoa(attempt.Number), "simpleswe.dev/attempt-id": attempt.ID,
	}
	for key, value := range want {
		if labels[key] != value {
			return fmt.Errorf("%w: %s is not owned by task %q attempt %q: label %s=%q, want %q", store.ErrConflict, kind, taskID, attempt.ID, key, labels[key], value)
		}
	}
	return nil
}

func (c *Controller) transition(ctx context.Context, taskID string, from, to task.State, reason, trigger string) error {
	return c.store.Transition(ctx, taskID, from, to, store.TransitionParams{Reason: reason, Trigger: trigger})
}

func (c *Controller) transitionWith(ctx context.Context, taskID string, from, to task.State, reason, trigger string, validation *store.ValidationTransition) error {
	return c.store.Transition(ctx, taskID, from, to, store.TransitionParams{Reason: reason, Trigger: trigger, Validation: validation})
}

func (c *Controller) repository(reference string) (config.RepositoryConfig, error) {
	for _, repository := range c.config.Repositories {
		// CloneURL remains a compatibility reference for existing API callers, but
		// every accepted value must resolve to a configured registry entry.
		if reference == repository.Name || reference == repository.CloneURL {
			return repository, nil
		}
	}
	return config.RepositoryConfig{}, fmt.Errorf("unknown configured repository %q", reference)
}

func (c *Controller) jobConfig(repository config.RepositoryConfig) (jobs.Config, error) {
	resources, err := resourceRequirements(repository.Worker.Resources)
	if err != nil {
		return jobs.Config{}, err
	}
	deadline := c.config.Controller.Deadline
	if repository.Worker.Deadline.Value() > 0 {
		deadline = repository.Worker.Deadline.Value()
	}
	nodeSelector := repository.Worker.Scheduling.NodeSelector
	if repository.Worker.NodeSelector != nil {
		nodeSelector = repository.Worker.NodeSelector
	}
	tolerations := repository.Worker.Scheduling.Tolerations
	if repository.Worker.Tolerations != nil {
		tolerations = repository.Worker.Tolerations
	}
	affinityMap := repository.Worker.Scheduling.Affinity
	if repository.Worker.Affinity != nil {
		affinityMap = repository.Worker.Affinity
	}
	affinity, err := affinityFromMap(affinityMap)
	if err != nil {
		return jobs.Config{}, err
	}
	priority := repository.Worker.Scheduling.PriorityClassName
	if repository.Worker.PriorityClassName != "" {
		priority = repository.Worker.PriorityClassName
	}
	serviceAccount := repository.Worker.Scheduling.ServiceAccountName
	if repository.Worker.ServiceAccountName != "" {
		serviceAccount = repository.Worker.ServiceAccountName
	}
	pullSecrets := repository.Worker.Scheduling.ImagePullSecrets
	if repository.Worker.ImagePullSecrets != nil {
		pullSecrets = repository.Worker.ImagePullSecrets
	}
	jobConfig := jobs.Config{
		Namespace:          c.config.Controller.Namespace,
		Image:              repository.Worker.Image,
		Resources:          resources,
		NodeSelector:       nodeSelector,
		Affinity:           affinity,
		PriorityClassName:  priority,
		ServiceAccountName: serviceAccount,
		ImagePullSecrets:   pullSecrets,
		Deadline:           deadline,
	}
	for _, toleration := range tolerations {
		jobConfig.Tolerations = append(jobConfig.Tolerations, corev1.Toleration{
			Key:               toleration.Key,
			Operator:          corev1.TolerationOperator(toleration.Operator),
			Value:             toleration.Value,
			Effect:            corev1.TaintEffect(toleration.Effect),
			TolerationSeconds: toleration.TolerationSeconds,
		})
	}
	cloneURL, _ := url.Parse(repository.CloneURL)
	githubHTTPS := repository.GitHub.Owner != "" && strings.EqualFold(cloneURL.Scheme, "https")
	if repository.Credentials.SecretName != "" {
		jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{
			Name: repository.Credentials.SecretName, MountPath: "/run/secrets/repository",
		})
	}
	if repository.Bitbucket.CredentialsSecret != "" && repository.Bitbucket.CredentialsSecret != repository.Credentials.SecretName {
		jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{Name: repository.Bitbucket.CredentialsSecret, MountPath: "/run/secrets/bitbucket"})
	}
	if githubHTTPS && repository.Credentials.SecretName == "" {
		jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{Name: repository.GitHub.CredentialsSecret, MountPath: "/run/secrets/github"})
	}
	for _, mount := range repository.Worker.Mounts {
		if mount.Secret != nil {
			jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{
				Name: mount.Secret.SecretName, MountPath: mount.MountPath,
			})
		}
		if mount.ConfigMap != nil {
			jobConfig.ConfigMaps = append(jobConfig.ConfigMaps, jobs.ConfigMapMount{Name: mount.ConfigMap.Name, MountPath: mount.MountPath})
		}
		if mount.EmptyDir != nil {
			var size *resource.Quantity
			if mount.EmptyDir.SizeLimit != nil {
				parsed := resource.MustParse(mount.EmptyDir.SizeLimit.String())
				size = &parsed
			}
			jobConfig.EmptyDirs = append(jobConfig.EmptyDirs, jobs.EmptyDirMount{Name: mount.Name, MountPath: mount.MountPath, Medium: corev1.StorageMedium(mount.EmptyDir.Medium), SizeLimit: size})
		}
	}
	for _, mount := range repository.Worker.MountedSecrets {
		jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{Name: mount.Name, MountPath: mount.MountPath})
	}
	for _, mount := range repository.Worker.MountedConfigMaps {
		jobConfig.ConfigMaps = append(jobConfig.ConfigMaps, jobs.ConfigMapMount{Name: mount.Name, MountPath: mount.MountPath})
	}
	if repository.Git.SSHSecret != "" {
		jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{Name: repository.Git.SSHSecret, MountPath: "/run/secrets/git"})
		jobConfig.Env = append(jobConfig.Env, corev1.EnvVar{Name: "GIT_SSH_COMMAND", Value: "ssh -i /run/secrets/git/ssh-privatekey -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/workspace/known_hosts"})
	}
	if repository.OpenCode.ConfigSecret != "" {
		jobConfig.CredentialSecrets = append(jobConfig.CredentialSecrets, jobs.SecretMount{Name: repository.OpenCode.ConfigSecret, MountPath: "/run/secrets/opencode"})
	}
	if githubHTTPS {
		credentialPath := "/run/secrets/github"
		if repository.Credentials.SecretName != "" {
			// #nosec G101 -- this is a mounted directory path, not a credential.
			credentialPath = "/run/secrets/repository"
		}
		credentialScope := "credential." + repository.CloneURL
		jobConfig.Env = append(jobConfig.Env,
			corev1.EnvVar{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
			corev1.EnvVar{Name: "GIT_CONFIG_COUNT", Value: "3"},
			corev1.EnvVar{Name: "GIT_CONFIG_KEY_0", Value: "credential.useHttpPath"},
			corev1.EnvVar{Name: "GIT_CONFIG_VALUE_0", Value: "true"},
			corev1.EnvVar{Name: "GIT_CONFIG_KEY_1", Value: credentialScope + ".username"},
			corev1.EnvVar{Name: "GIT_CONFIG_VALUE_1", Value: "x-access-token"},
			corev1.EnvVar{Name: "GIT_CONFIG_KEY_2", Value: credentialScope + ".helper"},
			corev1.EnvVar{Name: "GIT_CONFIG_VALUE_2", Value: "!f() { if test \"$1\" = get; then printf 'password=%s\\n' \"$(cat " + credentialPath + "/token)\"; fi; }; f"},
		)
	} else if repository.Credentials.SecretName != "" || repository.Bitbucket.CredentialsSecret != "" {
		credentialPath := "/run/secrets/bitbucket"
		if repository.Credentials.SecretName != "" {
			// #nosec G101 -- this is a mounted directory path, not a credential.
			credentialPath = "/run/secrets/repository"
		}
		jobConfig.Env = append(jobConfig.Env,
			corev1.EnvVar{Name: "GIT_TERMINAL_PROMPT", Value: "0"},
			corev1.EnvVar{Name: "GIT_CONFIG_COUNT", Value: "1"},
			corev1.EnvVar{Name: "GIT_CONFIG_KEY_0", Value: "credential.helper"},
			corev1.EnvVar{Name: "GIT_CONFIG_VALUE_0", Value: "!f() { if test \"$1\" = get; then printf 'username=%s\\npassword=%s\\n' \"$(cat " + credentialPath + "/username)\" \"$(cat " + credentialPath + "/app-password)\"; fi; }; f"},
		)
	}
	for _, env := range repository.Worker.Env {
		value := corev1.EnvVar{Name: env.Name, Value: env.Value}
		if env.Secret != nil {
			value.ValueFrom = &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: env.Secret.Name}, Key: env.Secret.Key}}
		}
		if env.ConfigMap != nil {
			value.ValueFrom = &corev1.EnvVarSource{ConfigMapKeyRef: &corev1.ConfigMapKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: env.ConfigMap.Name}, Key: env.ConfigMap.Key}}
		}
		jobConfig.Env = append(jobConfig.Env, value)
	}
	secretPaths := make([]string, 0, len(jobConfig.CredentialSecrets))
	for _, secret := range jobConfig.CredentialSecrets {
		secretPaths = append(secretPaths, secret.MountPath)
	}
	if len(secretPaths) > 0 {
		jobConfig.Env = append(jobConfig.Env, corev1.EnvVar{Name: "SIMPLESWE_SECRET_PATHS", Value: strings.Join(secretPaths, string(os.PathListSeparator))})
	}
	return jobConfig, nil
}

func forgeTarget(cfg config.Config, repository config.RepositoryConfig) (forge.Target, error) {
	bitbucketConfigured := repository.Bitbucket.Workspace != "" || repository.Bitbucket.Repository != ""
	githubConfigured := repository.GitHub.Owner != "" || repository.GitHub.Repository != ""
	if bitbucketConfigured == githubConfigured {
		return forge.Target{}, fmt.Errorf("repository %q must configure exactly one forge", repository.Name)
	}
	target := forge.Target{
		Provider: forge.ProviderBitbucket, BaseURL: cfg.Bitbucket.BaseURL,
		Owner: repository.Bitbucket.Workspace, Repository: repository.Bitbucket.Repository,
		CredentialsSecret: repository.Bitbucket.CredentialsSecret,
	}
	if githubConfigured {
		target = forge.Target{
			Provider: forge.ProviderGitHub, BaseURL: cfg.GitHub.BaseURL,
			Owner: repository.GitHub.Owner, Repository: repository.GitHub.Repository,
			CredentialsSecret: repository.GitHub.CredentialsSecret,
		}
	}
	if err := forge.ValidateTarget(target); err != nil {
		return forge.Target{}, fmt.Errorf("validate forge target: %w", err)
	}
	return target, nil
}

func affinityFromMap(input map[string]any) (*corev1.Affinity, error) {
	if len(input) == 0 {
		return nil, nil
	}
	data, err := json.Marshal(input)
	if err != nil {
		return nil, fmt.Errorf("marshal worker affinity: %w", err)
	}
	var result corev1.Affinity
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("decode worker affinity: %w", err)
	}
	return &result, nil
}

func (c *Controller) branchName(repository config.RepositoryConfig, taskID string, attempt int) string {
	prefix := repository.Git.BranchPrefix
	if prefix == "" {
		prefix = c.config.Worker.BranchPrefix
	}
	return prefix + taskID + "-a" + strconv.Itoa(attempt)
}

func (c *Controller) openCodeCommand(repository config.RepositoryConfig) []string {
	if len(repository.OpenCode.Command) > 0 {
		return append([]string(nil), repository.OpenCode.Command...)
	}
	return []string{c.config.Worker.Command}
}

func (c *Controller) validationCommands(repository config.RepositoryConfig) [][]string {
	if len(repository.Validation.Commands) > 0 {
		result := make([][]string, len(repository.Validation.Commands))
		for i := range repository.Validation.Commands {
			result[i] = append([]string(nil), repository.Validation.Commands[i]...)
		}
		return result
	}
	return [][]string{{"go", "test", "./..."}}
}

func (c *Controller) maxFixAttempts(repository config.RepositoryConfig) int {
	if repository.Validation.MaxFixAttempts != nil {
		return *repository.Validation.MaxFixAttempts
	}
	if repository.Validation.MaxFixes != nil {
		return *repository.Validation.MaxFixes
	}
	return c.config.Controller.MaxFixAttempts
}

func resourceRequirements(input config.ResourceRequirements) (corev1.ResourceRequirements, error) {
	result := corev1.ResourceRequirements{}
	parse := func(source string, values config.ResourceList) (corev1.ResourceList, error) {
		if len(values) == 0 {
			return nil, nil
		}
		parsed := make(corev1.ResourceList, len(values))
		for name, value := range values {
			quantity, err := resource.ParseQuantity(value.String())
			if err != nil {
				return nil, fmt.Errorf("parse resource %s %q: %w", source, name, err)
			}
			parsed[corev1.ResourceName(name)] = quantity
		}
		return parsed, nil
	}
	var err error
	result.Requests, err = parse("request", input.Requests)
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	result.Limits, err = parse("limit", input.Limits)
	if err != nil {
		return corev1.ResourceRequirements{}, err
	}
	return result, nil
}
