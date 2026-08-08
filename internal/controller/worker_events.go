package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

// HandleWorkerEvent accepts only the structured worker protocol boundary. Job
// and Pod names are provenance, never parsed for lifecycle instructions.
func (c *Controller) HandleWorkerEvent(ctx context.Context, jobName, podName string, event protocol.Event) error {
	return c.handleWorkerEvent(ctx, "", jobName, podName, event)
}

// HandleWorkerEventOnce makes replay of a durably identified log event safe.
func (c *Controller) HandleWorkerEventOnce(ctx context.Context, eventID, jobName, podName string, event protocol.Event) error {
	return c.handleWorkerEvent(ctx, eventID, jobName, podName, event)
}

func (c *Controller) handleWorkerEvent(ctx context.Context, eventID, jobName, podName string, event protocol.Event) error {
	if strings.TrimSpace(event.TaskID) == "" {
		return errors.New("worker event task ID is empty")
	}
	unlock, err := c.locks.lock(ctx, event.TaskID)
	if err != nil {
		return err
	}
	defer unlock()
	record, err := c.store.GetTask(ctx, event.TaskID)
	if err != nil {
		return err
	}
	attempt, err := c.store.CurrentAttempt(ctx, event.TaskID)
	if err != nil {
		return err
	}
	if record.CancellationRequested {
		return c.store.RecordObservation(ctx, record.ID, "ignored worker event "+event.Type+" because cancellation owns task outcome", "controller")
	}
	authorizedRecord, authorizedAttempt, err := c.authorizeWorkerEvent(ctx, jobName, podName, event.TaskID)
	if err != nil {
		return err
	}
	authorizedAttemptID := authorizedAttempt.ID
	if attempt.ID != authorizedAttemptID {
		return fmt.Errorf("%w: worker event attempt changed from %q to %q", store.ErrConflict, authorizedAttemptID, attempt.ID)
	}
	record = authorizedRecord
	if event.Type == protocol.EventBranchPushed {
		return c.handleBranchPushedLocked(ctx, record, attempt, jobName, podName, event)
	}

	switch event.Type {
	case "agent_started":
		return c.eventTransition(ctx, record, task.RUNNING, task.AGENT_RUNNING,
			"worker agent started job="+jobName+" pod="+podName)
	case "validation_started":
		name := validationName(event.Command)
		if record.State == task.VALIDATING {
			return c.store.RecordValidationStartedOnce(ctx, record.ID, attempt.ID, name, event.Message, eventID)
		}
		if record.State != task.AGENT_RUNNING {
			if stateAtOrAfter(record.State, task.COMMITTING) {
				return nil
			}
			return fmt.Errorf("validation_started for task %q in state %q", record.ID, record.State)
		}
		return c.transitionWith(ctx, record.ID, task.AGENT_RUNNING, task.VALIDATING,
			"worker validation started job="+jobName+" pod="+podName, "controller", &store.ValidationTransition{Name: name, State: "running", Summary: event.Message, EventID: eventID})
	case "validation_result":
		if record.State != task.VALIDATING {
			return fmt.Errorf("validation_result for task %q in state %q", record.ID, record.State)
		}
		return c.store.RecordValidationResultOnce(ctx, record.ID, attempt.ID, validationName(event.Command), event.Message, event.ExitCode, eventID)
	case "validation_succeeded":
		if stateAtOrAfter(record.State, task.COMMITTING) {
			return nil
		}
		if record.State != task.VALIDATING {
			return fmt.Errorf("validation_succeeded for task %q in state %q", record.ID, record.State)
		}
		return c.store.MarkValidationComplete(ctx, record.ID, attempt.ID, "succeeded")
	case "validation_failed":
		if record.State == task.FAILED {
			return nil
		}
		if record.State != task.VALIDATING {
			return fmt.Errorf("validation_failed for task %q in state %q", record.ID, record.State)
		}
		name := validationName(event.Command)
		if err := c.store.RecordValidationResultOnce(ctx, record.ID, attempt.ID, name, event.Message, event.ExitCode, eventID); err != nil {
			if !errors.Is(err, store.ErrConflict) {
				return err
			}
			if err := c.store.RecordValidationFailureDetail(ctx, attempt.ID, name, event.Message, event.ExitCode); err != nil {
				return err
			}
		}
		if err := c.store.MarkValidationComplete(ctx, record.ID, attempt.ID, "failed"); err != nil {
			return err
		}
		reason := failureMessage("validation", jobName, podName, event.Command, event.ExitCode, errors.New(event.Message))
		return c.transition(ctx, record.ID, task.VALIDATING, task.FAILED, reason, "controller")
	case "worker_failed":
		if terminal(record.State) {
			return nil
		}
		return c.transition(ctx, record.ID, record.State, task.FAILED,
			failureMessage("worker", jobName, podName, event.Command, event.ExitCode, errors.New(event.Message)), "controller")
	default:
		return fmt.Errorf("unsupported worker event type %q", event.Type)
	}
}

func (c *Controller) authorizeWorkerEvent(ctx context.Context, jobName, podName, taskID string) (store.Task, store.Attempt, error) {
	job, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return store.Task{}, store.Attempt{}, fmt.Errorf("get worker Job %q: %w", jobName, err)
	}
	record, err := c.store.GetTask(ctx, taskID)
	if err != nil {
		return store.Task{}, store.Attempt{}, err
	}
	attempt, err := c.store.CurrentAttempt(ctx, taskID)
	if err != nil {
		return store.Task{}, store.Attempt{}, err
	}
	if err := verifyResourceLabels("Job", job.Labels, taskID, attempt); err != nil {
		return store.Task{}, store.Attempt{}, err
	}
	if job.UID == "" {
		return store.Task{}, store.Attempt{}, fmt.Errorf("worker Job %q has no UID", jobName)
	}
	pod, err := c.kubernetes.CoreV1().Pods(c.config.Controller.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return store.Task{}, store.Attempt{}, fmt.Errorf("get worker Pod %q: %w", podName, err)
	}
	if err := verifyResourceLabels("Pod", pod.Labels, taskID, attempt); err != nil {
		return store.Task{}, store.Attempt{}, err
	}
	if !podOwnedByJob(pod, jobName, job.UID) {
		return store.Task{}, store.Attempt{}, fmt.Errorf("worker Pod %q is not owned by Job %q UID %q", podName, jobName, job.UID)
	}
	return record, attempt, nil
}

func (c *Controller) eventTransition(ctx context.Context, record store.Task, expected, next task.State, reason string) error {
	if record.State == next || stateAtOrAfter(record.State, next) {
		return nil
	}
	if record.State != expected {
		return fmt.Errorf("worker event for task %q requires state %q, got %q", record.ID, expected, record.State)
	}
	return c.transition(ctx, record.ID, expected, next, reason, "controller")
}

func (c *Controller) handleBranchPushedLocked(ctx context.Context, record store.Task, attempt store.Attempt, jobName, podName string, event protocol.Event) error {
	if record.CancellationRequested || terminal(record.State) {
		return nil
	}
	if strings.TrimSpace(event.Branch) == "" {
		return errors.New("branch_pushed event branch is empty")
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		return err
	}
	if event.Branch != manifest.TaskBranch {
		return fmt.Errorf("branch_pushed branch %q does not match attempt branch %q", event.Branch, manifest.TaskBranch)
	}
	if err := protocol.ValidateEvent(event, manifest.TaskBranch); err != nil {
		return err
	}
	if err := c.store.RecordForgeEventReplies(ctx, attempt.ID, event.Replies); err != nil {
		return fmt.Errorf("record forge event replies for attempt %q: %w", attempt.ID, err)
	}
	if stateAtOrAfter(record.State, task.PR_OPEN) {
		return c.completeForgeEventLocked(ctx, record, attempt)
	}
	if err := c.store.RecordGitResult(ctx, store.GitResult{
		AttemptID: attempt.ID,
		State:     "pushed",
		Branch:    event.Branch,
		CommitSHA: event.CommitSHA,
	}); err != nil {
		return err
	}
	return c.resumePullRequestLocked(ctx, record, attempt, "branch pushed job="+jobName+" pod="+podName+" branch="+event.Branch)
}

func (c *Controller) resumePullRequestLocked(ctx context.Context, record store.Task, attempt store.Attempt, reason string) error {
	expectedAttemptID := attempt.ID
	var err error
	record, err = c.store.GetTask(ctx, record.ID)
	if err != nil {
		return err
	}
	if record.CancellationRequested || record.State == task.CANCELLED {
		return nil
	}
	attempt, err = c.store.CurrentAttempt(ctx, record.ID)
	if err != nil {
		return err
	}
	if attempt.ID != expectedAttemptID {
		return fmt.Errorf("%w: pull request attempt changed from %q to %q", store.ErrConflict, expectedAttemptID, attempt.ID)
	}
	git, err := c.store.GetGitResult(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if git.State != "pushed" || git.Branch == "" || git.CommitSHA == "" {
		return fmt.Errorf("attempt %q has no complete durable pushed Git result", attempt.ID)
	}
	for {
		switch record.State {
		case task.VALIDATING:
			latest, err := c.store.GetAttempt(ctx, record.ID, attempt.ID)
			if err != nil {
				return err
			}
			if latest.ValidationState != "succeeded" {
				return fmt.Errorf("%w: attempt %q validation is %q, want succeeded", store.ErrConflict, attempt.ID, latest.ValidationState)
			}
			if err := c.transition(ctx, record.ID, task.VALIDATING, task.COMMITTING, reason, "controller"); err != nil {
				return err
			}
			record.State = task.COMMITTING
		case task.COMMITTING:
			if err := c.transition(ctx, record.ID, task.COMMITTING, task.PUSHING, reason, "controller"); err != nil {
				return err
			}
			record.State = task.PUSHING
		case task.PUSHING:
			if err := c.transition(ctx, record.ID, task.PUSHING, task.CREATING_PR, reason, "controller"); err != nil {
				return err
			}
			record.State = task.CREATING_PR
		case task.CREATING_PR:
			goto create
		case task.PR_OPEN, task.WAITING_CI, task.WAITING_REVIEW, task.READY:
			return c.completeForgeEventLocked(ctx, record, attempt)
		default:
			return fmt.Errorf("durable branch for task %q cannot resume from state %q", record.ID, record.State)
		}
	}

create:
	manifest, err := attemptManifest(attempt)
	if err != nil {
		return err
	}
	target, err := c.attemptForgeTarget(record, attempt)
	if err != nil {
		return c.handlePullRequestError(ctx, record, attempt, forge.MarkPermanent(err))
	}
	title := strings.TrimSpace(record.PRTitle)
	if title == "" {
		title = strings.TrimSpace(record.Prompt)
	}
	_, err = c.store.ReservePullRequest(ctx, attempt.ID, title, git.Branch, manifest.BaseBranch)
	if err != nil {
		return err
	}
	durable, err := c.store.GetPullRequest(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if durable.State == "creating" {
		providerCtx, cancel := context.WithTimeout(ctx, c.providerTimeout)
		pullRequest, found, findErr := c.pullRequests.FindPullRequest(providerCtx, target, git.Branch, manifest.BaseBranch, record.ID)
		cancel()
		if findErr != nil {
			return c.handlePullRequestError(ctx, record, attempt, findErr)
		}
		if !found {
			providerCtx, cancel = context.WithTimeout(ctx, c.providerTimeout)
			created, createErr := c.pullRequests.CreatePullRequest(providerCtx, target, forge.CreatePullRequestRequest{
				Title:             title,
				Description:       "Created by simpleswe task " + record.ID,
				SourceBranch:      git.Branch,
				DestinationBranch: manifest.BaseBranch,
			})
			cancel()
			if createErr != nil {
				return c.handlePullRequestError(ctx, record, attempt, createErr)
			}
			pullRequest = created
		}
		if err := c.store.CompletePullRequest(ctx, attempt.ID, pullRequest.ID, pullRequest.HTMLURL); err != nil {
			return err
		}
	}
	durable, err = c.store.GetPullRequest(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if durable.State != "open" {
		return fmt.Errorf("pull request for attempt %q is durably %q; refusing duplicate creation", attempt.ID, durable.State)
	}
	if durable.HeadBranch != git.Branch || durable.Number <= 0 || durable.URL == "" {
		return fmt.Errorf("pull request for attempt %q does not satisfy durable Git gate", attempt.ID)
	}
	if record.State == task.CREATING_PR {
		if err := c.transition(ctx, record.ID, task.CREATING_PR, task.PR_OPEN,
			fmt.Sprintf("pull request open number=%d url=%s", durable.Number, durable.URL), "controller"); err != nil {
			return err
		}
	}
	c.logger.InfoContext(ctx, "pull request opened", "task", record.ID, "attempt", attempt.ID, "url", durable.URL)
	return c.completeForgeEventLocked(ctx, record, attempt)
}

func (c *Controller) handlePullRequestError(ctx context.Context, record store.Task, attempt store.Attempt, providerErr error) error {
	if !forge.IsPermanent(providerErr) {
		if err := c.store.RecordObservation(ctx, record.ID, "transient pull request provider failure; retry pending: "+providerErr.Error(), "controller"); err != nil {
			return errors.Join(providerErr, err)
		}
		return providerErr
	}
	if err := c.store.FailPullRequest(ctx, attempt.ID, providerErr); err != nil {
		return errors.Join(providerErr, err)
	}
	if err := c.transition(ctx, record.ID, task.CREATING_PR, task.FAILED, "permanent pull request provider failure: "+providerErr.Error(), "controller"); err != nil {
		return errors.Join(providerErr, err)
	}
	return providerErr
}

func validationName(command []string) string {
	name := strings.Join(command, " ")
	if name == "" {
		return "validation"
	}
	return name
}

func attemptManifest(attempt store.Attempt) (protocol.TaskManifest, error) {
	var manifest protocol.TaskManifest
	if len(attempt.ManifestJSON) == 0 {
		return manifest, fmt.Errorf("attempt %q has no immutable task manifest", attempt.ID)
	}
	if err := json.Unmarshal(attempt.ManifestJSON, &manifest); err != nil {
		return manifest, fmt.Errorf("decode immutable manifest for attempt %q: %w", attempt.ID, err)
	}
	return manifest, nil
}

func (c *Controller) attemptForgeTarget(record store.Task, attempt store.Attempt) (forge.Target, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(attempt.ResourceSnapshot, &fields); err != nil {
		return forge.Target{}, forge.MarkPermanent(fmt.Errorf("decode immutable resource snapshot for attempt %q: %w", attempt.ID, err))
	}
	if fields == nil {
		return forge.Target{}, forge.MarkPermanent(fmt.Errorf("decode immutable resource snapshot for attempt %q: expected JSON object", attempt.ID))
	}
	raw, snapshotted := fields["forge_target"]
	if !snapshotted {
		repository, err := c.repository(record.Repository)
		if err != nil {
			return forge.Target{}, forge.MarkPermanent(err)
		}
		target, err := forgeTarget(c.config, repository)
		return target, forge.MarkPermanent(err)
	}
	var target *forge.Target
	if err := json.Unmarshal(raw, &target); err != nil {
		return forge.Target{}, forge.MarkPermanent(fmt.Errorf("decode immutable forge target for attempt %q: %w", attempt.ID, err))
	}
	if target == nil {
		return forge.Target{}, forge.MarkPermanent(fmt.Errorf("validate immutable forge target for attempt %q: target is null", attempt.ID))
	}
	if err := forge.ValidateTarget(*target); err != nil {
		return forge.Target{}, forge.MarkPermanent(fmt.Errorf("validate immutable forge target for attempt %q: %w", attempt.ID, err))
	}
	return *target, nil
}

// WorkerLogsExhausted durably records that structured replay has finished.
// A successful Job is only failed as indeterminate after this boundary.
func (c *Controller) WorkerLogsExhausted(ctx context.Context, jobName, podName string) error {
	job, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get exhausted worker Job %q: %w", jobName, err)
	}
	taskID := job.Labels["simpleswe.dev/task-id"]
	attemptNumber, err := strconv.Atoi(job.Labels["simpleswe.dev/attempt"])
	if taskID == "" || err != nil || attemptNumber <= 0 {
		return fmt.Errorf("exhausted worker Job %q is missing task attempt labels", jobName)
	}
	attempt, err := c.store.GetAttemptNumber(ctx, taskID, attemptNumber)
	if err != nil {
		return err
	}
	if err := verifyResourceLabels("Job", job.Labels, taskID, attempt); err != nil {
		return err
	}
	pod, err := c.kubernetes.CoreV1().Pods(c.config.Controller.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get exhausted worker Pod %q: %w", podName, err)
	}
	if err := verifyResourceLabels("Pod", pod.Labels, taskID, attempt); err != nil {
		return err
	}
	if job.UID == "" || !podOwnedByJob(pod, jobName, job.UID) {
		return fmt.Errorf("exhausted worker Pod %q is not owned by Job %q UID %q", podName, jobName, job.UID)
	}
	unlock, err := c.locks.lock(ctx, taskID)
	if err != nil {
		return err
	}
	defer unlock()
	record, err := c.store.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if err := c.store.MarkLogsExhausted(ctx, record.ID, attempt.ID); err != nil {
		return err
	}
	if record.CurrentAttemptID != attempt.ID {
		return nil
	}
	if record.CancellationRequested || terminal(record.State) {
		return nil
	}
	attempt.LogsExhausted = true
	if jobFailed(job) {
		return c.finishFailedJob(ctx, record, attempt, job)
	}
	if jobComplete(job) {
		return c.finishCompletedJob(ctx, record, attempt, job.Name)
	}
	return nil
}

func failureMessage(stage, job, pod string, command []string, exitCode int, cause error) string {
	parts := []string{"stage=" + stage, "job=" + job, "pod=" + pod}
	if len(command) > 0 {
		parts = append(parts, "command="+strings.Join(command, " "))
	}
	if exitCode >= 0 {
		parts = append(parts, fmt.Sprintf("exit_code=%d", exitCode))
	}
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		parts = append(parts, "error="+cause.Error())
	}
	return strings.Join(parts, " ")
}
