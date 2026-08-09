package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
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
	if eventID != "" {
		if err := c.authorizeDurableWorkerEvent(ctx, eventID, jobName, podName, event, record, attempt); err != nil {
			return err
		}
	} else {
		if err := c.authorizeWorkerEvent(ctx, jobName, podName, record, attempt); err != nil {
			return err
		}
	}
	if terminal(record.State) {
		return nil
	}
	if record.CancellationRequested {
		return c.store.RecordObservation(ctx, record.ID, "ignored worker event "+event.Type+" because cancellation owns task outcome", "controller")
	}
	if event.Type == protocol.EventBranchPushed {
		return c.handleLegacyBranchPushedLocked(ctx, record, attempt, event)
	}
	if event.Type == protocol.EventPullRequestPublished {
		return c.handlePullRequestPublishedLocked(ctx, record, attempt, event)
	}
	if event.Type == protocol.EventPullRequestReady {
		return c.handlePullRequestReadyLocked(ctx, record, attempt, jobName, podName, event)
	}

	switch event.Type {
	case "agent_started":
		if eventID != "" && (record.State == task.CREATING_JOB || record.State == task.JOB_PENDING) {
			if err := c.recoverTo(ctx, record, task.RUNNING, "recovery durable worker agent started job="+jobName+" pod="+podName); err != nil {
				return err
			}
			record.State = task.RUNNING
		}
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
	case protocol.EventWorkerFailed:
		if err := protocol.ValidateEvent(event, ""); err != nil {
			return fmt.Errorf("validate worker failure event: %w", err)
		}
		return c.transition(ctx, record.ID, record.State, task.FAILED,
			failureMessage("worker", jobName, podName, event.Command, event.ExitCode, errors.New(event.Message)), "controller")
	default:
		return fmt.Errorf("unsupported worker event type %q", event.Type)
	}
}

func (c *Controller) handlePullRequestPublishedLocked(ctx context.Context, record store.Task, attempt store.Attempt, event protocol.Event) error {
	if record.State != task.AGENT_RUNNING && record.State != task.VALIDATING {
		return fmt.Errorf("pull_request_published for task %q in state %q", record.ID, record.State)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		return err
	}
	if err := protocol.ValidateEvent(event, manifest.TaskBranch); err != nil {
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(err))
	}
	if manifest.ExistingPullRequestNumber > 0 && event.PullRequestNumber != manifest.ExistingPullRequestNumber {
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(fmt.Errorf("%w: follow-up must publish copied pull request %d", store.ErrConflict, manifest.ExistingPullRequestNumber)))
	}
	git := store.GitResult{AttemptID: attempt.ID, State: "candidate", Branch: event.Branch, CommitSHA: event.CommitSHA}
	pullRequest := store.PullRequest{
		AttemptID: attempt.ID, State: "reported", Number: event.PullRequestNumber,
		HeadBranch: event.Branch, BaseBranch: manifest.BaseBranch,
	}
	if err := c.store.RecordPullRequestCandidate(ctx, git, pullRequest); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(err))
		}
		return fmt.Errorf("record pull request candidate: %w", err)
	}
	return nil
}

func (c *Controller) authorizeDurableWorkerEvent(ctx context.Context, eventID, jobName, podName string, event protocol.Event, record store.Task, attempt store.Attempt) error {
	durable, err := c.store.GetWorkerLogEvent(ctx, eventID)
	if err != nil {
		return fmt.Errorf("get durable worker event: %w", err)
	}
	parsed, err := protocol.ParseLine(durable.Content)
	if err != nil || parsed.Event == nil {
		return fmt.Errorf("%w: durable worker event %q has invalid content", store.ErrConflict, eventID)
	}
	if durable.ID != eventID || durable.PodUID == "" || durable.TaskID != record.ID || durable.AttemptID != attempt.ID ||
		durable.JobName != jobName || durable.PodName != podName || !reflect.DeepEqual(*parsed.Event, event) {
		return fmt.Errorf("%w: durable worker event %q does not exactly match current task, attempt, Pod, Job, and content", store.ErrConflict, eventID)
	}
	return nil
}

func (c *Controller) authorizeWorkerEvent(ctx context.Context, jobName, podName string, record store.Task, attempt store.Attempt) error {
	job, err := c.kubernetes.BatchV1().Jobs(c.config.Controller.Namespace).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get worker Job %q: %w", jobName, err)
	}
	if err := verifyResourceLabels("Job", job.Labels, record.ID, attempt); err != nil {
		return err
	}
	if job.UID == "" {
		return fmt.Errorf("worker Job %q has no UID", jobName)
	}
	pod, err := c.kubernetes.CoreV1().Pods(c.config.Controller.Namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get worker Pod %q: %w", podName, err)
	}
	if err := verifyResourceLabels("Pod", pod.Labels, record.ID, attempt); err != nil {
		return err
	}
	if !podOwnedByJob(pod, jobName, job.UID) {
		return fmt.Errorf("worker Pod %q is not owned by Job %q UID %q", podName, jobName, job.UID)
	}
	return nil
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

func (c *Controller) handlePullRequestReadyLocked(ctx context.Context, record store.Task, attempt store.Attempt, jobName, podName string, event protocol.Event) error {
	manifest, err := attemptManifest(attempt)
	if err != nil {
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(err))
	}
	if manifest.TaskID != record.ID {
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(fmt.Errorf("%w: immutable manifest task %q does not match receipt task %q", store.ErrConflict, manifest.TaskID, record.ID)))
	}
	if err := protocol.ValidateEvent(event, manifest.TaskBranch); err != nil {
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(err))
	}
	if !stateAtOrAfter(record.State, task.VALIDATING) || attempt.ValidationState != "succeeded" {
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(fmt.Errorf("%w: pull_request_ready for attempt %q is out of order in task state %q with validation %q", store.ErrConflict, attempt.ID, record.State, attempt.ValidationState)))
	}
	target, err := c.attemptForgeTarget(record, attempt)
	if err != nil {
		return c.failPullRequestReceipt(ctx, record, err)
	}
	durableGit, gitErr := c.store.GetGitResult(ctx, attempt.ID)
	durablePR, pullRequestErr := c.store.GetPullRequest(ctx, attempt.ID)
	if gitErr != nil && !errors.Is(gitErr, store.ErrNotFound) {
		return fmt.Errorf("get durable candidate Git result: %w", gitErr)
	}
	if pullRequestErr != nil && !errors.Is(pullRequestErr, store.ErrNotFound) {
		return fmt.Errorf("get durable candidate pull request: %w", pullRequestErr)
	}
	if gitErr != nil || pullRequestErr != nil || !candidateMatchesReceipt(durableGit, durablePR, event, manifest) {
		cause := errors.Join(gitErr, pullRequestErr)
		if cause == nil {
			cause = errors.New("receipt identity differs from candidate")
		}
		return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(fmt.Errorf("%w: pull_request_ready does not match latest durable candidate: %w", store.ErrConflict, cause)))
	}
	if durableGit.State == "pushed" {
		if !durableResultMatchesReceipt(durableGit, durablePR, event, manifest, target) {
			return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(fmt.Errorf("%w: pull_request_ready conflicts with durable verified result", store.ErrConflict)))
		}
		return c.resumePullRequestLocked(ctx, record, attempt, "resumed durable reported pull request job="+jobName+" pod="+podName+" branch="+event.Branch)
	}
	providerCtx, cancel := context.WithTimeout(ctx, c.providerTimeout)
	live, err := c.pullRequests.GetPullRequest(providerCtx, target, event.PullRequestNumber)
	cancel()
	if err != nil {
		if !forge.IsPermanent(err) {
			return fmt.Errorf("inspect reported pull request: %w", err)
		}
		return c.failPullRequestReceipt(ctx, record, err)
	}
	if err := verifyReportedPullRequest(live, event, manifest, target); err != nil {
		return c.failPullRequestReceipt(ctx, record, err)
	}
	git := store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: event.Branch, CommitSHA: event.CommitSHA}
	pullRequest := store.PullRequest{
		AttemptID: attempt.ID, State: "open", Number: live.Number, URL: live.HTMLURL, Title: live.Title,
		HeadBranch: live.SourceBranch, BaseBranch: live.DestinationBranch,
	}
	if err := c.store.RecordVerifiedPullRequest(ctx, git, pullRequest); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return c.failPullRequestReceipt(ctx, record, forge.MarkPermanent(err))
		}
		return err
	}
	return c.resumePullRequestLocked(ctx, record, attempt, "verified reported pull request job="+jobName+" pod="+podName+" branch="+event.Branch)
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
	pullRequest, err := c.store.GetPullRequest(ctx, attempt.ID)
	if err != nil {
		return fmt.Errorf("get durable pull request: %w", err)
	}
	if pullRequest.State != "open" || pullRequest.Number <= 0 || pullRequest.URL == "" || pullRequest.Title == "" || pullRequest.HeadBranch != git.Branch {
		return fmt.Errorf("attempt %q has no complete provider-verified pull request", attempt.ID)
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
			if err := c.transition(ctx, record.ID, task.CREATING_PR, task.PR_OPEN,
				fmt.Sprintf("pull request open number=%d url=%s", pullRequest.Number, pullRequest.URL), "controller"); err != nil {
				return err
			}
			record.State = task.PR_OPEN
		case task.PR_OPEN, task.WAITING_CI, task.WAITING_REVIEW, task.READY:
			return c.completeForgeEventLocked(ctx, record, attempt, true)
		default:
			return fmt.Errorf("durable branch for task %q cannot resume from state %q", record.ID, record.State)
		}
	}
}

func verifyReportedPullRequest(live forge.PullRequestState, event protocol.Event, manifest protocol.TaskManifest, target forge.Target) error {
	if err := verifyLivePullRequestIdentity(live, store.PullRequest{Number: event.PullRequestNumber, HeadBranch: manifest.TaskBranch, BaseBranch: manifest.BaseBranch}, target); err != nil {
		return err
	}
	if !strings.EqualFold(live.HeadSHA, event.CommitSHA) {
		return fmt.Errorf("reported pull request mismatch: %w", forge.MarkPermanent(fmt.Errorf("%w: reported pull request does not match immutable repository, refs, state, number, or full head SHA", store.ErrConflict)))
	}
	return nil
}

func durableResultMatchesReceipt(git store.GitResult, pullRequest store.PullRequest, event protocol.Event, manifest protocol.TaskManifest, target forge.Target) bool {
	return git.State == "pushed" && git.Branch == event.Branch && git.CommitSHA == event.CommitSHA && git.Error == "" &&
		pullRequest.State == "open" && pullRequest.Number == event.PullRequestNumber && pullRequest.HeadBranch == manifest.TaskBranch &&
		pullRequest.BaseBranch == manifest.BaseBranch && pullRequest.Error == "" &&
		forge.ValidatePullRequestMetadata(pullRequest.URL, pullRequest.Title) == nil && validateProviderPullRequestURL(pullRequest.URL, target, pullRequest.Number) == nil
}

func candidateMatchesReceipt(git store.GitResult, pullRequest store.PullRequest, event protocol.Event, manifest protocol.TaskManifest) bool {
	validState := git.State == "candidate" && (pullRequest.State == "reported" || pullRequest.State == "open") || git.State == "pushed" && pullRequest.State == "open"
	return validState && git.Branch == event.Branch && git.CommitSHA == event.CommitSHA && git.Error == "" &&
		pullRequest.Number == event.PullRequestNumber && pullRequest.HeadBranch == manifest.TaskBranch && pullRequest.BaseBranch == manifest.BaseBranch && pullRequest.Error == "" &&
		(manifest.ExistingPullRequestNumber == 0 || pullRequest.Number == manifest.ExistingPullRequestNumber)
}

func validateProviderPullRequestURL(raw string, target forge.Target, number int) error {
	pullRequestURL, err := url.Parse(raw)
	if err != nil {
		return errors.New("invalid URL")
	}
	baseURL, err := url.Parse(target.BaseURL)
	if err != nil {
		return errors.New("invalid configured provider URL")
	}
	webHost := baseURL.Host
	if hostname := baseURL.Hostname(); len(hostname) > len("api.") && strings.EqualFold(hostname[:len("api.")], "api.") {
		webHost = hostname[len("api."):]
		if baseURL.Port() != "" {
			webHost += ":" + baseURL.Port()
		}
	}
	var segment string
	switch target.Provider {
	case forge.ProviderGitHub:
		segment = "pull"
	case forge.ProviderBitbucket:
		segment = "pull-requests"
	default:
		return errors.New("unsupported provider")
	}
	wantPath := "/" + url.PathEscape(target.Owner) + "/" + url.PathEscape(target.Repository) + "/" + segment + "/" + strconv.Itoa(number)
	validHost := strings.EqualFold(pullRequestURL.Host, baseURL.Host) || strings.EqualFold(pullRequestURL.Host, webHost)
	if !validHost || !strings.EqualFold(pullRequestURL.EscapedPath(), wantPath) {
		return fmt.Errorf("URL does not belong to configured %s repository and pull request %d", target.Provider, number)
	}
	return nil
}

func (c *Controller) handleLegacyBranchPushedLocked(ctx context.Context, record store.Task, attempt store.Attempt, event protocol.Event) error {
	var manifest struct {
		TaskBranch string `json:"task_branch"`
	}
	if len(attempt.ManifestJSON) == 0 {
		return fmt.Errorf("attempt %q has no immutable task manifest", attempt.ID)
	}
	if err := json.Unmarshal(attempt.ManifestJSON, &manifest); err != nil {
		return fmt.Errorf("decode legacy immutable manifest for attempt %q: %w", attempt.ID, err)
	}
	expectedBranch := manifest.TaskBranch
	if strings.TrimSpace(expectedBranch) == "" || expectedBranch != strings.TrimSpace(expectedBranch) {
		return fmt.Errorf("validate legacy immutable manifest for attempt %q: task_branch is required without surrounding whitespace", attempt.ID)
	}
	if err := protocol.ValidateBranch(expectedBranch); err != nil {
		return fmt.Errorf("validate legacy immutable manifest for attempt %q: task_branch: %w", attempt.ID, err)
	}
	if err := protocol.ValidateEvent(event, expectedBranch); err != nil {
		return fmt.Errorf("validate legacy branch_pushed event: %w", err)
	}
	git, gitErr := c.store.GetGitResult(ctx, attempt.ID)
	pullRequest, pullRequestErr := c.store.GetPullRequest(ctx, attempt.ID)
	if stateAtOrAfter(record.State, task.PR_OPEN) && gitErr == nil && pullRequestErr == nil &&
		git.AttemptID == attempt.ID && git.State == "pushed" && git.Branch == event.Branch && git.CommitSHA == event.CommitSHA && git.Error == "" &&
		pullRequest.AttemptID == attempt.ID && pullRequest.State == "open" && pullRequest.Number > 0 && pullRequest.HeadBranch == git.Branch && pullRequest.Error == "" &&
		forge.ValidatePullRequestMetadata(pullRequest.URL, pullRequest.Title) == nil {
		return nil
	}
	return c.transition(ctx, record.ID, record.State, task.FAILED,
		"legacy branch_pushed worker event cannot complete this attempt after upgrade; retry the task so OpenCode can report its pull request", "controller")
}

func (c *Controller) failPullRequestReceipt(ctx context.Context, record store.Task, cause error) error {
	if !forge.IsPermanent(cause) {
		return cause
	}
	if err := c.transition(ctx, record.ID, record.State, task.FAILED, "permanent reported pull request verification failure: "+cause.Error(), "controller"); err != nil {
		return errors.Join(cause, err)
	}
	return cause
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
	if err := protocol.ValidateManifest(manifest); err != nil {
		return manifest, fmt.Errorf("validate immutable manifest for attempt %q: %w", attempt.ID, err)
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
