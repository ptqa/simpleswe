package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

type forgeMatch uint8

const (
	forgeDefinitelyUnowned forgeMatch = iota
	forgeOwned
	forgeSettling
)

type forgeEventPersistenceError struct{ error }

func (e forgeEventPersistenceError) Unwrap() error { return e.error }

// ProcessForgeEvents recovers normalized webhook work from the durable inbox.
func (c *Controller) ProcessForgeEvents(ctx context.Context) error {
	events, err := c.store.ListIncompleteForgeEvents(ctx)
	if err != nil {
		return err
	}
	var persistenceErrors []error
	processed := make(map[string]bool, len(events))
	for _, event := range events {
		if processed[event.ID] {
			continue
		}
		if ctx.Err() != nil {
			return errors.Join(errors.Join(persistenceErrors...), ctx.Err())
		}
		if event.Status == store.ForgeEventRunning {
			associated := []store.ForgeEvent{event}
			if event.AttemptID != "" {
				var listErr error
				associated, listErr = c.store.ListForgeEventsByAttempt(ctx, event.AttemptID)
				if listErr != nil {
					persistenceErrors = append(persistenceErrors, listErr)
					continue
				}
			}
			for _, sibling := range associated {
				processed[sibling.ID] = true
			}
			if err := c.recoverRunningForgeEvent(ctx, event); err != nil {
				for _, sibling := range associated {
					if sibling.Status != store.ForgeEventRunning || sibling.NextAttemptAt != nil && sibling.NextAttemptAt.After(time.Now().UTC()) {
						continue
					}
					if persistErr := c.persistForgeEventFailure(ctx, sibling, "recover", err); persistErr != nil {
						persistenceErrors = append(persistenceErrors, persistErr)
					}
				}
			}
			continue
		}

		batchCandidates := []store.ForgeEvent{event}
		if event.Kind == "review_comment" {
			batchCandidates, err = c.store.ListDueForgeEventBatch(ctx, event)
			if err != nil {
				persistenceErrors = append(persistenceErrors, err)
				continue
			}
			if len(batchCandidates) == 0 {
				continue
			}
		}

		record, repository, match, err := c.matchForgeEvent(ctx, event)
		if err != nil {
			if persistErr := c.persistForgeEventFailure(ctx, event, "match", err); persistErr != nil {
				persistenceErrors = append(persistenceErrors, persistErr)
			}
			continue
		}
		if match == forgeSettling {
			continue
		}
		if match == forgeDefinitelyUnowned {
			if err := c.store.MarkForgeEventHandled(ctx, event.ID); err != nil {
				persistenceErrors = append(persistenceErrors, err)
			}
			continue
		}

		batch := batchCandidates
		for _, selected := range batch {
			processed[selected.ID] = true
		}
		if err := c.startForgeEvent(ctx, batch, record, repository); err != nil {
			if errors.Is(err, store.ErrForgeEventNotDue) {
				continue
			}
			for _, selected := range batch {
				if persistErr := c.persistForgeEventFailure(ctx, selected, "start", err); persistErr != nil {
					persistenceErrors = append(persistenceErrors, persistErr)
				}
			}
		}
	}
	return errors.Join(persistenceErrors...)
}

func (c *Controller) persistForgeEventFailure(ctx context.Context, event store.ForgeEvent, operation string, cause error) error {
	return c.persistForgeEventFailureWithCancellation(ctx, event, operation, cause, true)
}

func (c *Controller) persistForgeEventReplyFailure(ctx context.Context, event store.ForgeEvent, operation string, cause error) error {
	return c.persistForgeEventFailureWithCancellation(ctx, event, operation, cause, false)
}

func (c *Controller) persistForgeEventFailureWithCancellation(ctx context.Context, event store.ForgeEvent, operation string, cause error, cancelPermanent bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var persistenceError forgeEventPersistenceError
	if errors.As(cause, &persistenceError) {
		return cause
	}
	permanent := forge.IsPermanent(cause)
	var err error
	if permanent {
		if cancelPermanent {
			if _, err := c.store.RequestForgeEventCancellation(ctx, event.ID); err != nil {
				return forgeEventPersistenceError{fmt.Errorf("persist forge event %q %s cancellation: %w", event.ID, operation, errors.Join(cause, err))}
			}
		}
		err = c.store.MarkForgeEventFailed(ctx, event.ID, cause)
	} else {
		err = c.store.RecordForgeEventErrorAfter(ctx, event.ID, cause, forge.RetryDelay(cause))
	}
	if err != nil {
		return forgeEventPersistenceError{fmt.Errorf("persist forge event %q %s failure: %w", event.ID, operation, errors.Join(cause, err))}
	}
	message := "forge event processing deferred"
	if permanent {
		message = "forge event processing permanently failed"
	}
	c.logger.ErrorContext(ctx, message, "forge_event", event.ID, "operation", operation, "permanent", permanent, "error", cause)
	return nil
}

func (c *Controller) startForgeEvent(ctx context.Context, events []store.ForgeEvent, record store.Task, repository config.RepositoryConfig) error {
	if len(events) == 0 {
		return errors.New("forge event batch is empty")
	}
	unlock, err := c.locks.lock(ctx, record.ID)
	if err != nil {
		return err
	}
	current, err := c.store.GetTask(ctx, record.ID)
	if err != nil {
		unlock()
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	for _, event := range events {
		match, err := c.forgeEventMatchesTask(ctx, event, current)
		if err != nil || match != forgeOwned {
			unlock()
			if err != nil {
				return err
			}
			return fmt.Errorf("%w: task %q no longer owns forge event", store.ErrConflict, record.ID)
		}
	}
	currentAttempt, err := c.store.CurrentAttempt(ctx, current.ID)
	if err != nil {
		unlock()
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	if err := c.verifyAttemptProviderOwnership(ctx, current, currentAttempt, ""); err != nil {
		unlock()
		return err
	}
	eventIDs := make([]string, len(events))
	for i := range events {
		eventIDs[i] = events[i].ID
	}
	plan, err := c.store.PlanForgeEventAttempt(ctx, eventIDs, current.ID, forgeFollowUpPrompt(current.Prompt, events))
	if err != nil {
		unlock()
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	if len(plan.Attempt.ResourceSnapshot) == 0 {
		plan.Attempt, _, _, err = c.buildAttemptSnapshot(current, plan.Attempt, repository)
		if err != nil {
			unlock()
			return err
		}
	}
	attempt, _, err := c.store.StartForgeEventAttempt(ctx, plan)
	unlock()
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	err = c.startAttempt(ctx, current, attempt, repository)
	if permanentKubernetesError(err) {
		return forge.MarkPermanent(err)
	}
	return err
}

func verifyLivePullRequest(live forge.PullRequestState, durable store.PullRequest, target forge.Target, headSHA string) error {
	if err := verifyLivePullRequestIdentity(live, durable, target); err != nil {
		return err
	}
	if !providerCommitMatchesDurable(live.HeadSHA, headSHA) {
		return forge.MarkPermanent(fmt.Errorf("%w: provider pull request head SHA %q does not match durable pushed SHA %q", store.ErrConflict, live.HeadSHA, headSHA))
	}
	return nil
}

func verifyLivePullRequestIdentity(live forge.PullRequestState, durable store.PullRequest, target forge.Target) error {
	if live.State != "open" {
		return forge.MarkPermanent(fmt.Errorf("%w: pull request %d is %q at provider", store.ErrConflict, durable.Number, live.State))
	}
	if live.Number != durable.Number || live.SourceBranch != durable.HeadBranch || live.DestinationBranch != durable.BaseBranch ||
		!strings.EqualFold(live.SourceOwner, target.Owner) || !strings.EqualFold(live.SourceRepository, target.Repository) {
		return forge.MarkPermanent(fmt.Errorf("%w: provider pull request identity or refs no longer match durable ownership", store.ErrConflict))
	}
	return nil
}

func (c *Controller) latestPushedPullRequestGitResult(ctx context.Context, taskID string, durable store.PullRequest, excludeAttemptID string) (store.GitResult, error) {
	attempts, err := c.store.ListAttempts(ctx, taskID)
	if err != nil {
		return store.GitResult{}, err
	}
	for i := len(attempts) - 1; i >= 0; i-- {
		attempt := attempts[i]
		if attempt.ID == excludeAttemptID {
			continue
		}
		candidatePR, err := c.store.GetPullRequest(ctx, attempt.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return store.GitResult{}, err
		}
		if candidatePR.State != "open" || candidatePR.Number != durable.Number || candidatePR.HeadBranch != durable.HeadBranch || candidatePR.BaseBranch != durable.BaseBranch {
			continue
		}
		git, err := c.store.GetGitResult(ctx, attempt.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return store.GitResult{}, err
		}
		if git.State == "pushed" && git.Branch == durable.HeadBranch && strings.TrimSpace(git.CommitSHA) != "" {
			return git, nil
		}
	}
	return store.GitResult{}, fmt.Errorf("%w: task %q has no durable pushed Git result for pull request %d", store.ErrNotFound, taskID, durable.Number)
}

func (c *Controller) verifyAttemptProviderOwnership(ctx context.Context, record store.Task, attempt store.Attempt, excludeAttemptID string) error {
	pullRequest, err := c.store.GetPullRequest(ctx, attempt.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	target, err := c.attemptForgeTarget(record, attempt)
	if err != nil {
		return err
	}
	durableHead, err := c.latestPushedPullRequestGitResult(ctx, record.ID, pullRequest, excludeAttemptID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	providerCtx, cancel := context.WithTimeout(ctx, c.providerTimeout)
	live, err := c.pullRequests.GetPullRequest(providerCtx, target, pullRequest.Number)
	cancel()
	if err != nil {
		return fmt.Errorf("inspect current pull request: %w", err)
	}
	return verifyLivePullRequest(live, pullRequest, target, durableHead.CommitSHA)
}

func (c *Controller) recoverRunningForgeEvent(ctx context.Context, event store.ForgeEvent) error {
	if event.TaskID == "" || event.AttemptID == "" {
		return forge.MarkPermanent(fmt.Errorf("%w: running forge event has no task attempt", store.ErrConflict))
	}
	unlock, err := c.locks.lock(ctx, event.TaskID)
	if err != nil {
		return err
	}
	lockedTaskID := event.TaskID
	currentEvent, err := c.store.GetForgeEvent(ctx, event.ID)
	if err != nil || currentEvent.Status != store.ForgeEventRunning {
		unlock()
		return err
	}
	event = currentEvent
	if event.TaskID == "" || event.AttemptID == "" || event.TaskID != lockedTaskID {
		unlock()
		return forge.MarkPermanent(fmt.Errorf("%w: running forge event has invalid task attempt association", store.ErrConflict))
	}
	record, err := c.store.GetTask(ctx, event.TaskID)
	if err != nil {
		unlock()
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(fmt.Errorf("running forge event task association: %w", err))
		}
		return err
	}
	attempt, err := c.store.CurrentAttempt(ctx, event.TaskID)
	if err != nil {
		unlock()
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(fmt.Errorf("running forge event attempt association: %w", err))
		}
		return err
	}
	if attempt.ID != event.AttemptID {
		unlock()
		return forge.MarkPermanent(fmt.Errorf("%w: forge event attempt %q is not current", store.ErrConflict, event.AttemptID))
	}
	repository, err := c.repository(record.Repository)
	if err != nil {
		err = forge.MarkPermanent(err)
	}
	if err == nil && (len(attempt.ManifestJSON) == 0 || len(attempt.ResourceSnapshot) == 0 || strings.TrimSpace(attempt.ConfigDigest) == "") {
		err = forge.MarkPermanent(fmt.Errorf("%w: running forge follow-up attempt %q has no complete immutable snapshot", store.ErrConflict, attempt.ID))
	}
	if err == nil {
		_, _, err = c.prepareAttemptSnapshot(ctx, record, attempt, repository)
	}
	if err == nil {
		attempt, err = c.store.GetAttempt(ctx, event.TaskID, event.AttemptID)
	}
	if err == nil {
		_, err = c.attemptForgeTarget(record, attempt)
	}
	if err != nil {
		unlock()
		if errors.Is(err, store.ErrNotFound) {
			return forge.MarkPermanent(err)
		}
		return err
	}
	git, gitErr := c.store.GetGitResult(ctx, attempt.ID)
	if gitErr == nil && git.State == "pushed" && git.Branch != "" && git.CommitSHA != "" {
		err = c.completeForgeEventLocked(ctx, record, attempt)
		unlock()
		return err
	}
	if gitErr != nil && !errors.Is(gitErr, store.ErrNotFound) {
		unlock()
		return gitErr
	}
	if err := c.verifyAttemptProviderOwnership(ctx, record, attempt, attempt.ID); err != nil {
		unlock()
		return fmt.Errorf("inspect running forge follow-up ownership: %w", err)
	}
	unlock()
	err = c.startAttempt(ctx, record, attempt, repository)
	if permanentKubernetesError(err) {
		return forge.MarkPermanent(err)
	}
	return err
}

func (c *Controller) matchForgeEvent(ctx context.Context, event store.ForgeEvent) (store.Task, config.RepositoryConfig, forgeMatch, error) {
	tasks, err := c.store.ListTasks(ctx)
	if err != nil {
		return store.Task{}, config.RepositoryConfig{}, forgeDefinitelyUnowned, err
	}
	var matched store.Task
	var matchedRepository config.RepositoryConfig
	settling := false
	for _, record := range tasks {
		repository, err := c.repository(record.Repository)
		if err != nil {
			// Historical tasks may outlive their repository configuration. They
			// cannot own a newly accepted event and must not poison other matches.
			continue
		}
		target, err := forgeTarget(c.config, repository)
		if err != nil {
			return store.Task{}, config.RepositoryConfig{}, forgeDefinitelyUnowned, forge.MarkPermanent(err)
		}
		if !sameForgeCoordinates(event, target) {
			continue
		}
		match, err := c.forgeEventMatchesTask(ctx, event, record)
		if err != nil {
			return store.Task{}, config.RepositoryConfig{}, forgeDefinitelyUnowned, err
		}
		if match == forgeSettling {
			settling = true
			continue
		}
		if match != forgeOwned {
			continue
		}
		if matched.ID != "" && matched.ID != record.ID {
			return store.Task{}, config.RepositoryConfig{}, forgeDefinitelyUnowned, nil
		}
		matched, matchedRepository = record, repository
	}
	if matched.ID != "" && (!settling || event.PullRequestNumber > 0) {
		return matched, matchedRepository, forgeOwned, nil
	}
	if settling {
		return store.Task{}, config.RepositoryConfig{}, forgeSettling, nil
	}
	return store.Task{}, config.RepositoryConfig{}, forgeDefinitelyUnowned, nil
}

func (c *Controller) forgeEventMatchesTask(ctx context.Context, event store.ForgeEvent, record store.Task) (forgeMatch, error) {
	attempt, err := c.store.CurrentAttempt(ctx, record.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return forgeDefinitelyUnowned, forge.MarkPermanent(fmt.Errorf("task %q attempt association: %w", record.ID, err))
		}
		return forgeDefinitelyUnowned, err
	}
	pullRequest, err := c.store.GetPullRequest(ctx, attempt.ID)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		return forgeDefinitelyUnowned, err
	}
	if err == nil && pullRequest.State == "open" && pullRequest.Number > 0 && pullRequest.HeadBranch != "" {
		if event.Kind != "quality_gate_failed" {
			if event.PullRequestNumber == pullRequest.Number {
				return forgeOwned, nil
			}
			return forgeDefinitelyUnowned, nil
		}
		return c.qualityEventMatchesOpenPullRequest(ctx, event, record, attempt, pullRequest)
	}
	if (err == nil && pullRequest.State == "creating") || forgeOwnershipMaySettle(record.State) {
		return c.forgeEventMaySettle(ctx, event, attempt, pullRequest)
	}
	return forgeDefinitelyUnowned, nil
}

func (c *Controller) qualityEventMatchesOpenPullRequest(ctx context.Context, event store.ForgeEvent, record store.Task, current store.Attempt, pullRequest store.PullRequest) (forgeMatch, error) {
	if event.CommitSHA == "" || event.PullRequestNumber > 0 && event.PullRequestNumber != pullRequest.Number || event.Branch != "" && event.Branch != pullRequest.HeadBranch {
		return forgeDefinitelyUnowned, nil
	}
	attempts, err := c.store.ListAttempts(ctx, record.ID)
	if err != nil {
		return forgeDefinitelyUnowned, err
	}
	currentHasPushed := false
	for i := len(attempts) - 1; i >= 0; i-- {
		candidate := attempts[i]
		candidatePR, err := c.store.GetPullRequest(ctx, candidate.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return forgeDefinitelyUnowned, err
		}
		if candidatePR.State != "open" || candidatePR.Number != pullRequest.Number || candidatePR.HeadBranch != pullRequest.HeadBranch {
			continue
		}
		git, err := c.store.GetGitResult(ctx, candidate.ID)
		if errors.Is(err, store.ErrNotFound) {
			continue
		}
		if err != nil {
			return forgeDefinitelyUnowned, err
		}
		if git.State != "pushed" || git.Branch != pullRequest.HeadBranch || git.CommitSHA == "" {
			continue
		}
		currentHasPushed = candidate.ID == current.ID
		if providerCommitMatchesDurable(event.CommitSHA, git.CommitSHA) {
			return forgeOwned, nil
		}
		break
	}
	if !currentHasPushed && forgeOwnershipMaySettle(record.State) {
		return forgeSettling, nil
	}
	return forgeDefinitelyUnowned, nil
}

func (c *Controller) forgeEventMaySettle(ctx context.Context, event store.ForgeEvent, attempt store.Attempt, pullRequest store.PullRequest) (forgeMatch, error) {
	branch := pullRequest.HeadBranch
	if branch == "" {
		manifest, err := attemptManifest(attempt)
		if err != nil {
			return forgeDefinitelyUnowned, nil
		}
		branch = manifest.TaskBranch
	}
	if event.Branch != "" && branch != "" && event.Branch != branch {
		return forgeDefinitelyUnowned, nil
	}
	git, err := c.store.GetGitResult(ctx, attempt.ID)
	if err == nil && git.State == "pushed" {
		if branch != "" && git.Branch != branch || event.Branch != "" && git.Branch != event.Branch || event.CommitSHA != "" && !providerCommitMatchesDurable(event.CommitSHA, git.CommitSHA) {
			return forgeDefinitelyUnowned, nil
		}
	} else if err != nil && !errors.Is(err, store.ErrNotFound) {
		return forgeDefinitelyUnowned, err
	}
	return forgeSettling, nil
}

func forgeOwnershipMaySettle(state task.State) bool {
	switch state {
	case task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR:
		return true
	default:
		return false
	}
}

func (c *Controller) completeForgeEventLocked(ctx context.Context, record store.Task, attempt store.Attempt) error {
	events, err := c.store.ListForgeEventsByAttempt(ctx, attempt.ID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	due := make([]store.ForgeEvent, 0, len(events))
	for _, event := range events {
		if event.Status == store.ForgeEventRunning && (event.NextAttemptAt == nil || !event.NextAttemptAt.After(now)) {
			due = append(due, event)
		}
	}
	if len(due) == 0 {
		return nil
	}
	persistAll := func(cause error) error {
		const operation = "completion ownership"
		var persistenceErrors []error
		for _, event := range due {
			if persistErr := c.persistForgeEventFailure(ctx, event, operation, cause); persistErr != nil {
				persistenceErrors = append(persistenceErrors, persistErr)
			}
		}
		return errors.Join(persistenceErrors...)
	}
	git, err := c.store.GetGitResult(ctx, attempt.ID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	pullRequest, err := c.store.GetPullRequest(ctx, attempt.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return persistAll(forge.MarkPermanent(fmt.Errorf("running forge event durable pull request: %w", err)))
		}
		return err
	}
	if git.State != "pushed" || git.Branch == "" || git.CommitSHA == "" {
		return nil
	}
	if pullRequest.State != "open" || pullRequest.Number <= 0 || pullRequest.HeadBranch != git.Branch {
		return persistAll(forge.MarkPermanent(fmt.Errorf("%w: forge event durable pull request does not match pushed Git result", store.ErrConflict)))
	}
	target, err := c.attemptForgeTarget(record, attempt)
	if err != nil {
		return persistAll(err)
	}
	priorGit, err := c.latestPushedPullRequestGitResult(ctx, record.ID, pullRequest, attempt.ID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			err = forge.MarkPermanent(err)
		}
		return persistAll(err)
	}
	providerCtx, cancel := context.WithTimeout(ctx, c.providerTimeout)
	live, err := c.pullRequests.GetPullRequest(providerCtx, target, pullRequest.Number)
	cancel()
	if err != nil {
		return persistAll(fmt.Errorf("inspect completed pull request: %w", err))
	}
	if err := verifyLivePullRequestIdentity(live, pullRequest, target); err != nil {
		return persistAll(err)
	}
	if !providerCommitMatchesDurable(live.HeadSHA, git.CommitSHA) {
		if providerCommitMatchesDurable(live.HeadSHA, priorGit.CommitSHA) {
			return persistAll(fmt.Errorf("provider pull request head is still prior durable SHA %q; waiting for pushed SHA %q", priorGit.CommitSHA, git.CommitSHA))
		}
		return persistAll(forge.MarkPermanent(fmt.Errorf("%w: provider pull request head SHA %q is neither pushed SHA %q nor prior durable SHA %q", store.ErrConflict, live.HeadSHA, git.CommitSHA, priorGit.CommitSHA)))
	}
	var persistenceErrors []error
	for _, event := range due {
		if event.TaskID != record.ID {
			if err := c.persistForgeEventFailure(ctx, event, "completion ownership", forge.MarkPermanent(fmt.Errorf("%w: forge event %q is not running for task %q", store.ErrConflict, event.ID, record.ID))); err != nil {
				persistenceErrors = append(persistenceErrors, err)
			}
			continue
		}
		if event.PullRequestNumber > 0 && event.PullRequestNumber != pullRequest.Number || !sameForgeCoordinates(event, target) {
			if err := c.persistForgeEventFailure(ctx, event, "completion ownership", forge.MarkPermanent(fmt.Errorf("%w: forge event %q does not match durable pull request ownership", store.ErrConflict, event.ID))); err != nil {
				persistenceErrors = append(persistenceErrors, err)
			}
			continue
		}
		marker := forge.ReplyMarker(event.ID)
		body := "A fix was pushed in durable commit " + git.CommitSHA + "; quality gates are rerunning."
		if strings.TrimSpace(event.ReplyDraft) != "" && !forge.ContainsReplyMarker(event.ReplyDraft) {
			body = event.ReplyDraft
		}
		reply := forge.ReplyRequest{PullRequestNumber: pullRequest.Number, Body: marker + " " + body}
		if event.Kind == "review_comment" && event.CommentID > 0 {
			reply.CommentID = event.CommentID
			reply.CommentKind = event.CommentKind
		}
		providerCtx, cancel = context.WithTimeout(ctx, c.providerTimeout)
		exists, replyErr := c.pullRequests.PullRequestReplyExists(providerCtx, target, reply, marker)
		cancel()
		if replyErr != nil {
			if err := c.persistForgeEventReplyFailure(ctx, event, "reply check", fmt.Errorf("check provider reply marker: %w", replyErr)); err != nil {
				persistenceErrors = append(persistenceErrors, err)
			}
			continue
		}
		if !exists {
			providerCtx, cancel = context.WithTimeout(ctx, c.providerTimeout)
			replyErr = c.pullRequests.ReplyToPullRequest(providerCtx, target, reply)
			cancel()
			if replyErr != nil {
				if err := c.persistForgeEventReplyFailure(ctx, event, "reply post", fmt.Errorf("post provider reply: %w", replyErr)); err != nil {
					persistenceErrors = append(persistenceErrors, err)
				}
				continue
			}
			// A provider with eventually consistent comment reads can still expose a
			// duplicate only if the process dies here and the marker is not yet visible.
		}
		if err := c.store.MarkForgeEventHandled(ctx, event.ID); err != nil {
			persistenceErrors = append(persistenceErrors, forgeEventPersistenceError{err})
			continue
		}
		c.logger.InfoContext(ctx, "forge follow-up reply handled", "task", record.ID, "attempt", attempt.ID, "forge_event", event.ID, "already_exists", exists)
	}
	return errors.Join(persistenceErrors...)
}

func providerCommitMatchesDurable(providerSHA, durableSHA string) bool {
	if strings.EqualFold(providerSHA, durableSHA) {
		return true
	}
	if len(providerSHA) < 7 || len(providerSHA) >= len(durableSHA) || len(durableSHA) != 40 && len(durableSHA) != 64 {
		return false
	}
	for _, char := range providerSHA + durableSHA {
		if char < '0' || char > '9' && char < 'A' || char > 'F' && char < 'a' || char > 'f' {
			return false
		}
	}
	return strings.HasPrefix(strings.ToLower(durableSHA), strings.ToLower(providerSHA))
}

func sameForgeCoordinates(event store.ForgeEvent, target forge.Target) bool {
	return strings.EqualFold(event.Provider, string(target.Provider)) &&
		strings.EqualFold(event.Owner, target.Owner) && strings.EqualFold(event.Repository, target.Repository)
}

// Leave room below the 96 KiB target for the worker's fixed workflow suffix.
const forgeFollowUpPromptMaxBytes = 95 << 10

func forgeFollowUpPrompt(original string, events []store.ForgeEvent) string {
	parts := []string{"Original task: " + boundedForgeText(original, 12<<10)}
	if len(events) > 0 && events[0].Kind == "review_comment" {
		parts = append(parts, protocol.ReviewReplyInstruction)
	}

	fixed := make([]string, len(events))
	used := len(strings.Join(parts, "; "))
	for i, event := range events {
		fields := make([]string, 0, 12)
		if event.Kind == "review_comment" {
			fields = append(fields, "comment_id="+strconv.Itoa(event.CommentID))
		}
		fields = append(fields,
			"event_id="+boundedForgeText(event.ID, 128),
			"provider="+boundedForgeText(event.Provider, 32),
			"kind="+boundedForgeText(event.Kind, 32),
		)
		if event.PullRequestNumber > 0 {
			fields = append(fields, "pull_request="+strconv.Itoa(event.PullRequestNumber))
		}
		if event.CommentKind != "" {
			fields = append(fields, "comment_kind="+boundedForgeText(event.CommentKind, 32))
		}
		if event.CommitSHA != "" {
			fields = append(fields, "commit="+boundedForgeText(event.CommitSHA, 128))
		}
		if event.Branch != "" {
			fields = append(fields, "branch="+boundedForgeText(event.Branch, 256))
		}
		fields = append(fields,
			"author="+boundedForgeText(event.Author, 256),
			"url="+boundedForgeText(event.URL, 512),
			"title="+boundedForgeText(event.Title, 512),
		)
		fixed[i] = fmt.Sprintf("forge_event_%d: %s; body=", i+1, strings.Join(fields, "; "))
		used += len("; ") + len(fixed[i])
	}
	bodyBudget := 0
	if len(events) > 0 && used < forgeFollowUpPromptMaxBytes {
		bodyBudget = (forgeFollowUpPromptMaxBytes - used) / len(events)
	}
	for i, event := range events {
		parts = append(parts, fixed[i]+boundedForgeText(event.Body, bodyBudget))
	}
	return strings.Join(parts, "; ")
}

func boundedForgeText(value string, limit int) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= limit {
		return value
	}
	return strings.ToValidUTF8(value[:limit], "")
}
