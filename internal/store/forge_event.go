package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/task"
)

const (
	ForgeEventPending   = "pending"
	ForgeEventRunning   = "running"
	ForgeEventHandled   = "handled"
	ForgeEventFailed    = "failed"
	forgeEventBatchSize = 32
)

// ForgeEvent is normalized webhook content accepted for durable processing.
type ForgeEvent struct {
	ID                string
	Provider          string
	Kind              string
	Owner             string
	Repository        string
	PullRequestNumber int
	CommitSHA         string
	Branch            string
	CommentID         int
	CommentKind       string
	Title             string
	Body              string
	Author            string
	URL               string
	TaskID            string
	AttemptID         string
	Status            string
	Attempts          int
	LastError         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	HandledAt         *time.Time
	FailedAt          *time.Time
	NextAttemptAt     *time.Time
}

const forgeEventSelect = `
	SELECT id, provider, kind, owner, repository, pull_request_number, commit_sha,
	       branch, comment_id, comment_kind, title, body, author, url, task_id,
	       attempt_id, status, attempts, last_error, created_at, updated_at, handled_at, failed_at, next_attempt_at
	FROM forge_events`

// PutForgeEvent inserts normalized content once. A duplicate ID never replaces
// the originally accepted event.
func (s *Store) PutForgeEvent(ctx context.Context, event ForgeEvent) (ForgeEvent, error) {
	if err := validateForgeEvent(event); err != nil {
		return ForgeEvent{}, err
	}
	now := stamp(time.Now().UTC())
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO forge_events
			(id, provider, kind, owner, repository, pull_request_number, commit_sha,
			 branch, comment_id, comment_kind, title, body, author, url, status,
			 attempts, last_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)
		ON CONFLICT(id) DO NOTHING`,
		event.ID, event.Provider, event.Kind, event.Owner, event.Repository,
		event.PullRequestNumber, event.CommitSHA, event.Branch, event.CommentID,
		event.CommentKind, event.Title, event.Body, event.Author, event.URL, now, now,
	); err != nil {
		return ForgeEvent{}, fmt.Errorf("insert forge event %q: %w", event.ID, err)
	}
	stored, err := scanForgeEvent(s.db.QueryRowContext(ctx, forgeEventSelect+" WHERE id = ?", event.ID))
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("read forge event %q: %w", event.ID, err)
	}
	return stored, nil
}

func validateForgeEvent(event ForgeEvent) error {
	if event.Provider != string(forge.ProviderGitHub) && event.Provider != string(forge.ProviderBitbucket) {
		return fmt.Errorf("forge event provider %q is not supported", event.Provider)
	}
	if event.Kind != "review_comment" && event.Kind != "quality_gate_failed" {
		return fmt.Errorf("forge event kind %q is not supported", event.Kind)
	}
	for name, value := range map[string]string{
		"ID": event.ID, "provider": event.Provider, "kind": event.Kind, "owner": event.Owner,
		"repository": event.Repository, "commit SHA": event.CommitSHA, "branch": event.Branch,
		"comment kind": event.CommentKind, "author": event.Author, "URL": event.URL,
	} {
		required := name == "ID" || name == "provider" || name == "kind" || name == "owner" || name == "repository" || name == "author"
		if err := forge.ValidateNormalizedIdentity("forge event "+name, value, required); err != nil {
			return err
		}
	}
	for name, value := range map[string]string{"title": event.Title, "body": event.Body} {
		if err := forge.ValidateNormalizedText("forge event "+name, value, true); err != nil {
			return err
		}
	}
	if event.PullRequestNumber < 0 || event.CommentID < 0 {
		return errors.New("forge event numeric identities must not be negative")
	}
	if event.Kind == "quality_gate_failed" {
		if event.CommitSHA == "" {
			return errors.New("forge quality event commit SHA is required")
		}
		if event.CommentID != 0 || event.CommentKind != "" {
			return errors.New("forge quality event must not have comment identity")
		}
		return nil
	}
	if event.PullRequestNumber <= 0 || event.CommentID <= 0 {
		return errors.New("forge review event pull request and comment IDs must be positive")
	}
	if event.Provider == string(forge.ProviderGitHub) {
		switch event.CommentKind {
		case "issue_comment", "review_comment", "review":
			return nil
		default:
			return fmt.Errorf("forge GitHub comment kind %q is not supported", event.CommentKind)
		}
	}
	if event.CommentKind != "comment" {
		return fmt.Errorf("forge Bitbucket comment kind %q is not supported", event.CommentKind)
	}
	return nil
}

func (s *Store) GetForgeEvent(ctx context.Context, id string) (ForgeEvent, error) {
	event, err := scanForgeEvent(s.db.QueryRowContext(ctx, forgeEventSelect+" WHERE id = ?", id))
	if errors.Is(err, sql.ErrNoRows) {
		return ForgeEvent{}, fmt.Errorf("%w: forge event %s", ErrNotFound, id)
	}
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("get forge event %q: %w", id, err)
	}
	return event, nil
}

func (s *Store) GetForgeEventByAttempt(ctx context.Context, attemptID string) (ForgeEvent, error) {
	event, err := scanForgeEvent(s.db.QueryRowContext(ctx, forgeEventSelect+" WHERE attempt_id = ?", attemptID))
	if errors.Is(err, sql.ErrNoRows) {
		return ForgeEvent{}, fmt.Errorf("%w: forge event for attempt %s", ErrNotFound, attemptID)
	}
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("get forge event for attempt %q: %w", attemptID, err)
	}
	return event, nil
}

// ListIncompleteForgeEvents returns due pending and running events in acceptance order.
func (s *Store) ListIncompleteForgeEvents(ctx context.Context) (_ []ForgeEvent, resultErr error) {
	rows, err := s.db.QueryContext(ctx, forgeEventSelect+" WHERE status IN ('pending', 'running') AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY created_at, rowid LIMIT ?", stamp(time.Now().UTC()), forgeEventBatchSize)
	if err != nil {
		return nil, fmt.Errorf("list incomplete forge events: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var events []ForgeEvent
	for rows.Next() {
		event, err := scanForgeEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan incomplete forge event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list incomplete forge events: %w", err)
	}
	return events, nil
}

// RequestForgeEventCancellation records cancellation only while the event still
// owns the task's current non-terminal attempt. Missing or stale associations
// are safe no-ops.
func (s *Store) RequestForgeEventCancellation(ctx context.Context, eventID string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET cancellation_requested = 1, updated_at = ?
		WHERE state NOT IN (?, ?, ?)
		  AND EXISTS (
			SELECT 1 FROM forge_events
			WHERE id = ? AND status = 'running' AND task_id = tasks.id
			  AND attempt_id = tasks.current_attempt_id
		  )`, stamp(time.Now().UTC()), task.READY, task.FAILED, task.CANCELLED, eventID)
	if err != nil {
		return false, fmt.Errorf("request forge event %q cancellation: %w", eventID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("confirm forge event %q cancellation: %w", eventID, err)
	}
	return affected == 1, nil
}

// RecordForgeEventError records a failed processing attempt without completing
// the event.
func (s *Store) RecordForgeEventError(ctx context.Context, id string, cause error) error {
	return s.RecordForgeEventErrorAfter(ctx, id, cause, 0)
}

// RecordForgeEventErrorAfter defers transient work with bounded exponential
// pacing, honoring a longer provider-supplied delay when available.
func (s *Store) RecordForgeEventErrorAfter(ctx context.Context, id string, cause error, retryAfter time.Duration) error {
	retryAfter = min(retryAfter, 24*time.Hour)
	if strings.TrimSpace(id) == "" {
		return errors.New("forge event ID is empty")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	var attempts int
	if err := s.db.QueryRowContext(ctx, `SELECT attempts FROM forge_events WHERE id = ? AND status IN ('pending', 'running')`, id).Scan(&attempts); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: incomplete forge event %s", ErrNotFound, id)
	} else if err != nil {
		return fmt.Errorf("read forge event %q attempts: %w", id, err)
	}
	delay := forgeEventRetryDelay(attempts)
	if retryAfter > delay {
		delay = retryAfter
	}
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE forge_events
		SET attempts = attempts + 1, last_error = ?, updated_at = ?, next_attempt_at = ?
		WHERE id = ? AND status IN ('pending', 'running')`, message, stamp(now), stamp(now.Add(delay)), id)
	if err != nil {
		return fmt.Errorf("record forge event %q error: %w", id, err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm forge event %q error: %w", id, err)
	} else if affected != 1 {
		return fmt.Errorf("%w: incomplete forge event %s", ErrNotFound, id)
	}
	return nil
}

func forgeEventRetryDelay(attempts int) time.Duration {
	const base, maximum = 5 * time.Second, 5 * time.Minute
	delay := base
	for range min(attempts, 6) {
		delay *= 2
	}
	return min(delay, maximum)
}

// MarkForgeEventFailed records a permanent processing error without losing any
// task and attempt association needed for durable diagnostics.
func (s *Store) MarkForgeEventFailed(ctx context.Context, id string, cause error) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("forge event ID is empty")
	}
	if cause == nil || strings.TrimSpace(cause.Error()) == "" {
		return errors.New("forge event permanent error is empty")
	}
	now := stamp(time.Now().UTC())
	result, err := s.db.ExecContext(ctx, `
		UPDATE forge_events
		SET status = 'failed', attempts = attempts + 1, last_error = ?, next_attempt_at = NULL, failed_at = ?, updated_at = ?
		WHERE id = ? AND status IN ('pending', 'running')`, cause.Error(), now, now, id)
	if err != nil {
		return fmt.Errorf("mark forge event %q failed: %w", id, err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm forge event %q failed: %w", id, err)
	} else if affected != 1 {
		return fmt.Errorf("%w: incomplete forge event %s", ErrNotFound, id)
	}
	return nil
}

// ForgeEventAttemptPlan reserves no durable state. It supplies the identity
// needed to build immutable worker resources before the start transaction.
type ForgeEventAttemptPlan struct {
	Attempt           Attempt
	PreviousAttemptID string
}

// PlanForgeEventAttempt plans the next task-local identity without changing
// the event or task. The start transaction revalidates every observed value.
func (s *Store) PlanForgeEventAttempt(ctx context.Context, eventID, taskID, prompt string) (ForgeEventAttemptPlan, error) {
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(taskID) == "" {
		return ForgeEventAttemptPlan{}, errors.New("forge event and task IDs are required")
	}
	if strings.TrimSpace(prompt) == "" {
		return ForgeEventAttemptPlan{}, errors.New("forge follow-up prompt is empty")
	}
	var status string
	var associatedTaskID, associatedAttemptID sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT status, task_id, attempt_id FROM forge_events WHERE id = ?`, eventID).Scan(&status, &associatedTaskID, &associatedAttemptID); errors.Is(err, sql.ErrNoRows) {
		return ForgeEventAttemptPlan{}, fmt.Errorf("%w: forge event %s", ErrNotFound, eventID)
	} else if err != nil {
		return ForgeEventAttemptPlan{}, fmt.Errorf("read forge event %q: %w", eventID, err)
	}
	if status == ForgeEventRunning {
		if !associatedTaskID.Valid || !associatedAttemptID.Valid || associatedTaskID.String != taskID {
			return ForgeEventAttemptPlan{}, fmt.Errorf("%w: forge event %q is running for another task", ErrConflict, eventID)
		}
		attempt, err := s.GetAttempt(ctx, taskID, associatedAttemptID.String)
		if err != nil {
			return ForgeEventAttemptPlan{}, err
		}
		if err := requireCompleteAttemptSnapshot(attempt); err != nil {
			return ForgeEventAttemptPlan{}, err
		}
		return ForgeEventAttemptPlan{Attempt: attempt}, nil
	}
	if status != ForgeEventPending || associatedTaskID.Valid || associatedAttemptID.Valid {
		return ForgeEventAttemptPlan{}, fmt.Errorf("%w: forge event %q is %q or already associated", ErrConflict, eventID, status)
	}
	var currentAttemptID, headBranch string
	var number int
	err := s.db.QueryRowContext(ctx, `
		SELECT tasks.current_attempt_id, pull_requests.head_branch,
		       (SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = tasks.id)
		FROM tasks
		JOIN pull_requests ON pull_requests.attempt_id = tasks.current_attempt_id
		WHERE tasks.id = ?`, taskID).Scan(&currentAttemptID, &headBranch, &number)
	if errors.Is(err, sql.ErrNoRows) {
		return ForgeEventAttemptPlan{}, fmt.Errorf("%w: task %s with current pull request", ErrNotFound, taskID)
	}
	if err != nil {
		return ForgeEventAttemptPlan{}, fmt.Errorf("plan forge follow-up: %w", err)
	}
	attemptID, err := newID("swe-attempt-")
	if err != nil {
		return ForgeEventAttemptPlan{}, err
	}
	return ForgeEventAttemptPlan{
		PreviousAttemptID: currentAttemptID,
		Attempt: Attempt{
			ID: attemptID, TaskID: taskID, Number: number, Immutable: true,
			State: task.QUEUED, Prompt: prompt, BaseBranch: headBranch, TaskBranch: headBranch,
		},
	}, nil
}

// StartForgeEventAttempt atomically starts one fully snapshotted immutable
// follow-up attempt. Replaying a running event returns its original snapshot.
func (s *Store) StartForgeEventAttempt(ctx context.Context, eventID string, plan ForgeEventAttemptPlan) (Attempt, bool, error) {
	if strings.TrimSpace(eventID) == "" || strings.TrimSpace(plan.Attempt.TaskID) == "" {
		return Attempt{}, false, errors.New("forge event and task IDs are required")
	}
	taskID := plan.Attempt.TaskID

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("begin forge event attempt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var status string
	var associatedTaskID, associatedAttemptID sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT status, task_id, attempt_id FROM forge_events WHERE id = ?`, eventID).Scan(&status, &associatedTaskID, &associatedAttemptID); errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, false, fmt.Errorf("%w: forge event %s", ErrNotFound, eventID)
	} else if err != nil {
		return Attempt{}, false, fmt.Errorf("read forge event %q: %w", eventID, err)
	}
	if status == ForgeEventRunning {
		if !associatedTaskID.Valid || !associatedAttemptID.Valid || associatedTaskID.String != taskID {
			return Attempt{}, false, fmt.Errorf("%w: forge event %q is running for another task", ErrConflict, eventID)
		}
		attempt, err := scanAttempt(tx.QueryRowContext(ctx, `
			SELECT id, task_id, number, immutable, state, prompt, base_branch, task_branch,
			       logs_exhausted, validation_state, manifest_json, resource_snapshot,
			       config_digest, created_at
			FROM task_attempts WHERE id = ? AND task_id = ?`, associatedAttemptID.String, taskID))
		if err != nil {
			return Attempt{}, false, fmt.Errorf("read forge event %q attempt: %w", eventID, err)
		}
		if err := requireCompleteAttemptSnapshot(attempt); err != nil {
			return Attempt{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return Attempt{}, false, fmt.Errorf("commit forge event replay: %w", err)
		}
		return attempt, false, nil
	}
	if status != ForgeEventPending {
		return Attempt{}, false, fmt.Errorf("%w: forge event %q is %q", ErrConflict, eventID, status)
	}
	if associatedTaskID.Valid || associatedAttemptID.Valid {
		return Attempt{}, false, fmt.Errorf("%w: pending forge event %q is already associated", ErrConflict, eventID)
	}
	if strings.TrimSpace(plan.Attempt.ID) == "" || strings.TrimSpace(plan.PreviousAttemptID) == "" || plan.Attempt.Number <= 0 ||
		!plan.Attempt.Immutable || plan.Attempt.State != task.QUEUED || strings.TrimSpace(plan.Attempt.Prompt) == "" {
		return Attempt{}, false, errors.New("forge follow-up plan is incomplete")
	}
	if err := requireCompleteAttemptSnapshot(plan.Attempt); err != nil {
		return Attempt{}, false, err
	}

	var currentState, currentAttemptID, headBranch string
	var logsExhausted, cancellationRequested int
	var pullRequest PullRequest
	var number sql.NullInt64
	var notifiedAt sql.NullString
	err = tx.QueryRowContext(ctx, `
		SELECT tasks.state, tasks.current_attempt_id, tasks.cancellation_requested,
		       task_attempts.logs_exhausted, pull_requests.attempt_id,
		       pull_requests.state, pull_requests.number, pull_requests.url,
		       pull_requests.title, pull_requests.head_branch, pull_requests.base_branch,
		       pull_requests.error, pull_requests.notified_at
		FROM tasks
		JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id
		JOIN pull_requests ON pull_requests.attempt_id = task_attempts.id
		WHERE tasks.id = ?`, taskID).Scan(
		&currentState, &currentAttemptID, &cancellationRequested, &logsExhausted,
		&pullRequest.AttemptID, &pullRequest.State, &number, &pullRequest.URL,
		&pullRequest.Title, &headBranch, &pullRequest.BaseBranch, &pullRequest.Error, &notifiedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, false, fmt.Errorf("%w: task %s with current pull request", ErrNotFound, taskID)
	}
	if err != nil {
		return Attempt{}, false, fmt.Errorf("read task %q for forge event: %w", taskID, err)
	}
	pullRequest.HeadBranch = headBranch
	if number.Valid {
		pullRequest.Number = int(number.Int64)
	}
	if pullRequest.State != "open" || pullRequest.Number <= 0 || strings.TrimSpace(pullRequest.URL) == "" || strings.TrimSpace(headBranch) == "" {
		return Attempt{}, false, fmt.Errorf("%w: task %q current attempt has no open pull request", ErrConflict, taskID)
	}
	if currentAttemptID != plan.PreviousAttemptID || plan.Attempt.BaseBranch != headBranch || plan.Attempt.TaskBranch != headBranch {
		return Attempt{}, false, fmt.Errorf("%w: task %q current pull request changed while planning forge follow-up", ErrConflict, taskID)
	}
	if err := (task.Machine{}).ForgeFollowUp(task.State(currentState), task.QUEUED); err != nil {
		return Attempt{}, false, fmt.Errorf("%w: task %q: %w", ErrConflict, taskID, err)
	}
	if logsExhausted != 1 {
		return Attempt{}, false, fmt.Errorf("%w: current attempt %q logs are not exhausted", ErrConflict, currentAttemptID)
	}
	if cancellationRequested == 1 {
		return Attempt{}, false, fmt.Errorf("%w: task %q has cancellation requested", ErrConflict, taskID)
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM forge_events WHERE task_id = ? AND status = 'running'`, taskID).Scan(&active); err != nil {
		return Attempt{}, false, fmt.Errorf("check active forge follow-up: %w", err)
	}
	if active != 0 {
		return Attempt{}, false, fmt.Errorf("%w: task %q already has an active forge follow-up", ErrConflict, taskID)
	}

	var attemptNumber int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = ?`, taskID).Scan(&attemptNumber); err != nil {
		return Attempt{}, false, fmt.Errorf("read forge follow-up attempt number: %w", err)
	}
	if attemptNumber != plan.Attempt.Number {
		return Attempt{}, false, fmt.Errorf("%w: planned forge follow-up attempt %d is not next attempt %d", ErrConflict, plan.Attempt.Number, attemptNumber)
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts
			(id, task_id, number, immutable, state, prompt, base_branch, task_branch,
			 manifest_json, resource_snapshot, config_digest, created_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)`,
		plan.Attempt.ID, taskID, attemptNumber, task.QUEUED, plan.Attempt.Prompt, headBranch, headBranch,
		plan.Attempt.ManifestJSON, plan.Attempt.ResourceSnapshot, plan.Attempt.ConfigDigest, stamp(now)); err != nil {
		return Attempt{}, false, fmt.Errorf("insert forge follow-up attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO pull_requests
			(attempt_id, state, number, url, title, head_branch, base_branch, error, notified_at)
		VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?)`,
		plan.Attempt.ID, pullRequest.Number, pullRequest.URL, pullRequest.Title, headBranch,
		pullRequest.BaseBranch, pullRequest.Error, nullableString(notifiedAt.String)); err != nil {
		return Attempt{}, false, fmt.Errorf("copy forge follow-up pull request: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE tasks SET state = ?, current_attempt_id = ?, updated_at = ?
		WHERE id = ? AND current_attempt_id = ? AND state = ?`,
		task.QUEUED, plan.Attempt.ID, stamp(now), taskID, currentAttemptID, currentState)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("select forge follow-up attempt: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return Attempt{}, false, fmt.Errorf("%w: task %q changed while starting forge follow-up", ErrConflict, taskID)
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE forge_events
		SET task_id = ?, attempt_id = ?, status = 'running', attempts = attempts + 1,
		    last_error = '', next_attempt_at = NULL, updated_at = ?
		WHERE id = ? AND status = 'pending' AND task_id IS NULL AND attempt_id IS NULL`,
		taskID, plan.Attempt.ID, stamp(now), eventID)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("associate forge event %q: %w", eventID, err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return Attempt{}, false, fmt.Errorf("%w: forge event %q changed while starting follow-up", ErrConflict, eventID)
	}
	transitionID, err := newID("swe-event-")
	if err != nil {
		return Attempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_events
			(id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger,
			 resource_identity, metadata, error)
		VALUES (?, ?, ?, ?, ?, ?, 'forge follow-up requested', 'webhook', '{}', '{}', '')`,
		transitionID, taskID, plan.Attempt.ID, stamp(now), currentState, task.QUEUED); err != nil {
		return Attempt{}, false, fmt.Errorf("append forge follow-up transition: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Attempt{}, false, fmt.Errorf("commit forge follow-up attempt: %w", err)
	}
	plan.Attempt.CreatedAt = now
	return plan.Attempt, true, nil
}

func requireCompleteAttemptSnapshot(attempt Attempt) error {
	if len(attempt.ManifestJSON) == 0 || len(attempt.ResourceSnapshot) == 0 || strings.TrimSpace(attempt.ConfigDigest) == "" {
		return fmt.Errorf("%w: forge follow-up attempt %q has no complete immutable snapshot", ErrConflict, attempt.ID)
	}
	return nil
}

// MarkForgeEventHandled is idempotent so worker completion may be replayed.
func (s *Store) MarkForgeEventHandled(ctx context.Context, id string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("forge event ID is empty")
	}
	now := stamp(time.Now().UTC())
	result, err := s.db.ExecContext(ctx, `
		UPDATE forge_events
		SET status = 'handled', last_error = '', next_attempt_at = NULL, handled_at = COALESCE(handled_at, ?), updated_at = ?
		WHERE id = ? AND status IN ('pending', 'running', 'handled')`, now, now, id)
	if err != nil {
		return fmt.Errorf("mark forge event %q handled: %w", id, err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm forge event %q handled: %w", id, err)
	} else if affected != 1 {
		return fmt.Errorf("%w: forge event %s", ErrNotFound, id)
	}
	return nil
}

func scanForgeEvent(row scanner) (ForgeEvent, error) {
	var event ForgeEvent
	var taskID, attemptID, handledAt, failedAt, nextAttemptAt sql.NullString
	var createdAt, updatedAt string
	if err := row.Scan(
		&event.ID, &event.Provider, &event.Kind, &event.Owner, &event.Repository,
		&event.PullRequestNumber, &event.CommitSHA, &event.Branch, &event.CommentID,
		&event.CommentKind, &event.Title, &event.Body, &event.Author, &event.URL,
		&taskID, &attemptID, &event.Status, &event.Attempts, &event.LastError,
		&createdAt, &updatedAt, &handledAt, &failedAt, &nextAttemptAt,
	); err != nil {
		return ForgeEvent{}, err
	}
	event.TaskID, event.AttemptID = taskID.String, attemptID.String
	var err error
	event.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return ForgeEvent{}, err
	}
	event.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return ForgeEvent{}, err
	}
	if handledAt.Valid {
		handled, err := parseTime(handledAt.String)
		if err != nil {
			return ForgeEvent{}, err
		}
		event.HandledAt = &handled
	}
	if failedAt.Valid {
		failed, err := parseTime(failedAt.String)
		if err != nil {
			return ForgeEvent{}, err
		}
		event.FailedAt = &failed
	}
	if nextAttemptAt.Valid {
		next, err := parseTime(nextAttemptAt.String)
		if err != nil {
			return ForgeEvent{}, err
		}
		event.NextAttemptAt = &next
	}
	return event, nil
}
