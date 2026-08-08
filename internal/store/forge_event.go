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
	ForgeEventPending = "pending"
	ForgeEventRunning = "running"
	ForgeEventHandled = "handled"
	ForgeEventFailed  = "failed"
	// forgeEventBatchSize bounds each review follow-up and its reply mapping.
	forgeEventBatchSize = 32
)

var ErrForgeEventNotDue = errors.New("forge event is not due")

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
	ReplyDraft        string
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
	       attempt_id, reply_draft, status, attempts, last_error, created_at, updated_at, handled_at, failed_at, next_attempt_at
	FROM forge_events`

// PutForgeEvent inserts normalized content once. A duplicate ID never replaces
// the originally accepted event.
func (s *Store) PutForgeEvent(ctx context.Context, event ForgeEvent) (ForgeEvent, error) {
	return s.PutForgeEventAfter(ctx, event, 0)
}

// PutForgeEventAfter inserts normalized content once and, for a delayed review
// comment with a head SHA, atomically slides every matching pending review to
// one deadline. SHA-less review comments retain independent deadlines.
func (s *Store) PutForgeEventAfter(ctx context.Context, event ForgeEvent, delay time.Duration) (ForgeEvent, error) {
	if err := validateForgeEvent(event); err != nil {
		return ForgeEvent{}, err
	}
	if delay < 0 {
		return ForgeEvent{}, errors.New("forge event delay must not be negative")
	}
	now := stamp(time.Now().UTC())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("begin forge event insert: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `
		INSERT INTO forge_events
			(id, provider, kind, owner, repository, pull_request_number, commit_sha,
			 branch, comment_id, comment_kind, title, body, author, url, status,
			 attempts, last_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)
		ON CONFLICT(id) DO NOTHING`,
		event.ID, event.Provider, event.Kind, event.Owner, event.Repository,
		event.PullRequestNumber, event.CommitSHA, event.Branch, event.CommentID,
		event.CommentKind, event.Title, event.Body, event.Author, event.URL, now, now,
	)
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("insert forge event %q: %w", event.ID, err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("confirm forge event %q insert: %w", event.ID, err)
	}
	if inserted == 1 && event.Kind == "review_comment" && delay > 0 {
		deadline := stamp(time.Now().UTC().Add(delay))
		if _, err := tx.ExecContext(ctx, `
			UPDATE forge_events SET next_attempt_at = ?, updated_at = ?
			WHERE kind = 'review_comment' AND status = 'pending'
			  AND task_id IS NULL AND attempt_id IS NULL
			  AND provider = ? COLLATE NOCASE AND owner = ? COLLATE NOCASE
			  AND repository = ? COLLATE NOCASE AND pull_request_number = ?
			  AND commit_sha = ? COLLATE NOCASE
			  AND (? <> '' OR id = ?)`,
			deadline, now, event.Provider, event.Owner, event.Repository,
			event.PullRequestNumber, event.CommitSHA, event.CommitSHA, event.ID); err != nil {
			return ForgeEvent{}, fmt.Errorf("schedule forge review batch %q: %w", event.ID, err)
		}
	}
	stored, err := scanForgeEvent(tx.QueryRowContext(ctx, forgeEventSelect+" WHERE id = ?", event.ID))
	if err != nil {
		return ForgeEvent{}, fmt.Errorf("read forge event %q: %w", event.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return ForgeEvent{}, fmt.Errorf("commit forge event %q: %w", event.ID, err)
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
	if event.PullRequestNumber <= 0 {
		return errors.New("forge review event pull request and comment IDs must be positive")
	}
	if event.Provider == string(forge.ProviderGitHub) {
		if event.CommentID <= 0 {
			return errors.New("forge review event pull request and comment IDs must be positive")
		}
		switch event.CommentKind {
		case "issue_comment", "review_comment", "review":
			return nil
		default:
			return fmt.Errorf("forge GitHub comment kind %q is not supported", event.CommentKind)
		}
	}
	if event.CommentID > 0 && event.CommentKind == "comment" || event.CommentID == 0 && event.CommentKind == "changes_request" {
		return nil
	}
	return fmt.Errorf("forge Bitbucket comment identity %d/%q is not supported", event.CommentID, event.CommentKind)
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

// ListForgeEventsByAttempt returns all associated events in acceptance order.
func (s *Store) ListForgeEventsByAttempt(ctx context.Context, attemptID string) (_ []ForgeEvent, resultErr error) {
	if strings.TrimSpace(attemptID) == "" {
		return nil, errors.New("forge event attempt ID is empty")
	}
	rows, err := s.db.QueryContext(ctx, forgeEventSelect+" WHERE attempt_id = ? ORDER BY created_at, rowid", attemptID)
	if err != nil {
		return nil, fmt.Errorf("list forge events for attempt %q: %w", attemptID, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var events []ForgeEvent
	for rows.Next() {
		event, err := scanForgeEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan forge event for attempt %q: %w", attemptID, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list forge events for attempt %q: %w", attemptID, err)
	}
	return events, nil
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

// ListDueForgeEventBatch expands a due review seed to the oldest due pending
// siblings with the same provider coordinates, up to the per-follow-up safety
// ceiling. The global incomplete-event window only selects work.
func (s *Store) ListDueForgeEventBatch(ctx context.Context, seed ForgeEvent) (_ []ForgeEvent, resultErr error) {
	if seed.Kind != "review_comment" {
		return nil, errors.New("forge event batch seed is not a review comment")
	}
	rows, err := s.db.QueryContext(ctx, forgeEventSelect+`
		WHERE kind = 'review_comment' AND status = 'pending'
		  AND task_id IS NULL AND attempt_id IS NULL
		  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
		  AND provider = ? COLLATE NOCASE AND owner = ? COLLATE NOCASE
		  AND repository = ? COLLATE NOCASE AND pull_request_number = ?
		  AND commit_sha = ? COLLATE NOCASE
		  AND (? <> '' OR id = ?)
		ORDER BY created_at, rowid LIMIT ?`,
		stamp(time.Now().UTC()), seed.Provider, seed.Owner, seed.Repository,
		seed.PullRequestNumber, seed.CommitSHA, seed.CommitSHA, seed.ID, forgeEventBatchSize)
	if err != nil {
		return nil, fmt.Errorf("list due forge event batch for %q: %w", seed.ID, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var events []ForgeEvent
	for rows.Next() {
		event, err := scanForgeEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan due forge event batch for %q: %w", seed.ID, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list due forge event batch for %q: %w", seed.ID, err)
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

// RecordForgeEventReplies replaces the durable drafts for running review
// events on an attempt. Attempts without forge events are a safe no-op.
func (s *Store) RecordForgeEventReplies(ctx context.Context, attemptID string, replies map[int]string) error {
	if strings.TrimSpace(attemptID) == "" {
		return errors.New("forge event attempt ID is empty")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin forge event replies: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
		UPDATE forge_events SET reply_draft = '', updated_at = ?
		WHERE attempt_id = ? AND status = 'running' AND kind = 'review_comment'`,
		stamp(time.Now().UTC()), attemptID); err != nil {
		return fmt.Errorf("clear forge event replies for attempt %q: %w", attemptID, err)
	}
	for commentID, draft := range replies {
		if commentID <= 0 || strings.TrimSpace(draft) == "" || len(draft) > 2<<10 {
			return fmt.Errorf("invalid forge event reply for comment %d", commentID)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE forge_events SET reply_draft = ?, updated_at = ?
			WHERE attempt_id = ? AND status = 'running' AND kind = 'review_comment' AND comment_id = ?
			  AND (SELECT COUNT(*) FROM forge_events
			       WHERE attempt_id = ? AND kind = 'review_comment' AND comment_id = ?) = 1`,
			draft, stamp(time.Now().UTC()), attemptID, commentID, attemptID, commentID); err != nil {
			return fmt.Errorf("record forge event reply for comment %d: %w", commentID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit forge event replies for attempt %q: %w", attemptID, err)
	}
	return nil
}

// ForgeEventAttemptPlan reserves no durable state. It supplies the identity
// needed to build immutable worker resources before the start transaction.
type ForgeEventAttemptPlan struct {
	Attempt           Attempt
	PreviousAttemptID string
	EventIDs          []string
}

// PlanForgeEventAttempt plans the next task-local identity without changing
// the event or task. The start transaction revalidates every observed value.
func (s *Store) PlanForgeEventAttempt(ctx context.Context, eventIDs []string, taskID, prompt string) (ForgeEventAttemptPlan, error) {
	if err := validateForgeEventIDs(eventIDs); err != nil {
		return ForgeEventAttemptPlan{}, err
	}
	if strings.TrimSpace(taskID) == "" {
		return ForgeEventAttemptPlan{}, errors.New("forge event and task IDs are required")
	}
	if strings.TrimSpace(prompt) == "" {
		return ForgeEventAttemptPlan{}, errors.New("forge follow-up prompt is empty")
	}
	events, err := readForgeEventsByIDs(ctx, s.db, eventIDs)
	if err != nil {
		return ForgeEventAttemptPlan{}, err
	}
	runningAttemptID, err := validateForgeEventBatch(events, taskID, time.Now().UTC())
	if err != nil {
		return ForgeEventAttemptPlan{}, err
	}
	if runningAttemptID != "" {
		attempt, err := s.GetAttempt(ctx, taskID, runningAttemptID)
		if err != nil {
			return ForgeEventAttemptPlan{}, err
		}
		if err := requireCompleteAttemptSnapshot(attempt); err != nil {
			return ForgeEventAttemptPlan{}, err
		}
		return ForgeEventAttemptPlan{Attempt: attempt, EventIDs: append([]string(nil), eventIDs...)}, nil
	}
	var currentAttemptID, headBranch string
	var number int
	err = s.db.QueryRowContext(ctx, `
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
		EventIDs:          append([]string(nil), eventIDs...),
		Attempt: Attempt{
			ID: attemptID, TaskID: taskID, Number: number, Immutable: true,
			State: task.QUEUED, Prompt: prompt, BaseBranch: headBranch, TaskBranch: headBranch,
		},
	}, nil
}

// StartForgeEventAttempt atomically starts one fully snapshotted immutable
// follow-up attempt. Replaying a running event returns its original snapshot.
func (s *Store) StartForgeEventAttempt(ctx context.Context, plan ForgeEventAttemptPlan) (Attempt, bool, error) {
	eventIDs := plan.EventIDs
	if err := validateForgeEventIDs(eventIDs); err != nil {
		return Attempt{}, false, err
	}
	if strings.TrimSpace(plan.Attempt.TaskID) == "" {
		return Attempt{}, false, errors.New("forge event and task IDs are required")
	}
	taskID := plan.Attempt.TaskID

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("begin forge event attempt: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	events, err := readForgeEventsByIDs(ctx, tx, eventIDs)
	if err != nil {
		return Attempt{}, false, err
	}
	runningAttemptID, err := validateForgeEventBatch(events, taskID, time.Now().UTC())
	if err != nil {
		return Attempt{}, false, err
	}
	if runningAttemptID != "" {
		attempt, err := readForgeEventAttempt(ctx, tx, runningAttemptID, taskID)
		if err != nil {
			return Attempt{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return Attempt{}, false, fmt.Errorf("commit forge event replay: %w", err)
		}
		return attempt, false, nil
	}
	if err := validateForgeFollowUpPlan(plan); err != nil {
		return Attempt{}, false, err
	}

	current, err := readForgeFollowUpTask(ctx, tx, taskID)
	if err != nil {
		return Attempt{}, false, err
	}
	if err := validateForgeFollowUpTask(ctx, tx, taskID, current, plan.PreviousAttemptID, plan.Attempt.BaseBranch, plan.Attempt.TaskBranch); err != nil {
		return Attempt{}, false, err
	}
	attemptNumber, err := validateForgeFollowUpAttemptNumber(ctx, tx, taskID, plan.Attempt.Number)
	if err != nil {
		return Attempt{}, false, err
	}
	now := time.Now().UTC()
	if err := writeForgeFollowUpAttempt(ctx, tx, eventIDs, plan.Attempt, current, attemptNumber, now); err != nil {
		return Attempt{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Attempt{}, false, fmt.Errorf("commit forge follow-up attempt: %w", err)
	}
	plan.Attempt.CreatedAt = now
	return plan.Attempt, true, nil
}

func readForgeEventAttempt(ctx context.Context, tx *sql.Tx, attemptID, taskID string) (Attempt, error) {
	attempt, err := scanAttempt(tx.QueryRowContext(ctx, `
		SELECT id, task_id, number, immutable, state, prompt, base_branch, task_branch,
		       logs_exhausted, validation_state, manifest_json, resource_snapshot,
		       config_digest, created_at
		FROM task_attempts WHERE id = ? AND task_id = ?`, attemptID, taskID))
	if err != nil {
		return Attempt{}, fmt.Errorf("read forge event attempt %q: %w", attemptID, err)
	}
	if err := requireCompleteAttemptSnapshot(attempt); err != nil {
		return Attempt{}, err
	}
	return attempt, nil
}

func validateForgeFollowUpPlan(plan ForgeEventAttemptPlan) error {
	if strings.TrimSpace(plan.Attempt.ID) == "" || strings.TrimSpace(plan.PreviousAttemptID) == "" || plan.Attempt.Number <= 0 ||
		!plan.Attempt.Immutable || plan.Attempt.State != task.QUEUED || strings.TrimSpace(plan.Attempt.Prompt) == "" {
		return errors.New("forge follow-up plan is incomplete")
	}
	return requireCompleteAttemptSnapshot(plan.Attempt)
}

type forgeFollowUpTask struct {
	state                 string
	attemptID             string
	logsExhausted         int
	cancellationRequested int
	pullRequest           PullRequest
}

func readForgeFollowUpTask(ctx context.Context, tx *sql.Tx, taskID string) (forgeFollowUpTask, error) {
	var current forgeFollowUpTask
	var number sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT tasks.state, tasks.current_attempt_id, tasks.cancellation_requested,
		       task_attempts.logs_exhausted, pull_requests.attempt_id,
		       pull_requests.state, pull_requests.number, pull_requests.url,
		       pull_requests.title, pull_requests.head_branch, pull_requests.base_branch,
		       pull_requests.error
		FROM tasks
		JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id
		JOIN pull_requests ON pull_requests.attempt_id = task_attempts.id
		WHERE tasks.id = ?`, taskID).Scan(
		&current.state, &current.attemptID, &current.cancellationRequested, &current.logsExhausted,
		&current.pullRequest.AttemptID, &current.pullRequest.State, &number, &current.pullRequest.URL,
		&current.pullRequest.Title, &current.pullRequest.HeadBranch, &current.pullRequest.BaseBranch, &current.pullRequest.Error,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return forgeFollowUpTask{}, fmt.Errorf("%w: task %s with current pull request", ErrNotFound, taskID)
	}
	if err != nil {
		return forgeFollowUpTask{}, fmt.Errorf("read task %q for forge event: %w", taskID, err)
	}
	if number.Valid {
		current.pullRequest.Number = int(number.Int64)
	}
	return current, nil
}

func validateForgeFollowUpTask(ctx context.Context, tx *sql.Tx, taskID string, current forgeFollowUpTask, previousAttemptID, baseBranch, taskBranch string) error {
	pullRequest := current.pullRequest
	if pullRequest.State != "open" || pullRequest.Number <= 0 || strings.TrimSpace(pullRequest.URL) == "" || strings.TrimSpace(pullRequest.HeadBranch) == "" {
		return fmt.Errorf("%w: task %q current attempt has no open pull request", ErrConflict, taskID)
	}
	if current.attemptID != previousAttemptID || baseBranch != pullRequest.HeadBranch || taskBranch != pullRequest.HeadBranch {
		return fmt.Errorf("%w: task %q current pull request changed while planning forge follow-up", ErrConflict, taskID)
	}
	if err := (task.Machine{}).ForgeFollowUp(task.State(current.state), task.QUEUED); err != nil {
		return fmt.Errorf("%w: task %q: %w", ErrConflict, taskID, err)
	}
	if current.logsExhausted != 1 {
		return fmt.Errorf("%w: current attempt %q logs are not exhausted", ErrConflict, current.attemptID)
	}
	if current.cancellationRequested == 1 {
		return fmt.Errorf("%w: task %q has cancellation requested", ErrConflict, taskID)
	}
	var active int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM forge_events WHERE task_id = ? AND status = 'running'`, taskID).Scan(&active); err != nil {
		return fmt.Errorf("check active forge follow-up: %w", err)
	}
	if active != 0 {
		return fmt.Errorf("%w: task %q already has an active forge follow-up", ErrConflict, taskID)
	}
	return nil
}

func validateForgeFollowUpAttemptNumber(ctx context.Context, tx *sql.Tx, taskID string, planned int) (int, error) {
	var attemptNumber int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = ?`, taskID).Scan(&attemptNumber); err != nil {
		return 0, fmt.Errorf("read forge follow-up attempt number: %w", err)
	}
	if attemptNumber != planned {
		return 0, fmt.Errorf("%w: planned forge follow-up attempt %d is not next attempt %d", ErrConflict, planned, attemptNumber)
	}
	return attemptNumber, nil
}

func writeForgeFollowUpAttempt(ctx context.Context, tx *sql.Tx, eventIDs []string, attempt Attempt, current forgeFollowUpTask, attemptNumber int, now time.Time) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts
			(id, task_id, number, immutable, state, prompt, base_branch, task_branch,
			 manifest_json, resource_snapshot, config_digest, created_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)`,
		attempt.ID, attempt.TaskID, attemptNumber, task.QUEUED, attempt.Prompt, current.pullRequest.HeadBranch, current.pullRequest.HeadBranch,
		attempt.ManifestJSON, attempt.ResourceSnapshot, attempt.ConfigDigest, stamp(now)); err != nil {
		return fmt.Errorf("insert forge follow-up attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO pull_requests
			(attempt_id, state, number, url, title, head_branch, base_branch, error)
		VALUES (?, 'open', ?, ?, ?, ?, ?, ?)`,
		attempt.ID, current.pullRequest.Number, current.pullRequest.URL, current.pullRequest.Title, current.pullRequest.HeadBranch,
		current.pullRequest.BaseBranch, current.pullRequest.Error); err != nil {
		return fmt.Errorf("copy forge follow-up pull request: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE tasks SET state = ?, current_attempt_id = ?, updated_at = ?
		WHERE id = ? AND current_attempt_id = ? AND state = ?`,
		task.QUEUED, attempt.ID, stamp(now), attempt.TaskID, current.attemptID, current.state)
	if err != nil {
		return fmt.Errorf("select forge follow-up attempt: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: task %q changed while starting forge follow-up", ErrConflict, attempt.TaskID)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(eventIDs)), ",")
	args := make([]any, 0, len(eventIDs)+3)
	args = append(args, attempt.TaskID, attempt.ID, stamp(now))
	for _, id := range eventIDs {
		args = append(args, id)
	}
	result, err = tx.ExecContext(ctx, `
		UPDATE forge_events
		SET task_id = ?, attempt_id = ?, status = 'running', attempts = attempts + 1,
		    last_error = '', next_attempt_at = NULL, updated_at = ?
		WHERE id IN (`+placeholders+`) AND status = 'pending' AND task_id IS NULL AND attempt_id IS NULL`, args...)
	if err != nil {
		return fmt.Errorf("associate forge event batch: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != int64(len(eventIDs)) {
		return fmt.Errorf("%w: forge event batch changed while starting follow-up", ErrConflict)
	}
	transitionID, err := newID("swe-event-")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_events
			(id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger,
			 resource_identity, metadata, error)
		VALUES (?, ?, ?, ?, ?, ?, 'forge follow-up requested', 'webhook', '{}', '{}', '')`,
		transitionID, attempt.TaskID, attempt.ID, stamp(now), current.state, task.QUEUED); err != nil {
		return fmt.Errorf("append forge follow-up transition: %w", err)
	}
	return nil
}

type forgeEventQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func validateForgeEventIDs(eventIDs []string) error {
	if len(eventIDs) == 0 {
		return errors.New("forge event IDs are required")
	}
	if len(eventIDs) > forgeEventBatchSize {
		return fmt.Errorf("forge event batch exceeds %d events", forgeEventBatchSize)
	}
	seen := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		if strings.TrimSpace(id) == "" {
			return errors.New("forge event ID is empty")
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("forge event ID %q is duplicated", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func readForgeEventsByIDs(ctx context.Context, queryer forgeEventQueryer, eventIDs []string) (_ []ForgeEvent, resultErr error) {
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(eventIDs)), ",")
	args := make([]any, len(eventIDs))
	for i, id := range eventIDs {
		args[i] = id
	}
	rows, err := queryer.QueryContext(ctx, forgeEventSelect+" WHERE id IN ("+placeholders+") ORDER BY created_at, rowid", args...)
	if err != nil {
		return nil, fmt.Errorf("read forge event batch: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var events []ForgeEvent
	for rows.Next() {
		event, err := scanForgeEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan forge event batch: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read forge event batch: %w", err)
	}
	if len(events) != len(eventIDs) {
		return nil, fmt.Errorf("%w: one or more forge events are missing", ErrNotFound)
	}
	return events, nil
}

func validateForgeEventBatch(events []ForgeEvent, taskID string, now time.Time) (string, error) {
	first := events[0]
	if first.Kind != "review_comment" && len(events) != 1 {
		return "", fmt.Errorf("%w: non-review forge events cannot be batched", ErrConflict)
	}
	for _, event := range events[1:] {
		if event.Kind != "review_comment" || !sameForgeEventBatch(first, event) {
			return "", fmt.Errorf("%w: forge review events do not share batch coordinates", ErrConflict)
		}
	}
	if first.Status == ForgeEventRunning {
		attemptID := first.AttemptID
		for _, event := range events {
			if event.Status != ForgeEventRunning || event.TaskID != taskID || event.AttemptID == "" || event.AttemptID != attemptID {
				return "", fmt.Errorf("%w: forge event batch is running for another task or attempt", ErrConflict)
			}
		}
		return attemptID, nil
	}
	for _, event := range events {
		if event.Status != ForgeEventPending || event.TaskID != "" || event.AttemptID != "" {
			return "", fmt.Errorf("%w: forge event %q is %q or already associated", ErrConflict, event.ID, event.Status)
		}
		if event.NextAttemptAt != nil && event.NextAttemptAt.After(now) {
			return "", fmt.Errorf("%w: forge event %q is deferred until %s", ErrForgeEventNotDue, event.ID, event.NextAttemptAt.Format(time.RFC3339Nano))
		}
	}
	return "", nil
}

func sameForgeEventBatch(first, second ForgeEvent) bool {
	return strings.EqualFold(first.Provider, second.Provider) &&
		strings.EqualFold(first.Owner, second.Owner) &&
		strings.EqualFold(first.Repository, second.Repository) &&
		first.PullRequestNumber == second.PullRequestNumber &&
		first.CommitSHA != "" && second.CommitSHA != "" &&
		strings.EqualFold(first.CommitSHA, second.CommitSHA)
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
		&taskID, &attemptID, &event.ReplyDraft, &event.Status, &event.Attempts, &event.LastError,
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
