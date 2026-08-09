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
	// forgeEventBatchSize bounds each review follow-up.
	forgeEventBatchSize     = 32
	forgeEventErrorMaxBytes = 4 << 10
	forgeEventYieldDelay    = 5 * time.Second
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

// ListIncompleteForgeEvents returns due pending and running events in
// eligibility order, with acceptance order as the stable tie-breaker.
func (s *Store) ListIncompleteForgeEvents(ctx context.Context) (_ []ForgeEvent, resultErr error) {
	rows, err := s.db.QueryContext(ctx, forgeEventSelect+" WHERE status IN ('pending', 'running') AND (next_attempt_at IS NULL OR next_attempt_at <= ?) ORDER BY COALESCE(next_attempt_at, created_at), created_at, rowid LIMIT ?", stamp(time.Now().UTC()), forgeEventBatchSize)
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

// DeferForgeEvent yields incomplete work without changing its outcome,
// ownership, failure count, or diagnostic error.
func (s *Store) DeferForgeEvent(ctx context.Context, id, expectedStatus string) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("forge event ID is empty")
	}
	if expectedStatus != ForgeEventPending && expectedStatus != ForgeEventRunning {
		return fmt.Errorf("forge event expected status %q cannot be deferred", expectedStatus)
	}
	now := time.Now().UTC()
	deadline := stamp(now.Add(forgeEventYieldDelay))
	result, err := s.db.ExecContext(ctx, `
		UPDATE forge_events
		SET next_attempt_at = CASE
			WHEN next_attempt_at IS NULL OR next_attempt_at < ? THEN ?
			ELSE next_attempt_at
		END, updated_at = ?
		WHERE id = ? AND status = ?`, deadline, deadline, stamp(now), id, expectedStatus)
	if err != nil {
		return fmt.Errorf("defer forge event %q: %w", id, err)
	}
	if _, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm forge event %q deferral: %w", id, err)
	}
	return nil
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

// RecordForgeEventError records a failed processing attempt without completing
// the event.
func (s *Store) RecordForgeEventError(ctx context.Context, id string, cause error) error {
	return s.RecordForgeEventErrorAfter(ctx, id, cause, 0)
}

// RecordForgeEventErrorAfter defers transient work with bounded exponential
// pacing, honoring a longer provider-supplied delay when available.
func (s *Store) RecordForgeEventErrorAfter(ctx context.Context, id string, cause error, retryAfter time.Duration) error {
	retryAfter = max(0, min(retryAfter, 24*time.Hour))
	if strings.TrimSpace(id) == "" {
		return errors.New("forge event ID is empty")
	}
	message := boundedForgeEventError(cause)
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

// RecordForgeEventBatchErrorAfter atomically defers an exact selected batch.
// The batch must either still be due, pending, and unassociated or be the
// complete running association for one task attempt. A durable exact replay is
// a no-op.
func (s *Store) RecordForgeEventBatchErrorAfter(ctx context.Context, eventIDs []string, cause error, retryAfter time.Duration) error {
	if err := validateForgeEventIDs(eventIDs); err != nil {
		return err
	}
	retryAfter = max(0, min(retryAfter, 24*time.Hour))
	message := boundedForgeEventError(cause)
	return s.immediateTransaction(ctx, "forge event batch deferral", func(conn *sql.Conn) error {
		events, err := readForgeEventsByIDs(ctx, conn, eventIDs)
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		if replay, err := forgeEventBatchDeferralReplay(ctx, conn, events, message, retryAfter, now); replay || err != nil {
			return err
		}
		outcome, err := validateForgeEventOutcomeBatch(ctx, conn, events, now)
		if err != nil {
			return err
		}
		delay := retryAfter
		maxAttempts := 0
		for _, event := range events {
			delay = max(delay, forgeEventRetryDelay(event.Attempts))
			maxAttempts = max(maxAttempts, event.Attempts)
		}
		updatedAt, nextAttemptAt := stamp(now), stamp(now.Add(delay))
		args := make([]any, 0, forgeEventBatchSize+4)
		args = append(args, message, updatedAt, nextAttemptAt)
		for _, id := range eventIDs {
			args = append(args, id)
		}
		for range forgeEventBatchSize - len(eventIDs) {
			args = append(args, nil)
		}
		args = append(args, updatedAt)
		query := `
			UPDATE forge_events
			SET attempts = attempts + 1, last_error = ?, updated_at = ?, next_attempt_at = ?
			WHERE id IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) AND status = 'pending'
			  AND task_id IS NULL AND attempt_id IS NULL
			  AND (next_attempt_at IS NULL OR next_attempt_at <= ?)`
		if outcome.status == ForgeEventRunning {
			query = `
				UPDATE forge_events
				SET attempts = ?, last_error = ?, updated_at = ?, next_attempt_at = ?
				WHERE id IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) AND status = 'running'
				  AND task_id = ? AND attempt_id = ?`
			args = append([]any{maxAttempts + 1}, args[:len(args)-1]...)
			args = append(args, outcome.taskID, outcome.attemptID)
		}
		result, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("record forge event batch error: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("confirm forge event batch error: %w", err)
		} else if affected != int64(len(eventIDs)) {
			return fmt.Errorf("%w: forge event batch changed while recording error", ErrConflict)
		}
		return nil
	})
}

func forgeEventBatchDeferralReplay(ctx context.Context, conn *sql.Conn, events []ForgeEvent, message string, retryAfter time.Duration, now time.Time) (bool, error) {
	first := events[0]
	if first.Status != ForgeEventPending && first.Status != ForgeEventRunning || first.Attempts == 0 || first.LastError != message ||
		first.NextAttemptAt == nil || !first.NextAttemptAt.After(now) {
		return false, nil
	}
	associated := first.Status == ForgeEventRunning && first.TaskID != "" && first.AttemptID != ""
	if first.Status == ForgeEventPending && (first.TaskID != "" || first.AttemptID != "") || first.Status == ForgeEventRunning && !associated {
		return false, fmt.Errorf("%w: forge event batch deferral replay has an invalid association", ErrConflict)
	}
	for _, event := range events[1:] {
		if event.Status != first.Status || event.TaskID != first.TaskID || event.AttemptID != first.AttemptID || event.Attempts == 0 || event.LastError != message ||
			event.NextAttemptAt == nil || !event.NextAttemptAt.Equal(*first.NextAttemptAt) || !event.UpdatedAt.Equal(first.UpdatedAt) {
			if associated {
				return false, nil
			}
			return false, fmt.Errorf("%w: forge event batch deferral replay has different diagnostics", ErrConflict)
		}
	}
	delay := retryAfter
	for _, event := range events {
		delay = max(delay, forgeEventRetryDelay(event.Attempts-1))
	}
	if !first.NextAttemptAt.Equal(first.UpdatedAt.Add(delay)) {
		if associated {
			return false, nil
		}
		return false, fmt.Errorf("%w: forge event batch deferral replay has a different retry delay", ErrConflict)
	}
	count, err := forgeEventOutcomeReplayCount(ctx, conn, first, message)
	if err != nil {
		return false, fmt.Errorf("confirm forge event batch deferral replay: %w", err)
	}
	if count != len(events) {
		return false, fmt.Errorf("%w: forge event batch deferral replay has different members", ErrConflict)
	}
	return true, nil
}

func forgeEventRetryDelay(attempts int) time.Duration {
	const base, maximum = 5 * time.Second, 5 * time.Minute
	delay := base
	for range min(attempts, 6) {
		delay *= 2
	}
	return min(delay, maximum)
}

// FailForgeEvent atomically fails incomplete work and requests cancellation
// only when a running batch still owns the task's current non-terminal attempt.
// Missing, stale, and terminal task associations do not prevent event failure.
func (s *Store) FailForgeEvent(ctx context.Context, id string, cause error) error {
	if strings.TrimSpace(id) == "" {
		return errors.New("forge event ID is empty")
	}
	if cause == nil || strings.TrimSpace(cause.Error()) == "" {
		return errors.New("forge event permanent error is empty")
	}
	message := boundedForgeEventError(cause)
	return s.immediateTransaction(ctx, fmt.Sprintf("forge event %q failure", id), func(conn *sql.Conn) error {
		var status, lastError string
		var taskID, attemptID, failedAt sql.NullString
		if err := conn.QueryRowContext(ctx, `SELECT status, task_id, attempt_id, last_error, failed_at FROM forge_events WHERE id = ?`, id).
			Scan(&status, &taskID, &attemptID, &lastError, &failedAt); errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: incomplete forge event %s", ErrNotFound, id)
		} else if err != nil {
			return fmt.Errorf("read forge event %q failure target: %w", id, err)
		}
		associated := taskID.Valid && taskID.String != "" && attemptID.Valid && attemptID.String != ""
		if status == ForgeEventFailed && associated && lastError == message && failedAt.Valid {
			return nil
		}
		if status != ForgeEventPending && status != ForgeEventRunning {
			if status == ForgeEventFailed && associated {
				return fmt.Errorf("%w: forge event %s already failed with different diagnostics", ErrConflict, id)
			}
			return fmt.Errorf("%w: incomplete forge event %s", ErrNotFound, id)
		}

		now := stamp(time.Now().UTC())
		var result sql.Result
		var err error
		if status == ForgeEventRunning && associated {
			if _, err := conn.ExecContext(ctx, `
				UPDATE tasks SET cancellation_requested = 1, updated_at = ?
				WHERE id = ? AND current_attempt_id = ? AND state NOT IN (?, ?, ?)`,
				now, taskID.String, attemptID.String, task.READY, task.FAILED, task.CANCELLED); err != nil {
				return fmt.Errorf("request forge event %q cancellation: %w", id, err)
			}
			result, err = conn.ExecContext(ctx, `
				UPDATE forge_events
				SET status = 'failed', attempts = attempts + 1, last_error = ?, next_attempt_at = NULL, failed_at = ?, updated_at = ?
				WHERE task_id = ? AND attempt_id = ? AND status = 'running'`, message, now, now, taskID.String, attemptID.String)
		} else {
			result, err = conn.ExecContext(ctx, `
				UPDATE forge_events
				SET status = 'failed', attempts = attempts + 1, last_error = ?, next_attempt_at = NULL, failed_at = ?, updated_at = ?
				WHERE id = ? AND status IN ('pending', 'running')`, message, now, now, id)
		}
		if err != nil {
			return fmt.Errorf("fail forge event %q: %w", id, err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("confirm forge event %q failure: %w", id, err)
		} else if affected < 1 {
			return fmt.Errorf("%w: incomplete forge event %s", ErrNotFound, id)
		}
		return nil
	})
}

// FailForgeEventBatch atomically fails an exact selected batch. Pending work is
// failed without cancellation. A complete running association is failed and
// cancellation is requested only while it remains the current nonterminal
// attempt. A durable exact replay is a no-op.
func (s *Store) FailForgeEventBatch(ctx context.Context, eventIDs []string, cause error) error {
	if err := validateForgeEventIDs(eventIDs); err != nil {
		return err
	}
	if cause == nil || strings.TrimSpace(cause.Error()) == "" {
		return errors.New("forge event permanent error is empty")
	}
	message := boundedForgeEventError(cause)
	return s.immediateTransaction(ctx, "forge event batch failure", func(conn *sql.Conn) error {
		events, err := readForgeEventsByIDs(ctx, conn, eventIDs)
		if err != nil {
			return err
		}
		if replay, err := forgeEventBatchFailureReplay(ctx, conn, events, message); replay || err != nil {
			return err
		}
		now := time.Now().UTC()
		outcome, err := validateForgeEventOutcomeBatch(ctx, conn, events, now)
		if err != nil {
			return err
		}
		timestamp := stamp(now)
		if outcome.status == ForgeEventRunning {
			if _, err := conn.ExecContext(ctx, `
				UPDATE tasks SET cancellation_requested = 1, updated_at = ?
				WHERE id = ? AND current_attempt_id = ? AND state NOT IN (?, ?, ?)`,
				timestamp, outcome.taskID, outcome.attemptID, task.READY, task.FAILED, task.CANCELLED); err != nil {
				return fmt.Errorf("request forge event batch cancellation: %w", err)
			}
		}
		args := make([]any, 0, forgeEventBatchSize+3)
		args = append(args, message, timestamp, timestamp)
		for _, id := range eventIDs {
			args = append(args, id)
		}
		for range forgeEventBatchSize - len(eventIDs) {
			args = append(args, nil)
		}
		query := `
			UPDATE forge_events
			SET status = 'failed', attempts = attempts + 1, last_error = ?,
			    next_attempt_at = NULL, failed_at = ?, updated_at = ?
			WHERE id IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) AND status = 'pending'
			  AND task_id IS NULL AND attempt_id IS NULL`
		if outcome.status == ForgeEventRunning {
			query = `
				UPDATE forge_events
				SET status = 'failed', attempts = attempts + 1, last_error = ?,
				    next_attempt_at = NULL, failed_at = ?, updated_at = ?
				WHERE id IN (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) AND status = 'running'
				  AND task_id = ? AND attempt_id = ?`
			args = append(args, outcome.taskID, outcome.attemptID)
		}
		result, err := conn.ExecContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("fail forge event batch: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("confirm forge event batch failure: %w", err)
		} else if affected != int64(len(eventIDs)) {
			return fmt.Errorf("%w: forge event batch changed while failing", ErrConflict)
		}
		return nil
	})
}

func forgeEventBatchFailureReplay(ctx context.Context, conn *sql.Conn, events []ForgeEvent, message string) (bool, error) {
	first := events[0]
	if first.Status != ForgeEventFailed || first.FailedAt == nil || first.LastError != message || first.NextAttemptAt != nil {
		return false, nil
	}
	associated := first.TaskID != "" && first.AttemptID != ""
	if !associated && (first.TaskID != "" || first.AttemptID != "") {
		return false, fmt.Errorf("%w: forge event batch failure replay has an invalid association", ErrConflict)
	}
	for _, event := range events[1:] {
		if event.Status != ForgeEventFailed || event.TaskID != first.TaskID || event.AttemptID != first.AttemptID || event.FailedAt == nil || event.LastError != message ||
			event.NextAttemptAt != nil || !event.FailedAt.Equal(*first.FailedAt) || !event.UpdatedAt.Equal(first.UpdatedAt) {
			return false, fmt.Errorf("%w: forge event batch failure replay has different diagnostics", ErrConflict)
		}
	}
	count, err := forgeEventOutcomeReplayCount(ctx, conn, first, message)
	if err != nil {
		return false, fmt.Errorf("confirm forge event batch failure replay: %w", err)
	}
	if count != len(events) {
		return false, fmt.Errorf("%w: forge event batch failure replay has different members", ErrConflict)
	}
	return true, nil
}

type forgeEventBatchOutcome struct {
	status    string
	taskID    string
	attemptID string
}

func validateForgeEventOutcomeBatch(ctx context.Context, conn *sql.Conn, events []ForgeEvent, now time.Time) (forgeEventBatchOutcome, error) {
	first := events[0]
	if first.Kind != "review_comment" && len(events) != 1 {
		return forgeEventBatchOutcome{}, fmt.Errorf("%w: non-review forge events cannot be batched", ErrConflict)
	}
	for _, event := range events[1:] {
		if event.Kind != "review_comment" || !sameForgeEventBatch(first, event) {
			return forgeEventBatchOutcome{}, fmt.Errorf("%w: forge review events do not share batch coordinates", ErrConflict)
		}
	}

	outcome := forgeEventBatchOutcome{status: first.Status, taskID: first.TaskID, attemptID: first.AttemptID}
	for _, event := range events {
		if event.Status != outcome.status || event.TaskID != outcome.taskID || event.AttemptID != outcome.attemptID {
			return forgeEventBatchOutcome{}, fmt.Errorf("%w: forge event batch has mixed outcomes or associations", ErrConflict)
		}
		if outcome.status == ForgeEventPending && event.NextAttemptAt != nil && event.NextAttemptAt.After(now) {
			return forgeEventBatchOutcome{}, fmt.Errorf("%w: forge event %q is deferred until %s", ErrConflict, event.ID, event.NextAttemptAt.Format(time.RFC3339Nano))
		}
	}
	if outcome.status == ForgeEventPending {
		if outcome.taskID != "" || outcome.attemptID != "" {
			return forgeEventBatchOutcome{}, fmt.Errorf("%w: pending forge event batch is already associated", ErrConflict)
		}
		return outcome, nil
	}
	if outcome.status != ForgeEventRunning || outcome.taskID == "" || outcome.attemptID == "" {
		return forgeEventBatchOutcome{}, fmt.Errorf("%w: forge event batch is not pending or completely associated running work", ErrConflict)
	}
	var associated, validAttempt int
	if err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*), EXISTS(SELECT 1 FROM task_attempts WHERE id = ? AND task_id = ?)
		FROM forge_events WHERE task_id = ? AND attempt_id = ? AND status = 'running'`,
		outcome.attemptID, outcome.taskID, outcome.taskID, outcome.attemptID).Scan(&associated, &validAttempt); err != nil {
		return forgeEventBatchOutcome{}, fmt.Errorf("confirm exact running forge event batch: %w", err)
	}
	if validAttempt != 1 {
		return forgeEventBatchOutcome{}, fmt.Errorf("%w: running forge event batch has a mismatched task and attempt", ErrConflict)
	}
	if associated != len(events) {
		return forgeEventBatchOutcome{}, fmt.Errorf("%w: running forge event batch has unselected associated siblings", ErrConflict)
	}
	return outcome, nil
}

func forgeEventOutcomeReplayCount(ctx context.Context, conn *sql.Conn, first ForgeEvent, message string) (int, error) {
	var count int
	if first.TaskID != "" && first.AttemptID != "" {
		var validAttempt int
		query := `
			SELECT COUNT(*), EXISTS(SELECT 1 FROM task_attempts WHERE id = ? AND task_id = ?)
			FROM forge_events WHERE task_id = ? AND attempt_id = ? AND status = 'running'`
		args := []any{first.AttemptID, first.TaskID, first.TaskID, first.AttemptID}
		if first.Status == ForgeEventFailed {
			query = `
				SELECT COUNT(*), EXISTS(SELECT 1 FROM task_attempts WHERE id = ? AND task_id = ?)
				FROM forge_events WHERE task_id = ? AND attempt_id = ? AND status = 'failed'
				  AND last_error = ? AND failed_at = ? AND updated_at = ? AND next_attempt_at IS NULL`
			args = append(args, message, stamp(*first.FailedAt), stamp(first.UpdatedAt))
		}
		err := conn.QueryRowContext(ctx, query, args...).Scan(&count, &validAttempt)
		if err != nil {
			return 0, fmt.Errorf("count associated forge event replay members: %w", err)
		}
		if validAttempt != 1 {
			return 0, fmt.Errorf("%w: associated forge event replay has a mismatched task and attempt", ErrConflict)
		}
		return count, nil
	}
	if first.Status == ForgeEventPending {
		err := conn.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM forge_events
			WHERE status = 'pending' AND task_id IS NULL AND attempt_id IS NULL
			  AND attempts > 0 AND last_error = ? AND updated_at = ? AND next_attempt_at = ?`,
			message, stamp(first.UpdatedAt), stamp(*first.NextAttemptAt)).Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("count pending forge event replay members: %w", err)
		}
		return count, nil
	}
	err := conn.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM forge_events
		WHERE status = 'failed' AND task_id IS NULL AND attempt_id IS NULL
		  AND last_error = ? AND failed_at = ? AND updated_at = ? AND next_attempt_at IS NULL`,
		message, stamp(*first.FailedAt), stamp(first.UpdatedAt)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count failed forge event replay members: %w", err)
	}
	return count, nil
}

func boundedForgeEventError(cause error) string {
	if cause == nil {
		return ""
	}
	message := strings.ToValidUTF8(cause.Error(), "")
	if len(message) <= forgeEventErrorMaxBytes {
		return message
	}
	return strings.ToValidUTF8(message[:forgeEventErrorMaxBytes], "")
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
	var currentAttemptID, headBranch, baseBranch string
	var number int
	err = s.db.QueryRowContext(ctx, `
		SELECT tasks.current_attempt_id, pull_requests.head_branch, pull_requests.base_branch,
		       (SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = tasks.id)
		FROM tasks
		JOIN pull_requests ON pull_requests.attempt_id = tasks.current_attempt_id
		WHERE tasks.id = ?`, taskID).Scan(&currentAttemptID, &headBranch, &baseBranch, &number)
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
			State: task.QUEUED, Prompt: prompt, BaseBranch: baseBranch, TaskBranch: headBranch,
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
	if pullRequest.State != "open" || pullRequest.Number <= 0 || strings.TrimSpace(pullRequest.HeadBranch) == "" {
		return fmt.Errorf("%w: task %q current attempt has no open pull request", ErrConflict, taskID)
	}
	if current.attemptID != previousAttemptID || baseBranch != pullRequest.BaseBranch || taskBranch != pullRequest.HeadBranch {
		return fmt.Errorf("%w: task %q current pull request changed while planning forge follow-up", ErrConflict, taskID)
	}
	if err := (task.Machine{}).ForgeFollowUp(task.State(current.state), task.QUEUED); err != nil {
		return fmt.Errorf("%w: task %q: %w", ErrConflict, taskID, err)
	}
	if current.logsExhausted != 1 {
		return fmt.Errorf("%w: current attempt %q logs are not exhausted", ErrConflict, current.attemptID)
	}
	var pendingWorkerEvents int
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM worker_log_events WHERE task_id = ? AND attempt_id = ? AND processed = 0)`, taskID, current.attemptID).Scan(&pendingWorkerEvents); err != nil {
		return fmt.Errorf("check pending worker events for forge follow-up: %w", err)
	}
	if pendingWorkerEvents == 1 {
		return fmt.Errorf("%w: current attempt %q has pending durable worker events", ErrConflict, current.attemptID)
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
		attempt.ID, attempt.TaskID, attemptNumber, task.QUEUED, attempt.Prompt, attempt.BaseBranch, attempt.TaskBranch,
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
