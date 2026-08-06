package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"

	// Register the SQLite database/sql driver used by Open.
	_ "modernc.org/sqlite"
)

var (
	ErrNotFound = errors.New("task not found")
	ErrConflict = errors.New("task state conflict")
)

// Store is the SQLite-backed task store.
type Store struct {
	db *sql.DB
}

type CreateTaskParams struct {
	Repository     string
	Prompt         string
	IdempotencyKey string
	SlackEventID   string
	SlackOrigin    protocol.SlackOrigin
}

type Task struct {
	ID                    string
	Repository            string
	Prompt                string
	State                 task.State
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CurrentAttemptID      string
	SlackEventID          string
	SlackOrigin           protocol.SlackOrigin
	CancellationRequested bool
}

type Attempt struct {
	ID               string
	TaskID           string
	Number           int
	Immutable        bool
	State            task.State
	LogsExhausted    bool
	ValidationState  string
	ManifestJSON     []byte
	ResourceSnapshot []byte
	ConfigDigest     string
	CreatedAt        time.Time
}

const (
	SlackInboxPending  = "pending"
	SlackInboxHandled  = "handled"
	SlackInboxRejected = "rejected"
)

// SlackInboxEvent is one normalized Socket Mode event durably accepted for
// processing.
type SlackInboxEvent struct {
	EventID   string
	Kind      string
	Text      string
	Origin    protocol.SlackOrigin
	Status    string
	Attempts  int
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
	HandledAt *time.Time
}

type TransitionParams struct {
	Reason     string
	Trigger    string
	Validation *ValidationTransition
}

type ValidationTransition struct {
	Name    string
	State   string
	Summary string
	Error   string
	EventID string
}

type TransitionEvent struct {
	ID               string
	TaskID           string
	AttemptID        string
	OccurredAt       time.Time
	FromState        task.State
	ToState          task.State
	Reason           string
	Trigger          string
	ResourceIdentity string
	Metadata         string
	Error            string
}

// GitResult is the durable branch produced by one attempt.
type GitResult struct {
	AttemptID string
	State     string
	Branch    string
	CommitSHA string
	Error     string
}

// PullRequest is the durable pull-request creation record for one attempt.
type PullRequest struct {
	AttemptID  string
	State      string
	Number     int
	URL        string
	Title      string
	HeadBranch string
	BaseBranch string
	Error      string
	Notified   bool
}

// ValidationRun is one durable validation result for an attempt.
type ValidationRun struct {
	ID        string
	TaskID    string
	AttemptID string
	Sequence  int
	Name      string
	State     string
	Summary   string
	Error     string
	ExitCode  int
	CreatedAt time.Time
}

type SQLitePragmas struct {
	JournalMode string
	ForeignKeys bool
	BusyTimeout int
}

const schema = `
CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    repository TEXT NOT NULL,
    prompt TEXT NOT NULL,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    current_attempt_id TEXT NOT NULL,
    slack_event_id TEXT,
    slack_workspace_id TEXT NOT NULL DEFAULT '',
    slack_channel_id TEXT NOT NULL DEFAULT '',
    slack_message_ts TEXT NOT NULL DEFAULT '',
    slack_thread_ts TEXT NOT NULL DEFAULT '',
    slack_user_id TEXT NOT NULL DEFAULT '',
    cancellation_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancellation_requested IN (0, 1))
);

CREATE TABLE IF NOT EXISTS task_attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    number INTEGER NOT NULL CHECK (number > 0),
    immutable INTEGER NOT NULL DEFAULT 1 CHECK (immutable = 1),
    state TEXT NOT NULL,
	logs_exhausted INTEGER NOT NULL DEFAULT 0 CHECK (logs_exhausted IN (0, 1)),
	validation_state TEXT NOT NULL DEFAULT '',
	manifest_json BLOB NOT NULL DEFAULT '',
	resource_snapshot BLOB NOT NULL DEFAULT '',
	config_digest TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE (task_id, number)
);

CREATE TABLE IF NOT EXISTS task_create_intents (
	idempotency_key TEXT PRIMARY KEY,
	task_id TEXT NOT NULL REFERENCES tasks(id)
);

CREATE TABLE IF NOT EXISTS retry_intents (
	task_id TEXT NOT NULL REFERENCES tasks(id),
	idempotency_key TEXT NOT NULL,
	attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
	created_at TEXT NOT NULL,
	PRIMARY KEY (task_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS task_events (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
    occurred_at TEXT NOT NULL,
    from_state TEXT NOT NULL,
    to_state TEXT NOT NULL,
    reason TEXT NOT NULL,
    trigger TEXT NOT NULL,
    resource_identity TEXT NOT NULL DEFAULT '{}',
    metadata TEXT NOT NULL DEFAULT '{}',
    error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS slack_events (
    event_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    processed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS slack_inbox (
	event_id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	text TEXT NOT NULL,
	origin_json TEXT NOT NULL,
	status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'handled')),
	attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	handled_at TEXT
);

CREATE TABLE IF NOT EXISTS slack_inbox_rejections (
	event_id TEXT PRIMARY KEY,
	kind TEXT NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 1 CHECK (attempts > 0),
	last_error TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS log_chunks (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
    sequence INTEGER NOT NULL,
    content BLOB NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (attempt_id, sequence)
);

CREATE TABLE IF NOT EXISTS attempt_log_state (
	attempt_id TEXT PRIMARY KEY REFERENCES task_attempts(id),
	total_bytes INTEGER NOT NULL DEFAULT 0 CHECK (total_bytes >= 0),
	truncated INTEGER NOT NULL DEFAULT 0 CHECK (truncated IN (0, 1))
);

CREATE TABLE IF NOT EXISTS pod_log_state (
	pod_uid TEXT PRIMARY KEY,
	task_id TEXT NOT NULL REFERENCES tasks(id),
	attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
	last_timestamp TEXT NOT NULL DEFAULT '',
	timestamp_ordinal INTEGER NOT NULL DEFAULT 0,
	untimestamped_ordinal INTEGER NOT NULL DEFAULT 0,
	exhausted INTEGER NOT NULL DEFAULT 0 CHECK (exhausted IN (0, 1)),
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_log_events (
	id TEXT PRIMARY KEY,
	pod_uid TEXT NOT NULL REFERENCES pod_log_state(pod_uid),
	task_id TEXT NOT NULL REFERENCES tasks(id),
	attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
	job_name TEXT NOT NULL,
	pod_name TEXT NOT NULL,
	content TEXT NOT NULL,
	processed INTEGER NOT NULL DEFAULT 0 CHECK (processed IN (0, 1)),
	created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kubernetes_jobs (
	attempt_id TEXT PRIMARY KEY REFERENCES task_attempts(id),
	task_id TEXT NOT NULL REFERENCES tasks(id),
	api_version TEXT NOT NULL DEFAULT 'batch/v1',
	namespace TEXT NOT NULL,
	name TEXT NOT NULL,
	uid TEXT NOT NULL,
	state TEXT NOT NULL,
	reason TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL DEFAULT '',
	started_at TEXT,
	completed_at TEXT,
	observed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kubernetes_pods (
	uid TEXT PRIMARY KEY,
	attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
	task_id TEXT NOT NULL REFERENCES tasks(id),
	api_version TEXT NOT NULL DEFAULT 'v1',
	namespace TEXT NOT NULL,
	name TEXT NOT NULL,
	state TEXT NOT NULL,
	reason TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL DEFAULT '',
	node TEXT NOT NULL DEFAULT '',
	image TEXT NOT NULL DEFAULT '',
	container_states TEXT NOT NULL DEFAULT '{}',
	started_at TEXT,
	completed_at TEXT,
	observed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS kubernetes_pods_attempt_observed ON kubernetes_pods(attempt_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS secret_cleanups (
	attempt_id TEXT PRIMARY KEY REFERENCES task_attempts(id),
	attempt_number INTEGER NOT NULL,
	task_id TEXT NOT NULL REFERENCES tasks(id),
	namespace TEXT NOT NULL,
	job_name TEXT NOT NULL,
	job_uid TEXT NOT NULL,
	secret_name TEXT NOT NULL,
	secret_uid TEXT NOT NULL DEFAULT '',
	generation INTEGER NOT NULL DEFAULT 1,
	eligible_at TEXT,
	completed_at TEXT
);

CREATE TABLE IF NOT EXISTS validation_runs (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    attempt_id TEXT NOT NULL REFERENCES task_attempts(id),
	sequence INTEGER NOT NULL,
    name TEXT NOT NULL,
    state TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
	exit_code INTEGER,
	start_event_id TEXT,
	result_event_id TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS git_results (
    attempt_id TEXT PRIMARY KEY REFERENCES task_attempts(id),
    state TEXT NOT NULL,
    branch TEXT NOT NULL DEFAULT '',
    commit_sha TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS pull_requests (
    attempt_id TEXT PRIMARY KEY REFERENCES task_attempts(id),
    state TEXT NOT NULL,
    number INTEGER,
    url TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    head_branch TEXT NOT NULL DEFAULT '',
    base_branch TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    notified_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS tasks_slack_event_id
    ON tasks(slack_event_id) WHERE slack_event_id IS NOT NULL;
`

func Open(ctx context.Context, path string) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("store context is nil")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("store path is empty")
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open SQLite store: %w", err)
	}
	// The controller is single-replica today. One connection also makes the
	// connection-local foreign_keys setting apply to every store operation.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	closeOnError := func(err error) (*Store, error) {
		_ = db.Close()
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("ping SQLite store: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		return closeOnError(fmt.Errorf("set SQLite busy timeout: %w", err))
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		return closeOnError(fmt.Errorf("enable SQLite foreign keys: %w", err))
	}
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return closeOnError(fmt.Errorf("enable SQLite WAL: %w", err))
	}
	if !strings.EqualFold(journalMode, "wal") {
		return closeOnError(fmt.Errorf("SQLite journal mode is %q, want wal", journalMode))
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		return closeOnError(fmt.Errorf("create SQLite schema: %w", err))
	}
	for _, migration := range []string{
		"ALTER TABLE tasks ADD COLUMN slack_workspace_id TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE tasks ADD COLUMN slack_channel_id TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE tasks ADD COLUMN slack_message_ts TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE tasks ADD COLUMN slack_thread_ts TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE tasks ADD COLUMN slack_user_id TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE pull_requests ADD COLUMN notified_at TEXT",
		"ALTER TABLE task_attempts ADD COLUMN logs_exhausted INTEGER NOT NULL DEFAULT 0 CHECK (logs_exhausted IN (0, 1))",
		"ALTER TABLE task_attempts ADD COLUMN validation_state TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE task_attempts ADD COLUMN manifest_json BLOB NOT NULL DEFAULT ''",
		"ALTER TABLE task_attempts ADD COLUMN resource_snapshot BLOB NOT NULL DEFAULT ''",
		"ALTER TABLE task_attempts ADD COLUMN config_digest TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE validation_runs ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE validation_runs ADD COLUMN exit_code INTEGER",
		"ALTER TABLE validation_runs ADD COLUMN start_event_id TEXT",
		"ALTER TABLE validation_runs ADD COLUMN result_event_id TEXT",
		"ALTER TABLE secret_cleanups ADD COLUMN attempt_number INTEGER NOT NULL DEFAULT 0",
		"ALTER TABLE secret_cleanups ADD COLUMN secret_uid TEXT NOT NULL DEFAULT ''",
		"ALTER TABLE secret_cleanups ADD COLUMN generation INTEGER NOT NULL DEFAULT 1",
	} {
		if _, err := db.ExecContext(ctx, migration); err != nil && !strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
			return closeOnError(fmt.Errorf("migrate SQLite schema: %w", err))
		}
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE validation_runs SET sequence = (
			SELECT COUNT(*) FROM validation_runs AS earlier
			WHERE earlier.attempt_id = validation_runs.attempt_id AND earlier.rowid <= validation_runs.rowid
		) WHERE sequence = 0`); err != nil {
		return closeOnError(fmt.Errorf("migrate validation sequences: %w", err))
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS validation_runs_attempt_sequence ON validation_runs(attempt_id, sequence)`); err != nil {
		return closeOnError(fmt.Errorf("index validation sequences: %w", err))
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS validation_runs_start_event ON validation_runs(start_event_id) WHERE start_event_id IS NOT NULL`); err != nil {
		return closeOnError(fmt.Errorf("index validation start events: %w", err))
	}
	if _, err := db.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS validation_runs_result_event ON validation_runs(result_event_id) WHERE result_event_id IS NOT NULL`); err != nil {
		return closeOnError(fmt.Errorf("index validation result events: %w", err))
	}
	if _, err := db.ExecContext(ctx, `UPDATE secret_cleanups SET attempt_number = COALESCE((SELECT number FROM task_attempts WHERE id = secret_cleanups.attempt_id), attempt_number) WHERE attempt_number = 0`); err != nil {
		return closeOnError(fmt.Errorf("migrate Secret cleanup attempt numbers: %w", err))
	}
	if _, err := db.ExecContext(ctx, `
		UPDATE task_attempts
		SET state = (SELECT tasks.state FROM tasks WHERE tasks.current_attempt_id = task_attempts.id)
		WHERE id IN (SELECT current_attempt_id FROM tasks)`); err != nil {
		return closeOnError(fmt.Errorf("repair current attempt states: %w", err))
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// PutSlackInboxEvent inserts a normalized event once and returns the durable
// version. A duplicate event ID never replaces the original payload.
func (s *Store) PutSlackInboxEvent(ctx context.Context, event SlackInboxEvent) (SlackInboxEvent, error) {
	if strings.TrimSpace(event.EventID) == "" {
		return SlackInboxEvent{}, errors.New("Slack inbox event ID is empty")
	}
	if strings.TrimSpace(event.Kind) == "" {
		return SlackInboxEvent{}, errors.New("Slack inbox event kind is empty")
	}
	origin, err := json.Marshal(event.Origin)
	if err != nil {
		return SlackInboxEvent{}, fmt.Errorf("marshal Slack inbox origin: %w", err)
	}
	now := stamp(time.Now().UTC())
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO slack_inbox
			(event_id, kind, text, origin_json, status, attempts, last_error, created_at, updated_at)
		VALUES (?, ?, ?, ?, 'pending', 0, '', ?, ?)
		ON CONFLICT(event_id) DO NOTHING`, event.EventID, event.Kind, event.Text, string(origin), now, now); err != nil {
		return SlackInboxEvent{}, fmt.Errorf("insert Slack inbox event %q: %w", event.EventID, err)
	}
	stored, err := scanSlackInboxEvent(s.db.QueryRowContext(ctx, slackInboxSelect+" WHERE event_id = ?", event.EventID))
	if err != nil {
		return SlackInboxEvent{}, fmt.Errorf("read Slack inbox event %q: %w", event.EventID, err)
	}
	return stored, nil
}

// RecordRejectedSlackInboxEvent records a malformed supported envelope
// without placing it in the pending handler queue.
func (s *Store) RecordRejectedSlackInboxEvent(ctx context.Context, eventID, kind string, cause error) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("rejected Slack inbox event ID is empty")
	}
	if strings.TrimSpace(kind) == "" {
		return errors.New("rejected Slack inbox event kind is empty")
	}
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	now := stamp(time.Now().UTC())
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO slack_inbox_rejections
			(event_id, kind, attempts, last_error, created_at, updated_at)
		VALUES (?, ?, 1, ?, ?, ?)
		ON CONFLICT(event_id) DO UPDATE SET
			attempts = attempts + 1, last_error = excluded.last_error, updated_at = excluded.updated_at`,
		eventID, kind, message, now, now)
	if err != nil {
		return fmt.Errorf("record rejected Slack inbox event %q: %w", eventID, err)
	}
	return nil
}

// ListPendingSlackInboxEvents returns unhandled events in acceptance order.
func (s *Store) ListPendingSlackInboxEvents(ctx context.Context) (_ []SlackInboxEvent, resultErr error) {
	rows, err := s.db.QueryContext(ctx, slackInboxSelect+" WHERE status = 'pending' ORDER BY created_at, rowid")
	if err != nil {
		return nil, fmt.Errorf("list pending Slack inbox events: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var events []SlackInboxEvent
	for rows.Next() {
		event, err := scanSlackInboxEvent(rows)
		if err != nil {
			return nil, fmt.Errorf("scan pending Slack inbox event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list pending Slack inbox events: %w", err)
	}
	return events, nil
}

// StartSlackInboxAttempt records that handler execution has begun.
func (s *Store) StartSlackInboxAttempt(ctx context.Context, eventID string) error {
	return s.updatePendingSlackInbox(ctx, eventID, `
		UPDATE slack_inbox SET attempts = attempts + 1, updated_at = ?
		WHERE event_id = ? AND status = 'pending'`, stamp(time.Now().UTC()), eventID)
}

// RecordSlackInboxError leaves an event pending for startup or redelivery.
func (s *Store) RecordSlackInboxError(ctx context.Context, eventID string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	return s.updatePendingSlackInbox(ctx, eventID, `
		UPDATE slack_inbox SET last_error = ?, updated_at = ?
		WHERE event_id = ? AND status = 'pending'`, message, stamp(time.Now().UTC()), eventID)
}

// MarkSlackInboxHandled completes an event only after its handler succeeds.
func (s *Store) MarkSlackInboxHandled(ctx context.Context, eventID string) error {
	now := stamp(time.Now().UTC())
	return s.updatePendingSlackInbox(ctx, eventID, `
		UPDATE slack_inbox
		SET status = 'handled', last_error = '', updated_at = ?, handled_at = ?
		WHERE event_id = ? AND status = 'pending'`, now, now, eventID)
}

func (s *Store) updatePendingSlackInbox(ctx context.Context, eventID, query string, args ...any) error {
	if strings.TrimSpace(eventID) == "" {
		return errors.New("Slack inbox event ID is empty")
	}
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("update Slack inbox event %q: %w", eventID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm Slack inbox event %q update: %w", eventID, err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: pending Slack inbox event %s", ErrNotFound, eventID)
	}
	return nil
}

func (s *Store) CreateTask(ctx context.Context, params CreateTaskParams) (Task, error) {
	created, _, err := s.CreateTaskOnce(ctx, params)
	return created, err
}

// CreateTaskOnce creates a task or returns the task already associated with a
// generic idempotency key or Slack event. The boolean reports whether this call
// inserted the task.
func (s *Store) CreateTaskOnce(ctx context.Context, params CreateTaskParams) (Task, bool, error) {
	if params.IdempotencyKey != "" && params.SlackEventID != "" {
		return Task{}, false, fmt.Errorf("%w: idempotency key and Slack event ID are mutually exclusive", ErrConflict)
	}
	if params.IdempotencyKey != "" && strings.TrimSpace(params.IdempotencyKey) == "" {
		return Task{}, false, errors.New("task create idempotency key is empty")
	}
	if utf8.RuneCountInString(params.IdempotencyKey) > 256 {
		return Task{}, false, errors.New("task create idempotency key exceeds 256 characters")
	}
	if strings.TrimSpace(params.Repository) == "" {
		return Task{}, false, errors.New("repository is empty")
	}
	if strings.TrimSpace(params.Prompt) == "" {
		return Task{}, false, errors.New("prompt is empty")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Task{}, false, fmt.Errorf("begin task creation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var existingID string
	if params.IdempotencyKey != "" {
		err := tx.QueryRowContext(ctx,
			"SELECT task_id FROM task_create_intents WHERE idempotency_key = ?", params.IdempotencyKey,
		).Scan(&existingID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Task{}, false, fmt.Errorf("look up task create intent: %w", err)
		}
	}
	if existingID == "" && params.SlackEventID != "" {
		err := tx.QueryRowContext(ctx,
			"SELECT task_id FROM slack_events WHERE event_id = ?", params.SlackEventID,
		).Scan(&existingID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return Task{}, false, fmt.Errorf("look up Slack event: %w", err)
		}
	}
	if existingID != "" {
		existing, err := scanTask(tx.QueryRowContext(ctx, taskSelect, existingID))
		if err != nil {
			return Task{}, false, fmt.Errorf("read deduplicated task: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return Task{}, false, fmt.Errorf("commit task deduplication: %w", err)
		}
		return existing, false, nil
	}

	now := time.Now().UTC()
	taskID, err := newID("swe-")
	if err != nil {
		return Task{}, false, err
	}
	attemptID, err := newID("swe-attempt-")
	if err != nil {
		return Task{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO tasks
			(id, repository, prompt, state, created_at, updated_at, current_attempt_id, slack_event_id,
			 slack_workspace_id, slack_channel_id, slack_message_ts, slack_thread_ts, slack_user_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, params.Repository, params.Prompt, task.RECEIVED,
		stamp(now), stamp(now), attemptID, nullableString(params.SlackEventID),
		params.SlackOrigin.WorkspaceID, params.SlackOrigin.ChannelID, params.SlackOrigin.MessageTS,
		params.SlackOrigin.ThreadTS, params.SlackOrigin.UserID,
	); err != nil {
		return Task{}, false, fmt.Errorf("insert task: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts (id, task_id, number, immutable, state, created_at)
		VALUES (?, ?, 1, 1, ?, ?)`, attemptID, taskID, task.RECEIVED, stamp(now)); err != nil {
		return Task{}, false, fmt.Errorf("insert first task attempt: %w", err)
	}
	if params.IdempotencyKey != "" {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO task_create_intents (idempotency_key, task_id)
			VALUES (?, ?)`, params.IdempotencyKey, taskID); err != nil {
			return Task{}, false, fmt.Errorf("record task create intent: %w", err)
		}
	}
	if params.SlackEventID != "" {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO slack_events (event_id, task_id, processed_at)
			VALUES (?, ?, ?)`, params.SlackEventID, taskID, stamp(now)); err != nil {
			return Task{}, false, fmt.Errorf("record Slack event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Task{}, false, fmt.Errorf("commit task creation: %w", err)
	}
	return Task{
		ID:               taskID,
		Repository:       params.Repository,
		Prompt:           params.Prompt,
		State:            task.RECEIVED,
		CreatedAt:        now,
		UpdatedAt:        now,
		CurrentAttemptID: attemptID,
		SlackEventID:     params.SlackEventID,
		SlackOrigin:      params.SlackOrigin,
	}, true, nil
}

// GetTaskByIdempotencyKey returns the task already associated with a generic
// create idempotency key.
func (s *Store) GetTaskByIdempotencyKey(ctx context.Context, idempotencyKey string) (Task, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return Task{}, errors.New("task create idempotency key is empty")
	}
	if utf8.RuneCountInString(idempotencyKey) > 256 {
		return Task{}, errors.New("task create idempotency key exceeds 256 characters")
	}
	got, err := scanTask(s.db.QueryRowContext(ctx, `
		SELECT tasks.id, tasks.repository, tasks.prompt, tasks.state, tasks.created_at,
		       tasks.updated_at, tasks.current_attempt_id, tasks.slack_event_id,
		       tasks.slack_workspace_id, tasks.slack_channel_id, tasks.slack_message_ts,
		       tasks.slack_thread_ts, tasks.slack_user_id,
		       tasks.cancellation_requested
		FROM task_create_intents JOIN tasks ON tasks.id = task_create_intents.task_id
		WHERE task_create_intents.idempotency_key = ?`, idempotencyKey))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("%w: task create intent %s", ErrNotFound, idempotencyKey)
	}
	if err != nil {
		return Task{}, fmt.Errorf("get task for idempotency key %q: %w", idempotencyKey, err)
	}
	return got, nil
}

// GetTaskBySlackEventID returns the task already associated with an event.
func (s *Store) GetTaskBySlackEventID(ctx context.Context, eventID string) (Task, error) {
	if strings.TrimSpace(eventID) == "" {
		return Task{}, errors.New("Slack event ID is empty")
	}
	got, err := scanTask(s.db.QueryRowContext(ctx, `
		SELECT tasks.id, tasks.repository, tasks.prompt, tasks.state, tasks.created_at,
		       tasks.updated_at, tasks.current_attempt_id, tasks.slack_event_id,
		       tasks.slack_workspace_id, tasks.slack_channel_id, tasks.slack_message_ts,
		       tasks.slack_thread_ts, tasks.slack_user_id,
		       tasks.cancellation_requested
		FROM slack_events JOIN tasks ON tasks.id = slack_events.task_id
		WHERE slack_events.event_id = ?`, eventID))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("%w: Slack event %s", ErrNotFound, eventID)
	}
	if err != nil {
		return Task{}, fmt.Errorf("get task for Slack event %q: %w", eventID, err)
	}
	return got, nil
}

func (s *Store) GetTask(ctx context.Context, id string) (Task, error) {
	got, err := scanTask(s.db.QueryRowContext(ctx, taskSelect, id))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, fmt.Errorf("%w: %s", ErrNotFound, id)
	}
	if err != nil {
		return Task{}, fmt.Errorf("get task %q: %w", id, err)
	}
	return got, nil
}

// ListTasks returns durable tasks from newest to oldest.
func (s *Store) ListTasks(ctx context.Context) (_ []Task, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, repository, prompt, state, created_at, updated_at,
		       current_attempt_id, slack_event_id, slack_workspace_id, slack_channel_id,
		       slack_message_ts, slack_thread_ts, slack_user_id, cancellation_requested
		FROM tasks ORDER BY created_at DESC, rowid DESC`)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()

	var tasks []Task
	for rows.Next() {
		item, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		tasks = append(tasks, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	return tasks, nil
}

// ListActiveTasks returns non-terminal intents plus terminal current attempts
// whose durable log barrier still needs recovery.
func (s *Store) ListActiveTasks(ctx context.Context) (_ []Task, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT tasks.id, tasks.repository, tasks.prompt, tasks.state, tasks.created_at, tasks.updated_at,
		       tasks.current_attempt_id, tasks.slack_event_id, tasks.slack_workspace_id, tasks.slack_channel_id,
		       tasks.slack_message_ts, tasks.slack_thread_ts, tasks.slack_user_id, tasks.cancellation_requested
		FROM tasks JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id
		WHERE tasks.state NOT IN (?, ?, ?) OR task_attempts.logs_exhausted = 0
		ORDER BY tasks.created_at, tasks.rowid`, task.READY, task.FAILED, task.CANCELLED)
	if err != nil {
		return nil, fmt.Errorf("list active tasks: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var result []Task
	for rows.Next() {
		item, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan active task: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list active tasks: %w", err)
	}
	return result, nil
}

func (s *Store) ListAttempts(ctx context.Context, taskID string) (_ []Attempt, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_id, number, immutable, state, logs_exhausted, validation_state,
		       manifest_json, resource_snapshot, config_digest, created_at
		FROM task_attempts WHERE task_id = ? ORDER BY number`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list attempts for %q: %w", taskID, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()

	var attempts []Attempt
	for rows.Next() {
		attempt, err := scanAttempt(rows)
		if err != nil {
			return nil, fmt.Errorf("scan attempt for %q: %w", taskID, err)
		}
		attempts = append(attempts, attempt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list attempts for %q: %w", taskID, err)
	}
	return attempts, nil
}

// CurrentAttempt returns the immutable attempt currently selected by a task.
func (s *Store) CurrentAttempt(ctx context.Context, taskID string) (Attempt, error) {
	attempt, err := scanAttempt(s.db.QueryRowContext(ctx, `
		SELECT task_attempts.id, task_attempts.task_id, task_attempts.number,
		       task_attempts.immutable, task_attempts.state, task_attempts.logs_exhausted,
		       task_attempts.validation_state, task_attempts.manifest_json,
		       task_attempts.resource_snapshot, task_attempts.config_digest, task_attempts.created_at
		FROM tasks JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id
		WHERE tasks.id = ?`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, fmt.Errorf("%w: %s", ErrNotFound, taskID)
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("get current attempt for %q: %w", taskID, err)
	}
	return attempt, nil
}

// GetAttempt returns an attempt only when it belongs to taskID.
func (s *Store) GetAttempt(ctx context.Context, taskID, attemptID string) (Attempt, error) {
	attempt, err := scanAttempt(s.db.QueryRowContext(ctx, `
		SELECT id, task_id, number, immutable, state, logs_exhausted, validation_state,
		       manifest_json, resource_snapshot, config_digest, created_at
		FROM task_attempts WHERE task_id = ? AND id = ?`, taskID, attemptID))
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("get attempt %q for task %q: %w", attemptID, taskID, err)
	}
	return attempt, nil
}

// GetAttemptNumber returns one immutable attempt by its task-local number.
func (s *Store) GetAttemptNumber(ctx context.Context, taskID string, number int) (Attempt, error) {
	attempt, err := scanAttempt(s.db.QueryRowContext(ctx, `
		SELECT id, task_id, number, immutable, state, logs_exhausted, validation_state,
		       manifest_json, resource_snapshot, config_digest, created_at
		FROM task_attempts WHERE task_id = ? AND number = ?`, taskID, number))
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, fmt.Errorf("%w: attempt %d for task %s", ErrNotFound, number, taskID)
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("get attempt %d for task %q: %w", number, taskID, err)
	}
	return attempt, nil
}

func (s *Store) RetryTask(ctx context.Context, taskID string) (Attempt, error) {
	attempt, _, err := s.RetryTaskOnce(ctx, taskID, "")
	return attempt, err
}

// GetRetryIntent returns the immutable attempt originally assigned to an
// idempotency key, regardless of the task's current attempt.
func (s *Store) GetRetryIntent(ctx context.Context, taskID, idempotencyKey string) (Attempt, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" {
		return Attempt{}, fmt.Errorf("%w: empty retry idempotency key", ErrNotFound)
	}
	attempt, err := scanAttempt(s.db.QueryRowContext(ctx, `
		SELECT a.id, a.task_id, a.number, a.immutable, a.state, a.logs_exhausted,
		       a.validation_state, a.manifest_json, a.resource_snapshot, a.config_digest, a.created_at
		FROM retry_intents r JOIN task_attempts a ON a.id = r.attempt_id
		WHERE r.task_id = ? AND r.idempotency_key = ?`, taskID, idempotencyKey))
	if errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, fmt.Errorf("%w: retry intent %s", ErrNotFound, idempotencyKey)
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("get retry intent: %w", err)
	}
	return attempt, nil
}

// RetryTaskOnce atomically resets a failed aggregate onto a new queued
// attempt. A supplied idempotency key returns the original retry on replay.
func (s *Store) RetryTaskOnce(ctx context.Context, taskID, idempotencyKey string) (Attempt, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("begin task retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey != "" {
		attempt, err := scanAttempt(tx.QueryRowContext(ctx, `
			SELECT a.id, a.task_id, a.number, a.immutable, a.state, a.logs_exhausted,
			       a.validation_state, a.manifest_json, a.resource_snapshot, a.config_digest, a.created_at
			FROM retry_intents r JOIN task_attempts a ON a.id = r.attempt_id
			WHERE r.task_id = ? AND r.idempotency_key = ?`, taskID, idempotencyKey))
		if err == nil {
			if err := tx.Commit(); err != nil {
				return Attempt{}, false, fmt.Errorf("commit retry replay: %w", err)
			}
			return attempt, false, nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return Attempt{}, false, fmt.Errorf("read retry intent: %w", err)
		}
	}

	var cancellationRequested, logsExhausted int
	var state, currentAttemptID string
	if err := tx.QueryRowContext(ctx,
		`SELECT tasks.state, tasks.cancellation_requested, tasks.current_attempt_id, task_attempts.logs_exhausted
		 FROM tasks JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id WHERE tasks.id = ?`, taskID,
	).Scan(&state, &cancellationRequested, &currentAttemptID, &logsExhausted); errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, false, fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return Attempt{}, false, fmt.Errorf("read task for retry: %w", err)
	}
	if err := (task.Machine{}).Retry(task.State(state), task.QUEUED); err != nil {
		return Attempt{}, false, err
	}
	if logsExhausted != 1 {
		return Attempt{}, false, fmt.Errorf("%w: failed current attempt %q logs are not exhausted", ErrConflict, currentAttemptID)
	}

	var number int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = ?`, taskID).Scan(&number); err != nil {
		return Attempt{}, false, fmt.Errorf("read retry number: %w", err)
	}
	attemptID, err := newID("swe-attempt-")
	if err != nil {
		return Attempt{}, false, err
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts (id, task_id, number, immutable, state, created_at)
		VALUES (?, ?, ?, 1, ?, ?)`, attemptID, taskID, number, task.QUEUED, stamp(now)); err != nil {
		return Attempt{}, false, fmt.Errorf("insert retry attempt: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		"UPDATE tasks SET state = ?, current_attempt_id = ?, cancellation_requested = 0, updated_at = ? WHERE id = ? AND state = ?",
		task.QUEUED, attemptID, stamp(now), taskID, task.FAILED,
	); err != nil {
		return Attempt{}, false, fmt.Errorf("update current retry attempt: %w", err)
	}
	eventID, err := newID("swe-event-")
	if err != nil {
		return Attempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_events
			(id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger, resource_identity, metadata, error)
		VALUES (?, ?, ?, ?, ?, ?, 'retry requested', 'api', '{}', '{}', '')`,
		eventID, taskID, attemptID, stamp(now), task.FAILED, task.QUEUED); err != nil {
		return Attempt{}, false, fmt.Errorf("append retry event: %w", err)
	}
	if idempotencyKey != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO retry_intents (task_id, idempotency_key, attempt_id, created_at) VALUES (?, ?, ?, ?)`,
			taskID, idempotencyKey, attemptID, stamp(now)); err != nil {
			return Attempt{}, false, fmt.Errorf("record retry intent: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Attempt{}, false, fmt.Errorf("commit task retry: %w", err)
	}
	return Attempt{
		ID: attemptID, TaskID: taskID, Number: number, Immutable: true, State: task.QUEUED, CreatedAt: now,
	}, true, nil
}

// SaveAttemptSnapshot stores immutable worker inputs before any external
// resource is created. Replays may supply only the identical snapshot.
func (s *Store) SaveAttemptSnapshot(ctx context.Context, taskID, attemptID string, manifest, resources []byte, digest string) error {
	if len(manifest) == 0 || len(resources) == 0 || strings.TrimSpace(digest) == "" {
		return errors.New("attempt manifest, resources, and config digest are required")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE task_attempts SET manifest_json = ?, resource_snapshot = ?, config_digest = ?
		WHERE id = ? AND task_id = ? AND resource_snapshot = ''`, manifest, resources, digest, attemptID, taskID)
	if err != nil {
		return fmt.Errorf("save attempt snapshot %q: %w", attemptID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm attempt snapshot %q: %w", attemptID, err)
	}
	if affected == 1 {
		return nil
	}
	attempt, err := s.GetAttempt(ctx, taskID, attemptID)
	if err != nil {
		return err
	}
	if string(attempt.ManifestJSON) != string(manifest) || string(attempt.ResourceSnapshot) != string(resources) || attempt.ConfigDigest != digest {
		return fmt.Errorf("%w: attempt %q snapshot is already immutable", ErrConflict, attemptID)
	}
	return nil
}

func (s *Store) RequestCancellation(ctx context.Context, taskID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin cancellation request: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state string
	if err := tx.QueryRowContext(ctx, "SELECT state FROM tasks WHERE id = ?", taskID).Scan(&state); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return fmt.Errorf("read task for cancellation: %w", err)
	}
	if state == string(task.READY) || state == string(task.FAILED) || state == string(task.CANCELLED) {
		return fmt.Errorf("terminal task state %q cannot be cancelled", state)
	}
	result, err := tx.ExecContext(ctx, `UPDATE tasks SET cancellation_requested = 1, updated_at = ? WHERE id = ?`, stamp(time.Now().UTC()), taskID)
	if err != nil {
		return fmt.Errorf("request cancellation for %q: %w", taskID, err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm cancellation for %q: %w", taskID, err)
	} else if affected == 0 {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cancellation request for %q: %w", taskID, err)
	}
	return nil
}

func (s *Store) Transition(ctx context.Context, taskID string, from, to task.State, params TransitionParams) error {
	if err := (task.Machine{}).Transition(from, to); err != nil {
		return err
	}
	if strings.TrimSpace(params.Reason) == "" {
		return errors.New("transition reason is empty")
	}
	if !validTrigger(params.Trigger) {
		return fmt.Errorf("invalid transition trigger %q", params.Trigger)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin task transition: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var currentState string
	var attemptID string
	var cancellationRequested int
	if err := tx.QueryRowContext(ctx,
		"SELECT state, current_attempt_id, cancellation_requested FROM tasks WHERE id = ?", taskID,
	).Scan(&currentState, &attemptID, &cancellationRequested); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return fmt.Errorf("read task for transition: %w", err)
	}
	if task.State(currentState) != from {
		return fmt.Errorf("%w: task %q is %q, expected %q", ErrConflict, taskID, currentState, from)
	}
	if cancellationRequested == 1 && to != task.CANCELLED {
		return fmt.Errorf("%w: task %q cancellation owns outcome; transition to %q rejected", ErrConflict, taskID, to)
	}
	if to == task.PR_OPEN || to == task.READY {
		var durable int
		err := tx.QueryRowContext(ctx, `
			SELECT 1 FROM git_results g JOIN pull_requests p ON p.attempt_id = g.attempt_id
			WHERE g.attempt_id = ? AND g.state = 'pushed' AND g.branch <> '' AND g.commit_sha <> ''
			  AND p.state = 'open' AND p.head_branch = g.branch AND p.number > 0 AND p.url <> ''`, attemptID).Scan(&durable)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: task %q cannot enter %q without durable Git and open pull request", ErrConflict, taskID, to)
		}
		if err != nil {
			return fmt.Errorf("verify durable pull request gate: %w", err)
		}
	}

	now := time.Now().UTC()
	cancelled := 0
	if to != task.CANCELLED {
		cancelled = -1
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE tasks SET state = ?, updated_at = ?,
			cancellation_requested = CASE WHEN ? = 0 THEN 0 ELSE cancellation_requested END
		WHERE id = ? AND state = ?`, to, stamp(now), cancelled, taskID, from)
	if err != nil {
		return fmt.Errorf("update task state: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm task transition: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("%w: task %q changed during transition", ErrConflict, taskID)
	}
	result, err = tx.ExecContext(ctx, "UPDATE task_attempts SET state = ? WHERE id = ?", to, attemptID)
	if err != nil {
		return fmt.Errorf("update current attempt state: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm attempt transition: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("%w: current attempt %q is missing", ErrConflict, attemptID)
	}
	if params.Validation != nil {
		if err := applyValidationTransition(ctx, tx, taskID, attemptID, *params.Validation, now); err != nil {
			return err
		}
	}

	eventID, err := newID("swe-event-")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_events
			(id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger,
			 resource_identity, metadata, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', '{}', '')`,
		eventID, taskID, attemptID, stamp(now), from, to, params.Reason, params.Trigger,
	); err != nil {
		return fmt.Errorf("append task transition event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit task transition: %w", err)
	}
	return nil
}

// RecordValidationStarted persists subsequent validation commands while the
// task remains in VALIDATING. The first command is recorded by Transition.
func (s *Store) RecordValidationStarted(ctx context.Context, taskID, attemptID, name, summary string) error {
	return s.RecordValidationStartedOnce(ctx, taskID, attemptID, name, summary, "")
}

func (s *Store) RecordValidationStartedOnce(ctx context.Context, taskID, attemptID, name, summary, eventID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin validation run: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if eventID != "" {
		var found int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM validation_runs WHERE start_event_id = ?`, eventID).Scan(&found); err == nil {
			return tx.Commit()
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	var currentAttempt, state string
	if err := tx.QueryRowContext(ctx, "SELECT current_attempt_id, state FROM tasks WHERE id = ?", taskID).Scan(&currentAttempt, &state); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return fmt.Errorf("read task for validation run: %w", err)
	}
	if currentAttempt != attemptID || task.State(state) != task.VALIDATING {
		return fmt.Errorf("%w: task %q is not validating on attempt %q", ErrConflict, taskID, attemptID)
	}
	if err := applyValidationTransition(ctx, tx, taskID, attemptID, ValidationTransition{Name: name, State: "running", Summary: summary, EventID: eventID}, time.Now().UTC()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit validation run: %w", err)
	}
	return nil
}

// RecordValidationResult completes the one currently running command. Command
// identity and the durable sequence prevent a later command from implicitly
// completing an earlier row.
func (s *Store) RecordValidationResult(ctx context.Context, taskID, attemptID, name, summary string, exitCode int) error {
	return s.RecordValidationResultOnce(ctx, taskID, attemptID, name, summary, exitCode, "")
}

func (s *Store) RecordValidationResultOnce(ctx context.Context, taskID, attemptID, name, summary string, exitCode int, eventID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin validation result: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if eventID != "" {
		var found int
		if err := tx.QueryRowContext(ctx, `SELECT 1 FROM validation_runs WHERE result_event_id = ?`, eventID).Scan(&found); err == nil {
			return tx.Commit()
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	var currentAttempt, state string
	if err := tx.QueryRowContext(ctx, "SELECT current_attempt_id, state FROM tasks WHERE id = ?", taskID).Scan(&currentAttempt, &state); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return fmt.Errorf("read task for validation result: %w", err)
	}
	if currentAttempt != attemptID || task.State(state) != task.VALIDATING {
		return fmt.Errorf("%w: task %q is not validating on attempt %q", ErrConflict, taskID, attemptID)
	}
	var id, durableName string
	if err := tx.QueryRowContext(ctx, `SELECT id, name FROM validation_runs WHERE attempt_id = ? AND state = 'running' ORDER BY sequence LIMIT 1`, attemptID).Scan(&id, &durableName); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: attempt %q has no running validation", ErrConflict, attemptID)
	} else if err != nil {
		return fmt.Errorf("read running validation: %w", err)
	}
	if durableName != name {
		return fmt.Errorf("%w: validation result command %q does not match running command %q", ErrConflict, name, durableName)
	}
	resultState, resultError := "succeeded", ""
	if exitCode != 0 {
		resultState, resultError = "failed", summary
	}
	result, err := tx.ExecContext(ctx, `UPDATE validation_runs SET state = ?, summary = ?, error = ?, exit_code = ?, result_event_id = NULLIF(?, '') WHERE id = ? AND state = 'running'`, resultState, summary, resultError, exitCode, eventID, id)
	if err != nil {
		return fmt.Errorf("complete validation result: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: validation run %q changed during completion", ErrConflict, id)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit validation result: %w", err)
	}
	return nil
}

// MarkValidationComplete records the worker's aggregate terminal validation
// event only after every command result is durable.
func (s *Store) MarkValidationComplete(ctx context.Context, taskID, attemptID, state string) error {
	if state != "succeeded" && state != "failed" {
		return fmt.Errorf("invalid aggregate validation state %q", state)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE task_attempts SET validation_state = ?
		WHERE id = ? AND task_id = ?
		  AND NOT EXISTS (SELECT 1 FROM validation_runs WHERE attempt_id = ? AND state = 'running')
		  AND EXISTS (SELECT 1 FROM validation_runs WHERE attempt_id = ?)`, state, attemptID, taskID, attemptID, attemptID)
	if err != nil {
		return fmt.Errorf("mark validation %s for attempt %q: %w", state, attemptID, err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: attempt %q validation results are incomplete", ErrConflict, attemptID)
	}
	return nil
}

// RecordValidationFailureDetail enriches the exact failed result selected by
// command and exit code when the worker emits its terminal failure summary.
func (s *Store) RecordValidationFailureDetail(ctx context.Context, attemptID, name, detail string, exitCode int) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE validation_runs SET summary = ?, error = ?
		WHERE id = (
			SELECT id FROM validation_runs
			WHERE attempt_id = ? AND name = ? AND state = 'failed' AND exit_code = ?
			ORDER BY sequence DESC LIMIT 1
		)`, detail, detail, attemptID, name, exitCode)
	if err != nil {
		return fmt.Errorf("record validation failure detail: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: no failed validation result matches command %q and exit %d", ErrConflict, name, exitCode)
	}
	return nil
}

func applyValidationTransition(ctx context.Context, tx *sql.Tx, taskID, attemptID string, validation ValidationTransition, now time.Time) error {
	switch validation.State {
	case "running":
		if strings.TrimSpace(validation.Name) == "" {
			return errors.New("validation run name is empty")
		}
		var running int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM validation_runs WHERE attempt_id = ? AND state = 'running'`, attemptID).Scan(&running); err != nil {
			return fmt.Errorf("check running validation: %w", err)
		}
		if running != 0 {
			return fmt.Errorf("%w: attempt %q already has a running validation", ErrConflict, attemptID)
		}
		var sequence int
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(sequence), 0) + 1 FROM validation_runs WHERE attempt_id = ?`, attemptID).Scan(&sequence); err != nil {
			return fmt.Errorf("select validation sequence: %w", err)
		}
		id, err := newID("swe-validation-")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO validation_runs (id, task_id, attempt_id, sequence, name, state, summary, error, exit_code, start_event_id, created_at)
			VALUES (?, ?, ?, ?, ?, 'running', ?, '', NULL, NULLIF(?, ''), ?)`, id, taskID, attemptID, sequence, validation.Name, validation.Summary, validation.EventID, stamp(now)); err != nil {
			return fmt.Errorf("record validation run: %w", err)
		}
	case "succeeded", "failed":
		result, err := tx.ExecContext(ctx, `
			UPDATE validation_runs SET state = ?, summary = CASE WHEN ? = '' THEN summary ELSE ? END,
				error = CASE WHEN ? = '' THEN error ELSE ? END
			WHERE attempt_id = ? AND state = 'running'`, validation.State, validation.Summary, validation.Summary, validation.Error, validation.Error, attemptID)
		if err != nil {
			return fmt.Errorf("complete validation runs: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil {
			return fmt.Errorf("confirm validation completion: %w", err)
		} else if affected == 0 && validation.State == "failed" {
			return fmt.Errorf("%w: attempt %q has no running validation", ErrConflict, attemptID)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE task_attempts SET validation_state = ? WHERE id = ?`, validation.State, attemptID); err != nil {
			return fmt.Errorf("update aggregate validation state: %w", err)
		}
	default:
		return fmt.Errorf("invalid validation state %q", validation.State)
	}
	return nil
}

// RecordObservation appends an explicit recovery/audit event without changing
// lifecycle state.
func (s *Store) RecordObservation(ctx context.Context, taskID, reason, trigger string) error {
	if strings.TrimSpace(reason) == "" || !validTrigger(trigger) {
		return errors.New("observation reason and valid trigger are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin task observation: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var state, attemptID string
	if err := tx.QueryRowContext(ctx, "SELECT state, current_attempt_id FROM tasks WHERE id = ?", taskID).Scan(&state, &attemptID); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return fmt.Errorf("read task for observation: %w", err)
	}
	id, err := newID("swe-event-")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_events (id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger, resource_identity, metadata, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', '{}', '')`, id, taskID, attemptID, stamp(time.Now().UTC()), state, state, reason, trigger); err != nil {
		return fmt.Errorf("append task observation: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit task observation: %w", err)
	}
	return nil
}

func (s *Store) MarkLogsExhausted(ctx context.Context, taskID, attemptID string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE task_attempts SET logs_exhausted = 1
		WHERE id = ? AND task_id = ?`, attemptID, taskID)
	if err != nil {
		return fmt.Errorf("mark logs exhausted for attempt %q: %w", attemptID, err)
	}
	if affected, err := result.RowsAffected(); err != nil {
		return fmt.Errorf("confirm exhausted logs: %w", err)
	} else if affected != 1 {
		return fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	}
	return nil
}

func (s *Store) ListEvents(ctx context.Context, taskID string) (_ []TransitionEvent, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger,
		       resource_identity, metadata, error
		FROM task_events WHERE task_id = ? ORDER BY rowid`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list events for %q: %w", taskID, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()

	var events []TransitionEvent
	for rows.Next() {
		var event TransitionEvent
		var occurredAt string
		var fromState, toState string
		if err := rows.Scan(
			&event.ID, &event.TaskID, &event.AttemptID, &occurredAt, &fromState, &toState,
			&event.Reason, &event.Trigger, &event.ResourceIdentity, &event.Metadata, &event.Error,
		); err != nil {
			return nil, fmt.Errorf("scan event for %q: %w", taskID, err)
		}
		event.OccurredAt, err = parseTime(occurredAt)
		if err != nil {
			return nil, fmt.Errorf("parse event time for %q: %w", taskID, err)
		}
		event.FromState = task.State(fromState)
		event.ToState = task.State(toState)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list events for %q: %w", taskID, err)
	}
	return events, nil
}

// AppendLogChunk durably appends one bounded piece of raw worker output.
func (s *Store) AppendLogChunk(ctx context.Context, taskID, attemptID string, content []byte) error {
	if strings.TrimSpace(taskID) == "" || strings.TrimSpace(attemptID) == "" {
		return errors.New("log chunk task and attempt IDs are required")
	}
	if len(content) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin log append: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var exists int
	if err := tx.QueryRowContext(ctx,
		"SELECT 1 FROM task_attempts WHERE id = ? AND task_id = ?", attemptID, taskID,
	).Scan(&exists); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	} else if err != nil {
		return fmt.Errorf("verify log attempt: %w", err)
	}
	var sequence int
	if err := tx.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(sequence), 0) + 1 FROM log_chunks WHERE attempt_id = ?", attemptID,
	).Scan(&sequence); err != nil {
		return fmt.Errorf("select log sequence: %w", err)
	}
	id, err := newID("swe-log-")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO log_chunks (id, task_id, attempt_id, sequence, content, created_at)
		VALUES (?, ?, ?, ?, ?, ?)`, id, taskID, attemptID, sequence, content, stamp(time.Now().UTC())); err != nil {
		return fmt.Errorf("append log chunk for attempt %q: %w", attemptID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit log chunk: %w", err)
	}
	return nil
}

// ReadLogTail reads only enough newest chunks to produce the requested lines.
func (s *Store) ReadLogTail(ctx context.Context, taskID, attemptID string, lines int) (_ string, resultErr error) {
	if lines < 0 {
		return "", errors.New("log tail lines must not be negative")
	}
	if _, err := s.GetAttempt(ctx, taskID, attemptID); err != nil {
		return "", err
	}
	if lines == 0 {
		return "", nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT content FROM log_chunks
		WHERE task_id = ? AND attempt_id = ?
		ORDER BY sequence DESC`, taskID, attemptID)
	if err != nil {
		return "", fmt.Errorf("read log chunks for attempt %q: %w", attemptID, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()

	var reversed [][]byte
	newlines := 0
	required := lines
	first := true
	for rows.Next() {
		var chunk []byte
		if err := rows.Scan(&chunk); err != nil {
			return "", fmt.Errorf("scan log chunk for attempt %q: %w", attemptID, err)
		}
		if first {
			first = false
			if len(chunk) > 0 && chunk[len(chunk)-1] == '\n' {
				required++
			}
		}
		copyOfChunk := append([]byte(nil), chunk...)
		reversed = append(reversed, copyOfChunk)
		newlines += strings.Count(string(chunk), "\n")
		if newlines >= required {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("read log chunks for attempt %q: %w", attemptID, err)
	}
	var content strings.Builder
	for i := len(reversed) - 1; i >= 0; i-- {
		content.Write(reversed[i])
	}
	return lastLines(content.String(), lines), nil
}

func lastLines(content string, lines int) string {
	if lines <= 0 || content == "" {
		return ""
	}
	end := len(content)
	index := end - 1
	if content[index] == '\n' {
		index--
	}
	for count := 0; index >= 0; index-- {
		if content[index] != '\n' {
			continue
		}
		count++
		if count == lines {
			return content[index+1 : end]
		}
	}
	return content
}

// ListValidationRuns returns validation records for one attempt.
func (s *Store) ListValidationRuns(ctx context.Context, attemptID string) (_ []ValidationRun, resultErr error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, task_id, attempt_id, sequence, name, state, summary, error, COALESCE(exit_code, 0), created_at
		FROM validation_runs WHERE attempt_id = ? ORDER BY sequence`, attemptID)
	if err != nil {
		return nil, fmt.Errorf("list validation runs for attempt %q: %w", attemptID, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	var runs []ValidationRun
	for rows.Next() {
		var run ValidationRun
		var createdAt string
		if err := rows.Scan(&run.ID, &run.TaskID, &run.AttemptID, &run.Sequence, &run.Name, &run.State, &run.Summary, &run.Error, &run.ExitCode, &createdAt); err != nil {
			return nil, fmt.Errorf("scan validation run for attempt %q: %w", attemptID, err)
		}
		run.CreatedAt, err = parseTime(createdAt)
		if err != nil {
			return nil, fmt.Errorf("parse validation run time for attempt %q: %w", attemptID, err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list validation runs for attempt %q: %w", attemptID, err)
	}
	return runs, nil
}

// RecordGitResult stores the branch identity emitted by one worker attempt.
func (s *Store) RecordGitResult(ctx context.Context, result GitResult) error {
	if strings.TrimSpace(result.AttemptID) == "" {
		return errors.New("git result attempt ID is empty")
	}
	if strings.TrimSpace(result.State) == "" {
		return errors.New("git result state is empty")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO git_results (attempt_id, state, branch, commit_sha, error)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(attempt_id) DO NOTHING`,
		result.AttemptID, result.State, result.Branch, result.CommitSHA, result.Error)
	if err != nil {
		return fmt.Errorf("record git result for attempt %q: %w", result.AttemptID, err)
	}
	durable, err := s.GetGitResult(ctx, result.AttemptID)
	if err != nil {
		return err
	}
	if durable != result {
		return fmt.Errorf("%w: Git result for attempt %q is already durable", ErrConflict, result.AttemptID)
	}
	return nil
}

func (s *Store) GetGitResult(ctx context.Context, attemptID string) (GitResult, error) {
	var result GitResult
	err := s.db.QueryRowContext(ctx, `
		SELECT attempt_id, state, branch, commit_sha, error
		FROM git_results WHERE attempt_id = ?`, attemptID).Scan(
		&result.AttemptID, &result.State, &result.Branch, &result.CommitSHA, &result.Error,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return GitResult{}, fmt.Errorf("%w: git result for attempt %s", ErrNotFound, attemptID)
	}
	if err != nil {
		return GitResult{}, fmt.Errorf("get git result for attempt %q: %w", attemptID, err)
	}
	return result, nil
}

// ReservePullRequest durably claims pull-request creation for an attempt. A
// false return means another call has already claimed it.
func (s *Store) ReservePullRequest(ctx context.Context, attemptID, title, headBranch, baseBranch string) (bool, error) {
	if strings.TrimSpace(attemptID) == "" {
		return false, errors.New("pull request attempt ID is empty")
	}
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO pull_requests
			(attempt_id, state, number, url, title, head_branch, base_branch, error)
		VALUES (?, 'creating', NULL, '', ?, ?, ?, '')
		ON CONFLICT(attempt_id) DO NOTHING`, attemptID, title, headBranch, baseBranch)
	if err != nil {
		return false, fmt.Errorf("reserve pull request for attempt %q: %w", attemptID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("confirm pull request reservation for attempt %q: %w", attemptID, err)
	}
	return affected == 1, nil
}

// CompletePullRequest stores the provider result before any notification is
// sent. It only completes a previously reserved row.
func (s *Store) CompletePullRequest(ctx context.Context, attemptID string, number int, url string) error {
	if number <= 0 || strings.TrimSpace(url) == "" {
		return errors.New("pull request number and URL are required")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE pull_requests SET state = 'open', number = ?, url = ?, error = ''
		WHERE attempt_id = ? AND state = 'creating'`, number, url, attemptID)
	if err != nil {
		return fmt.Errorf("complete pull request for attempt %q: %w", attemptID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm pull request completion for attempt %q: %w", attemptID, err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: pull request for attempt %q is not creating", ErrConflict, attemptID)
	}
	return nil
}

func (s *Store) FailPullRequest(ctx context.Context, attemptID string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE pull_requests SET state = 'failed', error = ?
		WHERE attempt_id = ? AND state = 'creating'`, message, attemptID)
	if err != nil {
		return fmt.Errorf("fail pull request for attempt %q: %w", attemptID, err)
	}
	return nil
}

func (s *Store) GetPullRequest(ctx context.Context, attemptID string) (PullRequest, error) {
	var result PullRequest
	var number sql.NullInt64
	var notifiedAt sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT attempt_id, state, number, url, title, head_branch, base_branch, error, notified_at
		FROM pull_requests WHERE attempt_id = ?`, attemptID).Scan(
		&result.AttemptID, &result.State, &number, &result.URL, &result.Title,
		&result.HeadBranch, &result.BaseBranch, &result.Error, &notifiedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return PullRequest{}, fmt.Errorf("%w: pull request for attempt %s", ErrNotFound, attemptID)
	}
	if err != nil {
		return PullRequest{}, fmt.Errorf("get pull request for attempt %q: %w", attemptID, err)
	}
	if number.Valid {
		result.Number = int(number.Int64)
	}
	result.Notified = notifiedAt.Valid
	return result, nil
}

// MarkPullRequestNotified records successful delivery so event replay after a
// controller restart does not post the same durable result again.
func (s *Store) MarkPullRequestNotified(ctx context.Context, attemptID string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE pull_requests SET notified_at = COALESCE(notified_at, ?)
		WHERE attempt_id = ? AND state = 'open'`, stamp(time.Now().UTC()), attemptID)
	if err != nil {
		return fmt.Errorf("mark pull request notified for attempt %q: %w", attemptID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("confirm pull request notification for attempt %q: %w", attemptID, err)
	}
	if affected != 1 {
		return fmt.Errorf("%w: open pull request for attempt %s", ErrNotFound, attemptID)
	}
	return nil
}

func (s *Store) Pragmas(ctx context.Context) (SQLitePragmas, error) {
	var result SQLitePragmas
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&result.JournalMode); err != nil {
		return SQLitePragmas{}, fmt.Errorf("read journal mode: %w", err)
	}
	var foreignKeys int
	if err := s.db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return SQLitePragmas{}, fmt.Errorf("read foreign keys pragma: %w", err)
	}
	if err := s.db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&result.BusyTimeout); err != nil {
		return SQLitePragmas{}, fmt.Errorf("read busy timeout: %w", err)
	}
	result.ForeignKeys = foreignKeys == 1
	return result, nil
}

const taskSelect = `
	SELECT id, repository, prompt, state, created_at, updated_at,
	       current_attempt_id, slack_event_id, slack_workspace_id, slack_channel_id,
	       slack_message_ts, slack_thread_ts, slack_user_id, cancellation_requested
	FROM tasks WHERE id = ?`

const slackInboxSelect = `
	SELECT event_id, kind, text, origin_json, status, attempts, last_error,
	       created_at, updated_at, handled_at
	FROM slack_inbox`

type scanner interface {
	Scan(dest ...any) error
}

func scanTask(row scanner) (Task, error) {
	var result Task
	var state, createdAt, updatedAt string
	var slackEventID sql.NullString
	var cancellationRequested int
	if err := row.Scan(
		&result.ID, &result.Repository, &result.Prompt, &state, &createdAt, &updatedAt,
		&result.CurrentAttemptID, &slackEventID,
		&result.SlackOrigin.WorkspaceID, &result.SlackOrigin.ChannelID, &result.SlackOrigin.MessageTS,
		&result.SlackOrigin.ThreadTS, &result.SlackOrigin.UserID, &cancellationRequested,
	); err != nil {
		return Task{}, err
	}
	var err error
	result.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return Task{}, err
	}
	result.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return Task{}, err
	}
	result.State = task.State(state)
	if slackEventID.Valid {
		result.SlackEventID = slackEventID.String
	}
	result.CancellationRequested = cancellationRequested == 1
	return result, nil
}

func scanAttempt(row scanner) (Attempt, error) {
	var result Attempt
	var state, createdAt string
	var immutable, logsExhausted int
	if err := row.Scan(
		&result.ID, &result.TaskID, &result.Number, &immutable, &state, &logsExhausted,
		&result.ValidationState, &result.ManifestJSON, &result.ResourceSnapshot, &result.ConfigDigest, &createdAt,
	); err != nil {
		return Attempt{}, err
	}
	var err error
	result.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return Attempt{}, err
	}
	result.Immutable = immutable == 1
	result.LogsExhausted = logsExhausted == 1
	result.State = task.State(state)
	return result, nil
}

func scanSlackInboxEvent(row scanner) (SlackInboxEvent, error) {
	var event SlackInboxEvent
	var origin, createdAt, updatedAt string
	var handledAt sql.NullString
	if err := row.Scan(
		&event.EventID, &event.Kind, &event.Text, &origin, &event.Status,
		&event.Attempts, &event.LastError, &createdAt, &updatedAt, &handledAt,
	); err != nil {
		return SlackInboxEvent{}, err
	}
	if err := json.Unmarshal([]byte(origin), &event.Origin); err != nil {
		return SlackInboxEvent{}, fmt.Errorf("parse Slack inbox origin: %w", err)
	}
	var err error
	event.CreatedAt, err = parseTime(createdAt)
	if err != nil {
		return SlackInboxEvent{}, err
	}
	event.UpdatedAt, err = parseTime(updatedAt)
	if err != nil {
		return SlackInboxEvent{}, err
	}
	if handledAt.Valid {
		handled, err := parseTime(handledAt.String)
		if err != nil {
			return SlackInboxEvent{}, err
		}
		event.HandledAt = &handled
	}
	return event, nil
}

func validTrigger(trigger string) bool {
	switch trigger {
	case "api", "controller", "scheduler", "kubernetes", "webhook", "system":
		return true
	default:
		return false
	}
}

func newID(prefix string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate %s ID: %w", prefix, err)
	}
	return prefix + hex.EncodeToString(bytes[:]), nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func stamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	return time.Parse(time.RFC3339Nano, value)
}
