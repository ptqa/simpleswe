package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"

	"modernc.org/sqlite"
)

var (
	ErrNotFound = errors.New("task not found")
	ErrConflict = errors.New("task state conflict")
)

type sqliteConnector struct {
	driver driver.Driver
	dsn    string
}

func (c sqliteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("connect SQLite: %w", err)
	}
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite connection: %w", err)
	}
	return conn, nil
}

func (c sqliteConnector) Driver() driver.Driver {
	return c.driver
}

func sqliteDSN(path string) string {
	uriPath := filepath.ToSlash(path)
	absolute := filepath.IsAbs(path)
	if absolute && !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	query := url.Values{}
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(ON)")
	return (&url.URL{Scheme: "file", Path: uriPath, RawQuery: query.Encode(), OmitHost: !absolute}).String()
}

// Store is the SQLite-backed task store.
type Store struct {
	db *sql.DB
}

type CreateTaskParams struct {
	Repository     string
	Prompt         string
	PRTitle        string
	IdempotencyKey string
}

type Task struct {
	ID                    string
	Repository            string
	Prompt                string
	PRTitle               string
	State                 task.State
	CreatedAt             time.Time
	UpdatedAt             time.Time
	CurrentAttemptID      string
	CancellationRequested bool
}

type Attempt struct {
	ID               string
	TaskID           string
	Number           int
	Immutable        bool
	State            task.State
	Prompt           string
	BaseBranch       string
	TaskBranch       string
	LogsExhausted    bool
	ValidationState  string
	ManifestJSON     []byte
	ResourceSnapshot []byte
	ConfigDigest     string
	CreatedAt        time.Time
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
    pr_title TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    current_attempt_id TEXT NOT NULL,
    cancellation_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancellation_requested IN (0, 1))
);

CREATE TABLE IF NOT EXISTS task_attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    number INTEGER NOT NULL CHECK (number > 0),
    immutable INTEGER NOT NULL DEFAULT 1 CHECK (immutable = 1),
    state TEXT NOT NULL,
	prompt TEXT NOT NULL DEFAULT '',
	base_branch TEXT NOT NULL DEFAULT '',
	task_branch TEXT NOT NULL DEFAULT '',
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

CREATE TABLE IF NOT EXISTS forge_events (
	id TEXT PRIMARY KEY,
	provider TEXT NOT NULL,
	kind TEXT NOT NULL,
	owner TEXT NOT NULL,
	repository TEXT NOT NULL,
	pull_request_number INTEGER NOT NULL DEFAULT 0 CHECK (pull_request_number >= 0),
	commit_sha TEXT NOT NULL,
	branch TEXT NOT NULL,
	comment_id INTEGER NOT NULL DEFAULT 0 CHECK (comment_id >= 0),
	comment_kind TEXT NOT NULL DEFAULT '',
	title TEXT NOT NULL DEFAULT '',
	body TEXT NOT NULL DEFAULT '',
	author TEXT NOT NULL DEFAULT '',
	url TEXT NOT NULL DEFAULT '',
	task_id TEXT REFERENCES tasks(id),
	attempt_id TEXT REFERENCES task_attempts(id),
	reply_draft TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'handled', 'failed')),
	attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	handled_at TEXT,
	failed_at TEXT,
	next_attempt_at TEXT
);

CREATE INDEX IF NOT EXISTS forge_events_incomplete
	ON forge_events(status, created_at);

CREATE INDEX IF NOT EXISTS forge_events_task_status
	ON forge_events(task_id, status);

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

CREATE UNIQUE INDEX IF NOT EXISTS validation_runs_attempt_sequence ON validation_runs(attempt_id, sequence);
CREATE UNIQUE INDEX IF NOT EXISTS validation_runs_start_event ON validation_runs(start_event_id) WHERE start_event_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS validation_runs_result_event ON validation_runs(result_event_id) WHERE result_event_id IS NOT NULL;

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
    error TEXT NOT NULL DEFAULT ''
);
`

func Open(ctx context.Context, path string) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("store context is nil")
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("store path is empty")
	}

	db := sql.OpenDB(sqliteConnector{driver: &sqlite.Driver{}, dsn: sqliteDSN(path)})
	// The controller is single-replica today.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	closeOnError := func(err error) (*Store, error) {
		_ = db.Close()
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		return closeOnError(fmt.Errorf("ping SQLite store: %w", err))
	}
	var busyTimeout int
	if err := db.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
		return closeOnError(fmt.Errorf("read SQLite busy timeout: %w", err))
	}
	if busyTimeout != 5000 {
		return closeOnError(fmt.Errorf("SQLite busy timeout is %d, want 5000", busyTimeout))
	}
	var foreignKeys int
	if err := db.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
		return closeOnError(fmt.Errorf("read SQLite foreign keys: %w", err))
	}
	if foreignKeys != 1 {
		return closeOnError(fmt.Errorf("SQLite foreign keys is %d, want 1", foreignKeys))
	}
	var journalMode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return closeOnError(fmt.Errorf("enable SQLite WAL: %w", err))
	}
	if !strings.EqualFold(journalMode, "wal") {
		return closeOnError(fmt.Errorf("SQLite journal mode is %q, want wal", journalMode))
	}
	var version, userTables int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return closeOnError(fmt.Errorf("read SQLite schema version: %w", err))
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`).Scan(&userTables); err != nil {
		return closeOnError(fmt.Errorf("inspect SQLite schema: %w", err))
	}
	if version < 0 || version > 2 || version == 0 && userTables != 0 {
		return closeOnError(fmt.Errorf("unsupported SQLite schema version %d with %d user tables; recreate the controller PVC/database for schema version 2", version, userTables))
	}
	if version == 0 {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return closeOnError(fmt.Errorf("begin SQLite schema initialization: %w", err))
		}
		if _, err := tx.ExecContext(ctx, schema); err != nil {
			_ = tx.Rollback()
			return closeOnError(fmt.Errorf("create SQLite schema: %w", err))
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version = 2"); err != nil {
			_ = tx.Rollback()
			return closeOnError(fmt.Errorf("mark SQLite schema version: %w", err))
		}
		if err := tx.Commit(); err != nil {
			return closeOnError(fmt.Errorf("commit SQLite schema initialization: %w", err))
		}
	} else if version == 1 {
		if err := migrateSchemaV1ToV2(ctx, db); err != nil {
			return closeOnError(err)
		}
	}
	return &Store{db: db}, nil
}

func migrateSchemaV1ToV2(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin SQLite schema migration to version 2: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	hasPRTitle, err := migrationTableHasColumn(ctx, tx, "tasks", "pr_title")
	if err != nil {
		return fmt.Errorf("inspect tasks schema: %w", err)
	}
	if !hasPRTitle {
		if _, err := tx.ExecContext(ctx, "ALTER TABLE tasks ADD COLUMN pr_title TEXT NOT NULL DEFAULT ''"); err != nil {
			return fmt.Errorf("migrate tasks pr_title column: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE forge_events_v2 (
			id TEXT PRIMARY KEY,
			provider TEXT NOT NULL,
			kind TEXT NOT NULL,
			owner TEXT NOT NULL,
			repository TEXT NOT NULL,
			pull_request_number INTEGER NOT NULL DEFAULT 0 CHECK (pull_request_number >= 0),
			commit_sha TEXT NOT NULL,
			branch TEXT NOT NULL,
			comment_id INTEGER NOT NULL DEFAULT 0 CHECK (comment_id >= 0),
			comment_kind TEXT NOT NULL DEFAULT '',
			title TEXT NOT NULL DEFAULT '',
			body TEXT NOT NULL DEFAULT '',
			author TEXT NOT NULL DEFAULT '',
			url TEXT NOT NULL DEFAULT '',
			task_id TEXT REFERENCES tasks(id),
			attempt_id TEXT REFERENCES task_attempts(id),
			reply_draft TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'handled', 'failed')),
			attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			last_error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			handled_at TEXT,
			failed_at TEXT,
			next_attempt_at TEXT
		);
		INSERT INTO forge_events_v2
			(id, provider, kind, owner, repository, pull_request_number, commit_sha, branch,
			 comment_id, comment_kind, title, body, author, url, task_id, attempt_id,
			 status, attempts, last_error, created_at, updated_at, handled_at, failed_at, next_attempt_at)
		SELECT id, provider, kind, owner, repository, pull_request_number, commit_sha, branch,
		       comment_id, comment_kind, title, body, author, url, task_id, attempt_id,
		       status, attempts, last_error, created_at, updated_at, handled_at, failed_at, next_attempt_at
		FROM forge_events;
		DROP TABLE forge_events;
		ALTER TABLE forge_events_v2 RENAME TO forge_events;
		CREATE INDEX forge_events_incomplete ON forge_events(status, created_at);
		CREATE INDEX forge_events_task_status ON forge_events(task_id, status);
		PRAGMA user_version = 2;
	`); err != nil {
		return fmt.Errorf("migrate forge events to schema version 2: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit SQLite schema migration to version 2: %w", err)
	}
	return nil
}

func migrationTableHasColumn(ctx context.Context, tx *sql.Tx, table, column string) (_ bool, resultErr error) {
	rows, err := tx.QueryContext(ctx, `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		return false, fmt.Errorf("query migration table %q for column %q: %w", table, column, err)
	}
	defer func() { resultErr = errors.Join(resultErr, rows.Close()) }()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, fmt.Errorf("scan migration table %q for column %q: %w", table, column, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read migration table %q for column %q: %w", table, column, err)
	}
	return false, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// immediateTransaction runs the small set of writes that require SQLite's
// eager write lock. Cleanup uses a non-cancelled context so a cancelled caller
// cannot strand the pinned connection in a transaction.
func (s *Store) immediateTransaction(ctx context.Context, operation string, fn func(*sql.Conn) error) (resultErr error) {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", operation, err)
	}
	begun, committed := false, false
	defer func() {
		if begun && !committed {
			if _, err := conn.ExecContext(context.WithoutCancel(ctx), "ROLLBACK"); err != nil {
				resultErr = errors.Join(resultErr, fmt.Errorf("rollback %s: %w", operation, err))
				if discardErr := conn.Raw(func(any) error { return driver.ErrBadConn }); discardErr != nil && !errors.Is(discardErr, driver.ErrBadConn) {
					resultErr = errors.Join(resultErr, fmt.Errorf("discard %s connection: %w", operation, discardErr))
				}
			}
		}
		if err := conn.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close %s connection: %w", operation, err))
		}
	}()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin %s: %w", operation, err)
	}
	begun = true
	if err := fn(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fmt.Errorf("commit %s: %w", operation, err)
	}
	committed = true
	return nil
}

func (s *Store) CreateTask(ctx context.Context, params CreateTaskParams) (Task, error) {
	created, _, err := s.CreateTaskOnce(ctx, params)
	return created, err
}

// CreateTaskOnce creates a task or returns the task already associated with a
// generic idempotency key. The boolean reports whether this call inserted the task.
func (s *Store) CreateTaskOnce(ctx context.Context, params CreateTaskParams) (Task, bool, error) {
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
	if params.PRTitle != "" && strings.TrimSpace(params.PRTitle) == "" {
		return Task{}, false, errors.New("pr_title is empty")
	}
	if utf8.RuneCountInString(params.PRTitle) > 256 {
		return Task{}, false, errors.New("pr_title exceeds 256 characters")
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
			(id, repository, prompt, pr_title, state, created_at, updated_at, current_attempt_id)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, params.Repository, params.Prompt, params.PRTitle, task.RECEIVED,
		stamp(now), stamp(now), attemptID,
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
	if err := tx.Commit(); err != nil {
		return Task{}, false, fmt.Errorf("commit task creation: %w", err)
	}
	return Task{
		ID:               taskID,
		Repository:       params.Repository,
		Prompt:           params.Prompt,
		PRTitle:          params.PRTitle,
		State:            task.RECEIVED,
		CreatedAt:        now,
		UpdatedAt:        now,
		CurrentAttemptID: attemptID,
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
		SELECT tasks.id, tasks.repository, tasks.prompt, tasks.pr_title, tasks.state, tasks.created_at,
		       tasks.updated_at, tasks.current_attempt_id, tasks.cancellation_requested
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
		SELECT id, repository, prompt, pr_title, state, created_at, updated_at,
		       current_attempt_id, cancellation_requested
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
		SELECT tasks.id, tasks.repository, tasks.prompt, tasks.pr_title, tasks.state, tasks.created_at, tasks.updated_at,
		       tasks.current_attempt_id, tasks.cancellation_requested
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
		SELECT id, task_id, number, immutable, state, prompt, base_branch, task_branch, logs_exhausted, validation_state,
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
		       task_attempts.immutable, task_attempts.state, task_attempts.prompt,
		       task_attempts.base_branch, task_attempts.task_branch, task_attempts.logs_exhausted,
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
		SELECT id, task_id, number, immutable, state, prompt, base_branch, task_branch, logs_exhausted, validation_state,
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
		SELECT id, task_id, number, immutable, state, prompt, base_branch, task_branch, logs_exhausted, validation_state,
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

// RetryAttemptPlan reserves no state while allowing a controller to build the
// next attempt's immutable snapshot before the retry transaction.
type RetryAttemptPlan struct {
	Attempt           Attempt
	PreviousAttemptID string
}

func (s *Store) PlanRetryAttempt(ctx context.Context, taskID, idempotencyKey string) (RetryAttemptPlan, bool, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey != "" {
		attempt, err := s.GetRetryIntent(ctx, taskID, idempotencyKey)
		if err == nil {
			return RetryAttemptPlan{Attempt: attempt}, false, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return RetryAttemptPlan{}, false, err
		}
	}
	var state, currentAttemptID, prompt string
	var logsExhausted, pendingWorkerEvents int
	if err := s.db.QueryRowContext(ctx, `
		SELECT tasks.state, tasks.current_attempt_id, task_attempts.logs_exhausted,
		       task_attempts.prompt,
		       EXISTS(SELECT 1 FROM worker_log_events
		              WHERE task_id = tasks.id AND attempt_id = tasks.current_attempt_id AND processed = 0)
		FROM tasks JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id
		WHERE tasks.id = ?`, taskID).Scan(&state, &currentAttemptID, &logsExhausted, &prompt, &pendingWorkerEvents); errors.Is(err, sql.ErrNoRows) {
		return RetryAttemptPlan{}, false, fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return RetryAttemptPlan{}, false, fmt.Errorf("plan task retry: %w", err)
	}
	if err := (task.Machine{}).Retry(task.State(state), task.QUEUED); err != nil {
		return RetryAttemptPlan{}, false, err
	}
	if pendingWorkerEvents == 1 {
		return RetryAttemptPlan{}, false, fmt.Errorf("%w: current attempt %q has pending durable worker events", ErrConflict, currentAttemptID)
	}
	if logsExhausted != 1 {
		return RetryAttemptPlan{}, false, fmt.Errorf("%w: failed current attempt %q logs are not exhausted", ErrConflict, currentAttemptID)
	}
	var runningForgeEvents int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM forge_events WHERE task_id = ? AND attempt_id = ? AND status = 'running'`, taskID, currentAttemptID).Scan(&runningForgeEvents); err != nil {
		return RetryAttemptPlan{}, false, fmt.Errorf("plan running forge event retry: %w", err)
	}
	baseBranch, taskBranch := "", ""
	if runningForgeEvents == 0 {
		prompt = ""
	} else if err := s.db.QueryRowContext(ctx, `
		SELECT base_branch, head_branch FROM pull_requests
		WHERE attempt_id = ? AND state = 'open'`, currentAttemptID).Scan(&baseBranch, &taskBranch); errors.Is(err, sql.ErrNoRows) {
		return RetryAttemptPlan{}, false, fmt.Errorf("%w: running forge event batch has no open pull request", ErrConflict)
	} else if err != nil {
		return RetryAttemptPlan{}, false, fmt.Errorf("plan forge retry pull request: %w", err)
	}
	var number int
	if err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = ?`, taskID).Scan(&number); err != nil {
		return RetryAttemptPlan{}, false, fmt.Errorf("plan retry number: %w", err)
	}
	attemptID, err := newID("swe-attempt-")
	if err != nil {
		return RetryAttemptPlan{}, false, err
	}
	return RetryAttemptPlan{
		PreviousAttemptID: currentAttemptID,
		Attempt: Attempt{
			ID: attemptID, TaskID: taskID, Number: number, Immutable: true, State: task.QUEUED,
			Prompt: prompt, BaseBranch: baseBranch, TaskBranch: taskBranch,
		},
	}, true, nil
}

// GetRetryIntent returns the immutable attempt originally assigned to an
// idempotency key, regardless of the task's current attempt.
func (s *Store) GetRetryIntent(ctx context.Context, taskID, idempotencyKey string) (Attempt, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey == "" {
		return Attempt{}, fmt.Errorf("%w: empty retry idempotency key", ErrNotFound)
	}
	attempt, err := scanAttempt(s.db.QueryRowContext(ctx, `
		SELECT a.id, a.task_id, a.number, a.immutable, a.state, a.prompt, a.base_branch, a.task_branch, a.logs_exhausted,
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
	return s.retryTaskOnce(ctx, taskID, idempotencyKey, nil)
}

// StartPlannedRetryTaskOnce atomically selects a pre-snapshotted retry.
func (s *Store) StartPlannedRetryTaskOnce(ctx context.Context, taskID, idempotencyKey string, plan RetryAttemptPlan) (Attempt, bool, error) {
	return s.retryTaskOnce(ctx, taskID, idempotencyKey, &plan)
}

func (s *Store) retryTaskOnce(ctx context.Context, taskID, idempotencyKey string, plan *RetryAttemptPlan) (Attempt, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("begin task retry: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	idempotencyKey = strings.TrimSpace(idempotencyKey)
	if idempotencyKey != "" {
		attempt, err := scanAttempt(tx.QueryRowContext(ctx, `
			SELECT a.id, a.task_id, a.number, a.immutable, a.state, a.prompt, a.base_branch, a.task_branch, a.logs_exhausted,
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

	var cancellationRequested, logsExhausted, pendingWorkerEvents int
	var state, currentAttemptID, prompt string
	if err := tx.QueryRowContext(ctx,
		`SELECT tasks.state, tasks.cancellation_requested, tasks.current_attempt_id, task_attempts.logs_exhausted,
		        task_attempts.prompt,
		        EXISTS(SELECT 1 FROM worker_log_events
		               WHERE task_id = tasks.id AND attempt_id = tasks.current_attempt_id AND processed = 0)
		 FROM tasks JOIN task_attempts ON task_attempts.id = tasks.current_attempt_id WHERE tasks.id = ?`, taskID,
	).Scan(&state, &cancellationRequested, &currentAttemptID, &logsExhausted, &prompt, &pendingWorkerEvents); errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, false, fmt.Errorf("%w: %s", ErrNotFound, taskID)
	} else if err != nil {
		return Attempt{}, false, fmt.Errorf("read task for retry: %w", err)
	}
	if err := (task.Machine{}).Retry(task.State(state), task.QUEUED); err != nil {
		return Attempt{}, false, err
	}
	if pendingWorkerEvents == 1 {
		return Attempt{}, false, fmt.Errorf("%w: current attempt %q has pending durable worker events", ErrConflict, currentAttemptID)
	}
	if logsExhausted != 1 {
		return Attempt{}, false, fmt.Errorf("%w: failed current attempt %q logs are not exhausted", ErrConflict, currentAttemptID)
	}
	var runningForgeEvents int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM forge_events
		WHERE task_id = ? AND attempt_id = ? AND status = 'running'`, taskID, currentAttemptID).Scan(&runningForgeEvents); err != nil {
		return Attempt{}, false, fmt.Errorf("read running forge event for retry: %w", err)
	}
	retryPrompt, retryBaseBranch, retryTaskBranch := "", "", ""
	if runningForgeEvents > 0 {
		if err := tx.QueryRowContext(ctx, `
			SELECT base_branch, head_branch FROM pull_requests
			WHERE attempt_id = ? AND state = 'open'`, currentAttemptID).Scan(&retryBaseBranch, &retryTaskBranch); errors.Is(err, sql.ErrNoRows) {
			return Attempt{}, false, fmt.Errorf("%w: running forge event batch has no open pull request", ErrConflict)
		} else if err != nil {
			return Attempt{}, false, fmt.Errorf("read forge retry pull request: %w", err)
		}
		retryPrompt = prompt
	}

	var number int
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(number), 0) + 1 FROM task_attempts WHERE task_id = ?`, taskID).Scan(&number); err != nil {
		return Attempt{}, false, fmt.Errorf("read retry number: %w", err)
	}
	attempt := Attempt{
		TaskID: taskID, Number: number, Immutable: true, State: task.QUEUED,
		Prompt: retryPrompt, BaseBranch: retryBaseBranch, TaskBranch: retryTaskBranch,
	}
	if plan == nil {
		if runningForgeEvents > 0 {
			return Attempt{}, false, fmt.Errorf("%w: running forge follow-up retry requires a complete planned snapshot", ErrConflict)
		}
		attempt.ID, err = newID("swe-attempt-")
		if err != nil {
			return Attempt{}, false, err
		}
	} else {
		attempt = plan.Attempt
		if plan.PreviousAttemptID != currentAttemptID || attempt.TaskID != taskID || attempt.Number != number || !attempt.Immutable || attempt.State != task.QUEUED ||
			attempt.Prompt != retryPrompt || attempt.BaseBranch != retryBaseBranch || attempt.TaskBranch != retryTaskBranch || strings.TrimSpace(attempt.ID) == "" {
			return Attempt{}, false, fmt.Errorf("%w: retry plan no longer matches current task attempt", ErrConflict)
		}
		if len(attempt.ManifestJSON) == 0 || len(attempt.ResourceSnapshot) == 0 || strings.TrimSpace(attempt.ConfigDigest) == "" {
			return Attempt{}, false, fmt.Errorf("%w: planned retry attempt %q has no complete immutable snapshot", ErrConflict, attempt.ID)
		}
	}
	now := time.Now().UTC()
	if attempt.ManifestJSON == nil {
		attempt.ManifestJSON = []byte{}
	}
	if attempt.ResourceSnapshot == nil {
		attempt.ResourceSnapshot = []byte{}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_attempts
			(id, task_id, number, immutable, state, prompt, base_branch, task_branch,
			 manifest_json, resource_snapshot, config_digest, created_at)
		VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)`, attempt.ID, taskID, number, task.QUEUED,
		attempt.Prompt, attempt.BaseBranch, attempt.TaskBranch, attempt.ManifestJSON, attempt.ResourceSnapshot, attempt.ConfigDigest, stamp(now)); err != nil {
		return Attempt{}, false, fmt.Errorf("insert retry attempt: %w", err)
	}
	if runningForgeEvents > 0 {
		result, err := tx.ExecContext(ctx, `
			INSERT INTO pull_requests
				(attempt_id, state, number, url, title, head_branch, base_branch, error)
			SELECT ?, state, number, url, title, head_branch, base_branch, error
			FROM pull_requests WHERE attempt_id = ? AND state = 'open'`, attempt.ID, currentAttemptID)
		if err != nil {
			return Attempt{}, false, fmt.Errorf("copy forge retry pull request: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != 1 {
			return Attempt{}, false, fmt.Errorf("%w: running forge event batch has no open pull request", ErrConflict)
		}
		result, err = tx.ExecContext(ctx, `
			UPDATE forge_events SET attempt_id = ?, updated_at = ?
			WHERE task_id = ? AND attempt_id = ? AND status = 'running'`,
			attempt.ID, stamp(now), taskID, currentAttemptID)
		if err != nil {
			return Attempt{}, false, fmt.Errorf("rebind forge event retry: %w", err)
		}
		if affected, err := result.RowsAffected(); err != nil || affected != int64(runningForgeEvents) {
			return Attempt{}, false, fmt.Errorf("%w: running forge event batch changed during retry", ErrConflict)
		}
	}
	result, err := tx.ExecContext(ctx,
		"UPDATE tasks SET state = ?, current_attempt_id = ?, cancellation_requested = 0, updated_at = ? WHERE id = ? AND state = ?",
		task.QUEUED, attempt.ID, stamp(now), taskID, task.FAILED,
	)
	if err != nil {
		return Attempt{}, false, fmt.Errorf("update current retry attempt: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return Attempt{}, false, fmt.Errorf("%w: task %q changed during retry", ErrConflict, taskID)
	}
	eventID, err := newID("swe-event-")
	if err != nil {
		return Attempt{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO task_events
			(id, task_id, attempt_id, occurred_at, from_state, to_state, reason, trigger, resource_identity, metadata, error)
		VALUES (?, ?, ?, ?, ?, ?, 'retry requested', 'api', '{}', '{}', '')`,
		eventID, taskID, attempt.ID, stamp(now), task.FAILED, task.QUEUED); err != nil {
		return Attempt{}, false, fmt.Errorf("append retry event: %w", err)
	}
	if idempotencyKey != "" {
		if _, err := tx.ExecContext(ctx, `INSERT INTO retry_intents (task_id, idempotency_key, attempt_id, created_at) VALUES (?, ?, ?, ?)`,
			taskID, idempotencyKey, attempt.ID, stamp(now)); err != nil {
			return Attempt{}, false, fmt.Errorf("record retry intent: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Attempt{}, false, fmt.Errorf("commit task retry: %w", err)
	}
	attempt.CreatedAt = now
	return attempt, true, nil
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

// RecordPullRequestCandidate atomically records a freshly fetched published
// branch and reported pull-request identity without marking either validated.
// A later candidate may replace only the commit SHA.
func (s *Store) RecordPullRequestCandidate(ctx context.Context, git GitResult, pullRequest PullRequest) error {
	if strings.TrimSpace(git.AttemptID) == "" || git.State != "candidate" || strings.TrimSpace(git.Branch) == "" || !protocol.FullLowerGitObjectID(git.CommitSHA) || git.Error != "" {
		return errors.New("candidate Git result is incomplete or invalid")
	}
	if pullRequest.AttemptID != git.AttemptID || pullRequest.State != "reported" || pullRequest.Number <= 0 || pullRequest.URL != "" || pullRequest.Title != "" ||
		pullRequest.HeadBranch != git.Branch || strings.TrimSpace(pullRequest.BaseBranch) == "" || pullRequest.Error != "" {
		return fmt.Errorf("%w: reported pull request is incomplete or does not match candidate Git result", ErrConflict)
	}
	return s.immediateTransaction(ctx, "pull request candidate", func(conn *sql.Conn) error {
		return recordPullRequestCandidateTx(ctx, conn, git, pullRequest)
	})
}

func recordPullRequestCandidateTx(ctx context.Context, tx *sql.Conn, git GitResult, pullRequest PullRequest) error {
	taskID, baseBranch, taskBranch, err := readAttemptBranchIdentityTx(ctx, tx, git.AttemptID)
	if err != nil {
		return err
	}
	if baseBranch == "" || taskBranch == "" || git.Branch != taskBranch || pullRequest.HeadBranch != taskBranch || pullRequest.BaseBranch != baseBranch {
		return fmt.Errorf("%w: candidate does not match immutable attempt branch identity", ErrConflict)
	}
	durableGit, hasGit, err := readGitResultTx(ctx, tx, git.AttemptID)
	if err != nil {
		return err
	}
	durablePR, hasPR, err := readPullRequestTx(ctx, tx, git.AttemptID)
	if err != nil {
		return err
	}
	if hasGit && durableGit.State == "pushed" {
		if !hasPR || durableGit.Branch != git.Branch || durableGit.CommitSHA != git.CommitSHA || durableGit.Error != "" ||
			durablePR.State != "open" || durablePR.Number != pullRequest.Number || durablePR.HeadBranch != pullRequest.HeadBranch || durablePR.BaseBranch != pullRequest.BaseBranch {
			return fmt.Errorf("%w: candidate for attempt %q conflicts with durable verified data", ErrConflict, git.AttemptID)
		}
		return nil
	}
	if hasGit && (durableGit.State != "candidate" || durableGit.Branch != git.Branch || durableGit.Error != "") {
		return fmt.Errorf("%w: candidate for attempt %q conflicts with durable Git data", ErrConflict, git.AttemptID)
	}
	priorNumber, err := priorPullRequestNumberTx(ctx, tx, taskID, git.AttemptID, taskBranch, baseBranch)
	if err != nil {
		return err
	}
	if err := recordCandidatePullRequestTx(ctx, tx, git.AttemptID, taskBranch, baseBranch, priorNumber, durablePR, hasPR, pullRequest); err != nil {
		return err
	}
	return recordCandidateGitTx(ctx, tx, durableGit, hasGit, git)
}

func recordCandidatePullRequestTx(ctx context.Context, tx *sql.Conn, attemptID, taskBranch, baseBranch string, priorNumber int, durable PullRequest, hasDurable bool, reported PullRequest) error {
	if priorNumber > 0 {
		if !hasDurable || durable.State != "open" || durable.Number != priorNumber || durable.Number != reported.Number ||
			durable.HeadBranch != taskBranch || durable.BaseBranch != baseBranch || durable.Error != "" {
			return fmt.Errorf("%w: copied pull request for follow-up attempt %q does not match candidate identity", ErrConflict, attemptID)
		}
		return nil
	}
	if hasDurable {
		if durable.State != "reported" || durable.Number != reported.Number || durable.URL != "" || durable.Title != "" ||
			durable.HeadBranch != taskBranch || durable.BaseBranch != baseBranch || durable.Error != "" {
			return fmt.Errorf("%w: reported pull request for attempt %q conflicts with durable data", ErrConflict, attemptID)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO pull_requests (attempt_id, state, number, head_branch, base_branch) VALUES (?, 'reported', ?, ?, ?)`,
		reported.AttemptID, reported.Number, reported.HeadBranch, reported.BaseBranch); err != nil {
		return fmt.Errorf("insert reported pull request: %w", err)
	}
	return nil
}

func recordCandidateGitTx(ctx context.Context, tx *sql.Conn, durable GitResult, hasDurable bool, candidate GitResult) error {
	if hasDurable {
		if _, err := tx.ExecContext(ctx, `UPDATE git_results SET commit_sha = ? WHERE attempt_id = ? AND state = 'candidate' AND branch = ?`, candidate.CommitSHA, candidate.AttemptID, durable.Branch); err != nil {
			return fmt.Errorf("update candidate Git result: %w", err)
		}
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO git_results (attempt_id, state, branch, commit_sha) VALUES (?, 'candidate', ?, ?)`, candidate.AttemptID, candidate.Branch, candidate.CommitSHA); err != nil {
		return fmt.Errorf("insert candidate Git result: %w", err)
	}
	return nil
}

// RecordVerifiedPullRequest atomically promotes only the exact latest
// candidate to pushed Git and provider-verified open pull-request state.
func (s *Store) RecordVerifiedPullRequest(ctx context.Context, git GitResult, pullRequest PullRequest) error {
	if strings.TrimSpace(git.AttemptID) == "" || git.State != "pushed" || strings.TrimSpace(git.Branch) == "" || !protocol.FullLowerGitObjectID(git.CommitSHA) || git.Error != "" {
		return errors.New("verified Git result is incomplete or invalid")
	}
	if pullRequest.AttemptID != git.AttemptID || pullRequest.State != "open" || pullRequest.Number <= 0 ||
		strings.TrimSpace(pullRequest.URL) == "" || strings.TrimSpace(pullRequest.Title) == "" ||
		pullRequest.HeadBranch != git.Branch || strings.TrimSpace(pullRequest.BaseBranch) == "" || pullRequest.Error != "" {
		return fmt.Errorf("%w: verified pull request is incomplete or does not match Git result", ErrConflict)
	}
	if err := forge.ValidatePullRequestMetadata(pullRequest.URL, pullRequest.Title); err != nil {
		return fmt.Errorf("%w: verified pull request metadata: %w", ErrConflict, err)
	}
	return s.immediateTransaction(ctx, "verified pull request", func(conn *sql.Conn) error {
		return recordVerifiedPullRequestTx(ctx, conn, git, pullRequest)
	})
}

func recordVerifiedPullRequestTx(ctx context.Context, conn *sql.Conn, git GitResult, pullRequest PullRequest) error {
	_, baseBranch, taskBranch, err := readAttemptBranchIdentityTx(ctx, conn, git.AttemptID)
	if err != nil {
		return err
	}
	if git.Branch != taskBranch || pullRequest.HeadBranch != taskBranch || pullRequest.BaseBranch != baseBranch {
		return fmt.Errorf("%w: verified result does not match immutable attempt branch identity", ErrConflict)
	}
	durableGit, hasGit, err := readGitResultTx(ctx, conn, git.AttemptID)
	if err != nil {
		return err
	}
	durablePR, hasPR, err := readPullRequestTx(ctx, conn, git.AttemptID)
	if err != nil {
		return err
	}
	if hasGit && durableGit.State == "pushed" {
		if !hasPR || durableGit != git || durablePR != pullRequest {
			return fmt.Errorf("%w: verified result for attempt %q conflicts with durable data", ErrConflict, git.AttemptID)
		}
		return nil
	}
	if !hasGit || !hasPR || durableGit.State != "candidate" || durableGit.Branch != git.Branch || durableGit.CommitSHA != git.CommitSHA || durableGit.Error != "" ||
		(durablePR.State != "reported" && durablePR.State != "open") || durablePR.Number != pullRequest.Number || durablePR.HeadBranch != pullRequest.HeadBranch ||
		durablePR.BaseBranch != pullRequest.BaseBranch || durablePR.Error != "" {
		return fmt.Errorf("%w: verified receipt does not match the latest durable candidate", ErrConflict)
	}
	if _, err := conn.ExecContext(ctx, `UPDATE pull_requests SET state = 'open', number = ?, url = ?, title = ?, head_branch = ?, base_branch = ?, error = '' WHERE attempt_id = ?`,
		pullRequest.Number, pullRequest.URL, pullRequest.Title, pullRequest.HeadBranch, pullRequest.BaseBranch, pullRequest.AttemptID); err != nil {
		return fmt.Errorf("promote verified pull request: %w", err)
	}
	result, err := conn.ExecContext(ctx, `UPDATE git_results SET state = 'pushed', error = '' WHERE attempt_id = ? AND state = 'candidate' AND branch = ? AND commit_sha = ?`, git.AttemptID, git.Branch, git.CommitSHA)
	if err != nil {
		return fmt.Errorf("promote verified Git result: %w", err)
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return fmt.Errorf("%w: latest candidate changed during verified promotion", ErrConflict)
	}
	return nil
}

func readAttemptBranchIdentityTx(ctx context.Context, tx *sql.Conn, attemptID string) (taskID, baseBranch, taskBranch string, resultErr error) {
	var manifestJSON []byte
	if err := tx.QueryRowContext(ctx, `SELECT task_id, base_branch, task_branch, manifest_json FROM task_attempts WHERE id = ?`, attemptID).
		Scan(&taskID, &baseBranch, &taskBranch, &manifestJSON); errors.Is(err, sql.ErrNoRows) {
		return "", "", "", fmt.Errorf("%w: attempt %s", ErrNotFound, attemptID)
	} else if err != nil {
		return "", "", "", fmt.Errorf("read pull request attempt: %w", err)
	}
	if baseBranch == "" || taskBranch == "" {
		var manifest protocol.TaskManifest
		if len(manifestJSON) == 0 || json.Unmarshal(manifestJSON, &manifest) != nil {
			return "", "", "", fmt.Errorf("%w: attempt %q has no decodable immutable branch identity", ErrConflict, attemptID)
		}
		if baseBranch == "" {
			baseBranch = manifest.BaseBranch
		}
		if taskBranch == "" {
			taskBranch = manifest.TaskBranch
		}
	}
	return taskID, baseBranch, taskBranch, nil
}

func priorPullRequestNumberTx(ctx context.Context, tx *sql.Conn, taskID, attemptID, taskBranch, baseBranch string) (int, error) {
	var priorCount int
	var priorMinimum, priorMaximum sql.NullInt64
	err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*), MIN(COALESCE(p.number, 0)), MAX(COALESCE(p.number, 0)) FROM pull_requests p
		JOIN task_attempts a ON a.id = p.attempt_id
		WHERE a.task_id = ? AND p.attempt_id <> ? AND p.state = 'open' AND p.head_branch = ? AND p.base_branch = ?`,
		taskID, attemptID, taskBranch, baseBranch).Scan(&priorCount, &priorMinimum, &priorMaximum)
	if err != nil {
		return 0, fmt.Errorf("read prior pull request identity: %w", err)
	}
	priorNumber := 0
	if priorCount > 0 {
		if !priorMinimum.Valid || !priorMaximum.Valid || priorMinimum.Int64 <= 0 || priorMinimum.Int64 != priorMaximum.Int64 {
			return 0, fmt.Errorf("%w: prior open pull requests have invalid or conflicting identity", ErrConflict)
		}
		priorNumber = int(priorMinimum.Int64)
	}

	return priorNumber, nil
}

func readGitResultTx(ctx context.Context, tx *sql.Conn, attemptID string) (GitResult, bool, error) {
	var result GitResult
	err := tx.QueryRowContext(ctx, `SELECT attempt_id, state, branch, commit_sha, error FROM git_results WHERE attempt_id = ?`, attemptID).
		Scan(&result.AttemptID, &result.State, &result.Branch, &result.CommitSHA, &result.Error)
	if errors.Is(err, sql.ErrNoRows) {
		return GitResult{}, false, nil
	}
	if err != nil {
		return GitResult{}, false, fmt.Errorf("read durable verified Git result: %w", err)
	}
	return result, true, nil
}

func readPullRequestTx(ctx context.Context, tx *sql.Conn, attemptID string) (PullRequest, bool, error) {
	var result PullRequest
	var number sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT attempt_id, state, number, url, title, head_branch, base_branch, error FROM pull_requests WHERE attempt_id = ?`, attemptID).
		Scan(&result.AttemptID, &result.State, &number, &result.URL, &result.Title, &result.HeadBranch, &result.BaseBranch, &result.Error)
	if errors.Is(err, sql.ErrNoRows) {
		return PullRequest{}, false, nil
	}
	if err != nil {
		return PullRequest{}, false, fmt.Errorf("read durable verified pull request: %w", err)
	}
	if number.Valid {
		result.Number = int(number.Int64)
	}
	return result, true, nil
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

// LatestVerifiedPullRequestGitResult returns the newest durable pushed result
// from the exact pull-request number and branch lineage.
func (s *Store) LatestVerifiedPullRequestGitResult(ctx context.Context, taskID string, pullRequest PullRequest, excludeAttemptID string) (GitResult, error) {
	return s.latestPullRequestGitResult(ctx, taskID, pullRequest, excludeAttemptID, false)
}

// LatestPullRequestGitResult returns the newest published candidate or pushed
// result from the exact pull-request lineage.
func (s *Store) LatestPullRequestGitResult(ctx context.Context, taskID string, pullRequest PullRequest, excludeAttemptID string) (GitResult, error) {
	return s.latestPullRequestGitResult(ctx, taskID, pullRequest, excludeAttemptID, true)
}

func (s *Store) latestPullRequestGitResult(ctx context.Context, taskID string, pullRequest PullRequest, excludeAttemptID string, includeCandidate bool) (GitResult, error) {
	if strings.TrimSpace(taskID) == "" || pullRequest.Number <= 0 || strings.TrimSpace(pullRequest.HeadBranch) == "" || strings.TrimSpace(pullRequest.BaseBranch) == "" {
		return GitResult{}, errors.New("complete task and pull request lineage is required")
	}
	var result GitResult
	err := s.db.QueryRowContext(ctx, `
		SELECT g.attempt_id, g.state, g.branch, g.commit_sha, g.error
		FROM task_attempts a
		JOIN pull_requests p ON p.attempt_id = a.id
		JOIN git_results g ON g.attempt_id = a.id
		WHERE a.task_id = ? AND a.id <> ?
		  AND p.state IN ('reported', 'open') AND (? OR p.state = 'open')
		  AND p.number = ? AND p.head_branch = ? AND p.base_branch = ? AND p.error = ''
		  AND g.state IN ('candidate', 'pushed') AND (? OR g.state = 'pushed')
		  AND g.branch = p.head_branch AND g.error = ''
		ORDER BY a.number DESC LIMIT 1`,
		taskID, excludeAttemptID, includeCandidate, pullRequest.Number, pullRequest.HeadBranch, pullRequest.BaseBranch, includeCandidate,
	).Scan(&result.AttemptID, &result.State, &result.Branch, &result.CommitSHA, &result.Error)
	if errors.Is(err, sql.ErrNoRows) {
		if !includeCandidate {
			return GitResult{}, fmt.Errorf("%w: task %q has no durable pushed Git result for pull request %d", ErrNotFound, taskID, pullRequest.Number)
		}
		return GitResult{}, fmt.Errorf("%w: task %q has no durable published Git result for pull request %d", ErrNotFound, taskID, pullRequest.Number)
	}
	if err != nil {
		if !includeCandidate {
			return GitResult{}, fmt.Errorf("get latest verified pull request Git result: %w", err)
		}
		return GitResult{}, fmt.Errorf("get latest pull request Git result: %w", err)
	}
	if !protocol.FullLowerGitObjectID(result.CommitSHA) {
		if !includeCandidate {
			return GitResult{}, fmt.Errorf("%w: durable pushed Git result for pull request %d has invalid commit SHA", ErrConflict, pullRequest.Number)
		}
		return GitResult{}, fmt.Errorf("%w: durable published Git result for pull request %d has invalid commit SHA", ErrConflict, pullRequest.Number)
	}
	return result, nil
}

func (s *Store) GetPullRequest(ctx context.Context, attemptID string) (PullRequest, error) {
	var result PullRequest
	var number sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT attempt_id, state, number, url, title, head_branch, base_branch, error
		FROM pull_requests WHERE attempt_id = ?`, attemptID).Scan(
		&result.AttemptID, &result.State, &number, &result.URL, &result.Title,
		&result.HeadBranch, &result.BaseBranch, &result.Error,
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
	return result, nil
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
	SELECT id, repository, prompt, pr_title, state, created_at, updated_at,
	       current_attempt_id, cancellation_requested
	FROM tasks WHERE id = ?`

type scanner interface {
	Scan(dest ...any) error
}

func scanTask(row scanner) (Task, error) {
	var result Task
	var state, createdAt, updatedAt string
	var cancellationRequested int
	if err := row.Scan(
		&result.ID, &result.Repository, &result.Prompt, &result.PRTitle, &state, &createdAt, &updatedAt,
		&result.CurrentAttemptID, &cancellationRequested,
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
	result.CancellationRequested = cancellationRequested == 1
	return result, nil
}

func scanAttempt(row scanner) (Attempt, error) {
	var result Attempt
	var state, createdAt string
	var immutable, logsExhausted int
	if err := row.Scan(
		&result.ID, &result.TaskID, &result.Number, &immutable, &state,
		&result.Prompt, &result.BaseBranch, &result.TaskBranch, &logsExhausted,
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
