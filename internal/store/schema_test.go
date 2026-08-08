package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenInitializesAndReopensSchemaVersionTwo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.sqlite")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	var version int
	if err := db.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 2 {
		t.Fatalf("fresh user_version = %d, %v; want 2", version, err)
	}
	if !sqliteTableHasColumn(t, db.db, "forge_events", "reply_draft") {
		t.Fatal("fresh forge_events table has no reply_draft column")
	}
	if sqliteHasUniqueIndexOnColumn(t, db.db, "forge_events", "attempt_id") {
		t.Fatal("fresh forge_events table still has a unique attempt_id index")
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fresh store: %v", err)
	}
	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen version 2 store: %v", err)
	}
	defer db.Close()
	if _, err := db.CreateTask(context.Background(), CreateTaskParams{Repository: "widget", Prompt: "fix"}); err != nil {
		t.Fatalf("use reopened store: %v", err)
	}
}

func TestOpenRejectsUnknownSchemaVersions(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int
	}{
		{name: "future", version: 3},
		{name: "negative", version: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unknown.sqlite")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatalf("open unknown-version store: %v", err)
			}
			if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", test.version)); err != nil {
				t.Fatalf("mark unknown schema: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close unknown-version store: %v", err)
			}
			_, err = Open(context.Background(), path)
			if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("schema version %d", test.version)) || !strings.Contains(err.Error(), "recreate") {
				t.Fatalf("Open() error = %v, want actionable unknown schema rejection", err)
			}
		})
	}
}

func TestOpenMigratesVersionOneForgeEventsWithoutLoss(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "version-one.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open version 1 database: %v", err)
	}
	legacySchema := `
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,
    repository TEXT NOT NULL,
    prompt TEXT NOT NULL,
    pr_title TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    current_attempt_id TEXT NOT NULL,
    cancellation_requested INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE task_attempts (
    id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL REFERENCES tasks(id),
    number INTEGER NOT NULL,
    immutable INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL,
    prompt TEXT NOT NULL DEFAULT '',
    base_branch TEXT NOT NULL DEFAULT '',
    task_branch TEXT NOT NULL DEFAULT '',
    logs_exhausted INTEGER NOT NULL DEFAULT 0,
    validation_state TEXT NOT NULL DEFAULT '',
    manifest_json BLOB NOT NULL DEFAULT '',
    resource_snapshot BLOB NOT NULL DEFAULT '',
    config_digest TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);
CREATE TABLE forge_events (
    id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    kind TEXT NOT NULL,
    owner TEXT NOT NULL,
    repository TEXT NOT NULL,
    pull_request_number INTEGER NOT NULL DEFAULT 0,
    commit_sha TEXT NOT NULL,
    branch TEXT NOT NULL,
    comment_id INTEGER NOT NULL DEFAULT 0,
    comment_kind TEXT NOT NULL DEFAULT '',
    title TEXT NOT NULL DEFAULT '',
    body TEXT NOT NULL DEFAULT '',
    author TEXT NOT NULL DEFAULT '',
    url TEXT NOT NULL DEFAULT '',
    task_id TEXT REFERENCES tasks(id),
    attempt_id TEXT UNIQUE REFERENCES task_attempts(id),
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    handled_at TEXT,
    failed_at TEXT,
    next_attempt_at TEXT
);`
	if _, err := legacy.ExecContext(ctx, legacySchema); err != nil {
		_ = legacy.Close()
		t.Fatalf("create version 1 schema: %v", err)
	}
	event := testForgeEvent("migrated-forge-event", "review_comment")
	created := stamp(time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC))
	if _, err := legacy.ExecContext(ctx, `
INSERT INTO forge_events
    (id, provider, kind, owner, repository, pull_request_number, commit_sha, branch,
     comment_id, comment_kind, title, body, author, url, status, attempts,
     last_error, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, '', ?, ?)`,
		event.ID, event.Provider, event.Kind, event.Owner, event.Repository, event.PullRequestNumber,
		event.CommitSHA, event.Branch, event.CommentID, event.CommentKind, event.Title, event.Body,
		event.Author, event.URL, created, created); err != nil {
		_ = legacy.Close()
		t.Fatalf("insert version 1 forge event: %v", err)
	}
	if _, err := legacy.ExecContext(ctx, "PRAGMA user_version = 1"); err != nil {
		_ = legacy.Close()
		t.Fatalf("mark version 1 database: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close version 1 database: %v", err)
	}

	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open migrated database: %v", err)
	}
	defer db.Close()
	var version int
	if err := db.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 2 {
		t.Fatalf("migrated user_version = %d, %v; want 2", version, err)
	}
	migrated, err := db.GetForgeEvent(ctx, event.ID)
	if err != nil {
		t.Fatalf("get migrated forge event: %v", err)
	}
	if migrated.ID != event.ID || migrated.Kind != event.Kind || migrated.Body != event.Body || migrated.CommitSHA != event.CommitSHA || migrated.Status != ForgeEventPending {
		t.Fatalf("migrated forge event = %#v; want preserved event %#v", migrated, event)
	}
}

func TestFreshSchemaContainsNoSlackObjectsOrColumns(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()

	var intentTables int
	if err := db.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type = 'table' AND name = 'task_create_intents'`).Scan(&intentTables); err != nil {
		t.Fatalf("check task_create_intents: %v", err)
	}
	if intentTables != 1 {
		t.Fatalf("task_create_intents tables = %d, want 1", intentTables)
	}

	for _, object := range readSQLiteSchemaObjects(t, db.db, ctx) {
		if strings.Contains(strings.ToLower(object.name), "slack") {
			t.Errorf("SQLite %s %q contains Slack", object.objectType, object.name)
		}
		if object.objectType == "table" {
			checkSQLiteTableColumns(t, db.db, ctx, object.name)
		}
	}
}

type sqliteSchemaObject struct {
	objectType string
	name       string
}

func readSQLiteSchemaObjects(t *testing.T, db *sql.DB, ctx context.Context) []sqliteSchemaObject {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
		SELECT type, name FROM sqlite_master
		WHERE type IN ('table', 'index') AND name NOT LIKE 'sqlite_%'
		ORDER BY type, name`)
	if err != nil {
		t.Fatalf("list SQLite schema objects: %v", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			t.Errorf("close SQLite schema objects: %v", err)
		}
	}()

	var objects []sqliteSchemaObject
	for rows.Next() {
		var objectType, name string
		if err := rows.Scan(&objectType, &name); err != nil {
			t.Fatalf("scan SQLite schema object: %v", err)
		}
		objects = append(objects, sqliteSchemaObject{objectType: objectType, name: name})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read SQLite schema objects: %v", err)
	}
	return objects
}

func checkSQLiteTableColumns(t *testing.T, db *sql.DB, ctx context.Context, tableName string) {
	t.Helper()
	quotedName := strings.ReplaceAll(tableName, `"`, `""`)
	columns, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info("%s")`, quotedName))
	if err != nil {
		t.Fatalf("list columns for %s: %v", tableName, err)
	}
	defer func() {
		if err := columns.Close(); err != nil {
			t.Errorf("close %s columns: %v", tableName, err)
		}
	}()

	for columns.Next() {
		var cid, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue any
		if err := columns.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan %s column: %v", tableName, err)
		}
		if strings.Contains(strings.ToLower(columnName), "slack") {
			t.Errorf("SQLite table %q has Slack column %q", tableName, columnName)
		}
		if tableName == "pull_requests" && columnName == "notified_at" {
			t.Errorf("SQLite table %q has removed column %q", tableName, columnName)
		}
	}
	if err := columns.Err(); err != nil {
		t.Errorf("read %s columns: %v", tableName, err)
	}
}

func sqliteTableHasColumn(t *testing.T, db *sql.DB, tableName, wantColumn string) bool {
	t.Helper()
	quotedName := strings.ReplaceAll(tableName, `"`, `""`)
	rows, err := db.Query(fmt.Sprintf(`PRAGMA table_info("%s")`, quotedName))
	if err != nil {
		t.Fatalf("list columns for %s: %v", tableName, err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var columnName, columnType string
		var defaultValue any
		if err := rows.Scan(&cid, &columnName, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatalf("scan %s column: %v", tableName, err)
		}
		if columnName == wantColumn {
			return true
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read %s columns: %v", tableName, err)
	}
	return false
}

func sqliteHasUniqueIndexOnColumn(t *testing.T, db *sql.DB, tableName, wantColumn string) bool {
	t.Helper()
	for _, indexName := range sqliteUniqueIndexes(t, db, tableName) {
		if sqliteIndexHasColumn(t, db, tableName, indexName, wantColumn) {
			return true
		}
	}
	return false
}

func sqliteUniqueIndexes(t *testing.T, db *sql.DB, tableName string) []string {
	t.Helper()
	quotedTableName := strings.ReplaceAll(tableName, `"`, `""`)
	indexes, err := db.Query(fmt.Sprintf(`PRAGMA index_list("%s")`, quotedTableName))
	if err != nil {
		t.Fatalf("list %s indexes: %v", tableName, err)
	}
	var uniqueIndexes []string
	defer func() {
		if err := indexes.Close(); err != nil {
			t.Fatalf("close %s indexes: %v", tableName, err)
		}
	}()
	for indexes.Next() {
		var sequence, unique, partial int
		var indexName, origin string
		if err := indexes.Scan(&sequence, &indexName, &unique, &origin, &partial); err != nil {
			t.Fatalf("scan %s index: %v", tableName, err)
		}
		if unique == 0 {
			continue
		}
		uniqueIndexes = append(uniqueIndexes, indexName)
	}
	if err := indexes.Err(); err != nil {
		t.Fatalf("read %s indexes: %v", tableName, err)
	}
	return uniqueIndexes
}

func sqliteIndexHasColumn(t *testing.T, db *sql.DB, tableName, indexName, wantColumn string) bool {
	t.Helper()
	quotedIndexName := strings.ReplaceAll(indexName, `"`, `""`)
	columns, err := db.Query(fmt.Sprintf(`PRAGMA index_info("%s")`, quotedIndexName))
	if err != nil {
		t.Fatalf("list columns for %s index %s: %v", tableName, indexName, err)
	}
	defer func() {
		if err := columns.Close(); err != nil {
			t.Fatalf("close %s index %s columns: %v", tableName, indexName, err)
		}
	}()
	for columns.Next() {
		var indexSequence, columnID int
		var columnName string
		if err := columns.Scan(&indexSequence, &columnID, &columnName); err != nil {
			t.Fatalf("scan %s index %s column: %v", tableName, indexName, err)
		}
		if columnName == wantColumn {
			return true
		}
	}
	if err := columns.Err(); err != nil {
		t.Fatalf("read %s index %s columns: %v", tableName, indexName, err)
	}
	return false
}
