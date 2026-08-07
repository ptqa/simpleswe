package store

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenInitializesAndReopensSchemaVersionOne(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tasks.sqlite")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open fresh store: %v", err)
	}
	var version int
	if err := db.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil || version != 1 {
		t.Fatalf("fresh user_version = %d, %v; want 1", version, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close fresh store: %v", err)
	}
	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen version 1 store: %v", err)
	}
	defer db.Close()
	if _, err := db.CreateTask(context.Background(), CreateTaskParams{Repository: "widget", Prompt: "fix"}); err != nil {
		t.Fatalf("use reopened store: %v", err)
	}
}

func TestOpenRejectsUnknownFutureSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "future.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open future store: %v", err)
	}
	if _, err := db.Exec("PRAGMA user_version = 2"); err != nil {
		t.Fatalf("mark future schema: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close future store: %v", err)
	}
	_, err = Open(context.Background(), path)
	if err == nil || !strings.Contains(err.Error(), "schema version 2") || !strings.Contains(err.Error(), "recreate") {
		t.Fatalf("Open() error = %v, want actionable future schema rejection", err)
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
