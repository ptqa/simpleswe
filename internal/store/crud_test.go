package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/task"
)

func TestCreateTaskOnceUsesGenericIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	params := CreateTaskParams{
		Repository:     "repo",
		Prompt:         "generic request",
		IdempotencyKey: strings.Repeat("k", 256),
	}

	first, inserted, err := db.CreateTaskOnce(ctx, params)
	if err != nil || !inserted {
		t.Fatalf("first CreateTaskOnce() = %#v, %t, %v; want inserted task", first, inserted, err)
	}
	byKey, err := db.GetTaskByIdempotencyKey(ctx, params.IdempotencyKey)
	if err != nil || byKey.ID != first.ID {
		t.Fatalf("GetTaskByIdempotencyKey() = %#v, %v; want task %q", byKey, err, first.ID)
	}
	replayed, inserted, err := db.CreateTaskOnce(ctx, params)
	if err != nil || inserted || replayed.ID != first.ID {
		t.Fatalf("replayed CreateTaskOnce() = %#v, %t, %v; want original task without insert", replayed, inserted, err)
	}
	tasks, err := db.ListTasks(ctx)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks after replay = %#v, %v; want one atomically recorded task", tasks, err)
	}
	attempts, err := db.ListAttempts(ctx, first.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts after replay = %#v, %v; want one attempt", attempts, err)
	}
	if _, err := db.GetTaskByIdempotencyKey(ctx, " "); err == nil {
		t.Fatal("GetTaskByIdempotencyKey() accepted a blank key")
	}
	if _, err := db.GetTaskByIdempotencyKey(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing idempotency lookup error = %v, want ErrNotFound", err)
	}
}

func TestCreateTaskIdempotencySurvivesStoreRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tasks.sqlite")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	params := CreateTaskParams{Repository: "repo", Prompt: "original", IdempotencyKey: "create-1"}
	first, inserted, err := db.CreateTaskOnce(ctx, params)
	if err != nil || !inserted {
		t.Fatalf("first CreateTaskOnce() = %#v, %t, %v; want inserted task", first, inserted, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err = Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	byKey, err := db.GetTaskByIdempotencyKey(ctx, params.IdempotencyKey)
	if err != nil || byKey.ID != first.ID {
		t.Fatalf("reopened GetTaskByIdempotencyKey() = %#v, %v; want task %q", byKey, err, first.ID)
	}
	replayed, inserted, err := db.CreateTaskOnce(ctx, CreateTaskParams{
		Repository: "different-repo", Prompt: "different payload", IdempotencyKey: params.IdempotencyKey,
	})
	if err != nil || inserted || replayed.ID != first.ID || replayed.Repository != first.Repository || replayed.Prompt != first.Prompt {
		t.Fatalf("reopened replay = %#v, %t, %v; want original task %#v without insert", replayed, inserted, err, first)
	}
	tasks, err := db.ListTasks(ctx)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("tasks after reopened replay = %#v, %v; want one task", tasks, err)
	}
	attempts, err := db.ListAttempts(ctx, first.ID)
	if err != nil || len(attempts) != 1 || attempts[0].ID != first.CurrentAttemptID {
		t.Fatalf("attempts after reopened replay = %#v, %v; want original attempt %q", attempts, err, first.CurrentAttemptID)
	}
}

func TestCreateTaskOnceRejectsInvalidIdempotencyKeysWithoutWrites(t *testing.T) {
	tests := []struct {
		name   string
		params CreateTaskParams
	}{
		{
			name: "whitespace-only generic key",
			params: CreateTaskParams{
				Repository: "repo", Prompt: "prompt", IdempotencyKey: " \t\n",
			},
		},
		{
			name: "generic key over 256 characters",
			params: CreateTaskParams{
				Repository: "repo", Prompt: "prompt", IdempotencyKey: strings.Repeat("k", 257),
			},
		},
		{
			name: "whitespace-only PR title",
			params: CreateTaskParams{
				Repository: "repo", Prompt: "prompt", PRTitle: " \t\n",
			},
		},
		{
			name: "PR title over 256 characters",
			params: CreateTaskParams{
				Repository: "repo", Prompt: "prompt", PRTitle: strings.Repeat("t", 257),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db := openTestStore(t)
			created, inserted, err := db.CreateTaskOnce(context.Background(), test.params)
			if err == nil {
				t.Error("CreateTaskOnce() error = nil, want rejection")
			}
			if inserted || created.ID != "" {
				t.Errorf("CreateTaskOnce() = %#v, inserted %t; want no task", created, inserted)
			}
			assertNoCreateRows(t, db)
		})
	}
}

func assertNoCreateRows(t *testing.T, db *Store) {
	t.Helper()
	for _, table := range []string{"tasks", "task_attempts", "task_create_intents", "task_events"} {
		var count int
		if err := db.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 0 {
			t.Errorf("%s rows = %d, want 0", table, count)
		}
	}
}

func TestStoreOpenValidationEdges(t *testing.T) {
	ctx := context.Background()
	var nilContext context.Context
	if _, err := Open(nilContext, "store.sqlite"); err == nil {
		t.Fatal("Open accepted a nil context")
	}
	if _, err := Open(ctx, "  "); err == nil {
		t.Fatal("Open accepted an empty path")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := Open(cancelled, filepath.Join(t.TempDir(), "cancelled.sqlite")); err == nil {
		t.Fatal("Open ignored a cancelled context")
	}
	var nilStore *Store
	if err := nilStore.Close(); err != nil {
		t.Fatalf("close nil store: %v", err)
	}
}

func TestTaskCRUDAndSnapshotEdges(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	if _, err := db.CreateTask(ctx, CreateTaskParams{Prompt: "prompt"}); err == nil {
		t.Fatal("task creation accepted an empty repository")
	}
	if _, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo"}); err == nil {
		t.Fatal("task creation accepted an empty prompt")
	}
	first, inserted, err := db.CreateTaskOnce(ctx, CreateTaskParams{Repository: "repo-1", Prompt: "prompt-1"})
	if err != nil || !inserted {
		t.Fatalf("create task once = %#v, %t, %v", first, inserted, err)
	}
	if _, err := db.GetTask(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing task lookup = %v, want ErrNotFound", err)
	}

	second, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo-2", Prompt: "prompt-2"})
	if err != nil {
		t.Fatalf("create second task: %v", err)
	}
	tasks, err := db.ListTasks(ctx)
	if err != nil || len(tasks) != 2 || tasks[0].ID != second.ID || tasks[1].ID != first.ID {
		t.Fatalf("list tasks = %#v, %v", tasks, err)
	}
	active, err := db.ListActiveTasks(ctx)
	if err != nil || len(active) != 2 {
		t.Fatalf("initial active tasks = %#v, %v", active, err)
	}

	attempt, err := db.GetAttemptNumber(ctx, first.ID, 1)
	if err != nil || attempt.ID != first.CurrentAttemptID {
		t.Fatalf("get attempt one = %#v, %v", attempt, err)
	}
	if _, err := db.GetAttemptNumber(ctx, first.ID, 2); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing attempt number = %v, want ErrNotFound", err)
	}
	if _, err := db.GetAttempt(ctx, second.ID, attempt.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-task attempt lookup = %v, want ErrNotFound", err)
	}
	if _, err := db.CurrentAttempt(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing current attempt = %v, want ErrNotFound", err)
	}

	if err := db.SaveAttemptSnapshot(ctx, first.ID, attempt.ID, nil, []byte("resources"), "digest"); err == nil {
		t.Fatal("snapshot accepted missing manifest")
	}
	manifest, resources := []byte(`{"job":"one"}`), []byte(`{"secret":"one"}`)
	if err := db.SaveAttemptSnapshot(ctx, first.ID, attempt.ID, manifest, resources, "digest-1"); err != nil {
		t.Fatalf("save attempt snapshot: %v", err)
	}
	if err := db.SaveAttemptSnapshot(ctx, first.ID, attempt.ID, manifest, resources, "digest-1"); err != nil {
		t.Fatalf("replay identical attempt snapshot: %v", err)
	}
	if err := db.SaveAttemptSnapshot(ctx, first.ID, attempt.ID, manifest, []byte("changed"), "digest-1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("replace immutable snapshot = %v, want ErrConflict", err)
	}
	if err := db.SaveAttemptSnapshot(ctx, first.ID, "missing", manifest, resources, "digest-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("snapshot missing attempt = %v, want ErrNotFound", err)
	}
	storedAttempt, err := db.GetAttempt(ctx, first.ID, attempt.ID)
	if err != nil || string(storedAttempt.ManifestJSON) != string(manifest) || string(storedAttempt.ResourceSnapshot) != string(resources) || storedAttempt.ConfigDigest != "digest-1" {
		t.Fatalf("stored snapshot = %#v, %v", storedAttempt, err)
	}
}

func TestTaskListingRetryAndNotFoundEdges(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	first, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo-1", Prompt: "prompt-1"})
	if err != nil {
		t.Fatalf("create first task: %v", err)
	}
	second, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo-2", Prompt: "prompt-2"})
	if err != nil {
		t.Fatalf("create second task: %v", err)
	}
	attempt, err := db.CurrentAttempt(ctx, first.ID)
	if err != nil {
		t.Fatalf("get current attempt: %v", err)
	}
	active, err := db.ListActiveTasks(ctx)
	if err != nil || len(active) != 2 {
		t.Fatalf("initial active tasks = %#v, %v", active, err)
	}
	if err := db.Transition(ctx, first.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "failed", Trigger: "system"}); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	active, err = db.ListActiveTasks(ctx)
	if err != nil || len(active) != 2 {
		t.Fatalf("terminal task with pending logs should remain active: %#v, %v", active, err)
	}
	if err := db.MarkLogsExhausted(ctx, first.ID, attempt.ID); err != nil {
		t.Fatalf("mark logs exhausted: %v", err)
	}
	active, err = db.ListActiveTasks(ctx)
	if err != nil || len(active) != 1 || active[0].ID != second.ID {
		t.Fatalf("active tasks after log barrier = %#v, %v", active, err)
	}
	if _, err := db.GetRetryIntent(ctx, first.ID, " "); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty retry intent = %v, want ErrNotFound", err)
	}
	if _, err := db.GetRetryIntent(ctx, first.ID, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing retry intent = %v, want ErrNotFound", err)
	}
	retry, created, err := db.RetryTaskOnce(ctx, first.ID, "retry-key")
	if err != nil || !created {
		t.Fatalf("create retry intent = %#v, %t, %v", retry, created, err)
	}
	intent, err := db.GetRetryIntent(ctx, first.ID, "retry-key")
	if err != nil || intent.ID != retry.ID {
		t.Fatalf("get retry intent = %#v, %v", intent, err)
	}
	if _, err := db.RetryTask(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retry missing task = %v, want ErrNotFound", err)
	}
	if err := db.RequestCancellation(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel missing task = %v, want ErrNotFound", err)
	}
	if err := db.MarkLogsExhausted(ctx, first.ID, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mark missing logs exhausted = %v, want ErrNotFound", err)
	}
}

func TestLogChunkValidationAndEmptyTails(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "logs"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.AppendLogChunk(ctx, "", created.CurrentAttemptID, []byte("log")); err == nil {
		t.Fatal("log chunk accepted an empty task ID")
	}
	if err := db.AppendLogChunk(ctx, created.ID, created.CurrentAttemptID, nil); err != nil {
		t.Fatalf("append empty log chunk: %v", err)
	}
	if err := db.AppendLogChunk(ctx, created.ID, "missing", []byte("log")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("append log to missing attempt = %v, want ErrNotFound", err)
	}
	if tail, err := db.ReadLogTail(ctx, created.ID, created.CurrentAttemptID, 0); err != nil || tail != "" {
		t.Fatalf("zero-line log tail = %q, %v", tail, err)
	}
	if _, err := db.ReadLogTail(ctx, created.ID, created.CurrentAttemptID, -1); err == nil {
		t.Fatal("log tail accepted negative lines")
	}
	if _, err := db.ReadLogTail(ctx, created.ID, "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("log tail for missing attempt = %v, want ErrNotFound", err)
	}
}
