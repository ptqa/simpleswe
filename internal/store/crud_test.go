package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/simpleswe/simpleswe/internal/task"
)

func TestStoreOpenAndInboxValidationEdges(t *testing.T) {
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

	db := openTestStore(t)
	if _, err := db.PutSlackInboxEvent(ctx, SlackInboxEvent{Kind: "mention"}); err == nil {
		t.Fatal("Slack inbox accepted an empty event ID")
	}
	if _, err := db.PutSlackInboxEvent(ctx, SlackInboxEvent{EventID: "event"}); err == nil {
		t.Fatal("Slack inbox accepted an empty kind")
	}
	if err := db.RecordRejectedSlackInboxEvent(ctx, "", "event", nil); err == nil {
		t.Fatal("rejected inbox accepted an empty event ID")
	}
	if err := db.RecordRejectedSlackInboxEvent(ctx, "event", "", nil); err == nil {
		t.Fatal("rejected inbox accepted an empty kind")
	}
	if err := db.StartSlackInboxAttempt(ctx, " "); err == nil {
		t.Fatal("inbox update accepted an empty event ID")
	}
	if err := db.MarkSlackInboxHandled(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mark missing inbox event = %v, want ErrNotFound", err)
	}
	if err := db.RecordRejectedSlackInboxEvent(ctx, "rejected", "event", nil); err != nil {
		t.Fatalf("record rejection without cause: %v", err)
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
	first, inserted, err := db.CreateTaskOnce(ctx, CreateTaskParams{Repository: "repo-1", Prompt: "prompt-1", SlackEventID: "slack-1"})
	if err != nil || !inserted {
		t.Fatalf("create task once = %#v, %t, %v", first, inserted, err)
	}
	bySlack, err := db.GetTaskBySlackEventID(ctx, "slack-1")
	if err != nil || bySlack.ID != first.ID {
		t.Fatalf("get by Slack event = %#v, %v", bySlack, err)
	}
	if _, err := db.GetTaskBySlackEventID(ctx, " "); err == nil {
		t.Fatal("Slack lookup accepted an empty event ID")
	}
	if _, err := db.GetTaskBySlackEventID(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Slack lookup = %v, want ErrNotFound", err)
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

func TestPullRequestCRUDAndNotificationEdges(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt := created.CurrentAttemptID
	if _, err := db.GetGitResult(ctx, attempt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing Git result = %v, want ErrNotFound", err)
	}
	if err := db.RecordGitResult(ctx, GitResult{}); err == nil {
		t.Fatal("Git result accepted an empty attempt ID")
	}
	if err := db.RecordGitResult(ctx, GitResult{AttemptID: attempt}); err == nil {
		t.Fatal("Git result accepted an empty state")
	}
	if _, err := db.ReservePullRequest(ctx, "", "title", "head", "main"); err == nil {
		t.Fatal("pull request reservation accepted an empty attempt ID")
	}
	reserved, err := db.ReservePullRequest(ctx, attempt, "title", "work/branch", "main")
	if err != nil || !reserved {
		t.Fatalf("reserve pull request = %t, %v", reserved, err)
	}
	reserved, err = db.ReservePullRequest(ctx, attempt, "replacement", "other", "main")
	if err != nil || reserved {
		t.Fatalf("duplicate pull request reservation = %t, %v", reserved, err)
	}
	creating, err := db.GetPullRequest(ctx, attempt)
	if err != nil || creating.State != "creating" || creating.Title != "title" || creating.Number != 0 || creating.Notified {
		t.Fatalf("creating pull request = %#v, %v", creating, err)
	}
	if err := db.MarkPullRequestNotified(ctx, attempt); !errors.Is(err, ErrNotFound) {
		t.Fatalf("notify creating pull request = %v, want ErrNotFound", err)
	}
	if err := db.CompletePullRequest(ctx, attempt, 0, ""); err == nil {
		t.Fatal("pull request completion accepted an invalid result")
	}
	if err := db.FailPullRequest(ctx, attempt, errors.New("provider unavailable")); err != nil {
		t.Fatalf("fail pull request: %v", err)
	}
	failed, err := db.GetPullRequest(ctx, attempt)
	if err != nil || failed.State != "failed" || failed.Error != "provider unavailable" {
		t.Fatalf("failed pull request = %#v, %v", failed, err)
	}
	if err := db.CompletePullRequest(ctx, attempt, 1, "https://example/pr/1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("complete failed pull request = %v, want ErrConflict", err)
	}
	if _, err := db.GetPullRequest(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing pull request = %v, want ErrNotFound", err)
	}
	if err := db.MarkPullRequestNotified(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("notify missing pull request = %v, want ErrNotFound", err)
	}

	second, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "second"})
	if err != nil {
		t.Fatalf("create second task: %v", err)
	}
	if _, err := db.ReservePullRequest(ctx, second.CurrentAttemptID, "title", "head", "main"); err != nil {
		t.Fatalf("reserve second pull request: %v", err)
	}
	if err := db.CompletePullRequest(ctx, second.CurrentAttemptID, 7, "https://example/pr/7"); err != nil {
		t.Fatalf("complete pull request: %v", err)
	}
	if err := db.MarkPullRequestNotified(ctx, second.CurrentAttemptID); err != nil {
		t.Fatalf("mark pull request notified: %v", err)
	}
	if err := db.MarkPullRequestNotified(ctx, second.CurrentAttemptID); err != nil {
		t.Fatalf("replay pull request notification: %v", err)
	}
	opened, err := db.GetPullRequest(ctx, second.CurrentAttemptID)
	if err != nil || opened.State != "open" || opened.Number != 7 || !opened.Notified {
		t.Fatalf("open pull request = %#v, %v", opened, err)
	}
}
