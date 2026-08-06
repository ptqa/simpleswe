package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestSlackInboxPersistsPendingEventsAndDeduplicatesByEventID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.sqlite")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	want := SlackInboxEvent{
		EventID: "Ev-inbox-1",
		Kind:    "app_mention",
		Text:    "run workspace/repository fix it",
		Origin: protocol.SlackOrigin{
			WorkspaceID: "T1", ChannelID: "C1", MessageTS: "1.2", ThreadTS: "1.1", UserID: "U1",
		},
	}
	stored, err := db.PutSlackInboxEvent(ctx, want)
	if err != nil {
		t.Fatalf("put inbox event: %v", err)
	}
	if stored.Status != SlackInboxPending || stored.Attempts != 0 || stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatalf("new inbox event = %#v; want pending with timestamps", stored)
	}

	changed := want
	changed.Text = "retry swe-other"
	duplicate, err := db.PutSlackInboxEvent(ctx, changed)
	if err != nil {
		t.Fatalf("put duplicate inbox event: %v", err)
	}
	if duplicate.Text != want.Text || duplicate.Origin != want.Origin {
		t.Fatalf("duplicate replaced durable event: %#v", duplicate)
	}
	if err := db.StartSlackInboxAttempt(ctx, want.EventID); err != nil {
		t.Fatalf("start inbox attempt: %v", err)
	}
	if err := db.RecordSlackInboxError(ctx, want.EventID, errors.New("temporary failure")); err != nil {
		t.Fatalf("record inbox error: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	pending, err := db.ListPendingSlackInboxEvents(ctx)
	if err != nil {
		t.Fatalf("list pending inbox: %v", err)
	}
	if len(pending) != 1 || pending[0].EventID != want.EventID || pending[0].Attempts != 1 || pending[0].LastError != "temporary failure" {
		t.Fatalf("reopened pending inbox = %#v", pending)
	}
	if pending[0].Origin != want.Origin {
		t.Fatalf("reopened origin = %#v; want %#v", pending[0].Origin, want.Origin)
	}
	if !pending[0].UpdatedAt.After(stored.UpdatedAt) {
		t.Fatalf("error timestamp = %v; want after insertion timestamp %v", pending[0].UpdatedAt, stored.UpdatedAt)
	}
	if err := db.StartSlackInboxAttempt(ctx, want.EventID); err != nil {
		t.Fatalf("start replay attempt: %v", err)
	}
	beforeHandled := time.Now().UTC()
	if err := db.MarkSlackInboxHandled(ctx, want.EventID); err != nil {
		t.Fatalf("mark inbox handled: %v", err)
	}
	if pending, err := db.ListPendingSlackInboxEvents(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("pending inbox after success = %#v, %v; want none", pending, err)
	}
	handled, err := db.PutSlackInboxEvent(ctx, want)
	if err != nil {
		t.Fatalf("read handled duplicate: %v", err)
	}
	if handled.Status != SlackInboxHandled || handled.Attempts != 2 || handled.LastError != "" || handled.HandledAt == nil || handled.HandledAt.Before(beforeHandled) {
		t.Fatalf("handled inbox event = %#v", handled)
	}
}

func TestSlackInboxRecordsRejectedEnvelopeAttemptsAndTimestamps(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	before := time.Now().UTC()
	if err := db.RecordRejectedSlackInboxEvent(ctx, "envelope-bad-1", "events_api", errors.New("invalid JSON")); err != nil {
		t.Fatalf("record rejected Slack envelope: %v", err)
	}
	if err := db.RecordRejectedSlackInboxEvent(ctx, "envelope-bad-1", "events_api", errors.New("invalid JSON again")); err != nil {
		t.Fatalf("record repeated rejected Slack envelope: %v", err)
	}

	var kind, lastError, createdAt, updatedAt string
	var attempts int
	if err := db.db.QueryRowContext(ctx, `
		SELECT kind, attempts, last_error, created_at, updated_at
		FROM slack_inbox_rejections WHERE event_id = ?`, "envelope-bad-1").Scan(
		&kind, &attempts, &lastError, &createdAt, &updatedAt,
	); err != nil {
		t.Fatalf("read rejected Slack envelope: %v", err)
	}
	created, err := parseTime(createdAt)
	if err != nil {
		t.Fatalf("parse rejection creation time: %v", err)
	}
	updated, err := parseTime(updatedAt)
	if err != nil {
		t.Fatalf("parse rejection update time: %v", err)
	}
	if kind != "events_api" || attempts != 2 || lastError != "invalid JSON again" || created.Before(before) || updated.Before(created) {
		t.Fatalf("rejected Slack envelope = kind %q, attempts %d, error %q, created %v, updated %v", kind, attempts, lastError, created, updated)
	}
	if pending, err := db.ListPendingSlackInboxEvents(ctx); err != nil || len(pending) != 0 {
		t.Fatalf("rejected envelope entered pending inbox: %#v, %v", pending, err)
	}
}

func TestCreateTaskIsIdempotentForSlackEventsAndCreatesOneAttempt(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()

	first, err := db.CreateTask(ctx, CreateTaskParams{
		Repository:   "https://bitbucket.example/repo",
		Prompt:       "fix the bug",
		SlackEventID: "event-123",
	})
	if err != nil {
		t.Fatalf("create first task: %v", err)
	}
	second, err := db.CreateTask(ctx, CreateTaskParams{
		Repository:   "https://bitbucket.example/other-repo",
		Prompt:       "a different payload",
		SlackEventID: "event-123",
	})
	if err != nil {
		t.Fatalf("replay Slack event: %v", err)
	}

	if second.ID != first.ID {
		t.Fatalf("replayed event created task %q; want existing task %q", second.ID, first.ID)
	}
	attempts, err := db.ListAttempts(ctx, first.ID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Number != 1 {
		t.Fatalf("replayed event has attempts %#v; want exactly attempt 1", attempts)
	}
}

func TestRetryCreatesAttemptTwoWithoutChangingAttemptOne(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()

	created, err := db.CreateTask(ctx, CreateTaskParams{
		Repository: "https://bitbucket.example/repo",
		Prompt:     "fix the bug",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	before, err := db.ListAttempts(ctx, created.ID)
	if err != nil {
		t.Fatalf("list initial attempts: %v", err)
	}
	if len(before) != 1 {
		t.Fatalf("initial attempts = %d; want 1", len(before))
	}

	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "failed", Trigger: "system"}); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	if err := db.MarkLogsExhausted(ctx, created.ID, created.CurrentAttemptID); err != nil {
		t.Fatalf("mark failed attempt logs exhausted: %v", err)
	}
	before, err = db.ListAttempts(ctx, created.ID)
	if err != nil {
		t.Fatalf("reload initial attempts: %v", err)
	}
	retried, inserted, err := db.RetryTaskOnce(ctx, created.ID, "retry-1")
	if err != nil {
		t.Fatalf("retry task: %v", err)
	}
	if !inserted {
		t.Fatal("first retry was reported as an idempotent replay")
	}
	if retried.Number != 2 {
		t.Fatalf("retry number = %d; want 2", retried.Number)
	}
	if retried.State != task.QUEUED {
		t.Fatalf("retry state = %q; want %q", retried.State, task.QUEUED)
	}

	after, err := db.ListAttempts(ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts after retry: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("attempt count after retry = %d; want 2", len(after))
	}
	if !reflect.DeepEqual(after[0], before[0]) {
		t.Fatalf("attempt 1 changed from %#v to %#v", before[0], after[0])
	}
	if after[1].Number != 2 {
		t.Fatalf("second attempt number = %d; want 2", after[1].Number)
	}
	got, err := db.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("get retried task: %v", err)
	}
	if got.State != task.QUEUED || got.CurrentAttemptID != retried.ID {
		t.Fatalf("retried task = %#v; want queued on attempt 2", got)
	}
	events, err := db.ListEvents(ctx, created.ID)
	if err != nil {
		t.Fatalf("list retry events: %v", err)
	}
	last := events[len(events)-1]
	if last.FromState != task.FAILED || last.ToState != task.QUEUED || last.AttemptID != retried.ID {
		t.Fatalf("retry event = %#v; want failed -> queued on attempt 2", last)
	}

	replayed, inserted, err := db.RetryTaskOnce(ctx, created.ID, "retry-1")
	if err != nil {
		t.Fatalf("replay retry: %v", err)
	}
	if inserted || replayed.ID != retried.ID {
		t.Fatalf("replayed retry = %#v, inserted=%t; want attempt %q", replayed, inserted, retried.ID)
	}
	final, err := db.ListAttempts(ctx, created.ID)
	if err != nil || len(final) != 2 {
		t.Fatalf("attempts after replay = %#v, %v; want two", final, err)
	}
}

func TestRetryWaitsForFailedCurrentAttemptLogsAndOldAttemptCanStillBeMarked(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	first, _ := db.CurrentAttempt(ctx, created.ID)
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "validation failed", Trigger: "controller"}); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	if _, _, err := db.RetryTaskOnce(ctx, created.ID, "retry-after-eof"); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry before logs exhausted = %v, want ErrConflict", err)
	}
	if err := db.MarkLogsExhausted(ctx, created.ID, first.ID); err != nil {
		t.Fatalf("mark first attempt logs exhausted: %v", err)
	}
	second, _, err := db.RetryTaskOnce(ctx, created.ID, "retry-after-eof")
	if err != nil {
		t.Fatalf("retry after logs exhausted: %v", err)
	}
	if second.ID == first.ID {
		t.Fatal("retry did not create a new attempt")
	}
	if err := db.MarkLogsExhausted(ctx, created.ID, first.ID); err != nil {
		t.Fatalf("idempotently mark immutable old attempt after retry: %v", err)
	}
}

func TestRequestCancellationMarksTaskWithoutDeletingHistory(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()

	created, err := db.CreateTask(ctx, CreateTaskParams{
		Repository: "https://bitbucket.example/repo",
		Prompt:     "fix the bug",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.RequestCancellation(ctx, created.ID); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	got, err := db.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("get cancelled task: %v", err)
	}
	if !got.CancellationRequested {
		t.Fatal("cancellation request was not recorded")
	}
	attempts, err := db.ListAttempts(ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts after cancellation: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Number != 1 {
		t.Fatalf("cancellation deleted or changed history: %#v", attempts)
	}
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "late worker failure", Trigger: "controller"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("late transition after cancellation = %v, want ErrConflict", err)
	}
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.CANCELLED, TransitionParams{Reason: "resource absence confirmed", Trigger: "controller"}); err != nil {
		t.Fatalf("cancellation-owned terminal transition: %v", err)
	}
}

func TestRequestCancellationRejectsTerminalTaskWithoutMutation(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "failed", Trigger: "system"}); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	if err := db.RequestCancellation(ctx, created.ID); err == nil {
		t.Fatal("terminal cancellation was accepted")
	}
	got, err := db.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if got.CancellationRequested {
		t.Fatal("terminal cancellation mutated cancellation_requested")
	}
}

func TestTransitionAtomicallyUpdatesTaskAndAppendsCompleteEvent(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()

	created, err := db.CreateTask(ctx, CreateTaskParams{
		Repository: "https://bitbucket.example/repo",
		Prompt:     "fix the bug",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempts, err := db.ListAttempts(ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}

	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.QUEUED, TransitionParams{
		Reason:  "accepted",
		Trigger: "api",
	}); err != nil {
		t.Fatalf("transition task: %v", err)
	}

	got, err := db.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("get transitioned task: %v", err)
	}
	if got.State != task.QUEUED {
		t.Fatalf("task state = %v; want %v", got.State, task.QUEUED)
	}
	current, err := db.CurrentAttempt(ctx, created.ID)
	if err != nil {
		t.Fatalf("get transitioned attempt: %v", err)
	}
	if current.State != task.QUEUED {
		t.Fatalf("attempt state = %v; want %v", current.State, task.QUEUED)
	}
	events, err := db.ListEvents(ctx, created.ID)
	if err != nil {
		t.Fatalf("list transition events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("event count = %d; want 1", len(events))
	}
	event := events[0]
	if event.ID == "" || event.TaskID != created.ID || event.AttemptID != attempts[0].ID ||
		event.FromState != task.RECEIVED || event.ToState != task.QUEUED ||
		event.OccurredAt.IsZero() || event.Reason != "accepted" || event.Trigger != "api" {
		t.Fatalf("incomplete transition event: %#v", event)
	}
}

func TestRejectedTransitionLeavesTaskAndEventHistoryUnchanged(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()

	created, err := db.CreateTask(ctx, CreateTaskParams{
		Repository: "https://bitbucket.example/repo",
		Prompt:     "fix the bug",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}

	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.RUNNING, TransitionParams{
		Reason:  "skip is forbidden",
		Trigger: "api",
	}); err == nil {
		t.Fatal("invalid transition was accepted")
	}
	got, err := db.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("get task after rejected transition: %v", err)
	}
	if got.State != task.RECEIVED {
		t.Fatalf("task state after rejection = %v; want %v", got.State, task.RECEIVED)
	}
	events, err := db.ListEvents(ctx, created.ID)
	if err != nil {
		t.Fatalf("list events after rejected transition: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("rejected transition appended events: %#v", events)
	}
}

func TestGitResultReplayCannotReplaceDurableBranch(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt, err := db.CurrentAttempt(ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	first := GitResult{AttemptID: attempt.ID, State: "pushed", Branch: "work/task-a1", CommitSHA: "abc"}
	if err := db.RecordGitResult(ctx, first); err != nil {
		t.Fatalf("record Git result: %v", err)
	}
	if err := db.RecordGitResult(ctx, first); err != nil {
		t.Fatalf("replay identical Git result: %v", err)
	}
	replacement := first
	replacement.Branch = "work/other"
	if err := db.RecordGitResult(ctx, replacement); err == nil {
		t.Fatal("replacement durable Git branch was accepted")
	}
	got, err := db.GetGitResult(ctx, attempt.ID)
	if err != nil || got != first {
		t.Fatalf("durable Git result = %#v, %v; want %#v", got, err, first)
	}
}

func TestPullRequestStateRequiresDurableGitAndOpenPullRequest(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	path := []task.State{task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING, task.RUNNING, task.AGENT_RUNNING, task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR}
	for i := 1; i < len(path); i++ {
		if err := db.Transition(ctx, created.ID, path[i-1], path[i], TransitionParams{Reason: "advance", Trigger: "system"}); err != nil {
			t.Fatalf("transition %q -> %q: %v", path[i-1], path[i], err)
		}
	}
	if err := db.Transition(ctx, created.ID, task.CREATING_PR, task.PR_OPEN, TransitionParams{Reason: "unsafe", Trigger: "system"}); err == nil {
		t.Fatal("PR_OPEN without durable results was accepted")
	}
	attempt, err := db.CurrentAttempt(ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	if err := db.RecordGitResult(ctx, GitResult{AttemptID: attempt.ID, State: "pushed", Branch: "work/task-a1", CommitSHA: "abc"}); err != nil {
		t.Fatalf("record Git result: %v", err)
	}
	if _, err := db.ReservePullRequest(ctx, attempt.ID, "prompt", "work/task-a1", "main"); err != nil {
		t.Fatalf("reserve pull request: %v", err)
	}
	if err := db.CompletePullRequest(ctx, attempt.ID, 42, "https://bitbucket.example/pr/42"); err != nil {
		t.Fatalf("complete pull request: %v", err)
	}
	if err := db.Transition(ctx, created.ID, task.CREATING_PR, task.PR_OPEN, TransitionParams{Reason: "durable", Trigger: "system"}); err != nil {
		t.Fatalf("PR_OPEN with durable results: %v", err)
	}
}

func TestOpenEnablesRequiredSQLitePragmas(t *testing.T) {
	db := openTestStore(t)
	pragmas, err := db.Pragmas(context.Background())
	if err != nil {
		t.Fatalf("read SQLite pragmas: %v", err)
	}
	if !strings.EqualFold(pragmas.JournalMode, "wal") {
		t.Fatalf("journal_mode = %q; want wal", pragmas.JournalMode)
	}
	if !pragmas.ForeignKeys {
		t.Fatal("foreign_keys is disabled")
	}
	if pragmas.BusyTimeout <= 0 {
		t.Fatalf("busy_timeout = %d; want enabled", pragmas.BusyTimeout)
	}
}

func TestPodLogCheckpointQuotaAndWorkerEventIdentitySurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs.sqlite")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt, err := db.CurrentAttempt(ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	timestamp := time.Date(2026, 8, 6, 12, 0, 0, 123, time.UTC)
	first, err := db.AppendPodLog(ctx, AppendPodLogParams{
		TaskID: created.ID, AttemptID: attempt.ID, PodUID: "pod-uid", Timestamp: timestamp,
		TimestampOrdinal: 1, Content: []byte(strings.Repeat("x", 80) + "\n"), WorkerEventID: "pod-uid/event-1",
		WorkerEvent: `@@simpleswe:{"type":"agent_started","task_id":"` + created.ID + `"}`,
	}, 64, 16)
	if err != nil {
		t.Fatalf("append capped Pod log: %v", err)
	}
	if !first.Truncated || first.AppendedBytes != 64 {
		t.Fatalf("first append = %#v, want exactly 64 durable bytes and truncation", first)
	}
	duplicate, err := db.AppendPodLog(ctx, AppendPodLogParams{
		TaskID: created.ID, AttemptID: attempt.ID, PodUID: "pod-uid", Timestamp: timestamp,
		TimestampOrdinal: 1, Content: []byte("duplicate\n"), WorkerEventID: "pod-uid/event-1",
		WorkerEvent: `@@simpleswe:{"type":"agent_started","task_id":"` + created.ID + `"}`,
	}, 64, 16)
	if err != nil {
		t.Fatalf("append duplicate checkpoint: %v", err)
	}
	if !duplicate.Duplicate || duplicate.AppendedBytes != 0 {
		t.Fatalf("duplicate append = %#v", duplicate)
	}
	for i := 2; i <= 100; i++ {
		result, err := db.AppendPodLog(ctx, AppendPodLogParams{
			TaskID: created.ID, AttemptID: attempt.ID, PodUID: "pod-uid", Timestamp: timestamp.Add(time.Duration(i) * time.Nanosecond),
			TimestampOrdinal: 1, Content: []byte(strings.Repeat("growth", 100) + "\n"),
		}, 64, 16)
		if err != nil || result.AppendedBytes != 0 {
			t.Fatalf("post-quota append %d = %#v, %v", i, result, err)
		}
	}
	var durableBytes int
	if err := db.db.QueryRowContext(ctx, `SELECT COALESCE(SUM(length(content)), 0) FROM log_chunks WHERE attempt_id = ?`, attempt.ID).Scan(&durableBytes); err != nil || durableBytes != 64 {
		t.Fatalf("durable log growth = %d bytes, %v; want fixed 64-byte bound", durableBytes, err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	cursor, err := db.GetPodLogCursor(ctx, "pod-uid")
	if err != nil || !cursor.Timestamp.Equal(timestamp.Add(100*time.Nanosecond)) || cursor.TimestampOrdinal != 1 || !cursor.Truncated {
		t.Fatalf("reopened cursor = %#v, %v", cursor, err)
	}
	logs, _, err := db.ReadLogTailCursor(ctx, created.ID, attempt.ID, 100)
	if err != nil {
		t.Fatalf("read capped logs: %v", err)
	}
	if len(logs) != 64 || !strings.Contains(logs, "log truncated") {
		t.Fatalf("capped logs length/content = %d/%q", len(logs), logs)
	}
	pending, err := db.ListPendingWorkerEvents(ctx, "pod-uid")
	if err != nil || len(pending) != 1 || pending[0].ID != "pod-uid/event-1" {
		t.Fatalf("pending worker events = %#v, %v", pending, err)
	}
	events, err := db.ListEvents(ctx, created.ID)
	if err != nil {
		t.Fatalf("list truncation events: %v", err)
	}
	count := 0
	for _, event := range events {
		if strings.Contains(event.Reason, "log byte quota") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("truncation event count = %d, want 1", count)
	}
}

func TestKubernetesObservationsAreSeparatePerAttempt(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt, _ := db.CurrentAttempt(ctx, created.ID)
	if err := db.ObserveKubernetesJob(ctx, KubernetesJobObservation{
		TaskID: created.ID, AttemptID: attempt.ID, Namespace: "workers", Name: "job-1", UID: "job-uid", State: "running", Reason: "Active",
	}); err != nil {
		t.Fatalf("observe Job: %v", err)
	}
	if err := db.ObserveKubernetesPod(ctx, KubernetesPodObservation{
		TaskID: created.ID, AttemptID: attempt.ID, Namespace: "workers", Name: "pod-1", UID: "pod-uid", State: "running",
		Reason: "Running", Node: "node-a", Image: "worker:v1", ContainerStates: `{"worker":"running"}`,
	}); err != nil {
		t.Fatalf("observe Pod: %v", err)
	}
	job, pod, err := db.AttemptKubernetes(ctx, attempt.ID)
	if err != nil {
		t.Fatalf("read observations: %v", err)
	}
	if job.UID != "job-uid" || job.State != "running" || pod.UID != "pod-uid" || pod.Node != "node-a" || pod.Image != "worker:v1" || pod.ContainerStates != `{"worker":"running"}` {
		t.Fatalf("observations = Job %#v, Pod %#v", job, pod)
	}
}

func TestFailedAttemptFollowRemainsOpenForTrailingLogsUntilExhausted(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	attempt, _ := db.CurrentAttempt(ctx, created.ID)
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "worker failed", Trigger: "controller"}); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	if err := db.AppendLogChunk(ctx, created.ID, attempt.ID, []byte("trailing raw log\n")); err != nil {
		t.Fatalf("append trailing log: %v", err)
	}
	complete, err := db.AttemptFollowComplete(ctx, created.ID, attempt.ID)
	if err != nil || complete {
		t.Fatalf("follow complete before exhaustion = %t, %v; want false", complete, err)
	}
	logs, err := db.ReadLogTail(ctx, created.ID, attempt.ID, 10)
	if err != nil || logs != "trailing raw log\n" {
		t.Fatalf("trailing logs = %q, %v", logs, err)
	}
	if err := db.MarkLogsExhausted(ctx, created.ID, attempt.ID); err != nil {
		t.Fatalf("mark logs exhausted: %v", err)
	}
	complete, err = db.AttemptFollowComplete(ctx, created.ID, attempt.ID)
	if err != nil || !complete {
		t.Fatalf("follow complete after exhaustion = %t, %v; want true", complete, err)
	}
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	db, err := Open(context.Background(), filepath.Join(t.TempDir(), "tasks.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	return db
}
