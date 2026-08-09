package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/task"
	"modernc.org/sqlite"
)

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
	if retried.Prompt != "" || retried.BaseBranch != "" || retried.TaskBranch != "" {
		t.Fatalf("ordinary retry overrides = %q/%q/%q; want existing empty fallback behavior", retried.Prompt, retried.BaseBranch, retried.TaskBranch)
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

func TestRetryWaitsForFailedCurrentAttemptLogsAndPendingWorkerEvents(t *testing.T) {
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
	if _, err := db.AppendPodLog(ctx, AppendPodLogParams{
		TaskID: created.ID, AttemptID: first.ID, PodUID: "pending-retry-pod", JobName: "job", PodName: "pod",
		Content: []byte("pending event"), WorkerEventID: "pending-retry-event", WorkerEvent: "pending event",
	}, 256, 64); err != nil {
		t.Fatalf("append pending worker event: %v", err)
	}
	if err := db.MarkLogsExhausted(ctx, created.ID, first.ID); err != nil {
		t.Fatalf("mark first attempt logs exhausted: %v", err)
	}
	if _, _, err := db.PlanRetryAttempt(ctx, created.ID, "retry-after-eof"); !errors.Is(err, ErrConflict) {
		t.Fatalf("plan retry with pending worker event = %v, want ErrConflict", err)
	}
	if _, _, err := db.RetryTaskOnce(ctx, created.ID, "retry-after-eof"); !errors.Is(err, ErrConflict) {
		t.Fatalf("retry with pending worker event = %v, want ErrConflict", err)
	}
	if err := db.MarkWorkerEventProcessed(ctx, "pending-retry-event"); err != nil {
		t.Fatalf("process pending worker event: %v", err)
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

func TestRetrySelectionMakesOldAttemptLogAppendConflict(t *testing.T) {
	db := openTestStore(t)
	ctx := context.Background()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "retry append race"})
	if err != nil {
		t.Fatal(err)
	}
	oldAttempt := created.CurrentAttemptID
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.FAILED, TransitionParams{Reason: "failed", Trigger: "system"}); err != nil {
		t.Fatal(err)
	}
	if err := db.MarkLogsExhausted(ctx, created.ID, oldAttempt); err != nil {
		t.Fatal(err)
	}

	retryGate, appendGate := make(chan struct{}), make(chan struct{})
	retryResult := make(chan error, 1)
	appendResult := make(chan error, 1)
	go func() {
		<-retryGate
		_, _, err := db.RetryTaskOnce(ctx, created.ID, "selected-first")
		retryResult <- err
	}()
	go func() {
		<-appendGate
		_, err := db.AppendPodLog(ctx, AppendPodLogParams{
			TaskID: created.ID, AttemptID: oldAttempt, PodUID: "stale-pod", JobName: "old-job", PodName: "old-pod",
			Content: []byte("late worker failure"), WorkerEventID: "late-old-event", WorkerEvent: "late worker failure",
		}, 256, 64)
		appendResult <- err
	}()
	close(retryGate)
	if err := <-retryResult; err != nil {
		t.Fatalf("retry selection: %v", err)
	}
	close(appendGate)
	if err := <-appendResult; !errors.Is(err, ErrConflict) {
		t.Fatalf("old-attempt append after retry selection = %v, want ErrConflict", err)
	}
	pending, err := db.HasPendingWorkerEvents(ctx, created.ID, oldAttempt)
	if err != nil || pending {
		t.Fatalf("stale attempt pending event = %t, %v; want false", pending, err)
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
	git := GitResult{AttemptID: attempt.ID, State: "pushed", Branch: "work/task-a1", CommitSHA: "0123456789abcdef0123456789abcdef01234567"}
	pr := PullRequest{AttemptID: attempt.ID, State: "open", Number: 42, URL: "https://bitbucket.example/pr/42", Title: "Provider title", HeadBranch: git.Branch, BaseBranch: "main"}
	if _, err := db.db.ExecContext(ctx, `UPDATE task_attempts SET base_branch = ?, task_branch = ? WHERE id = ?`, pr.BaseBranch, git.Branch, attempt.ID); err != nil {
		t.Fatalf("set attempt branch identity: %v", err)
	}
	recordCandidateForTest(t, db, git, pr)
	if err := db.RecordVerifiedPullRequest(ctx, git, pr); err != nil {
		t.Fatalf("record verified Git and pull request result: %v", err)
	}
	if err := db.Transition(ctx, created.ID, task.CREATING_PR, task.PR_OPEN, TransitionParams{Reason: "durable", Trigger: "system"}); err != nil {
		t.Fatalf("PR_OPEN with durable results: %v", err)
	}
}

func TestRecordPullRequestCandidateCommitFailureRollsBackAndReusesConnection(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatal(err)
	}
	git := GitResult{AttemptID: created.CurrentAttemptID, State: "candidate", Branch: "work/candidate", CommitSHA: strings.Repeat("a", 40)}
	pullRequest := PullRequest{AttemptID: git.AttemptID, State: "reported", Number: 42, HeadBranch: git.Branch, BaseBranch: "main"}
	setAttemptBranches(t, db, git.AttemptID, pullRequest.BaseBranch, git.Branch)
	if _, err := db.db.ExecContext(ctx, `
		CREATE TABLE forced_candidate_commit_failure (
			attempt_id TEXT REFERENCES task_attempts(id) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TRIGGER reject_candidate_commit AFTER INSERT ON git_results
		WHEN NEW.state = 'candidate'
		BEGIN
			INSERT INTO forced_candidate_commit_failure(attempt_id) VALUES ('missing-attempt');
		END;
	`); err != nil {
		t.Fatal(err)
	}

	err = db.RecordPullRequestCandidate(ctx, git, pullRequest)
	if err == nil || !strings.Contains(err.Error(), "commit pull request candidate") {
		t.Fatalf("RecordPullRequestCandidate() = %v, want commit failure", err)
	}
	if got, err := db.GetGitResult(ctx, git.AttemptID); !errors.Is(err, ErrNotFound) {
		t.Errorf("Git result after failed commit = %#v, %v; want absent", got, err)
	}
	if got, err := db.GetPullRequest(ctx, pullRequest.AttemptID); !errors.Is(err, ErrNotFound) {
		t.Errorf("pull request after failed commit = %#v, %v; want absent", got, err)
	}
	var forcedRows int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM forced_candidate_commit_failure`).Scan(&forcedRows); err != nil || forcedRows != 0 {
		t.Errorf("forced rows after failed commit = %d, %v; want 0", forcedRows, err)
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER reject_candidate_commit`); err != nil {
		t.Fatalf("reuse connection to drop trigger: %v", err)
	}
	if err := db.RecordPullRequestCandidate(ctx, git, pullRequest); err != nil {
		t.Fatalf("valid candidate after failed commit: %v", err)
	}
	if got, err := db.GetGitResult(ctx, git.AttemptID); err != nil || got != git {
		t.Fatalf("Git result after valid retry = %#v, %v; want %#v", got, err, git)
	}
	if got, err := db.GetPullRequest(ctx, pullRequest.AttemptID); err != nil || got != pullRequest {
		t.Fatalf("pull request after valid retry = %#v, %v; want %#v", got, err, pullRequest)
	}
}

func TestRecordVerifiedPullRequestCommitFailureRollsBackAndReusesConnection(t *testing.T) {
	db := openTestStore(t)
	ctx := t.Context()
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "prompt"})
	if err != nil {
		t.Fatal(err)
	}
	git := GitResult{AttemptID: created.CurrentAttemptID, State: "pushed", Branch: "work/verified", CommitSHA: strings.Repeat("b", 40)}
	pullRequest := PullRequest{AttemptID: git.AttemptID, State: "open", Number: 42, URL: "https://github.example/acme/repo/pull/42", Title: "Provider title", HeadBranch: git.Branch, BaseBranch: "main"}
	setAttemptBranches(t, db, git.AttemptID, pullRequest.BaseBranch, git.Branch)
	recordCandidateForTest(t, db, git, pullRequest)
	wantGit, gitErr := db.GetGitResult(ctx, git.AttemptID)
	wantPullRequest, pullRequestErr := db.GetPullRequest(ctx, pullRequest.AttemptID)
	if gitErr != nil || pullRequestErr != nil {
		t.Fatalf("read candidate rows: %v / %v", gitErr, pullRequestErr)
	}
	if _, err := db.db.ExecContext(ctx, `
		CREATE TABLE forced_verified_commit_failure (
			attempt_id TEXT REFERENCES task_attempts(id) DEFERRABLE INITIALLY DEFERRED
		);
		CREATE TRIGGER reject_verified_commit AFTER UPDATE OF state ON git_results
		WHEN NEW.state = 'pushed'
		BEGIN
			INSERT INTO forced_verified_commit_failure(attempt_id) VALUES ('missing-attempt');
		END;
	`); err != nil {
		t.Fatal(err)
	}

	err = db.RecordVerifiedPullRequest(ctx, git, pullRequest)
	if err == nil || !strings.Contains(err.Error(), "commit verified pull request") {
		t.Fatalf("RecordVerifiedPullRequest() = %v, want commit failure", err)
	}
	if got, err := db.GetGitResult(ctx, git.AttemptID); err != nil || got != wantGit {
		t.Errorf("Git result after failed commit = %#v, %v; want unchanged %#v", got, err, wantGit)
	}
	if got, err := db.GetPullRequest(ctx, pullRequest.AttemptID); err != nil || got != wantPullRequest {
		t.Errorf("pull request after failed commit = %#v, %v; want unchanged %#v", got, err, wantPullRequest)
	}
	var forcedRows int
	if err := db.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM forced_verified_commit_failure`).Scan(&forcedRows); err != nil || forcedRows != 0 {
		t.Errorf("forced rows after failed commit = %d, %v; want 0", forcedRows, err)
	}
	if _, err := db.db.ExecContext(ctx, `DROP TRIGGER reject_verified_commit`); err != nil {
		t.Fatalf("reuse connection to drop trigger: %v", err)
	}
	if err := db.RecordVerifiedPullRequest(ctx, git, pullRequest); err != nil {
		t.Fatalf("valid verification after failed commit: %v", err)
	}
	if got, err := db.GetGitResult(ctx, git.AttemptID); err != nil || got != git {
		t.Fatalf("Git result after valid retry = %#v, %v; want %#v", got, err, git)
	}
	if got, err := db.GetPullRequest(ctx, pullRequest.AttemptID); err != nil || got != pullRequest {
		t.Fatalf("pull request after valid retry = %#v, %v; want %#v", got, err, pullRequest)
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
	if pragmas.BusyTimeout != 5000 {
		t.Fatalf("busy_timeout = %d; want 5000", pragmas.BusyTimeout)
	}
}

func TestOpenPreservesSpecialFilesystemPaths(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("question marks are not valid Windows filename characters")
	}
	for _, relative := range []bool{false, true} {
		name := "absolute"
		if relative {
			name = "relative"
		}
		t.Run(name, func(t *testing.T) {
			absolutePath := filepath.Join(t.TempDir(), "store with spaces ?#% sqlite")
			path := absolutePath
			if relative {
				workingDirectory, err := os.Getwd()
				if err != nil {
					t.Fatalf("get working directory: %v", err)
				}
				path, err = filepath.Rel(workingDirectory, absolutePath)
				if err != nil {
					t.Fatalf("make relative path: %v", err)
				}
			}
			db, err := Open(t.Context(), path)
			if err != nil {
				t.Fatalf("open store at %q using %q: %v", path, sqliteDSN(path), err)
			}
			if _, err := db.CreateTask(t.Context(), CreateTaskParams{Repository: "repo", Prompt: "special path"}); err != nil {
				_ = db.Close()
				t.Fatalf("use store at %q: %v", path, err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close store at %q: %v", path, err)
			}
			if _, err := os.Stat(absolutePath); err != nil {
				t.Fatalf("stat exact store path %q: %v", absolutePath, err)
			}
		})
	}
}

func TestOpenRejectsLegacyVersionZeroDatabaseBeforeApplyingSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.sqlite")
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	_, err = legacy.Exec(`CREATE TABLE slack_inbox (event_id TEXT PRIMARY KEY, origin_json TEXT NOT NULL)`)
	if err != nil {
		_ = legacy.Close()
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy database: %v", err)
	}

	_, err = Open(context.Background(), path)
	if err == nil || !strings.Contains(err.Error(), "schema version 0") || !strings.Contains(err.Error(), "recreate the controller PVC/database") {
		t.Fatalf("Open() error = %v, want actionable legacy schema rejection", err)
	}
	check, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen rejected database: %v", err)
	}
	defer check.Close()
	var tasks int
	if err := check.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'tasks'`).Scan(&tasks); err != nil || tasks != 0 {
		t.Fatalf("tasks table after rejected open = %d, %v; want schema untouched", tasks, err)
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

func TestImmediateTransactionDiscardsConnectionWhenRollbackFails(t *testing.T) {
	wrapped := &rollbackFailureDriver{inner: &sqlite.Driver{}}
	db := sql.OpenDB(sqliteConnector{driver: wrapped, dsn: sqliteDSN(filepath.Join(t.TempDir(), "rollback.sqlite"))})
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(t.Context(), `
		CREATE TABLE rollback_parents (id INTEGER PRIMARY KEY);
		CREATE TABLE rollback_children (parent_id INTEGER NOT NULL REFERENCES rollback_parents(id));
		CREATE TABLE values_for_rollback_test (value INTEGER NOT NULL);
	`); err != nil {
		t.Fatal(err)
	}

	wrapped.failRollback.Store(true)
	primary := errors.New("primary transaction failure")
	store := &Store{db: db}
	err := store.immediateTransaction(t.Context(), "forced rollback", func(conn *sql.Conn) error {
		if _, err := conn.ExecContext(t.Context(), `INSERT INTO values_for_rollback_test(value) VALUES (1)`); err != nil {
			return fmt.Errorf("insert rollback test value: %w", err)
		}
		return primary
	})
	if !errors.Is(err, primary) || !strings.Contains(err.Error(), "forced rollback failure") {
		t.Fatalf("immediateTransaction() = %v; want joined primary and rollback errors", err)
	}
	var foreignKeys, busyTimeout int
	if err := db.QueryRowContext(t.Context(), `PRAGMA foreign_keys`).Scan(&foreignKeys); err != nil {
		t.Fatalf("read replacement foreign_keys: %v", err)
	}
	if err := db.QueryRowContext(t.Context(), `PRAGMA busy_timeout`).Scan(&busyTimeout); err != nil {
		t.Fatalf("read replacement busy_timeout: %v", err)
	}
	if foreignKeys != 1 || busyTimeout != 5000 {
		t.Fatalf("replacement pragmas = foreign_keys %d, busy_timeout %d; want 1 and 5000", foreignKeys, busyTimeout)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO rollback_children(parent_id) VALUES (404)`); err == nil || !strings.Contains(strings.ToLower(err.Error()), "foreign key constraint failed") {
		t.Fatalf("replacement connection foreign-key violation = %v; want constraint failure", err)
	}
	if _, err := db.ExecContext(t.Context(), `
		INSERT INTO rollback_parents(id) VALUES (1);
		INSERT INTO rollback_children(parent_id) VALUES (1);
		INSERT INTO values_for_rollback_test(value) VALUES (2);
	`); err != nil {
		t.Fatalf("valid work after rollback failure did not get a fresh connection: %v", err)
	}
	var count, value int
	if err := db.QueryRowContext(t.Context(), `SELECT COUNT(*), MAX(value) FROM values_for_rollback_test`).Scan(&count, &value); err != nil || count != 1 || value != 2 {
		t.Fatalf("fresh connection values = count %d max %d, %v; want only committed value 2", count, value, err)
	}
	if got := wrapped.opens.Load(); got != 2 {
		t.Fatalf("driver opens = %d; want poisoned singleton replaced exactly once", got)
	}
	if got := wrapped.closes.Load(); got < 1 {
		t.Fatalf("driver closes = %d; want poisoned connection discarded", got)
	}
}

type rollbackFailureDriver struct {
	inner        driver.Driver
	opens        atomic.Int32
	closes       atomic.Int32
	failRollback atomic.Bool
}

func (d *rollbackFailureDriver) Open(name string) (driver.Conn, error) {
	conn, err := d.inner.Open(name)
	if err != nil {
		return nil, fmt.Errorf("open wrapped SQLite connection: %w", err)
	}
	d.opens.Add(1)
	return &rollbackFailureConn{Conn: conn, owner: d}, nil
}

type rollbackFailureConn struct {
	driver.Conn
	owner    *rollbackFailureDriver
	poisoned bool
}

func (c *rollbackFailureConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.poisoned {
		return nil, errors.New("poisoned connection reused")
	}
	if strings.EqualFold(strings.TrimSpace(query), "ROLLBACK") && c.owner.failRollback.CompareAndSwap(true, false) {
		c.poisoned = true
		return nil, errors.New("forced rollback failure")
	}
	execer, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	result, err := execer.ExecContext(ctx, query, args)
	if errors.Is(err, driver.ErrBadConn) {
		return nil, driver.ErrBadConn
	}
	if err != nil {
		return nil, fmt.Errorf("execute wrapped SQLite query: %w", err)
	}
	return result, nil
}

func (c *rollbackFailureConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.poisoned {
		return nil, errors.New("poisoned connection reused")
	}
	queryer, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, driver.ErrSkip
	}
	rows, err := queryer.QueryContext(ctx, query, args)
	if errors.Is(err, driver.ErrBadConn) {
		return nil, driver.ErrBadConn
	}
	if err != nil {
		return nil, fmt.Errorf("query wrapped SQLite connection: %w", err)
	}
	return rows, nil
}

func (c *rollbackFailureConn) Close() error {
	c.owner.closes.Add(1)
	if err := c.Conn.Close(); err != nil {
		return fmt.Errorf("close wrapped SQLite connection: %w", err)
	}
	return nil
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
