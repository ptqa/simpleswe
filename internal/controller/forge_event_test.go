package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestProcessForgeEventsIgnoresUnownedPullRequest(t *testing.T) {
	fixture := newFixture(t)
	event := controllerForgeEvent("unowned-review", "review_comment", 777)
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents(): %v", err)
	}

	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != "handled" || stored.TaskID != "" || stored.AttemptID != "" {
		t.Fatalf("unowned forge event = %#v, %v; want handled without task association", stored, err)
	}
	jobList, err := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil || len(jobList.Items) != 0 {
		t.Fatalf("Jobs for unowned event = %d, %v; want none", len(jobList.Items), err)
	}
	if len(fixture.pullRequests.calls) != 0 || len(fixture.pullRequests.replyTargets) != 0 {
		t.Fatalf("unowned event called forge: create=%d reply=%d", len(fixture.pullRequests.calls), len(fixture.pullRequests.replyTargets))
	}
}

func TestProcessForgeEventsDurablyDefersOneFailureAndContinues(t *testing.T) {
	fixture := newFixture(t)
	_, _, branch := createOwnedOpenPullRequest(t, fixture)
	active := controllerForgeEvent("active-follow-up", "review_comment", 42)
	active.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, active); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("start active event: %v", err)
	}

	blocked := controllerForgeEvent("blocked-follow-up", "review_comment", 42)
	blocked.Branch = branch
	unrelated := controllerForgeEvent("unrelated-follow-up", "review_comment", 777)
	for _, event := range []store.ForgeEvent{blocked, unrelated} {
		if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("durably deferred event caused global callback error: %v", err)
	}
	deferred, err := fixture.store.GetForgeEvent(fixture.ctx, blocked.ID)
	if err != nil || deferred.Status != store.ForgeEventPending || deferred.Attempts != 1 || deferred.NextAttemptAt == nil || !deferred.NextAttemptAt.After(time.Now().UTC()) {
		t.Fatalf("deferred event = %#v, %v", deferred, err)
	}
	handled, err := fixture.store.GetForgeEvent(fixture.ctx, unrelated.ID)
	if err != nil || handled.Status != store.ForgeEventHandled {
		t.Fatalf("unrelated due event = %#v, %v; want handled", handled, err)
	}
}

func TestProcessForgeEventsReturnsOutcomePersistenceFailure(t *testing.T) {
	fixture := newFixture(t)
	event := controllerForgeEvent("outcome-persistence-error", "review_comment", 777)
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", fixture.databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TRIGGER reject_forge_outcome BEFORE UPDATE ON forge_events BEGIN SELECT RAISE(FAIL, 'forced outcome persistence failure'); END`); err != nil {
		t.Fatal(err)
	}
	err = fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx)
	if err == nil || !strings.Contains(err.Error(), "forced outcome persistence failure") {
		t.Fatalf("ProcessForgeEvents persistence error = %v", err)
	}
}

func TestForgeEventPersistenceFailureIsNotReclassified(t *testing.T) {
	fixture := newFixture(t)
	event := controllerForgeEvent("nested-persistence-error", "review_comment", 777)
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("outcome write failed")
	err := fixture.controller.(*Controller).persistForgeEventFailure(fixture.ctx, event, "recover", forgeEventPersistenceError{cause})
	if !errors.Is(err, cause) {
		t.Fatalf("persistence failure was discarded: %v", err)
	}
	stored, getErr := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if getErr != nil || stored.Attempts != 0 || stored.NextAttemptAt != nil {
		t.Fatalf("persistence failure was reclassified as processing failure: %#v, %v", stored, getErr)
	}
}

func TestPermanentRunningForgeFailureRequiresDurableCancellation(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("cancellation-persistence-error", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	running, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil {
		t.Fatal(err)
	}
	execFixtureSQL(t, fixture, `CREATE TRIGGER reject_forge_cancellation BEFORE UPDATE OF cancellation_requested ON tasks WHEN NEW.cancellation_requested = 1 BEGIN SELECT RAISE(FAIL, 'forced cancellation persistence failure'); END`)
	cause := forge.MarkPermanent(errors.New("provider ownership lost"))
	err = control.persistForgeEventFailure(fixture.ctx, running, "recover", cause)
	var persistenceErr forgeEventPersistenceError
	if !errors.As(err, &persistenceErr) || !errors.Is(err, cause) || !strings.Contains(err.Error(), "forced cancellation persistence failure") {
		t.Fatalf("persistForgeEventFailure() = %v; want cancellation persistence error", err)
	}
	stored, getErr := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	current := getTask(t, fixture, record.ID)
	if getErr != nil || stored.Status != store.ForgeEventRunning || current.CancellationRequested {
		t.Fatalf("failed cancellation persistence changed outcomes: event=%#v task=%#v err=%v", stored, current, getErr)
	}
}

func TestPermanentFailureUsesDurableRunningAssociation(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("durable-running-association", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	if err := control.persistForgeEventFailure(fixture.ctx, event, "start", forge.MarkPermanent(errors.New("permanent post-start failure"))); err != nil {
		t.Fatal(err)
	}
	failed, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	current := getTask(t, fixture, record.ID)
	if err != nil || failed.Status != store.ForgeEventFailed || !current.CancellationRequested {
		t.Fatalf("durable running association outcomes: event=%#v task=%#v err=%v", failed, current, err)
	}
}

func TestForgeReplyFailurePersistenceErrorRemainsGlobal(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("reply-persistence-error", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	recordPushedFollowUp(t, fixture, followUp)
	providerErr := errors.New("provider unavailable")
	fixture.pullRequests.replyExistsErr = providerErr
	execFixtureSQL(t, fixture, `CREATE TRIGGER reject_reply_failure BEFORE UPDATE ON forge_events BEGIN SELECT RAISE(FAIL, 'forced reply persistence failure'); END`)

	err = control.ProcessForgeEvents(fixture.ctx)
	var persistenceErr forgeEventPersistenceError
	if !errors.As(err, &persistenceErr) || !errors.Is(err, providerErr) || !strings.Contains(err.Error(), "forced reply persistence failure") {
		t.Fatalf("ProcessForgeEvents reply persistence error = %v", err)
	}
	stored, getErr := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if getErr != nil || stored.Status != store.ForgeEventRunning || stored.LastError != "" {
		t.Fatalf("reply persistence failure changed event outcome: %#v, %v", stored, getErr)
	}
}

func TestRunningForgeEventDeterministicRecoveryFailuresAreTerminal(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *fixture, store.Task, store.Attempt, store.Attempt, string) *Controller
	}{
		{
			name: "missing task attempt association",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, _ store.Attempt, _ store.Attempt, eventID string) *Controller {
				execFixtureSQL(t, fixture, `UPDATE forge_events SET status = 'running' WHERE id = ?`, eventID)
				return fixture.controller.(*Controller)
			},
		},
		{
			name: "current attempt mismatch",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, original, _ store.Attempt, eventID string) *Controller {
				execFixtureSQL(t, fixture, `UPDATE forge_events SET attempt_id = ? WHERE id = ?`, original.ID, eventID)
				return fixture.controller.(*Controller)
			},
		},
		{
			name: "removed repository config",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, _, _ store.Attempt, _ string) *Controller {
				cfg := fixture.config
				cfg.Repositories = nil
				control, err := New(fixture.store, fixture.kube, cfg, fixture.pullRequests)
				if err != nil {
					t.Fatal(err)
				}
				return control
			},
		},
		{
			name: "malformed immutable snapshot",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, _, followUp store.Attempt, _ string) *Controller {
				execFixtureSQL(t, fixture, `UPDATE task_attempts SET resource_snapshot = '{' WHERE id = ?`, followUp.ID)
				return fixture.controller.(*Controller)
			},
		},
		{
			name: "malformed immutable target",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, _, followUp store.Attempt, _ string) *Controller {
				recordPushedFollowUp(t, fixture, followUp)
				var snapshot map[string]json.RawMessage
				if err := json.Unmarshal(followUp.ResourceSnapshot, &snapshot); err != nil {
					t.Fatal(err)
				}
				snapshot["forge_target"] = json.RawMessage("null")
				encoded, err := json.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				execFixtureSQL(t, fixture, `UPDATE task_attempts SET resource_snapshot = ? WHERE id = ?`, encoded, followUp.ID)
				return fixture.controller.(*Controller)
			},
		},
		{
			name: "durable pull request mismatch",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, _, followUp store.Attempt, _ string) *Controller {
				recordPushedFollowUp(t, fixture, followUp)
				execFixtureSQL(t, fixture, `UPDATE pull_requests SET number = 43 WHERE attempt_id = ?`, followUp.ID)
				return fixture.controller.(*Controller)
			},
		},
		{
			name: "immutable repository coordinate mismatch",
			mutate: func(t *testing.T, fixture *fixture, _ store.Task, _, followUp store.Attempt, _ string) *Controller {
				recordPushedFollowUp(t, fixture, followUp)
				var snapshot attemptResourceSnapshot
				if err := json.Unmarshal(followUp.ResourceSnapshot, &snapshot); err != nil {
					t.Fatal(err)
				}
				snapshot.ForgeTarget.Repository = "other-widget"
				encoded, err := json.Marshal(snapshot)
				if err != nil {
					t.Fatal(err)
				}
				execFixtureSQL(t, fixture, `UPDATE task_attempts SET resource_snapshot = ? WHERE id = ?`, encoded, followUp.ID)
				return fixture.controller.(*Controller)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			event := controllerForgeEvent("terminal-"+strings.ReplaceAll(test.name, " ", "-"), "review_comment", 42)
			var record store.Task
			var original, followUp store.Attempt
			if test.name == "missing task attempt association" {
				if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
					t.Fatal(err)
				}
			} else {
				var branch string
				record, original, branch = createOwnedOpenPullRequest(t, fixture)
				event.Branch = branch
				if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
					t.Fatal(err)
				}
				if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
					t.Fatal(err)
				}
				var err error
				followUp, err = fixture.store.CurrentAttempt(fixture.ctx, record.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			control := test.mutate(t, fixture, record, original, followUp, event.ID)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("durably terminal recovery returned callback error: %v", err)
			}
			failed, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if err != nil || failed.Status != store.ForgeEventFailed || failed.FailedAt == nil || failed.NextAttemptAt != nil {
				t.Fatalf("terminal event = %#v, %v", failed, err)
			}
		})
	}
}

func TestRunningForgeEventRecoveryKeepsAtomicSnapshotAndDoesNotDuplicateWork(t *testing.T) {
	fixture := newFixture(t)
	record, original, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("recover-atomic-snapshot", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || len(attempt.ManifestJSON) == 0 || len(attempt.ResourceSnapshot) == 0 || attempt.ConfigDigest == "" {
		t.Fatalf("started attempt = %#v, %v; want complete atomic snapshot", attempt, err)
	}
	running, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.(*Controller).recoverRunningForgeEvent(fixture.ctx, running); err != nil {
		t.Fatalf("recoverRunningForgeEvent: %v", err)
	}

	recovered, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || recovered.ID != attempt.ID || !recovered.Immutable || string(recovered.ResourceSnapshot) != string(attempt.ResourceSnapshot) || recovered.ConfigDigest != attempt.ConfigDigest {
		t.Fatalf("recovered attempt = %#v, %v; want snapshotted immutable attempt %q", recovered, err, attempt.ID)
	}
	storedEvent, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || storedEvent.Status != store.ForgeEventRunning || storedEvent.AttemptID != attempt.ID {
		t.Fatalf("recovered event = %#v, %v; want running on original attempt", storedEvent, err)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, record.ID)
	jobsList, jobsErr := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil || jobsErr != nil || len(attempts) != 2 || attempts[0].ID != original.ID || attempts[1].ID != attempt.ID || len(jobsList.Items) != 2 || len(fixture.pullRequests.calls) != 1 {
		t.Fatalf("recovery duplicated lifecycle: attempts=%#v Jobs=%d PR creates=%d errors=%v/%v", attempts, len(jobsList.Items), len(fixture.pullRequests.calls), err, jobsErr)
	}
	manifest := followUpManifest(t, fixture, record.ID, recovered)
	if manifest.Prompt != attempt.Prompt || manifest.TaskBranch != attempt.TaskBranch {
		t.Fatalf("recovered manifest = %#v; want immutable attempt prompt/branch", manifest)
	}
}

func execFixtureSQL(t *testing.T, fixture *fixture, statement string, args ...any) {
	t.Helper()
	db, err := sql.Open("sqlite", fixture.databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.Exec(statement, args...); err != nil {
		t.Fatal(err)
	}
}

func recordPushedFollowUp(t *testing.T, fixture *fixture, attempt store.Attempt) {
	t.Helper()
	if err := fixture.store.RecordGitResult(fixture.ctx, store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: attempt.TaskBranch, CommitSHA: fullCommitSHA}); err != nil {
		t.Fatal(err)
	}
}

func TestProcessForgeEventsStartsOneOwnedFollowUpWithCurrentPRContext(t *testing.T) {
	for _, test := range []struct {
		name      string
		kind      string
		configure func(*store.ForgeEvent)
		want      []string
	}{
		{
			name: "review comment",
			kind: "review_comment",
			want: []string{"fix the parser", "Ada", "Preserve quoted commas", "501", "https://bitbucket.example/acme/widget/pull-requests/42#comment-501"},
		},
		{
			name: "quality gate",
			kind: "quality_gate_failed",
			configure: func(event *store.ForgeEvent) {
				event.PullRequestNumber = 0
				event.Branch = ""
				event.CommentID, event.CommentKind = 0, ""
				event.Title = "go test ./..."
				event.Body = "TestWidget failed: got 2, want 1"
				event.Author = "buildkite"
				event.URL = ""
			},
			want: []string{"fix the parser", "go test ./...", "TestWidget failed: got 2, want 1", "buildkite"},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			record, originalAttempt, branch := createOwnedOpenPullRequest(t, fixture)
			event := controllerForgeEvent("owned-"+strings.ReplaceAll(test.name, " ", "-"), test.kind, 42)
			event.Branch = branch
			if test.configure != nil {
				test.configure(&event)
			}
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
				t.Fatalf("put forge event: %v", err)
			}

			control := fixture.controller.(*Controller)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents(): %v", err)
			}
			followUp, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
			if err != nil {
				t.Fatalf("current follow-up attempt: %v", err)
			}
			if followUp.ID == originalAttempt.ID || followUp.Number != 2 || followUp.State != task.JOB_PENDING || followUp.BaseBranch != branch || followUp.TaskBranch != branch {
				t.Fatalf("follow-up attempt = %#v; want attempt 2 on current PR head %q", followUp, branch)
			}
			for _, value := range append(test.want, event.Title, event.CommitSHA, event.Branch) {
				if !strings.Contains(followUp.Prompt, value) {
					t.Errorf("follow-up prompt %q does not contain %q", followUp.Prompt, value)
				}
			}
			manifest := followUpManifest(t, fixture, record.ID, followUp)
			if manifest.BaseBranch != branch || manifest.TaskBranch != branch || manifest.Prompt != followUp.Prompt {
				t.Fatalf("follow-up manifest = %#v; want current PR branch and immutable attempt prompt", manifest)
			}
			jobsList, err := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
			if err != nil || len(jobsList.Items) != 2 {
				t.Fatalf("Jobs after follow-up = %d, %v; want original plus one follow-up", len(jobsList.Items), err)
			}
			attempts, err := fixture.store.ListAttempts(fixture.ctx, record.ID)
			if err != nil || len(attempts) != 2 {
				t.Fatalf("attempts after follow-up = %#v, %v; want exactly two", attempts, err)
			}

			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("replay ProcessForgeEvents(): %v", err)
			}
			jobsList, _ = fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
			attempts, _ = fixture.store.ListAttempts(fixture.ctx, record.ID)
			if len(jobsList.Items) != 2 || len(attempts) != 2 || len(fixture.pullRequests.calls) != 1 {
				t.Fatalf("replayed processing duplicated resources: Jobs=%d attempts=%d PR creates=%d", len(jobsList.Items), len(attempts), len(fixture.pullRequests.calls))
			}
		})
	}
}

func TestForgeFollowUpRejectsPullRequestThatIsNoLongerExactAndOpen(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*forge.PullRequestState)
	}{
		{name: "closed", mutate: func(state *forge.PullRequestState) { state.State = "closed" }},
		{name: "merged", mutate: func(state *forge.PullRequestState) { state.State = "merged" }},
		{name: "retargeted base", mutate: func(state *forge.PullRequestState) { state.DestinationBranch = "release" }},
		{name: "changed head", mutate: func(state *forge.PullRequestState) { state.SourceBranch = "other/head" }},
		{name: "changed source repository", mutate: func(state *forge.PullRequestState) { state.SourceRepository = "fork" }},
		{name: "changed head SHA", mutate: func(state *forge.PullRequestState) { state.HeadSHA = strings.Repeat("f", 40) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			record, original, branch := createOwnedOpenPullRequest(t, fixture)
			event := controllerForgeEvent("live-pr-"+strings.ReplaceAll(test.name, " ", "-"), "review_comment", 42)
			event.Branch = branch
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
				t.Fatal(err)
			}
			state := forge.PullRequestState{
				Number: 42, State: "open", SourceOwner: "acme", SourceRepository: "widget",
				SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA,
			}
			test.mutate(&state)
			fixture.pullRequests.getResult = &state

			if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents: %v", err)
			}
			stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if err != nil || stored.Status != store.ForgeEventFailed || stored.FailedAt == nil || stored.AttemptID != "" {
				t.Fatalf("rejected forge event = %#v, %v; want terminal failure before attempt", stored, err)
			}
			attempts, err := fixture.store.ListAttempts(fixture.ctx, record.ID)
			jobsList, jobsErr := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
			if err != nil || jobsErr != nil || len(attempts) != 1 || attempts[0].ID != original.ID || len(jobsList.Items) != 1 {
				t.Fatalf("rejected event created work: attempts=%#v Jobs=%d errors=%v/%v", attempts, len(jobsList.Items), err, jobsErr)
			}
			current := getTask(t, fixture, record.ID)
			if current.CancellationRequested || current.State != task.PR_OPEN {
				t.Fatalf("pending event failure cancelled original task: %#v", current)
			}
			if fixture.pullRequests.getCalls != 1 || fixture.pullRequests.getNumbers[0] != 42 {
				t.Fatalf("provider inspections = %d numbers=%v; want one for PR 42", fixture.pullRequests.getCalls, fixture.pullRequests.getNumbers)
			}
		})
	}
}

func TestForgeFollowUpBuildFailureLeavesPendingEventAndCurrentAttemptUntouched(t *testing.T) {
	fixture := newFixture(t)
	record, original, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("follow-up-build-failure", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	fixture.controller.(*Controller).config.Repositories[0].Worker.Image = ""

	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents: %v", err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventPending || stored.AttemptID != "" || stored.TaskID != "" {
		t.Fatalf("event after pre-commit build failure = %#v, %v", stored, err)
	}
	current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || current.ID != original.ID {
		t.Fatalf("current attempt after pre-commit build failure = %#v, %v", current, err)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, record.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts after pre-commit build failure = %#v, %v", attempts, err)
	}
}

func TestForgeFollowUpRecoveryUsesAtomicSnapshotAfterConfigMutation(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("atomic-snapshot-config-mutation", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || len(followUp.ManifestJSON) == 0 || len(followUp.ResourceSnapshot) == 0 || followUp.ConfigDigest == "" {
		t.Fatalf("atomically snapshotted follow-up = %#v, %v", followUp, err)
	}
	var snapshot attemptResourceSnapshot
	if err := json.Unmarshal(followUp.ResourceSnapshot, &snapshot); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.BatchV1().Jobs(workerNamespace).Delete(fixture.ctx, snapshot.Job.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.CoreV1().Secrets(workerNamespace).Delete(fixture.ctx, snapshot.Secret.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	control.config.Repositories[0].Worker.Image = "registry.example/mutated:v99"
	control.config.Repositories[0].OpenCode.Command = []string{"mutated-command"}
	control.config.Repositories[0].Bitbucket.Repository = "mutated-repository"

	if err := control.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile after config mutation: %v", err)
	}
	recreated, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, snapshot.Job.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get recreated Job: %v", err)
	}
	if recreated.Spec.Template.Spec.Containers[0].Image != workerImage {
		t.Fatalf("recreated Job image = %q; want %q", recreated.Spec.Template.Spec.Containers[0].Image, workerImage)
	}
	recreatedSecret, err := fixture.kube.CoreV1().Secrets(workerNamespace).Get(fixture.ctx, snapshot.Secret.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get recreated Secret: %v", err)
	}
	var manifest protocol.TaskManifest
	if err := json.Unmarshal(recreatedSecret.Data["task.json"], &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.OpenCodeCommand) != 1 || manifest.OpenCodeCommand[0] != "opencode" {
		t.Fatalf("recreated worker command = %#v; want original command", manifest.OpenCodeCommand)
	}
	current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || string(current.ManifestJSON) != string(followUp.ManifestJSON) || string(current.ResourceSnapshot) != string(followUp.ResourceSnapshot) || current.ConfigDigest != followUp.ConfigDigest {
		t.Fatalf("snapshot changed after recovery = %#v, %v", current, err)
	}
	target, err := control.attemptForgeTarget(record, current)
	if err != nil || target.Repository != "widget" {
		t.Fatalf("immutable forge target = %#v, %v", target, err)
	}
}

func TestForgeCompletionDefersPriorHeadLagThenRecovers(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("completion-prior-head-lag", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	newSHA := strings.Repeat("1", 40)
	if err := fixture.store.RecordGitResult(fixture.ctx, store.GitResult{AttemptID: followUp.ID, State: "pushed", Branch: branch, CommitSHA: newSHA}); err != nil {
		t.Fatal(err)
	}
	fixture.pullRequests.getResult = &forge.PullRequestState{
		Number: 42, State: "open", SourceOwner: "acme", SourceRepository: "widget",
		SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA,
	}
	if err := control.completeForgeEventLocked(fixture.ctx, record, followUp); err != nil {
		t.Fatalf("complete during provider lag: %v", err)
	}
	lagged, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || lagged.Status != store.ForgeEventRunning || lagged.NextAttemptAt == nil || len(fixture.pullRequests.replies) != 0 {
		t.Fatalf("lagged completion = %#v, replies=%d, %v", lagged, len(fixture.pullRequests.replies), err)
	}
	execFixtureSQL(t, fixture, `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), event.ID)
	fixture.pullRequests.getResult.HeadSHA = newSHA
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("recover completion after provider catches up: %v", err)
	}
	handled, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || handled.Status != store.ForgeEventHandled || len(fixture.pullRequests.replies) != 1 {
		t.Fatalf("recovered completion = %#v, replies=%d, %v", handled, len(fixture.pullRequests.replies), err)
	}
}

func TestForgeCompletionPermanentlyRejectsProviderOwnershipDrift(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*forge.PullRequestState)
	}{
		{name: "closed", mutate: func(state *forge.PullRequestState) { state.State = "declined" }},
		{name: "retargeted base", mutate: func(state *forge.PullRequestState) { state.DestinationBranch = "release" }},
		{name: "changed head ref", mutate: func(state *forge.PullRequestState) { state.SourceBranch = "external/head" }},
		{name: "unrelated head SHA", mutate: func(state *forge.PullRequestState) { state.HeadSHA = strings.Repeat("f", 40) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			record, _, branch := createOwnedOpenPullRequest(t, fixture)
			event := controllerForgeEvent("completion-drift-"+strings.ReplaceAll(test.name, " ", "-"), "review_comment", 42)
			event.Branch = branch
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
				t.Fatal(err)
			}
			control := fixture.controller.(*Controller)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatal(err)
			}
			followUp, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
			if err != nil {
				t.Fatal(err)
			}
			newSHA := strings.Repeat("1", 40)
			if err := fixture.store.RecordGitResult(fixture.ctx, store.GitResult{AttemptID: followUp.ID, State: "pushed", Branch: branch, CommitSHA: newSHA}); err != nil {
				t.Fatal(err)
			}
			live := forge.PullRequestState{
				Number: 42, State: "open", SourceOwner: "acme", SourceRepository: "widget",
				SourceBranch: branch, DestinationBranch: "main", HeadSHA: newSHA,
			}
			test.mutate(&live)
			fixture.pullRequests.getResult = &live
			if err := control.completeForgeEventLocked(fixture.ctx, record, followUp); err != nil {
				t.Fatalf("complete drift: %v", err)
			}
			failed, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if err != nil || failed.Status != store.ForgeEventFailed || failed.FailedAt == nil || len(fixture.pullRequests.replies) != 0 {
				t.Fatalf("drifted completion = %#v, replies=%d, %v", failed, len(fixture.pullRequests.replies), err)
			}
		})
	}
}

func TestRunningForgeFollowUpRejectsClosureOrRefDriftBeforeMoreWork(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*forge.PullRequestState)
	}{
		{name: "closed", mutate: func(state *forge.PullRequestState) { state.State = "declined" }},
		{name: "changed head ref", mutate: func(state *forge.PullRequestState) { state.SourceBranch = "external/head" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			record, _, branch := createOwnedOpenPullRequest(t, fixture)
			event := controllerForgeEvent("running-drift-"+strings.ReplaceAll(test.name, " ", "-"), "review_comment", 42)
			event.Branch = branch
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
				t.Fatal(err)
			}
			control := fixture.controller.(*Controller)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatal(err)
			}
			live := forge.PullRequestState{
				Number: 42, State: "open", SourceOwner: "acme", SourceRepository: "widget",
				SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA,
			}
			test.mutate(&live)
			fixture.pullRequests.getResult = &live
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents drift: %v", err)
			}
			failed, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if err != nil || failed.Status != store.ForgeEventFailed || failed.FailedAt == nil || len(fixture.pullRequests.replies) != 0 {
				t.Fatalf("running drift event = %#v, replies=%d, %v", failed, len(fixture.pullRequests.replies), err)
			}
			current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
			if err != nil || current.ID != failed.AttemptID {
				t.Fatalf("current follow-up after drift = %#v, %v", current, err)
			}
		})
	}
}

func TestPermanentRunningForgeFailureCancelsAttemptAndReconciliationRemovesResources(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("running-orphan-prevention", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName := jobs.Name(record.ID, followUp.Number)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.UID, job.Status.Active = types.UID("running-follow-up-job"), 1
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := control.Reconcile(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	secretName := job.Spec.Template.Spec.Volumes[0].Secret.SecretName
	fixture.pullRequests.getResult = &forge.PullRequestState{
		Number: 42, State: "declined", SourceOwner: "acme", SourceRepository: "widget",
		SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA,
	}

	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents permanent ownership loss: %v", err)
	}
	failed, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	current := getTask(t, fixture, record.ID)
	if err != nil || failed.Status != store.ForgeEventFailed || !current.CancellationRequested || current.State != task.RUNNING {
		t.Fatalf("permanent failure outcomes: event=%#v task=%#v err=%v", failed, current, err)
	}
	if err := control.HandleWorkerEvent(fixture.ctx, jobName, "late-pod", protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: strings.Repeat("1", 40)}); err != nil {
		t.Fatalf("late branch_pushed: %v", err)
	}
	current = getTask(t, fixture, record.ID)
	if current.State != task.RUNNING || len(fixture.pullRequests.replies) != 0 {
		t.Fatalf("late branch_pushed changed cancelled follow-up: task=%#v replies=%d", current, len(fixture.pullRequests.replies))
	}
	if _, err := fixture.store.GetGitResult(fixture.ctx, followUp.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("late branch_pushed Git result error = %v; want not found", err)
	}

	if err := control.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile cancellation: %v", err)
	}
	current = getTask(t, fixture, record.ID)
	if current.State != task.CANCELLED || current.CancellationRequested {
		t.Fatalf("reconciled task = %#v; want CANCELLED", current)
	}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cancelled follow-up Job still exists: %v", err)
	}
	if _, err := fixture.kube.CoreV1().Secrets(workerNamespace).Get(fixture.ctx, secretName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("cancelled follow-up Secret still exists: %v", err)
	}
	failed, err = fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || failed.Status != store.ForgeEventFailed || len(fixture.pullRequests.replies) != 0 {
		t.Fatalf("reconciled failed event = %#v, replies=%d, %v", failed, len(fixture.pullRequests.replies), err)
	}
}

func TestForgeFollowUpDefersTransientPullRequestInspectionBeforeAttempt(t *testing.T) {
	fixture := newFixture(t)
	record, original, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("live-pr-transient", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	fixture.pullRequests.getErr = errors.New("provider temporarily unavailable")

	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents: %v", err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventPending || stored.Attempts != 1 || stored.NextAttemptAt == nil || stored.AttemptID != "" {
		t.Fatalf("deferred forge event = %#v, %v; want paced pending event before attempt", stored, err)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, record.ID)
	jobsList, jobsErr := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil || jobsErr != nil || len(attempts) != 1 || attempts[0].ID != original.ID || len(jobsList.Items) != 1 {
		t.Fatalf("transient inspection created work: attempts=%#v Jobs=%d errors=%v/%v", attempts, len(jobsList.Items), err, jobsErr)
	}
}

func TestForgeFollowUpBranchPushReusesPullRequestAndRepliesOnce(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("owned-reply", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents(): %v", err)
	}
	followUp := startCurrentAttemptWorker(t, fixture, record.ID)
	jobName, podName := jobs.Name(record.ID, followUp.Number), "worker-pod-a2"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_result", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_succeeded", TaskID: record.ID})
	branchPushed := protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA}
	handleEvent(t, fixture, jobName, podName, branchPushed)

	if got := getTask(t, fixture, record.ID).State; got != task.PR_OPEN {
		t.Fatalf("follow-up branch state = %q; want %q", got, task.PR_OPEN)
	}
	if len(fixture.pullRequests.calls) != 1 {
		t.Fatalf("pull request create calls = %d; want original PR only", len(fixture.pullRequests.calls))
	}
	wantTarget := forge.Target{Provider: forge.ProviderBitbucket, BaseURL: "https://api.bitbucket.org", Owner: "acme", Repository: "widget", CredentialsSecret: "bitbucket-widget"}
	if len(fixture.pullRequests.replyTargets) != 1 || fixture.pullRequests.replyTargets[0] != wantTarget {
		t.Fatalf("pull request reply targets = %#v; want one reply to %#v", fixture.pullRequests.replyTargets, wantTarget)
	}
	if reply := fixture.pullRequests.replies[0]; reply.PullRequestNumber != 42 || reply.CommentID != event.CommentID || reply.CommentKind != event.CommentKind || strings.TrimSpace(reply.Body) == "" {
		t.Fatalf("pull request reply = %#v; want original PR/comment identity and a body", reply)
	}
	handled, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || handled.Status != "handled" || handled.AttemptID != followUp.ID || handled.HandledAt == nil {
		t.Fatalf("handled forge event = %#v, %v", handled, err)
	}

	handleEvent(t, fixture, jobName, podName, branchPushed)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("replay handled forge event: %v", err)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, record.ID)
	if err != nil || len(attempts) != 2 || len(fixture.pullRequests.calls) != 1 || len(fixture.pullRequests.replyTargets) != 1 {
		t.Fatalf("reply replay duplicated lifecycle: attempts=%d PR creates=%d replies=%d err=%v", len(attempts), len(fixture.pullRequests.calls), len(fixture.pullRequests.replyTargets), err)
	}
}

func TestFailedForgeFollowUpRetryReusesPromptBranchAndPullRequest(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("owned-retry", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("start follow-up: %v", err)
	}
	failed, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatalf("current failed follow-up: %v", err)
	}
	transition(t, fixture, record.ID, task.JOB_PENDING, task.FAILED, "follow-up Job failed", "kubernetes")
	exhaustAttemptLogs(t, fixture, record.ID, failed.ID)

	retry, err := control.RetryWithKey(fixture.ctx, record.ID, "retry-owned-follow-up")
	if err != nil {
		t.Fatalf("RetryWithKey: %v", err)
	}
	if retry.Number != 3 || retry.Prompt != failed.Prompt || retry.BaseBranch != branch || retry.TaskBranch != branch {
		t.Fatalf("retry attempt = %#v; want attempt 3 with failed follow-up context", retry)
	}
	manifest := followUpManifest(t, fixture, record.ID, retry)
	if manifest.Prompt != failed.Prompt || manifest.BaseBranch != branch || manifest.TaskBranch != branch {
		t.Fatalf("retry manifest = %#v; want same follow-up prompt and PR head", manifest)
	}
	if len(fixture.pullRequests.calls) != 1 {
		t.Fatalf("pull request creates before retry push = %d; want original only", len(fixture.pullRequests.calls))
	}

	startCurrentAttemptWorker(t, fixture, record.ID)
	jobName, podName := jobs.Name(record.ID, retry.Number), "worker-pod-a2"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_result", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_succeeded", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA})
	if len(fixture.pullRequests.calls) != 1 || len(fixture.pullRequests.replies) != 1 {
		t.Fatalf("retry completion created/replied = %d/%d; want one original PR and one reply", len(fixture.pullRequests.calls), len(fixture.pullRequests.replies))
	}
	handled, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || handled.Status != store.ForgeEventHandled || handled.AttemptID != retry.ID {
		t.Fatalf("retried forge event = %#v, %v", handled, err)
	}
}

func TestForgeReplyProviderFailuresAreIsolatedAndRecoveryContinues(t *testing.T) {
	for _, test := range []struct {
		name       string
		err        error
		wantStatus string
	}{
		{name: "transient", err: errors.New("temporary provider outage"), wantStatus: store.ForgeEventRunning},
		{name: "permanent", err: forge.MarkPermanent(errors.New("invalid provider route")), wantStatus: store.ForgeEventFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			record, _, branch := createOwnedOpenPullRequest(t, fixture)
			event := controllerForgeEvent("reply-error-"+test.name, "review_comment", 42)
			event.Branch = branch
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
				t.Fatalf("put forge event: %v", err)
			}
			control := fixture.controller.(*Controller)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("start follow-up: %v", err)
			}
			followUp := startCurrentAttemptWorker(t, fixture, record.ID)
			jobName, podName := jobs.Name(record.ID, followUp.Number), "worker-pod-a2"
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: record.ID})
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: record.ID})
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_result", TaskID: record.ID})
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_succeeded", TaskID: record.ID})
			fixture.pullRequests.replyExistsErr = test.err
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA})
			stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if err != nil || stored.Status != test.wantStatus || !strings.Contains(stored.LastError, test.err.Error()) {
				t.Fatalf("provider-failed event = %#v, %v", stored, err)
			}
			if test.wantStatus == store.ForgeEventRunning && (stored.NextAttemptAt == nil || !stored.NextAttemptAt.After(time.Now().UTC())) {
				t.Fatalf("transient provider failure was not deferred: %#v", stored)
			}
			if test.wantStatus == store.ForgeEventFailed && (stored.FailedAt == nil || stored.HandledAt != nil) {
				t.Fatalf("permanently failed event timestamps = %#v", stored)
			}
			exhaustAttemptLogs(t, fixture, record.ID, followUp.ID)
			next := controllerForgeEvent("next-after-"+test.name, "review_comment", 42)
			next.Branch = branch
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, next); err != nil {
				t.Fatalf("put next same-task event: %v", err)
			}
			other := controllerForgeEvent("unowned-after-"+test.name, "review_comment", 777)
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, other); err != nil {
				t.Fatalf("put unrelated event: %v", err)
			}
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents globally backed off provider reply error: %v", err)
			}
			processed, err := fixture.store.GetForgeEvent(fixture.ctx, other.ID)
			if err != nil || processed.Status != store.ForgeEventHandled {
				t.Fatalf("unrelated event = %#v, %v; want handled", processed, err)
			}
			current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
			if test.wantStatus == store.ForgeEventFailed {
				taskRecord := getTask(t, fixture, record.ID)
				pending, getErr := fixture.store.GetForgeEvent(fixture.ctx, next.ID)
				if err != nil || getErr != nil || current.ID != followUp.ID || !taskRecord.CancellationRequested || pending.Status != store.ForgeEventPending {
					t.Fatalf("next event after permanent failure escaped cancellation: current=%#v task=%#v event=%#v errors=%v/%v", current, taskRecord, pending, err, getErr)
				}
			} else if err != nil || current.ID != followUp.ID {
				t.Fatalf("transient failure allowed next same-task event: %#v, %v", current, err)
			}
		})
	}
}

func TestRunningForgeEventRecoversMarkedReplyWithoutSecondPost(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("recover-marked-reply", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("start follow-up: %v", err)
	}
	followUp := startCurrentAttemptWorker(t, fixture, record.ID)
	jobName, podName := jobs.Name(record.ID, followUp.Number), "worker-pod-a2"
	for _, workerEvent := range []protocol.Event{
		{Type: "agent_started", TaskID: record.ID}, {Type: "validation_started", TaskID: record.ID},
		{Type: "validation_result", TaskID: record.ID}, {Type: "validation_succeeded", TaskID: record.ID},
	} {
		handleEvent(t, fixture, jobName, podName, workerEvent)
	}
	fixture.pullRequests.replyErr = errors.New("connection lost after provider accepted reply")
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA})
	if len(fixture.pullRequests.replies) != 1 {
		t.Fatalf("initial reply posts = %d; want one", len(fixture.pullRequests.replies))
	}
	fixture.pullRequests.replyErr = nil
	fixture.pullRequests.replyExists = true
	running, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := control.recoverRunningForgeEvent(fixture.ctx, running); err != nil {
		t.Fatalf("recover marked reply: %v", err)
	}
	if len(fixture.pullRequests.replies) != 1 {
		t.Fatalf("recovery duplicate reply posts = %d; want one", len(fixture.pullRequests.replies))
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventHandled {
		t.Fatalf("recovered event = %#v, %v", stored, err)
	}
	if len(fixture.pullRequests.replyMarkers) < 2 || !strings.Contains(fixture.pullRequests.replies[0].Body, fixture.pullRequests.replyMarkers[0]) {
		t.Fatalf("reply marker checks/body = %#v / %q", fixture.pullRequests.replyMarkers, fixture.pullRequests.replies[0].Body)
	}
}

func TestCancelledForgeFollowUpAbandonsRunningEventWithoutReply(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("cancel-follow-up", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("start follow-up: %v", err)
	}
	if err := control.Cancel(fixture.ctx, record.ID); err != nil {
		t.Fatalf("cancel follow-up: %v", err)
	}
	if err := control.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile cancellation: %v", err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventHandled || stored.HandledAt == nil {
		t.Fatalf("cancelled forge event = %#v, %v", stored, err)
	}
	if len(fixture.pullRequests.replies) != 0 {
		t.Fatalf("cancelled follow-up fabricated %d replies", len(fixture.pullRequests.replies))
	}
}

func TestMatchForgeEventSkipsTaskForRemovedRepository(t *testing.T) {
	fixture := newFixture(t)
	record, original, branch := createOwnedOpenPullRequest(t, fixture)
	if _, err := fixture.store.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: "removed-repository", Prompt: "historical task"}); err != nil {
		t.Fatalf("create historical task: %v", err)
	}
	event := controllerForgeEvent("owned-after-removed", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents: %v", err)
	}
	current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || current.ID == original.ID || current.Number != 2 {
		t.Fatalf("owned task follow-up = %#v, %v", current, err)
	}
}

func TestQualityGateRequiresLatestPushedSHAWithAndWithoutPullRequestNumber(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("advance-quality-head", "quality_gate_failed", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("start follow-up: %v", err)
	}
	followUp := startCurrentAttemptWorker(t, fixture, record.ID)
	jobName, podName := jobs.Name(record.ID, followUp.Number), "worker-pod-a2"
	for _, workerEvent := range []protocol.Event{
		{Type: "agent_started", TaskID: record.ID}, {Type: "validation_started", TaskID: record.ID},
		{Type: "validation_result", TaskID: record.ID}, {Type: "validation_succeeded", TaskID: record.ID},
	} {
		handleEvent(t, fixture, jobName, podName, workerEvent)
	}
	newSHA := strings.Repeat("1", 40)
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: newSHA})

	for _, pullRequestNumber := range []int{0, 42} {
		delayed := controllerForgeEvent("delayed", "quality_gate_failed", pullRequestNumber)
		delayed.Branch, delayed.CommitSHA = "", fullCommitSHA
		matched, err := control.forgeEventMatchesTask(fixture.ctx, delayed, getTask(t, fixture, record.ID))
		if err != nil || matched != forgeDefinitelyUnowned {
			t.Fatalf("delayed quality event with PR %d matched=%v, err=%v; want superseded", pullRequestNumber, matched, err)
		}
		latest := delayed
		latest.CommitSHA = newSHA
		matched, err = control.forgeEventMatchesTask(fixture.ctx, latest, getTask(t, fixture, record.ID))
		if err != nil || matched != forgeOwned {
			t.Fatalf("latest quality event with PR %d matched=%v, err=%v", pullRequestNumber, matched, err)
		}
	}
	wrongBranch := controllerForgeEvent("wrong-branch-quality", "quality_gate_failed", 42)
	wrongBranch.Branch, wrongBranch.CommitSHA = "other/branch", newSHA
	matched, err := control.forgeEventMatchesTask(fixture.ctx, wrongBranch, getTask(t, fixture, record.ID))
	if err != nil || matched != forgeDefinitelyUnowned {
		t.Fatalf("wrong-branch quality event matched=%v, err=%v", matched, err)
	}
	blank := controllerForgeEvent("blank-quality", "quality_gate_failed", 42)
	blank.Branch, blank.CommitSHA = "", ""
	matched, err = control.forgeEventMatchesTask(fixture.ctx, blank, getTask(t, fixture, record.ID))
	if err != nil || matched != forgeDefinitelyUnowned {
		t.Fatalf("blank quality identity matched=%v, err=%v", matched, err)
	}
}

func controllerForgeEvent(id, kind string, pullRequestNumber int) store.ForgeEvent {
	event := store.ForgeEvent{
		ID:                id,
		Provider:          "bitbucket",
		Kind:              kind,
		Owner:             "acme",
		Repository:        "widget",
		PullRequestNumber: pullRequestNumber,
		CommitSHA:         fullCommitSHA,
		CommentID:         501,
		CommentKind:       "comment",
		Title:             "Parser review",
		Body:              "Preserve quoted commas",
		Author:            "Ada",
		URL:               "https://bitbucket.example/acme/widget/pull-requests/42#comment-501",
	}
	if kind == "quality_gate_failed" {
		event.CommentID, event.CommentKind = 0, ""
	}
	return event
}

func storedForgeEvent(event forge.Event) store.ForgeEvent {
	return store.ForgeEvent{
		ID: event.DeliveryID, Provider: string(event.Provider), Kind: event.Kind,
		Owner: event.Owner, Repository: event.Repository, PullRequestNumber: event.PullRequestNumber,
		CommitSHA: event.CommitSHA, Branch: event.Branch, CommentID: event.CommentID,
		CommentKind: event.CommentKind, Title: event.Title, Body: event.Body, Author: event.Author, URL: event.URL,
	}
}

func prepareAttemptForPush(t *testing.T, fixture *fixture, record store.Task) (store.Attempt, string) {
	t.Helper()
	jobName, podName := jobs.Name(record.ID, 1), "worker-pod-a1"
	for _, workerEvent := range []protocol.Event{
		{Type: "agent_started", TaskID: record.ID},
		{Type: "validation_started", TaskID: record.ID},
		{Type: "validation_result", TaskID: record.ID},
		{Type: "validation_succeeded", TaskID: record.ID},
	} {
		handleEvent(t, fixture, jobName, podName, workerEvent)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatalf("attempt manifest: %v", err)
	}
	return attempt, manifest.TaskBranch
}

func createOwnedOpenPullRequest(t *testing.T, fixture *fixture) (store.Task, store.Attempt, string) {
	t.Helper()
	record := createRunningTask(t, fixture, "fix the parser", "forge-owned-"+t.Name())
	jobName, podName := jobs.Name(record.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_result", TaskID: record.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_succeeded", TaskID: record.ID})
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatalf("current original attempt: %v", err)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatalf("original attempt manifest: %v", err)
	}
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA})
	exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
	return record, attempt, manifest.TaskBranch
}

func followUpManifest(t *testing.T, fixture *fixture, taskID string, attempt store.Attempt) protocol.TaskManifest {
	t.Helper()
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobs.Name(taskID, attempt.Number), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get follow-up Job: %v", err)
	}
	secretName := job.Spec.Template.Spec.Volumes[0].Secret.SecretName
	secret, err := fixture.kube.CoreV1().Secrets(workerNamespace).Get(fixture.ctx, secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get follow-up task Secret: %v", err)
	}
	var manifest protocol.TaskManifest
	if err := json.Unmarshal(secret.Data["task.json"], &manifest); err != nil {
		t.Fatalf("decode follow-up manifest: %v", err)
	}
	return manifest
}

func startCurrentAttemptWorker(t *testing.T, fixture *fixture, taskID string) store.Attempt {
	t.Helper()
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, taskID)
	if err != nil {
		t.Fatalf("current follow-up attempt: %v", err)
	}
	jobName := jobs.Name(taskID, attempt.Number)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get follow-up Job: %v", err)
	}
	job.UID = types.UID("job-uid-" + attempt.ID)
	job.Status.Active = 1
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark follow-up Job running: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() follow-up running Job: %v", err)
	}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-pod-a2", Namespace: workerNamespace, Labels: copyLabels(job.Labels),
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}},
	}}
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Create(fixture.ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create follow-up worker Pod: %v", err)
	}
	return attempt
}
