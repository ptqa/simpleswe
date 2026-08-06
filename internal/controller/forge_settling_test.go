package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestBranchlessBitbucketCommitStatusOwnsCurrentPushedSHA(t *testing.T) {
	fixture := newFixture(t)
	record, original, _ := createOwnedOpenPullRequest(t, fixture)
	payload := `{"actor":{"display_name":"External CI"},"repository":{"slug":"widget","workspace":{"slug":"acme"}},"commit_status":{"name":"unit tests","description":"failed","state":"FAILED","links":{"commit":{"href":"https://api.bitbucket.org/2.0/repositories/acme/widget/commit/` + fullCommitSHA + `"}}}}`
	parsed, actionable, err := bitbucket.ParseWebhook("branchless-controller", "repo:commit_status_updated", []byte(payload))
	if err != nil || !actionable {
		t.Fatalf("ParseWebhook() = %#v, %t, %v", parsed, actionable, err)
	}
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, storedForgeEvent(parsed)); err != nil {
		t.Fatalf("put parsed forge event: %v", err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents(): %v", err)
	}
	current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || current.ID == original.ID || current.Number != 2 {
		t.Fatalf("branchless status follow-up = %#v, %v", current, err)
	}
}

func TestQualityEventSettlesUntilProviderPushIsDurableLocally(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix the parser", "provider-push-race")
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	event := controllerForgeEvent("provider-push-race", "quality_gate_failed", 0)
	event.Branch, event.CommentID, event.CommentKind = branch, 0, ""
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents before local push: %v", err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventPending || stored.Attempts != 0 || stored.LastError != "" {
		t.Fatalf("settling event = %#v, %v; want untouched pending", stored, err)
	}

	handleEvent(t, fixture, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", protocol.Event{
		Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA,
	})
	exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents after local push: %v", err)
	}
	current, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || current.ID == attempt.ID || current.Number != 2 {
		t.Fatalf("eventual owned follow-up = %#v, %v", current, err)
	}
}

func TestReviewEventSettlesAcrossCreatingPullRequestReconciliation(t *testing.T) {
	for _, test := range []struct {
		name         string
		pullRequest  int
		wantFollowUp bool
	}{
		{name: "eventual owned", pullRequest: 42, wantFollowUp: true},
		{name: "eventual mismatch", pullRequest: 777},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			record := createRunningTask(t, fixture, "fix the parser", "creating-pr-"+test.name)
			attempt, branch := prepareAttemptForPush(t, fixture, record)
			if err := fixture.store.RecordGitResult(fixture.ctx, store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: fullCommitSHA}); err != nil {
				t.Fatalf("record pushed Git result: %v", err)
			}
			transition(t, fixture, record.ID, task.VALIDATING, task.COMMITTING, "push raced", "controller")
			transition(t, fixture, record.ID, task.COMMITTING, task.PUSHING, "push raced", "controller")
			transition(t, fixture, record.ID, task.PUSHING, task.CREATING_PR, "push raced", "controller")
			if _, err := fixture.store.ReservePullRequest(fixture.ctx, attempt.ID, record.Prompt, branch, "main"); err != nil {
				t.Fatalf("reserve creating pull request: %v", err)
			}
			exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
			event := controllerForgeEvent("creating-pr-"+test.name, "review_comment", test.pullRequest)
			event.Branch = branch
			if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
				t.Fatalf("put forge event: %v", err)
			}
			control := fixture.controller.(*Controller)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents before Reconcile: %v", err)
			}
			pending, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if err != nil || pending.Status != store.ForgeEventPending || pending.Attempts != 0 {
				t.Fatalf("creating-PR event = %#v, %v; want pending", pending, err)
			}
			if err := control.Reconcile(fixture.ctx); err != nil {
				t.Fatalf("Reconcile creating pull request: %v", err)
			}
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents after Reconcile: %v", err)
			}
			stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
			if test.wantFollowUp {
				current, currentErr := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
				if err != nil || stored.Status != store.ForgeEventRunning || currentErr != nil || current.ID == attempt.ID || current.Number != 2 {
					t.Fatalf("eventual owned event/current = %#v / %#v, errors %v / %v", stored, current, err, currentErr)
				}
				return
			}
			if err != nil || stored.Status != store.ForgeEventHandled || stored.TaskID != "" {
				t.Fatalf("eventual mismatched event = %#v, %v", stored, err)
			}
			jobList, listErr := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
			if listErr != nil || len(jobList.Items) != 1 {
				t.Fatalf("mismatched review Jobs = %d, %v; want original only", len(jobList.Items), listErr)
			}
		})
	}
}

func TestPositivePullRequestNumberSelectsDefinitiveOwnerDespiteSettlingTask(t *testing.T) {
	fixture := newFixture(t)
	owner, ownerAttempt, _ := createOwnedOpenPullRequest(t, fixture)
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, "worker-pod-a1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete owner worker Pod: %v", err)
	}
	settling := createRunningTask(t, fixture, "other work", "same-repository-settling")
	_, _ = prepareAttemptForPush(t, fixture, settling)

	event := controllerForgeEvent("owned-despite-settling", "review_comment", 42)
	event.Branch = ""
	record, _, match, err := fixture.controller.(*Controller).matchForgeEvent(fixture.ctx, event)
	if err != nil || match != forgeOwned || record.ID != owner.ID {
		t.Fatalf("matchForgeEvent = task %q, match %v, error %v; want definitive owner %q", record.ID, match, err, owner.ID)
	}
	quality := controllerForgeEvent("pr-less-quality-still-settles", "quality_gate_failed", 0)
	quality.Branch, quality.CommentID, quality.CommentKind = "", 0, ""
	record, _, match, err = fixture.controller.(*Controller).matchForgeEvent(fixture.ctx, quality)
	if err != nil || match != forgeSettling || record.ID != "" {
		t.Fatalf("matchForgeEvent PR-less quality = task %q, match %v, error %v; want settling precedence", record.ID, match, err)
	}
	current, err := fixture.store.CurrentAttempt(fixture.ctx, owner.ID)
	if err != nil || current.ID != ownerAttempt.ID {
		t.Fatalf("owner current attempt = %#v, %v", current, err)
	}
}

func TestBranchlessQualitySHAIsRejectedWhenAmbiguousAcrossTasks(t *testing.T) {
	fixture := newFixture(t)
	first, firstAttempt, _ := createOwnedOpenPullRequest(t, fixture)
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, "worker-pod-a1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete first worker Pod: %v", err)
	}
	second := createRunningTask(t, fixture, "fix the second parser", "forge-owned-second")
	secondAttempt, secondBranch := prepareAttemptForPush(t, fixture, second)
	handleEvent(t, fixture, jobs.Name(second.ID, secondAttempt.Number), "worker-pod-a1", protocol.Event{
		Type: protocol.EventBranchPushed, TaskID: second.ID, Branch: secondBranch, CommitSHA: fullCommitSHA,
	})
	exhaustAttemptLogs(t, fixture, second.ID, secondAttempt.ID)
	event := controllerForgeEvent("ambiguous-branchless-sha", "quality_gate_failed", 0)
	event.Branch, event.CommentID, event.CommentKind = "", 0, ""
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatalf("put forge event: %v", err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatalf("ProcessForgeEvents(): %v", err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventHandled || stored.TaskID != "" {
		t.Fatalf("ambiguous event = %#v, %v; want rejected without owner", stored, err)
	}
	for _, want := range []struct{ task, attempt string }{{first.ID, firstAttempt.ID}, {second.ID, secondAttempt.ID}} {
		current, err := fixture.store.CurrentAttempt(fixture.ctx, want.task)
		if err != nil || current.ID != want.attempt {
			t.Fatalf("task %q current attempt = %#v, %v; ambiguity started follow-up", want.task, current, err)
		}
	}
}
