package controller

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestCandidateReplacementSHASettlesUntilDurableOrExhausted(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "repair validation", "candidate-replacement-settling")
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	control := fixture.controller.(*Controller)
	replacementSHA := strings.Repeat("b", 40)
	webhook := controllerForgeEvent("replacement-webhook", "review_comment", 42)
	webhook.Branch, webhook.CommitSHA = branch, replacementSHA

	match, err := control.forgeEventMatchesTask(fixture.ctx, webhook, getTask(t, fixture, record.ID))
	if err != nil || match != forgeSettling {
		t.Fatalf("replacement webhook before durable candidate = %v, %v; want settling", match, err)
	}
	for name, mutate := range map[string]func(*store.ForgeEvent){
		"provider":   func(event *store.ForgeEvent) { event.Provider = "github" },
		"owner":      func(event *store.ForgeEvent) { event.Owner = "other" },
		"repository": func(event *store.ForgeEvent) { event.Repository = "other" },
		"pull request": func(event *store.ForgeEvent) {
			event.PullRequestNumber++
		},
		"branch": func(event *store.ForgeEvent) { event.Branch = "other/branch" },
	} {
		t.Run("reject "+name, func(t *testing.T) {
			mismatched := webhook
			mutate(&mismatched)
			match, err := control.forgeEventMatchesTask(fixture.ctx, mismatched, getTask(t, fixture, record.ID))
			if err != nil || match != forgeDefinitelyUnowned {
				t.Fatalf("mismatched replacement webhook = %v, %v; want unowned", match, err)
			}
		})
	}

	fixture.pullRequests.getResult = &forge.PullRequestState{
		Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget",
		SourceBranch: branch, DestinationBranch: "main", HeadSHA: replacementSHA,
	}
	err = control.verifyAttemptProviderOwnership(fixture.ctx, getTask(t, fixture, record.ID), attempt, "")
	if err == nil || forge.IsPermanent(err) {
		t.Fatalf("active provider replacement error = %v; want transient settling", err)
	}
	validLive := *fixture.pullRequests.getResult
	for name, mutate := range map[string]func(*forge.PullRequestState){
		"pull request": func(state *forge.PullRequestState) { state.Number++ },
		"branch":       func(state *forge.PullRequestState) { state.SourceBranch = "other/branch" },
		"base":         func(state *forge.PullRequestState) { state.DestinationBranch = "release" },
		"owner":        func(state *forge.PullRequestState) { state.SourceOwner = "other" },
		"repository":   func(state *forge.PullRequestState) { state.SourceRepository = "other" },
	} {
		t.Run("reject provider "+name, func(t *testing.T) {
			mismatched := validLive
			mutate(&mismatched)
			fixture.pullRequests.getResult = &mismatched
			err := control.verifyAttemptProviderOwnership(fixture.ctx, getTask(t, fixture, record.ID), attempt, "")
			if err == nil || !forge.IsPermanent(err) {
				t.Fatalf("mismatched active provider identity error = %v; want permanent", err)
			}
		})
	}
	fixture.pullRequests.getResult = &validLive
	handleEvent(t, fixture, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", protocol.Event{
		Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: replacementSHA,
	})
	if err := control.verifyAttemptProviderOwnership(fixture.ctx, getTask(t, fixture, record.ID), attempt, ""); err != nil {
		t.Fatalf("durable replacement provider ownership: %v", err)
	}
	git, err := fixture.store.GetGitResult(fixture.ctx, attempt.ID)
	if err != nil || git.State != "candidate" || git.CommitSHA != replacementSHA {
		t.Fatalf("durable replacement candidate = %#v, %v", git, err)
	}

	exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
	attempt, err = fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil {
		t.Fatal(err)
	}
	unrelatedSHA := strings.Repeat("c", 40)
	webhook.CommitSHA = unrelatedSHA
	match, err = control.forgeEventMatchesTask(fixture.ctx, webhook, getTask(t, fixture, record.ID))
	if err != nil || match != forgeDefinitelyUnowned {
		t.Fatalf("exhausted unknown webhook = %v, %v; want unowned", match, err)
	}
	fixture.pullRequests.getResult.HeadSHA = unrelatedSHA
	err = control.verifyAttemptProviderOwnership(fixture.ctx, getTask(t, fixture, record.ID), attempt, "")
	if err == nil || !forge.IsPermanent(err) {
		t.Fatalf("exhausted provider drift error = %v; want permanent", err)
	}
}

func TestReviewWebhookSettlesThroughoutOpenCodeOwnedReceiptWindow(t *testing.T) {
	for _, target := range []task.State{task.RUNNING, task.AGENT_RUNNING, task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR} {
		t.Run(string(target), func(t *testing.T) {
			fixture := newFixture(t)
			record := createRunningTask(t, fixture, "fix it", "settling-"+string(target))
			jobName := jobs.Name(record.ID, 1)
			if target != task.RUNNING {
				handleEvent(t, fixture, jobName, "worker-pod-a1", protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID})
			}
			if target != task.RUNNING && target != task.AGENT_RUNNING {
				handleEvent(t, fixture, jobName, "worker-pod-a1", protocol.Event{Type: protocol.EventValidationStarted, TaskID: record.ID})
				if target != task.VALIDATING {
					handleEvent(t, fixture, jobName, "worker-pod-a1", protocol.Event{Type: protocol.EventValidationResult, TaskID: record.ID})
					handleEvent(t, fixture, jobName, "worker-pod-a1", protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: record.ID})
					transition(t, fixture, record.ID, task.VALIDATING, task.COMMITTING, "receipt settling", "controller")
					if target == task.PUSHING || target == task.CREATING_PR {
						transition(t, fixture, record.ID, task.COMMITTING, task.PUSHING, "receipt settling", "controller")
					}
					if target == task.CREATING_PR {
						transition(t, fixture, record.ID, task.PUSHING, task.CREATING_PR, "receipt settling", "controller")
					}
				}
			}
			event := controllerForgeEvent("settling-event", "review_comment", 42)
			match, err := fixture.controller.(*Controller).forgeEventMatchesTask(fixture.ctx, event, getTask(t, fixture, record.ID))
			if err != nil || match != forgeSettling {
				t.Fatalf("state %q match/error = %v/%v, want settling", target, match, err)
			}
		})
	}
}

func TestTerminalOrCancellingTaskDoesNotOwnNewForgeEvent(t *testing.T) {
	for _, test := range []struct {
		name                  string
		state                 task.State
		cancellationRequested bool
	}{
		{name: "failed", state: task.FAILED},
		{name: "cancelled", state: task.CANCELLED},
		{name: "cancellation requested", state: task.JOB_PENDING, cancellationRequested: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			assertTerminalTaskDoesNotOwnNewForgeEvent(t, test.name, test.state, test.cancellationRequested)
		})
	}
}

func assertTerminalTaskDoesNotOwnNewForgeEvent(t *testing.T, name string, state task.State, cancellationRequested bool) {
	t.Helper()
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	seed := controllerForgeEvent("terminal-seed-"+strings.ReplaceAll(name, " ", "-"), "review_comment", 42)
	seed.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, seed); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	candidateSHA := strings.Repeat("7", 40)
	if err := fixture.store.RecordPullRequestCandidate(fixture.ctx,
		store.GitResult{AttemptID: attempt.ID, State: "candidate", Branch: branch, CommitSHA: candidateSHA},
		store.PullRequest{AttemptID: attempt.ID, State: "reported", Number: 42, HeadBranch: branch, BaseBranch: "main"},
	); err != nil {
		t.Fatal(err)
	}
	exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
	if err := fixture.store.MarkForgeEventHandled(fixture.ctx, seed.ID); err != nil {
		t.Fatal(err)
	}
	if cancellationRequested {
		if err := fixture.store.RequestCancellation(fixture.ctx, record.ID); err != nil {
			t.Fatal(err)
		}
	} else {
		transition(t, fixture, record.ID, task.JOB_PENDING, state, "terminal ownership test", "controller")
	}

	event := controllerForgeEvent("terminal-new-"+strings.ReplaceAll(name, " ", "-"), "review_comment", 42)
	event.Branch, event.CommitSHA = branch, candidateSHA
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	if err != nil || stored.Status != store.ForgeEventHandled || stored.TaskID != "" || stored.AttemptID != "" {
		t.Fatalf("terminal task event = %#v, %v; want handled as unowned", stored, err)
	}
}

func TestReadyTaskStillOwnsNewForgeEvent(t *testing.T) {
	fixture := newFixture(t)
	record, attempt, branch := createOwnedOpenPullRequest(t, fixture)
	transition(t, fixture, record.ID, task.PR_OPEN, task.WAITING_CI, "CI started", "webhook")
	transition(t, fixture, record.ID, task.WAITING_CI, task.WAITING_REVIEW, "CI passed", "webhook")
	transition(t, fixture, record.ID, task.WAITING_REVIEW, task.READY, "review passed", "webhook")
	event := controllerForgeEvent("ready-follow-up", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := fixture.store.GetForgeEvent(fixture.ctx, event.ID)
	current, attemptErr := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || attemptErr != nil || stored.Status != store.ForgeEventRunning || current.ID == attempt.ID || current.Number != 2 {
		t.Fatalf("READY follow-up event/attempt = %#v/%#v, errors %v/%v", stored, current, err, attemptErr)
	}
}

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
	if err != nil || stored.Status != store.ForgeEventPending || stored.Attempts != 0 || stored.LastError != "" || stored.NextAttemptAt == nil {
		t.Fatalf("settling event = %#v, %v; want yielded pending", stored, err)
	}

	handleEvent(t, fixture, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", protocol.Event{
		Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA,
	})
	exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
	execFixtureSQL(t, fixture, `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), event.ID)
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
			if test.wantFollowUp && (err != nil || pending.Status != store.ForgeEventPending || pending.Attempts != 0 || pending.NextAttemptAt == nil) {
				t.Fatalf("creating-PR event = %#v, %v; want pending", pending, err)
			}
			if !test.wantFollowUp && (err != nil || pending.Status != store.ForgeEventHandled) {
				t.Fatalf("candidate-mismatched event = %#v, %v; want handled", pending, err)
			}
			fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
			handleEvent(t, fixture, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA})
			exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
			execFixtureSQL(t, fixture, `UPDATE forge_events SET next_attempt_at = ? WHERE id = ?`, time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), event.ID)
			if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
				t.Fatalf("ProcessForgeEvents after receipt verification: %v", err)
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
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: secondBranch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
	handleEvent(t, fixture, jobs.Name(second.ID, secondAttempt.Number), "worker-pod-a1", protocol.Event{
		Type: protocol.EventPullRequestReady, TaskID: second.ID, PullRequestNumber: 42, Branch: secondBranch, CommitSHA: fullCommitSHA,
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
