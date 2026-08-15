package controller

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestDurableWorkerEventRequiresExactStoredProvenance(t *testing.T) {
	for _, mismatch := range []string{"event ID", "content", "task", "attempt", "job", "pod"} {
		t.Run(mismatch, func(t *testing.T) {
			fixture := newFixture(t)
			record := createRunningTask(t, fixture, "durable provenance", "durable-provenance-"+mismatch)
			attempt, branch := prepareAttemptForPush(t, fixture, record)
			jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
			event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
			queueDurableWorkerEvent(t, fixture, "receipt", "pod-uid", jobName, podName, event)
			eventID := "receipt"
			switch mismatch {
			case "event ID":
				eventID = "other-receipt"
			case "content":
				event.PullRequestNumber++
			case "task":
				other := createTask(t, fixture, "other task", "durable-provenance-other-task")
				event.TaskID = other.ID
			case "attempt":
				other := createTask(t, fixture, "other attempt", "durable-provenance-other-attempt")
				db, err := sql.Open("sqlite", fixture.databasePath)
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.ExecContext(fixture.ctx, `UPDATE worker_log_events SET attempt_id = ? WHERE id = 'receipt'`, other.CurrentAttemptID); err != nil {
					t.Fatal(err)
				}
			case "job":
				jobName = "other-job"
			case "pod":
				podName = "other-pod"
			}
			err := fixture.controller.(*Controller).HandleWorkerEventOnce(fixture.ctx, eventID, jobName, podName, event)
			if err == nil || fixture.pullRequests.getCalls != 0 || getTask(t, fixture, record.ID).State != task.VALIDATING {
				t.Fatalf("%s mismatch error/provider calls/state = %v/%d/%q", mismatch, err, fixture.pullRequests.getCalls, getTask(t, fixture, record.ID).State)
			}
		})
	}
}

func TestCompletedJobDefersMissingReportWhileDurableEventIsPending(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "pending receipt", "pending-receipt-reconcile")
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	queueDurableWorkerEvent(t, fixture, "pending-receipt", "pending-pod-uid", jobName, podName, event)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.Status.Active = 0
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	currentAttempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil || !currentAttempt.LogsExhausted || getTask(t, fixture, record.ID).State != task.VALIDATING {
		t.Fatalf("pending receipt reconciliation attempt/state = %#v/%q, %v", currentAttempt, getTask(t, fixture, record.ID).State, err)
	}
	for _, transition := range listEvents(t, fixture, record.ID) {
		if strings.Contains(transition.Reason, "did not report a pull request") {
			t.Fatalf("pending receipt emitted missing-report failure: %#v", transition)
		}
	}
}

func TestPullRequestReadyRejectsUnvalidatedOrUnauthorizedReceipt(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix it", "ready-authorization")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatal(err)
	}
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA}
	jobName := jobs.Name(record.ID, attempt.Number)
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, "missing-pod", event); err == nil {
		t.Fatal("receipt from an unauthorized Pod was accepted")
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, "worker-pod-a1", event); err == nil || !strings.Contains(strings.ToLower(err.Error()), "validation") {
		t.Fatalf("receipt before successful validation error = %v", err)
	}
	if fixture.pullRequests.getCalls != 0 {
		t.Fatalf("untrusted receipt reached provider inspection %d times", fixture.pullRequests.getCalls)
	}
}

func TestPullRequestPublishedPersistsNonterminalCandidateAndReplaysAfterRestart(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "publish first", "published-candidate")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatal(err)
	}
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID})
	event := protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA}
	handleEvent(t, fixture, jobName, podName, event)
	git, gitErr := fixture.store.GetGitResult(fixture.ctx, attempt.ID)
	pullRequest, prErr := fixture.store.GetPullRequest(fixture.ctx, attempt.ID)
	if gitErr != nil || prErr != nil || git.State != "candidate" || pullRequest.State != "reported" || pullRequest.Number != 42 || pullRequest.URL != "" || getTask(t, fixture, record.ID).State != task.AGENT_RUNNING || fixture.pullRequests.getCalls != 0 {
		t.Fatalf("published candidate = %#v/%#v state=%q calls=%d errors=%v/%v", git, pullRequest, getTask(t, fixture, record.ID).State, fixture.pullRequests.getCalls, gitErr, prErr)
	}
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.HandleWorkerEvent(fixture.ctx, jobName, podName, event); err != nil {
		t.Fatalf("replay published candidate: %v", err)
	}
	if err := restarted.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile after published candidate restart: %v", err)
	}
	if got := getTask(t, fixture, record.ID).State; got == task.PR_OPEN {
		t.Fatal("candidate without validated ready event entered PR_OPEN")
	}
}

func TestPullRequestReadyPermanentlyRejectsProviderMismatchWithoutAdoption(t *testing.T) {
	for name, mutate := range map[string]func(*forge.PullRequestState){
		"number":            func(pr *forge.PullRequestState) { pr.Number = 43 },
		"closed":            func(pr *forge.PullRequestState) { pr.State = "closed" },
		"source owner":      func(pr *forge.PullRequestState) { pr.SourceOwner = "other" },
		"source repository": func(pr *forge.PullRequestState) { pr.SourceRepository = "fork" },
		"source branch":     func(pr *forge.PullRequestState) { pr.SourceBranch = "other/branch" },
		"base branch":       func(pr *forge.PullRequestState) { pr.DestinationBranch = "release" },
		"head SHA":          func(pr *forge.PullRequestState) { pr.HeadSHA = strings.Repeat("f", 40) },
		"canonical URL":     func(pr *forge.PullRequestState) { pr.HTMLURL = "" },
		"actual title":      func(pr *forge.PullRequestState) { pr.Title = "" },
	} {
		t.Run(name, func(t *testing.T) {
			fixture := newFixture(t)
			record := createRunningTask(t, fixture, "fix it", "ready-mismatch-"+name)
			attempt, branch := prepareAttemptForPush(t, fixture, record)
			live := forge.PullRequestState{
				Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title",
				SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch,
				DestinationBranch: "main", HeadSHA: fullCommitSHA,
			}
			mutate(&live)
			fixture.pullRequests.getResult = &live
			event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
			err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", event)
			if err == nil || getTask(t, fixture, record.ID).State != task.FAILED {
				t.Fatalf("provider mismatch error/state = %v/%q, want permanent failure", err, getTask(t, fixture, record.ID).State)
			}
			if candidate, err := fixture.store.GetGitResult(fixture.ctx, attempt.ID); err != nil || candidate.State != "candidate" || candidate.CommitSHA != fullCommitSHA {
				t.Fatalf("mismatch changed candidate Git result: %#v, %v", candidate, err)
			}
			if candidate, err := fixture.store.GetPullRequest(fixture.ctx, attempt.ID); err != nil || candidate.State != "reported" || candidate.URL != "" {
				t.Fatalf("mismatch adopted provider pull request: %#v, %v", candidate, err)
			}
		})
	}
}

func TestPullRequestReadyRetriesUntilProviderHeadSettles(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix it", "ready-head-settling")
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	unsettled := forge.PullRequestState{
		Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title",
		SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch,
		DestinationBranch: "main", HeadSHA: strings.Repeat("f", 40),
	}
	settled := unsettled
	settled.HeadSHA = fullCommitSHA
	fixture.pullRequests.getResults = []forge.PullRequestState{unsettled, settled}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", event); err != nil {
		t.Fatalf("settled provider head: %v", err)
	}
	if got := getTask(t, fixture, record.ID).State; got != task.PR_OPEN {
		t.Fatalf("settled provider head state = %q, want %q", got, task.PR_OPEN)
	}
	if fixture.pullRequests.getCalls != 2 {
		t.Fatalf("provider inspections = %d, want 2", fixture.pullRequests.getCalls)
	}
}

func TestPullRequestReadyTransientInspectionReplaysAtomically(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix it", "ready-transient")
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	fixture.pullRequests.getErr = errors.New("provider temporarily unavailable")
	err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", event)
	if err == nil || getTask(t, fixture, record.ID).State != task.VALIDATING {
		t.Fatalf("transient inspection error/state = %v/%q", err, getTask(t, fixture, record.ID).State)
	}
	if candidate, err := fixture.store.GetGitResult(fixture.ctx, attempt.ID); err != nil || candidate.State != "candidate" {
		t.Fatalf("transient inspection candidate Git result = %#v, %v", candidate, err)
	}

	fixture.pullRequests.getErr = nil
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", event); err != nil {
		t.Fatalf("replay transient receipt: %v", err)
	}
	if getTask(t, fixture, record.ID).State != task.PR_OPEN || fixture.pullRequests.getCalls != 2 {
		t.Fatalf("replayed receipt state/get calls = %q/%d", getTask(t, fixture, record.ID).State, fixture.pullRequests.getCalls)
	}
}

func TestPullRequestReadyRestartResumesDurableIntermediateStatesWithoutProvider(t *testing.T) {
	for _, state := range []task.State{task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR} {
		for _, providerState := range []string{"unavailable", "advanced"} {
			t.Run(string(state)+"_"+providerState, func(t *testing.T) { testReadyRestartState(t, state, providerState) })
		}
	}
}

func testReadyRestartState(t *testing.T, state task.State, providerState string) {
	t.Helper()
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix it", "durable-restart-"+string(state)+providerState)
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	git := store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: fullCommitSHA}
	pullRequest := store.PullRequest{AttemptID: attempt.ID, State: "open", Number: 42, URL: pullRequestURL, Title: "Provider title", HeadBranch: branch, BaseBranch: "main"}
	if err := fixture.store.RecordVerifiedPullRequest(fixture.ctx, git, pullRequest); err != nil {
		t.Fatal(err)
	}
	current := task.VALIDATING
	for _, next := range []task.State{task.COMMITTING, task.PUSHING, task.CREATING_PR} {
		if state == current {
			break
		}
		transition(t, fixture, record.ID, current, next, "persisted before restart", "controller")
		current = next
	}
	if providerState == "unavailable" {
		fixture.pullRequests.getErr = errors.New("provider unavailable")
	} else {
		fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "merged"}
	}
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	if err := restarted.HandleWorkerEvent(fixture.ctx, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", event); err != nil {
		t.Fatalf("resume durable receipt: %v", err)
	}
	if got := getTask(t, fixture, record.ID).State; got != task.PR_OPEN || fixture.pullRequests.getCalls != 0 {
		t.Fatalf("resumed state/provider calls = %q/%d", got, fixture.pullRequests.getCalls)
	}
}

func TestPullRequestReadyPoisonsConflictingDurableReceiptsForReplay(t *testing.T) {
	for _, kind := range []string{"different receipt", "partial Git", "partial pull request"} {
		t.Run(kind, func(t *testing.T) { testConflictingDurableReceipt(t, kind) })
	}
}

func testConflictingDurableReceipt(t *testing.T, kind string) {
	t.Helper()
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix it", "durable-conflict-"+kind)
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	git := store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: fullCommitSHA}
	pullRequest := store.PullRequest{AttemptID: attempt.ID, State: "open", Number: 42, URL: pullRequestURL, Title: "Provider title", HeadBranch: branch, BaseBranch: "main"}
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	switch kind {
	case "different receipt":
		if err := fixture.store.RecordVerifiedPullRequest(fixture.ctx, git, pullRequest); err != nil {
			t.Fatal(err)
		}
		event.CommitSHA = strings.Repeat("1", 40)
	case "partial Git":
		db, err := sql.Open("sqlite", fixture.databasePath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(fixture.ctx, `DELETE FROM pull_requests WHERE attempt_id = ?`, attempt.ID); err != nil {
			t.Fatal(err)
		}
	case "partial pull request":
		db, err := sql.Open("sqlite", fixture.databasePath)
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		if _, err := db.ExecContext(fixture.ctx, `DELETE FROM git_results WHERE attempt_id = ?`, attempt.ID); err != nil {
			t.Fatal(err)
		}
	}

	control := fixture.controller.(*Controller)
	jobName := jobs.Name(record.ID, attempt.Number)
	queueDurableWorkerEvent(t, fixture, "durable-event", "worker-pod-uid-a1", jobName, "worker-pod-a1", event)
	err := control.HandleWorkerEventOnce(fixture.ctx, "durable-event", jobName, "worker-pod-a1", event)
	if err == nil || !forge.IsPermanent(err) || getTask(t, fixture, record.ID).State != task.FAILED {
		t.Fatalf("first conflicting receipt error/state = %v/%q, want permanent FAILED", err, getTask(t, fixture, record.ID).State)
	}
	if err := control.HandleWorkerEventOnce(fixture.ctx, "durable-event", jobName, "worker-pod-a1", event); err != nil {
		t.Fatalf("terminal receipt replay was not consumable: %v", err)
	}

}

func TestProviderPullRequestURLMatchesConfiguredCoordinates(t *testing.T) {
	for _, test := range []struct {
		name   string
		target forge.Target
		url    string
		ok     bool
	}{
		{name: "GitHub public", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.github.com", Owner: "Acme", Repository: "Widget"}, url: "https://github.com/acme/widget/pull/42", ok: true},
		{name: "GitHub public API host", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.github.com", Owner: "Acme", Repository: "Widget"}, url: "https://api.github.com/acme/widget/pull/42", ok: true},
		{name: "Bitbucket public", target: forge.Target{Provider: forge.ProviderBitbucket, BaseURL: "https://api.bitbucket.org", Owner: "Acme", Repository: "Widget"}, url: "https://bitbucket.org/acme/widget/pull-requests/42", ok: true},
		{name: "same-host custom", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://github.enterprise.example/api/v3", Owner: "acme", Repository: "widget"}, url: "https://github.enterprise.example/acme/widget/pull/42", ok: true},
		{name: "split-host enterprise web", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.tenant.ghe.com", Owner: "acme", Repository: "widget"}, url: "https://tenant.ghe.com/acme/widget/pull/42", ok: true},
		{name: "split-host enterprise API", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.tenant.ghe.com", Owner: "acme", Repository: "widget"}, url: "https://api.tenant.ghe.com/acme/widget/pull/42", ok: true},
		{name: "split-host enterprise wrong host", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.tenant.ghe.com", Owner: "acme", Repository: "widget"}, url: "https://other.tenant.ghe.com/acme/widget/pull/42"},
		{name: "foreign host", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.github.com", Owner: "acme", Repository: "widget"}, url: "https://evil.example/acme/widget/pull/42"},
		{name: "wrong coordinates", target: forge.Target{Provider: forge.ProviderBitbucket, BaseURL: "https://api.bitbucket.org", Owner: "acme", Repository: "widget"}, url: "https://bitbucket.org/other/widget/pull-requests/42"},
		{name: "wrong number", target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.github.com", Owner: "acme", Repository: "widget"}, url: "https://github.com/acme/widget/pull/43"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateProviderPullRequestURL(test.url, test.target, 42)
			if (err == nil) != test.ok {
				t.Fatalf("validateProviderPullRequestURL() error = %v, want ok %t", err, test.ok)
			}
		})
	}
}

func TestLegacyBranchPushedFailsActiveAttemptWithRetryReason(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "fix it", "legacy-branch-pushed")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	currentManifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatal(err)
	}
	branch := currentManifest.TaskBranch
	legacyManifest, err := json.Marshal(map[string]any{
		"task_id":            record.ID,
		"repository":         repositoryURL,
		"base_branch":        "main",
		"task_branch":        branch,
		"prompt":             record.Prompt,
		"opencode_command":   []string{"opencode"},
		"validation_command": []string{"go", "test", "./..."},
		"max_fix_attempts":   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	execFixtureSQL(t, fixture, `UPDATE task_attempts SET manifest_json = ? WHERE id = ?`, legacyManifest, attempt.ID)
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	event := protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA}
	queueDurableWorkerEvent(t, fixture, "legacy-branch-pushed", "legacy-pod-uid", jobName, podName, event)
	control := fixture.controller.(*Controller)
	if err := control.HandleWorkerEventOnce(fixture.ctx, "legacy-branch-pushed", jobName, podName, event); err != nil {
		t.Fatalf("legacy event: %v", err)
	}
	if got := getTask(t, fixture, record.ID); got.State != task.FAILED {
		t.Fatalf("legacy event state = %q", got.State)
	}
	events := listEvents(t, fixture, record.ID)
	if reason := events[len(events)-1].Reason; !strings.Contains(reason, "branch_pushed") || !strings.Contains(reason, "retry") {
		t.Fatalf("legacy failure reason = %q", reason)
	}
	if _, err := control.Retry(fixture.ctx, record.ID); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("retry before terminal replay consumed event = %v, want conflict", err)
	}
	if err := control.HandleWorkerEventOnce(fixture.ctx, "legacy-branch-pushed", jobName, podName, event); err != nil {
		t.Fatalf("terminal legacy event replay: %v", err)
	}
	if err := fixture.store.MarkWorkerEventProcessed(fixture.ctx, "legacy-branch-pushed"); err != nil {
		t.Fatal(err)
	}
	exhaustAttemptLogs(t, fixture, record.ID, attempt.ID)
	retry, err := control.Retry(fixture.ctx, record.ID)
	if err != nil || retry.Number != 2 {
		t.Fatalf("retry after terminal replay = %#v, %v; want attempt 2", retry, err)
	}
}

func TestLegacyBranchPushedReplayKeepsDurableOpenPullRequest(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "replay completed legacy event", "legacy-branch-pushed-replay")
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	if err := fixture.store.RecordVerifiedPullRequest(fixture.ctx,
		store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: fullCommitSHA},
		store.PullRequest{AttemptID: attempt.ID, State: "open", Number: 42, URL: pullRequestURL, Title: "Provider title", HeadBranch: branch, BaseBranch: "main"},
	); err != nil {
		t.Fatal(err)
	}
	current := task.VALIDATING
	for _, next := range []task.State{task.COMMITTING, task.PUSHING, task.CREATING_PR, task.PR_OPEN} {
		transition(t, fixture, record.ID, current, next, "recovered durable state", "controller")
		current = next
	}
	event := protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA}
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	queueDurableWorkerEvent(t, fixture, "legacy-replay", "legacy-replay-pod", jobName, podName, event)
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.HandleWorkerEventOnce(fixture.ctx, "legacy-replay", jobName, podName, event); err != nil {
		t.Fatalf("legacy replay: %v", err)
	}
	if got := getTask(t, fixture, record.ID).State; got != task.PR_OPEN {
		t.Fatalf("legacy replay state = %q; want PR_OPEN", got)
	}
	if fixture.pullRequests.getCalls != 0 {
		t.Fatalf("legacy replay called provider %d times", fixture.pullRequests.getCalls)
	}
	if err := fixture.store.MarkWorkerEventProcessed(fixture.ctx, "legacy-replay"); err != nil {
		t.Fatal(err)
	}
	pending, err := fixture.store.ListPendingWorkerEvents(fixture.ctx, "legacy-replay-pod")
	if err != nil || len(pending) != 0 {
		t.Fatalf("legacy replay pending events = %d, %v; want processed", len(pending), err)
	}
}

func TestLegacyBranchPushedReplayRequiresMatchingDurableState(t *testing.T) {
	for _, name := range []string{"SHA mismatch", "branch mismatch", "missing PR", "non-open PR", "wrong attempt", "earlier task state"} {
		t.Run(name, func(t *testing.T) { testLegacyBranchPushedReplayCase(t, name) })
	}
}

func testLegacyBranchPushedReplayCase(t *testing.T, name string) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "reject incomplete legacy replay", "legacy-replay-"+name)
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	if err := fixture.store.RecordVerifiedPullRequest(fixture.ctx,
		store.GitResult{AttemptID: attempt.ID, State: "pushed", Branch: branch, CommitSHA: fullCommitSHA},
		store.PullRequest{AttemptID: attempt.ID, State: "open", Number: 42, URL: pullRequestURL, Title: "Provider title", HeadBranch: branch, BaseBranch: "main"},
	); err != nil {
		t.Fatal(err)
	}
	if name != "earlier task state" {
		current := task.VALIDATING
		for _, next := range []task.State{task.COMMITTING, task.PUSHING, task.CREATING_PR, task.PR_OPEN} {
			transition(t, fixture, record.ID, current, next, "recovered durable state", "controller")
			current = next
		}
	}
	event := protocol.Event{Type: protocol.EventBranchPushed, TaskID: record.ID, Branch: branch, CommitSHA: fullCommitSHA}
	switch name {
	case "SHA mismatch":
		event.CommitSHA = strings.Repeat("1", 40)
	case "branch mismatch":
		execFixtureSQL(t, fixture, `UPDATE git_results SET branch = ? WHERE attempt_id = ?`, "other/branch", attempt.ID)
	case "missing PR":
		execFixtureSQL(t, fixture, `DELETE FROM pull_requests WHERE attempt_id = ?`, attempt.ID)
	case "non-open PR":
		execFixtureSQL(t, fixture, `UPDATE pull_requests SET state = 'reported' WHERE attempt_id = ?`, attempt.ID)
	case "wrong attempt":
		wrongAttemptID := attempt.ID + "-wrong"
		execFixtureSQL(t, fixture, `DELETE FROM git_results WHERE attempt_id = ?`, attempt.ID)
		execFixtureSQL(t, fixture, `DELETE FROM pull_requests WHERE attempt_id = ?`, attempt.ID)
		execFixtureSQL(t, fixture, `INSERT INTO task_attempts (id, task_id, number, state, base_branch, task_branch, created_at) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`, wrongAttemptID, record.ID, attempt.Number+1, task.PR_OPEN, "main", branch)
		execFixtureSQL(t, fixture, `INSERT INTO git_results (attempt_id, state, branch, commit_sha) VALUES (?, 'pushed', ?, ?)`, wrongAttemptID, branch, fullCommitSHA)
		execFixtureSQL(t, fixture, `INSERT INTO pull_requests (attempt_id, state, number, url, title, head_branch, base_branch) VALUES (?, 'open', ?, ?, ?, ?, ?)`, wrongAttemptID, 42, pullRequestURL, "Provider title", branch, "main")
	}
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, event); err != nil {
		t.Fatalf("legacy replay: %v", err)
	}
	if got := getTask(t, fixture, record.ID).State; got != task.FAILED {
		t.Fatalf("legacy replay state = %q; want FAILED", got)
	}
	if fixture.pullRequests.getCalls != 0 {
		t.Fatalf("legacy replay called provider %d times", fixture.pullRequests.getCalls)
	}
}

func TestFollowUpReceiptMustReuseCopiedDurablePullRequestNumber(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	event := controllerForgeEvent("same-pr-follow-up", "review_comment", 42)
	event.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp := startCurrentAttemptWorker(t, fixture, record.ID)
	manifest := followUpManifest(t, fixture, record.ID, followUp)
	if manifest.ExistingPullRequestNumber != 42 || manifest.ExistingPullRequestHeadSHA != fullCommitSHA || manifest.TaskBranch != branch || manifest.BaseBranch != "main" {
		t.Fatalf("follow-up manifest did not copy durable PR identity: %#v", manifest)
	}
	jobName := jobs.Name(record.ID, followUp.Number)
	for _, workerEvent := range []protocol.Event{
		{Type: protocol.EventAgentStarted, TaskID: record.ID},
		{Type: protocol.EventValidationStarted, TaskID: record.ID},
		{Type: protocol.EventValidationResult, TaskID: record.ID},
		{Type: protocol.EventValidationSucceeded, TaskID: record.ID},
	} {
		handleEvent(t, fixture, jobName, "worker-pod-a2", workerEvent)
	}
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 43, State: "open", HTMLURL: "https://bitbucket.example/pull-requests/43", Title: "Wrong PR", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
	err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, "worker-pod-a2", protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 43, Branch: branch, CommitSHA: fullCommitSHA})
	if err == nil || getTask(t, fixture, record.ID).State != task.FAILED {
		t.Fatalf("changed follow-up PR error/state = %v/%q", err, getTask(t, fixture, record.ID).State)
	}
	durable, getErr := fixture.store.GetPullRequest(fixture.ctx, followUp.ID)
	if getErr != nil || durable.Number != 42 {
		t.Fatalf("changed follow-up adopted mismatched PR: %#v, %v", durable, getErr)
	}
}

func TestPullRequestReadyUsesConfiguredGitHubAndBitbucketTargets(t *testing.T) {
	for _, provider := range []forge.Provider{forge.ProviderBitbucket, forge.ProviderGitHub} {
		t.Run(string(provider), func(t *testing.T) {
			fixture := newFixture(t)
			owner, repository := "acme", "widget"
			if provider == forge.ProviderGitHub {
				fixture.config.Repositories[0].Bitbucket = config.RepositoryBitbucketConfig{}
				fixture.config.Repositories[0].GitHub = config.RepositoryGitHubConfig{Owner: owner, Repository: repository, CredentialsSecret: "github-widget"}
				control, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
				if err != nil {
					t.Fatal(err)
				}
				fixture.controller = control
			}
			record := createRunningTask(t, fixture, "fix it", "provider-"+string(provider))
			attempt, branch := prepareAttemptForPush(t, fixture, record)
			url := "https://bitbucket.org/acme/widget/pull-requests/42"
			if provider == forge.ProviderGitHub {
				url = "https://github.com/acme/widget/pull/42"
			}
			fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: url, Title: "Provider title", SourceOwner: owner, SourceRepository: repository, SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
			handleEvent(t, fixture, jobs.Name(record.ID, attempt.Number), "worker-pod-a1", protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA})
			if len(fixture.pullRequests.getTargets) != 1 || fixture.pullRequests.getTargets[0].Provider != provider || fixture.pullRequests.getTargets[0].Owner != owner || fixture.pullRequests.getTargets[0].Repository != repository {
				t.Fatalf("provider target = %#v", fixture.pullRequests.getTargets)
			}
		})
	}
}
