package controller

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stesting "k8s.io/client-go/testing"

	controllerruntime "github.com/simpleswe/simpleswe/internal/controller/runtime"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

const fullCommitSHA = "0123456789abcdef0123456789abcdef01234567"

func TestDurablePullRequestReceiptRecoversAfterPodJobAndRuntimeDeletion(t *testing.T) {
	for _, poison := range []bool{false, true} {
		name := "verified"
		if poison {
			name = "poison"
		}
		t.Run(name, func(t *testing.T) { testDurableReceiptRecovery(t, poison, name) })
	}
}

func testDurableReceiptRecovery(t *testing.T, poison bool, name string) {
	t.Helper()
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "durable replay", "deleted-runtime-replay-"+name)
	attempt, branch := prepareAttemptForPush(t, fixture, record)
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	encoded, err := protocol.EncodeEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	pod, err := fixture.kube.CoreV1().Pods(workerNamespace).Get(fixture.ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pod.UID = "captured-pod-uid"
	fixture.pullRequests.getErr = errors.New("provider temporarily unavailable")
	logs := &staticWorkerLogs{content: time.Now().UTC().Format(time.RFC3339Nano) + " " + encoded + "\n"}
	initialRuntime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, fixture.controller, controllerruntime.NewBackend(fixture.store, fixture.controller), controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: logs,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := initialRuntime.CollectPodLogs(fixture.ctx, pod); err == nil || !strings.Contains(err.Error(), "provider temporarily unavailable") {
		t.Fatalf("capture transient receipt error = %v", err)
	}
	pending, err := fixture.store.ListPendingWorkerEvents(fixture.ctx, string(pod.UID))
	if err != nil || len(pending) != 1 {
		t.Fatalf("captured pending receipt = %#v, %v", pending, err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.BatchV1().Jobs(workerNamespace).Delete(fixture.ctx, jobName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	fixture.pullRequests.getErr = nil
	headSHA := fullCommitSHA
	if poison {
		headSHA = strings.Repeat("f", 40)
	}
	fixture.pullRequests.getResult = &forge.PullRequestState{
		Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget",
		SourceBranch: branch, DestinationBranch: "main", HeadSHA: headSHA,
	}
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	backend := controllerruntime.NewBackend(fixture.store, restarted)
	_, updates, err := backend.GetLogs(fixture.ctx, record.ID, true, attempt.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	recoveryInterval := time.Millisecond
	if poison {
		recoveryInterval = 25 * time.Millisecond
	}
	restartedRuntime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, restarted, backend, controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: recoveryInterval,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- restartedRuntime.Run(ctx) }()
	if poison {
		waitForPoisonReceipt(t, fixture, restarted, record, attempt, cancel, done)
	}
	want := task.PR_OPEN
	if poison {
		want = task.FAILED
	}
	waitForReceiptRecovery(t, fixture, record, attempt, want, cancel, done)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("restarted runtime: %v", err)
	}
	assertLogFollowerClosed(t, updates)
	assertRecoveredReceipt(t, fixture, restarted, record, attempt, branch, poison)
}

func waitForPoisonReceipt(t *testing.T, fixture *fixture, restarted *Controller, record store.Task, attempt store.Attempt, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		current := getTask(t, fixture, record.ID)
		pending, err := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
		if err == nil && current.State == task.FAILED && pending {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("poison receipt did not remain pending after failing task: state=%q pending=%t error=%v", current.State, pending, err)
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := restarted.Retry(fixture.ctx, record.ID); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("retry while poison receipt pending = %v, want ErrConflict", err)
	}
}

func waitForReceiptRecovery(t *testing.T, fixture *fixture, record store.Task, attempt store.Attempt, want task.State, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		current, getErr := fixture.store.GetTask(fixture.ctx, record.ID)
		pending, pendingErr := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
		currentAttempt, attemptErr := fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
		if getErr == nil && pendingErr == nil && attemptErr == nil && current.State == want && !pending && currentAttempt.LogsExhausted {
			return
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("restarted replay state/pending/attempt/errors = %q/%t/%#v/%v/%v/%v, want %q/false/exhausted", current.State, pending, currentAttempt, getErr, pendingErr, attemptErr, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertLogFollowerClosed(t *testing.T, updates <-chan string) {
	t.Helper()
	select {
	case _, open := <-updates:
		if open {
			t.Fatal("deleted attempt log follower remained open after log barrier")
		}
	case <-time.After(time.Second):
		t.Fatal("deleted attempt log follower did not close")
	}
}

func assertRecoveredReceipt(t *testing.T, fixture *fixture, restarted *Controller, record store.Task, attempt store.Attempt, branch string, poison bool) {
	t.Helper()
	if poison {
		if candidate, err := fixture.store.GetGitResult(fixture.ctx, attempt.ID); err != nil || candidate.State != "candidate" || candidate.CommitSHA != fullCommitSHA {
			t.Fatalf("poison receipt changed candidate Git result: %#v, %v", candidate, err)
		}
		if retried, err := restarted.Retry(fixture.ctx, record.ID); err != nil || retried.Number != 2 {
			t.Fatalf("retry after poison receipt recovery = %#v, %v", retried, err)
		}
	} else {
		git, err := fixture.store.GetGitResult(fixture.ctx, attempt.ID)
		if err != nil || git.Branch != branch || git.CommitSHA != fullCommitSHA {
			t.Fatalf("verified exact Git result = %#v, %v", git, err)
		}
	}
}

func TestRuntimeConsumesPermanentPullRequestReadyPoisonAfterFailingTask(t *testing.T) {
	for _, kind := range []string{"wrong branch", "wrong pull request", "out of order", "malformed manifest"} {
		t.Run(kind, func(t *testing.T) { testRuntimeConsumesPullRequestReadyPoison(t, kind) })
	}
}

func testRuntimeConsumesPullRequestReadyPoison(t *testing.T, kind string) {
	t.Helper()
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "durable ready poison", "ready-poison-"+kind)
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
	handleEvent(t, fixture, jobName, podName, protocol.Event{
		Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA,
	})
	if kind != "out of order" {
		for _, event := range []protocol.Event{
			{Type: protocol.EventValidationStarted, TaskID: record.ID, Command: []string{"go", "test"}},
			{Type: protocol.EventValidationResult, TaskID: record.ID, Command: []string{"go", "test"}},
			{Type: protocol.EventValidationSucceeded, TaskID: record.ID},
		} {
			handleEvent(t, fixture, jobName, podName, event)
		}
	}
	ready := protocol.Event{
		Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA,
	}
	switch kind {
	case "wrong branch":
		ready.Branch = "other/branch"
	case "wrong pull request":
		ready.PullRequestNumber++
	case "malformed manifest":
		execFixtureSQL(t, fixture, `UPDATE task_attempts SET manifest_json = '{' WHERE id = ?`, attempt.ID)
	}
	queueDurableWorkerEvent(t, fixture, "ready-poison-"+kind, "ready-poison-pod-uid", jobName, podName, ready)
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, restarted, controllerruntime.NewBackend(fixture.store, restarted), controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, pendingErr := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
		if pendingErr == nil && !pending && getTask(t, fixture, record.ID).State == task.FAILED {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("poison was not failed then consumed: pending=%t error=%v state=%q", pending, pendingErr, getTask(t, fixture, record.ID).State)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	git, gitErr := fixture.store.GetGitResult(fixture.ctx, attempt.ID)
	pullRequest, pullRequestErr := fixture.store.GetPullRequest(fixture.ctx, attempt.ID)
	if gitErr != nil || pullRequestErr != nil || git.State != "candidate" || git.CommitSHA != fullCommitSHA || pullRequest.State != "reported" || pullRequest.Number != 42 {
		t.Fatalf("poison adopted data: git=%#v pull_request=%#v errors=%v/%v", git, pullRequest, gitErr, pullRequestErr)
	}
	if fixture.pullRequests.getCalls != 0 {
		t.Fatalf("poison called provider %d times", fixture.pullRequests.getCalls)
	}
}

func TestRuntimeRecoveryPersistsReplacementCandidateBeforeForgeRecovery(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	running := controllerForgeEvent("replacement-running", "review_comment", 42)
	running.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, running); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.(*Controller).ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp := startCurrentAttemptWorker(t, fixture, record.ID)
	jobName, podName := jobs.Name(record.ID, followUp.Number), "worker-pod-a2"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID})
	candidateA, candidateB := strings.Repeat("a", 40), strings.Repeat("b", 40)
	handleEvent(t, fixture, jobName, podName, protocol.Event{
		Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: candidateA,
	})
	pendingWebhook := controllerForgeEvent("replacement-pending", "review_comment", 42)
	pendingWebhook.Branch, pendingWebhook.CommitSHA = branch, candidateB
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, pendingWebhook); err != nil {
		t.Fatal(err)
	}
	queueDurableWorkerEvent(t, fixture, "replacement-candidate-b", "replacement-pod-uid", jobName, podName, protocol.Event{
		Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: candidateB,
	})
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	inspector := &candidateObservingInspector{
		store: fixture.store, attemptID: followUp.ID,
		result: forge.PullRequestState{
			Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget",
			SourceBranch: branch, DestinationBranch: "main", HeadSHA: candidateB,
		},
	}
	restarted, err := New(fixture.store, fixture.kube, fixture.config, inspector)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, restarted, controllerruntime.NewBackend(fixture.store, restarted), controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: time.Millisecond, ProcessForgeEvents: restarted.ProcessForgeEvents,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, pendingErr := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, followUp.ID)
		if pendingErr == nil && !pending && inspector.callCount() > 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("replacement recovery did not complete: pending=%t error=%v provider_calls=%d", pending, pendingErr, inspector.callCount())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	git, err := fixture.store.GetGitResult(fixture.ctx, followUp.ID)
	if err != nil || git.State != "candidate" || git.CommitSHA != candidateB {
		t.Fatalf("recovered candidate = %#v, %v; want B", git, err)
	}
	for _, observed := range inspector.observedSHAs() {
		if observed != candidateB {
			t.Fatalf("forge recovery observed stale candidate %q before B", observed)
		}
	}
	running, err = fixture.store.GetForgeEvent(fixture.ctx, running.ID)
	pendingWebhook, pendingErr := fixture.store.GetForgeEvent(fixture.ctx, pendingWebhook.ID)
	if err != nil || pendingErr != nil || running.Status != store.ForgeEventRunning || pendingWebhook.Status != store.ForgeEventPending || getTask(t, fixture, record.ID).CancellationRequested {
		t.Fatalf("forge recovery outcomes: running=%#v pending=%#v task=%#v errors=%v/%v", running, pendingWebhook, getTask(t, fixture, record.ID), err, pendingErr)
	}
}

func TestRuntimeRecoveryIsolatesTransientWorkerReceiptFromForgeWork(t *testing.T) {
	fixture := newFixture(t)
	record, _, branch := createOwnedOpenPullRequest(t, fixture)
	affected := controllerForgeEvent("transient-receipt-running", "review_comment", 42)
	affected.Branch = branch
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, affected); err != nil {
		t.Fatal(err)
	}
	control := fixture.controller.(*Controller)
	if err := control.ProcessForgeEvents(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	followUp := startCurrentAttemptWorker(t, fixture, record.ID)
	jobName, podName := jobs.Name(record.ID, followUp.Number), "worker-pod-a2"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID})
	candidateSHA := strings.Repeat("c", 40)
	handleEvent(t, fixture, jobName, podName, protocol.Event{
		Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: candidateSHA,
	})
	command := []string{"go", "test", "./..."}
	for _, event := range []protocol.Event{
		{Type: protocol.EventValidationStarted, TaskID: record.ID, Command: command},
		{Type: protocol.EventValidationResult, TaskID: record.ID, Command: command},
		{Type: protocol.EventValidationSucceeded, TaskID: record.ID},
	} {
		handleEvent(t, fixture, jobName, podName, event)
	}
	queueDurableWorkerEvent(t, fixture, "transient-ready-receipt", "transient-ready-pod-uid", jobName, podName, protocol.Event{
		Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: candidateSHA,
	})
	unrelated := controllerForgeEvent("transient-receipt-unrelated", "review_comment", 777)
	if _, err := fixture.store.PutForgeEvent(fixture.ctx, unrelated); err != nil {
		t.Fatal(err)
	}
	fixture.pullRequests.mu.Lock()
	fixture.pullRequests.getErr = errors.New("provider temporarily unavailable")
	fixture.pullRequests.mu.Unlock()
	var logs bytes.Buffer
	runtime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, control, controllerruntime.NewBackend(fixture.store, control), controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: time.Millisecond,
		ProcessForgeEvents: control.ProcessForgeEvents, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	providerCalls := func() int {
		fixture.pullRequests.mu.Lock()
		defer fixture.pullRequests.mu.Unlock()
		return fixture.pullRequests.getCalls
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, pendingErr := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, followUp.ID)
		stored, eventErr := fixture.store.GetForgeEvent(fixture.ctx, unrelated.ID)
		if pendingErr == nil && eventErr == nil && pending && stored.Status == store.ForgeEventHandled && providerCalls() >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("isolated recovery did not progress: pending=%t unrelated=%#v errors=%v/%v provider_calls=%d", pending, stored, pendingErr, eventErr, providerCalls())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	running, eventErr := fixture.store.GetForgeEvent(fixture.ctx, affected.ID)
	current := getTask(t, fixture, record.ID)
	if eventErr != nil || running.Status != store.ForgeEventRunning || running.AttemptID != followUp.ID || running.FailedAt != nil || running.NextAttemptAt == nil || current.CancellationRequested {
		t.Fatalf("same-attempt forge outcome: event=%#v task=%#v error=%v", running, current, eventErr)
	}
	logText := logs.String()
	if !strings.Contains(logText, "recover pending worker events") || !strings.Contains(logText, "provider temporarily unavailable") {
		t.Fatalf("recovery logs = %q; want contextual worker error", logText)
	}
	for line := range strings.SplitSeq(logText, "\n") {
		if strings.Contains(line, "recover durable controller work") && strings.Contains(line, "provider temporarily unavailable") {
			t.Fatalf("worker transient entered global recovery backoff log: %q", line)
		}
	}
}

type candidateObservingInspector struct {
	mu        sync.Mutex
	store     *store.Store
	attemptID string
	result    forge.PullRequestState
	observed  []string
}

func (i *candidateObservingInspector) GetPullRequest(ctx context.Context, _ forge.Target, _ int) (forge.PullRequestState, error) {
	git, err := i.store.GetGitResult(ctx, i.attemptID)
	if err != nil {
		return forge.PullRequestState{}, fmt.Errorf("observe candidate before provider inspection: %w", err)
	}
	i.mu.Lock()
	i.observed = append(i.observed, git.CommitSHA)
	i.mu.Unlock()
	return i.result, nil
}

func (i *candidateObservingInspector) callCount() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.observed)
}

func (i *candidateObservingInspector) observedSHAs() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]string(nil), i.observed...)
}

type staticWorkerLogs struct{ content string }

func (s *staticWorkerLogs) Open(context.Context, string, string, string, *time.Time) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(s.content)), nil
}

func TestCompletedJobRestartDrainsDurableAgentStartFromPreRunningStates(t *testing.T) {
	for _, start := range []task.State{task.CREATING_JOB, task.JOB_PENDING} {
		t.Run(string(start), func(t *testing.T) { testCompletedJobRestart(t, start) })
	}
}

func testCompletedJobRestart(t *testing.T, start task.State) {
	t.Helper()
	fixture := newFixture(t)
	record := createTask(t, fixture, "recover ordered durable worker events", "durable-agent-start-"+string(start))
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := attemptManifest(attempt)
	if err != nil {
		t.Fatal(err)
	}
	if start == task.CREATING_JOB {
		execFixtureSQL(t, fixture, `UPDATE tasks SET state = ? WHERE id = ?`, start, record.ID)
		execFixtureSQL(t, fixture, `UPDATE task_attempts SET state = ? WHERE id = ?`, start, attempt.ID)
	}

	jobName, podName := jobs.Name(record.ID, attempt.Number), "completed-recovery-pod"
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.UID = "completed-recovery-job-uid"
	job.Status.Active = 0
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: podName, Namespace: workerNamespace, UID: "completed-recovery-pod-uid", Labels: copyLabels(job.Labels),
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: jobName, UID: job.UID, Controller: &controller}},
	}}
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Create(fixture.ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: record.ID}); err == nil {
		t.Fatal("direct agent_started bypassed its RUNNING expected-state check")
	}
	if got := getTask(t, fixture, record.ID).State; got != start {
		t.Fatalf("direct agent_started changed state to %q, want %q", got, start)
	}

	command := []string{"go", "test", "./..."}
	workerEvents := []protocol.Event{
		{Type: protocol.EventAgentStarted, TaskID: record.ID},
		{Type: protocol.EventAgentStarted, TaskID: record.ID},
		{Type: protocol.EventPullRequestPublished, TaskID: record.ID, PullRequestNumber: 42, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA},
		{Type: protocol.EventValidationStarted, TaskID: record.ID, Command: command},
		{Type: protocol.EventValidationResult, TaskID: record.ID, Command: command, Message: "passed", ExitCode: 0},
		{Type: protocol.EventValidationSucceeded, TaskID: record.ID},
		{Type: protocol.EventPullRequestReady, TaskID: record.ID, PullRequestNumber: 42, Branch: manifest.TaskBranch, CommitSHA: fullCommitSHA},
	}
	for i, event := range workerEvents {
		queueDurableWorkerEvent(t, fixture, fmt.Sprintf("ordered-recovery-%s-%d", start, i), string(pod.UID), jobName, podName, event)
	}
	fixture.pullRequests.getResult = &forge.PullRequestState{
		Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget",
		SourceBranch: manifest.TaskBranch, DestinationBranch: manifest.BaseBranch, HeadSHA: fullCommitSHA,
	}
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, restarted, controllerruntime.NewBackend(fixture.store, restarted), controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	waitForOrderedRecovery(t, fixture, record, attempt, cancel, done)
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("restarted runtime: %v", err)
	}
	runs, err := fixture.store.ListValidationRuns(fixture.ctx, attempt.ID)
	if err != nil || len(runs) != 1 || runs[0].State != "succeeded" || fixture.pullRequests.getCalls != 1 {
		t.Fatalf("later validation/provider progress = %#v, %v, provider calls=%d", runs, err, fixture.pullRequests.getCalls)
	}

}

func waitForOrderedRecovery(t *testing.T, fixture *fixture, record store.Task, attempt store.Attempt, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, pendingErr := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
		currentAttempt, attemptErr := fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
		if pendingErr == nil && attemptErr == nil && !pending && currentAttempt.LogsExhausted && getTask(t, fixture, record.ID).State == task.PR_OPEN {
			return
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("ordered recovery did not finish: state=%q pending=%t attempt=%#v errors=%v/%v", getTask(t, fixture, record.ID).State, pending, currentAttempt, pendingErr, attemptErr)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestValidationResultsCompleteExactRunsAcrossCommandsAndFixRounds(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "validate exactly", "validation-sequence")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})

	events := []protocol.Event{
		{Type: protocol.EventValidationStarted, TaskID: created.ID, Message: "attempt 1", Command: []string{"go", "test", "./..."}},
		{Type: protocol.EventValidationResult, TaskID: created.ID, Message: "tests passed", Command: []string{"go", "test", "./..."}, ExitCode: 0},
		{Type: protocol.EventValidationStarted, TaskID: created.ID, Message: "attempt 1", Command: []string{"go", "vet", "./..."}},
		{Type: protocol.EventValidationResult, TaskID: created.ID, Message: "vet failed", Command: []string{"go", "vet", "./..."}, ExitCode: 2},
		{Type: protocol.EventAgentStarted, TaskID: created.ID, Message: "validation_fix"},
		{Type: protocol.EventValidationStarted, TaskID: created.ID, Message: "attempt 2", Command: []string{"go", "vet", "./..."}},
		{Type: protocol.EventValidationResult, TaskID: created.ID, Message: "vet passed", Command: []string{"go", "vet", "./..."}, ExitCode: 0},
		{Type: protocol.EventValidationSucceeded, TaskID: created.ID, Message: "attempt 2"},
	}
	for _, event := range events {
		handleEvent(t, fixture, jobName, podName, event)
	}
	runs, err := fixture.store.ListValidationRuns(fixture.ctx, created.CurrentAttemptID)
	if err != nil {
		t.Fatalf("ListValidationRuns(): %v", err)
	}
	if len(runs) != 3 {
		t.Fatalf("validation runs = %#v; want three exact command runs", runs)
	}
	wantStates := []string{"succeeded", "failed", "succeeded"}
	wantExits := []int{0, 2, 0}
	for i := range runs {
		if runs[i].Sequence != i+1 || runs[i].State != wantStates[i] || runs[i].ExitCode != wantExits[i] {
			t.Errorf("run %d = %#v; want sequence/state/exit %d/%s/%d", i, runs[i], i+1, wantStates[i], wantExits[i])
		}
	}
}

func TestTransientJobCreateKeepsCreatingIntentAndAdoptsPartialSecret(t *testing.T) {
	fixture := newFixture(t)
	fail := true
	fixture.kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		if fail {
			fail = false
			return true, nil, apierrors.NewTimeoutError("temporary Job create", 1)
		}
		return false, nil, nil
	})

	created, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "retry Kubernetes"})
	if err != nil {
		t.Fatalf("CreateTask() transient error = %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.CREATING_JOB {
		t.Fatalf("state after transient create = %q; want CREATING_JOB", got.State)
	}
	secrets, _ := fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if len(secrets.Items) != 1 {
		t.Fatalf("partial Secrets = %d; want one durable deterministic Secret", len(secrets.Items))
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() after transient create: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.JOB_PENDING {
		t.Fatalf("reconciled state = %q; want JOB_PENDING", got.State)
	}
	secrets, _ = fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if len(secrets.Items) != 1 {
		t.Fatalf("adopted Secrets = %d; want one", len(secrets.Items))
	}
}

func TestTransientPullRequestFailureResumesAfterCompletedJob(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "resume provider", "provider-resume")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}, Message: "passed"})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	fixture.pullRequests.getErr = errors.New("temporary Bitbucket outage")
	branch := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, branch); err == nil {
		t.Fatal("transient provider failure was not reported")
	}
	if got := getTask(t, fixture, created.ID).State; got != task.VALIDATING {
		t.Fatalf("provider failure state = %q; want receipt pending in VALIDATING", got)
	}
	if candidate, err := fixture.store.GetPullRequest(fixture.ctx, created.CurrentAttemptID); err != nil || candidate.State != "reported" || candidate.URL != "" {
		t.Fatalf("transient inspection candidate = %#v, %v", candidate, err)
	}
	fixture.pullRequests.getErr = nil
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch.Branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, branch); err != nil {
		t.Fatalf("replay transient receipt: %v", err)
	}

	job, _ := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	job.Status.Active = 0
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("complete Job: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); err != nil {
		t.Fatalf("WorkerLogsExhausted() after durable push: %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.PR_OPEN {
		t.Fatalf("log EOF changed verified PR state: %q", got)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("completed-Job Reconcile(): %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.PR_OPEN {
		t.Fatalf("resumed provider state = %q; want PR_OPEN", got)
	}
}

func TestReconcileContinuesAfterOneTaskErrors(t *testing.T) {
	fixture := newFixture(t)
	first := createTask(t, fixture, "first", "all-first")
	second := createTask(t, fixture, "second", "all-second")
	secondJob, _ := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobs.Name(second.ID, 1), metav1.GetOptions{})
	secondJob.Status.Active = 1
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, secondJob, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("activate second Job: %v", err)
	}
	fixture.kube.PrependReactor("get", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.(k8stesting.GetAction).GetName() == jobs.Name(first.ID, 1) {
			return true, nil, apierrors.NewServiceUnavailable("first unavailable")
		}
		return false, nil, nil
	})
	if err := fixture.controller.Reconcile(fixture.ctx); err == nil || !strings.Contains(err.Error(), "first unavailable") {
		t.Fatalf("Reconcile() error = %v; want accumulated first task error", err)
	}
	if got := getTask(t, fixture, second.ID).State; got != task.RUNNING {
		t.Fatalf("second task was starved in state %q; want RUNNING", got)
	}
}

func TestProviderCallForOneTaskDoesNotBlockAnotherTaskLifecycle(t *testing.T) {
	fixture := newFixture(t)
	first := createRunningTask(t, fixture, "blocked provider", "blocked-provider")
	jobName, podName := jobs.Name(first.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: first.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: first.ID, PullRequestNumber: 42, Branch: "simpleswe/" + first.ID + "-a1", CommitSHA: fullCommitSHA})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: first.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: first.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: first.ID})
	fixture.pullRequests.blocked = make(chan struct{}, 1)
	fixture.pullRequests.release = make(chan struct{})
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: "simpleswe/" + first.ID + "-a1", DestinationBranch: "main", HeadSHA: fullCommitSHA}
	done := make(chan error, 1)
	go func() {
		done <- fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventPullRequestReady, TaskID: first.ID, PullRequestNumber: 42, Branch: "simpleswe/" + first.ID + "-a1", CommitSHA: fullCommitSHA})
	}()
	select {
	case <-fixture.pullRequests.blocked:
	case <-time.After(time.Second):
		t.Fatal("provider call did not block")
	}
	otherDone := make(chan error, 1)
	go func() {
		_, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "independent"})
		otherDone <- err
	}()
	select {
	case err := <-otherDone:
		if err != nil {
			t.Fatalf("independent CreateTask(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("provider call held a global lifecycle lock")
	}
	close(fixture.pullRequests.release)
	if err := <-done; err != nil {
		t.Fatalf("blocked branch event: %v", err)
	}
	control := fixture.controller.(*Controller)
	control.locks.mu.Lock()
	defer control.locks.mu.Unlock()
	if len(control.locks.locks) != 0 {
		t.Fatalf("released task locks retained %d keys", len(control.locks.locks))
	}
}

func TestRetryOldIdempotencyKeyReturnsOriginalAttempt(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "retry history", "retry-history")
	transition(t, fixture, created.ID, task.JOB_PENDING, task.FAILED, "one failed", "kubernetes")
	exhaustAttemptLogs(t, fixture, created.ID, created.CurrentAttemptID)
	second, err := fixture.controller.RetryWithKey(fixture.ctx, created.ID, "retry-one")
	if err != nil {
		t.Fatalf("first retry: %v", err)
	}
	transition(t, fixture, created.ID, task.JOB_PENDING, task.FAILED, "two failed", "kubernetes")
	exhaustAttemptLogs(t, fixture, created.ID, second.ID)
	third, err := fixture.controller.RetryWithKey(fixture.ctx, created.ID, "retry-two")
	if err != nil {
		t.Fatalf("second retry: %v", err)
	}
	replayed, err := fixture.controller.RetryWithKey(fixture.ctx, created.ID, "retry-one")
	if err != nil {
		t.Fatalf("old retry replay: %v", err)
	}
	if replayed.ID != second.ID || replayed.Number != 2 || replayed.ID == third.ID {
		t.Fatalf("old retry replay = %#v; want original attempt %#v, not latest %#v", replayed, second, third)
	}
}

func TestBranchPushedRejectsMalformedSHAAtControllerBoundary(t *testing.T) {
	for _, sha := range []string{"abc123", "0123456789ABCDEF0123456789ABCDEF01234567"} {
		t.Run(sha, func(t *testing.T) {
			fixture := newFixture(t)
			created := createRunningTask(t, fixture, "validate SHA", "validate-sha-"+sha)
			jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
			handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test"}})
			err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: sha})
			if err == nil {
				t.Errorf("malformed SHA %q was accepted", sha)
			}
			if _, err := fixture.store.GetGitResult(fixture.ctx, created.CurrentAttemptID); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("malformed SHA persisted Git result: %v", err)
			}
		})
	}
}

func TestRecreatedResourcesUseImmutableAttemptSnapshot(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "immutable resources", "immutable-resources")
	jobName := jobs.Name(created.ID, 1)
	job, _ := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	secretName := job.Spec.Template.Spec.Volumes[0].Secret.SecretName
	if err := fixture.kube.BatchV1().Jobs(workerNamespace).Delete(fixture.ctx, jobName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Job: %v", err)
	}
	if err := fixture.kube.CoreV1().Secrets(workerNamespace).Delete(fixture.ctx, secretName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Secret: %v", err)
	}
	changed := fixture.config
	changed.Repositories[0].Worker.Image = "registry.example/simpleswe/widget-worker:v99"
	restarted, err := New(fixture.store, fixture.kube, changed, fixture.pullRequests)
	if err != nil {
		t.Fatalf("restart controller: %v", err)
	}
	if err := restarted.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile(): %v", err)
	}
	job, _ = fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if got := job.Spec.Template.Spec.Containers[0].Image; got != workerImage {
		t.Fatalf("recreated image = %q; want immutable attempt image %q", got, workerImage)
	}
}

func TestPermanentManifestErrorIsRejectedBeforeTaskAcceptance(t *testing.T) {
	fixture := newFixture(t)
	fixture.config.Repositories[0].Worker.Image = ""
	control, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	if _, err := control.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "invalid manifest"}); err == nil {
		t.Fatal("invalid manifest was accepted")
	}
	tasks, err := fixture.store.ListTasks(fixture.ctx)
	if err != nil || len(tasks) != 0 {
		t.Fatalf("tasks after invalid manifest = %#v, %v; want none", tasks, err)
	}
}

func TestPermanentKubernetesCreateErrorFailsExplicitly(t *testing.T) {
	fixture := newFixture(t)
	fixture.kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "bad", nil)
	})
	created, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "bad API manifest"})
	if err == nil {
		t.Fatal("permanent Kubernetes error was not returned")
	}
	if got := getTask(t, fixture, created.ID).State; got != task.FAILED {
		t.Fatalf("permanent Kubernetes state = %q; want FAILED", got)
	}
}

func TestFailedJobWaitsForLogsAndAppliesFinalValidationResultBeforeFallback(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "failed logs barrier", "failed-logs-barrier")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})

	job, _ := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	job.Status.Active = 0
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("fail Job: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile before logs exhausted: %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.VALIDATING {
		t.Fatalf("failed Job state before log barrier = %q; want VALIDATING", got)
	}

	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}, Message: "final failure detail", ExitCode: 2})
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); err != nil {
		t.Fatalf("WorkerLogsExhausted: %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.FAILED {
		t.Fatalf("failed Job state after log barrier = %q; want FAILED", got)
	}
	runs, err := fixture.store.ListValidationRuns(fixture.ctx, created.CurrentAttemptID)
	if err != nil || len(runs) != 1 || runs[0].ExitCode != 2 || runs[0].Error != "final failure detail" {
		t.Fatalf("final durable validation result = %#v, %v", runs, err)
	}
}

func TestFailedJobRestartDefersGenericFailureForPendingTrustedWorkerFailure(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "durable failed Job replay", "failed-job-worker-replay")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	event := protocol.Event{
		Type: protocol.EventWorkerFailed, TaskID: record.ID,
		Message: "OpenCode failed after its bounded invocation", Command: []string{"opencode", "run"}, ExitCode: 17,
	}
	queueDurableWorkerEvent(t, fixture, "trusted-worker-failure", "failed-worker-pod-uid", jobName, podName, event)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.Status.Active = 0
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}

	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile retained failed Job before replay: %v", err)
	}
	currentAttempt, err := fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || !currentAttempt.LogsExhausted || getTask(t, fixture, record.ID).State != task.RUNNING {
		t.Fatalf("failed Job before durable replay attempt/state = %#v/%q, %v", currentAttempt, getTask(t, fixture, record.ID).State, err)
	}

	backend := controllerruntime.NewBackend(fixture.store, restarted)
	_, updates, err := backend.GetLogs(fixture.ctx, record.ID, true, attempt.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := controllerruntime.NewRuntime(fixture.kube, fixture.store, restarted, backend, controllerruntime.Options{
		Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, pendingErr := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
		if pendingErr == nil && !pending && getTask(t, fixture, record.ID).State == task.FAILED {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("trusted worker failure was not recovered: pending=%t error=%v state=%q", pending, pendingErr, getTask(t, fixture, record.ID).State)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("restart runtime: %v", err)
	}
	select {
	case _, open := <-updates:
		if open {
			t.Fatal("failed attempt follower remained open after trusted replay")
		}
	case <-time.After(time.Second):
		t.Fatal("failed attempt follower did not close")
	}
	events := listEvents(t, fixture, record.ID)
	wantReason := failureMessage("worker", jobName, podName, event.Command, event.ExitCode, errors.New(event.Message))
	if got := events[len(events)-1].Reason; got != wantReason {
		t.Fatalf("durable failure reason = %q, want exact trusted reason %q", got, wantReason)
	}
	if retried, err := restarted.Retry(fixture.ctx, record.ID); err != nil || retried.Number != 2 {
		t.Fatalf("retry after trusted failed-Job replay = %#v, %v", retried, err)
	}
}

func TestFailedJobBarrierRejectsLateWorkerFailureAfterGenericReconciliation(t *testing.T) {
	fixture := newFixture(t)
	record := createRunningTask(t, fixture, "generic failure wins closed barrier", "failed-job-closed-barrier")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName, podName := jobs.Name(record.ID, attempt.Number), "worker-pod-a1"
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job.Status.Active = 0
	job.Status.Failed = 1
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile failed Job: %v", err)
	}
	currentAttempt, err := fixture.store.GetAttempt(fixture.ctx, record.ID, attempt.ID)
	if err != nil || !currentAttempt.LogsExhausted || getTask(t, fixture, record.ID).State != task.FAILED {
		t.Fatalf("generic failure did not close barrier: attempt=%#v state=%q error=%v", currentAttempt, getTask(t, fixture, record.ID).State, err)
	}
	events := listEvents(t, fixture, record.ID)
	genericReason := events[len(events)-1].Reason
	if !strings.Contains(genericReason, "recovery failed") {
		t.Fatalf("generic failure reason = %q", genericReason)
	}
	late := protocol.Event{Type: protocol.EventWorkerFailed, TaskID: record.ID, Message: "late trusted detail", ExitCode: 17}
	encoded, err := protocol.EncodeEvent(late)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.AppendPodLog(fixture.ctx, store.AppendPodLogParams{
		TaskID: record.ID, AttemptID: attempt.ID, PodUID: "late-failed-pod-uid", JobName: jobName, PodName: podName,
		Content: []byte(encoded), WorkerEventID: "late-worker-failed", WorkerEvent: encoded,
	}, 1<<20, 64<<10); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("late worker_failed append = %v, want ErrConflict", err)
	}
	pending, err := fixture.store.HasPendingWorkerEvents(fixture.ctx, record.ID, attempt.ID)
	if err != nil || pending {
		t.Fatalf("late worker_failed pending = %t, %v; want false", pending, err)
	}
	events = listEvents(t, fixture, record.ID)
	if got := events[len(events)-1].Reason; got != genericReason {
		t.Fatalf("late worker_failed overtook generic failure: got %q, want %q", got, genericReason)
	}
}

func TestValidationFailureBlocksRetryUntilOldLogFollowerCloses(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "retry after log EOF", "retry-after-log-eof")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationFailed, TaskID: created.ID, Command: []string{"go", "test", "./..."}, Message: "failed", ExitCode: 1})

	backend := controllerruntime.NewBackend(fixture.store, fixture.controller)
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	_, updates, err := backend.GetLogs(ctx, created.ID, true, created.CurrentAttemptID, 10)
	if err != nil {
		t.Fatalf("follow failed attempt logs: %v", err)
	}
	if _, err := fixture.controller.Retry(fixture.ctx, created.ID); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("Retry before log EOF = %v, want ErrConflict", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); err != nil {
		t.Fatalf("old attempt log EOF: %v", err)
	}
	select {
	case _, open := <-updates:
		if open {
			t.Fatal("old attempt follower emitted unexpected update")
		}
	case <-time.After(time.Second):
		t.Fatal("old attempt follower remained open after EOF")
	}
	if _, err := fixture.controller.Retry(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Retry after log EOF: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); err != nil {
		t.Fatalf("repeat immutable old-attempt EOF after retry: %v", err)
	}
}

func TestTaskLockWaitHonorsCallerDeadline(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "lock deadline", "lock-deadline")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	fixture.pullRequests.blocked = make(chan struct{}, 1)
	fixture.pullRequests.release = make(chan struct{})
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: "simpleswe/" + created.ID + "-a1", DestinationBranch: "main", HeadSHA: fullCommitSHA}
	branchDone := make(chan error, 1)
	go func() {
		branchDone <- fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	}()
	select {
	case <-fixture.pullRequests.blocked:
	case <-time.After(time.Second):
		t.Fatal("provider call did not acquire task lock")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- fixture.controller.Cancel(ctx, created.ID) }()
	select {
	case err := <-cancelDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Cancel while same-task lock held = %v; want deadline exceeded", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(fixture.pullRequests.release)
		<-branchDone
		t.Fatal("same-task lock waiter ignored caller deadline")
	}
	close(fixture.pullRequests.release)
	if err := <-branchDone; err != nil {
		t.Fatalf("release provider call: %v", err)
	}
}

func TestSecretCleanupIntentExistsBeforePermanentJobCreateFailure(t *testing.T) {
	fixture := newFixture(t)
	fixture.kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "bad", nil)
	})
	created, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "secret-only failure"})
	if err == nil {
		t.Fatal("permanent Job creation error was not returned")
	}
	cleanups, listErr := fixture.store.ListSecretCleanups(fixture.ctx)
	if listErr != nil || len(cleanups) != 1 {
		t.Fatalf("Secret cleanup intents = %#v, %v; want one", cleanups, listErr)
	}
	if cleanups[0].TaskID != created.ID || cleanups[0].AttemptID != created.CurrentAttemptID || cleanups[0].SecretName == "" || cleanups[0].AttemptNumber != 1 {
		t.Fatalf("Secret cleanup intent = %#v", cleanups[0])
	}
}

func TestCancellationAtSecretOnlyBoundaryBecomesRecoverablyTerminal(t *testing.T) {
	fixture := newFixture(t)
	fixture.kube.PrependReactor("create", "jobs", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewTimeoutError("temporary Job API outage", 1)
	})
	created, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "cancel secret only"})
	if err != nil {
		t.Fatalf("CreateTask transient Job failure: %v", err)
	}
	if err := fixture.controller.Cancel(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Cancel secret-only task: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile secret-only cancellation: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.CANCELLED {
		t.Fatalf("secret-only cancellation state = %q; want CANCELLED", got.State)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil || !attempt.LogsExhausted {
		t.Fatalf("secret-only cancelled attempt = %#v, %v; want exhausted log barrier", attempt, err)
	}
	cleanups, err := fixture.store.ListSecretCleanups(fixture.ctx)
	if err != nil || len(cleanups) != 1 {
		t.Fatalf("secret-only cleanup intent = %#v, %v", cleanups, err)
	}
}

func TestQueuedBranchPushedAfterCancellationIsLogicalNoop(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "cancel before queued branch", "cancel-before-branch")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA}
	queueDurableWorkerEvent(t, fixture, "cancelled-pull-request-ready", "cancelled-pull-request-ready-pod", jobName, podName, event)

	if err := fixture.controller.Cancel(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	if err := fixture.controller.(*Controller).HandleWorkerEventOnce(fixture.ctx, "cancelled-pull-request-ready", jobName, podName, event); err != nil {
		t.Fatalf("queued pull_request_ready after accepted cancellation: %v", err)
	}
	if _, err := fixture.store.GetGitResult(fixture.ctx, created.CurrentAttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled branch event persisted Git result: %v", err)
	}
	if _, err := fixture.store.GetPullRequest(fixture.ctx, created.CurrentAttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled branch event persisted pull request: %v", err)
	}
	if got := getTask(t, fixture, created.ID); !got.CancellationRequested || got.State != task.VALIDATING {
		t.Fatalf("cancelled branch event changed outcome owner: %#v", got)
	}
}

func TestCancelledTaskDrainsEveryDurableWorkerEventType(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "drain cancelled worker events", "cancelled-worker-events")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	jobName, podName := jobs.Name(created.ID, attempt.Number), "worker-pod-a1"
	const podUID = "cancelled-worker-events-pod-uid"
	events := []protocol.Event{
		{Type: protocol.EventAgentStarted, TaskID: created.ID},
		{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}},
		{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}},
		{Type: protocol.EventValidationSucceeded, TaskID: created.ID},
		{Type: protocol.EventValidationFailed, TaskID: created.ID, Message: "late validation failure", ExitCode: 1},
		{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA},
		{Type: protocol.EventWorkerFailed, TaskID: created.ID, Message: "late worker failure", ExitCode: 1},
		{Type: protocol.EventBranchPushed, TaskID: created.ID, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA},
	}
	for i, event := range events {
		queueDurableWorkerEvent(t, fixture, fmt.Sprintf("cancelled-worker-event-%d", i), podUID, jobName, podName, event)
	}

	if err := fixture.controller.Cancel(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete cancelled Pod: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("finalize cancellation: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.CANCELLED || got.CancellationRequested {
		t.Fatalf("cancelled task = %#v, want terminal state with cleared request", got)
	}
	pending, err := fixture.store.ListPendingWorkerEvents(fixture.ctx, podUID)
	if err != nil || len(pending) != len(events) {
		t.Fatalf("pending events before terminal drain = %d, %v; want %d", len(pending), err, len(events))
	}

	runtime, err := controllerruntime.NewRuntime(
		fixture.kube,
		fixture.store,
		fixture.controller,
		controllerruntime.NewBackend(fixture.store, fixture.controller),
		controllerruntime.Options{Namespace: workerNamespace, PodLogs: &staticWorkerLogs{}, RecoveryInterval: time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(fixture.ctx)
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		pending, err = fixture.store.ListPendingWorkerEvents(fixture.ctx, podUID)
		if err == nil && len(pending) == 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("terminal durable events did not drain: pending=%d error=%v", len(pending), err)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("terminal event drain runtime: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.CANCELLED {
		t.Fatalf("drained events changed cancelled task state to %q", got.State)
	}
}

func TestCancelSerializesWithInFlightInitialResourceCreation(t *testing.T) {
	fixture := newFixture(t)
	secretCreate := make(chan string, 1)
	releaseCreate := make(chan struct{})
	fixture.kube.PrependReactor("create", "secrets", func(action k8stesting.Action) (bool, runtime.Object, error) {
		secret := action.(k8stesting.CreateAction).GetObject().(*corev1.Secret)
		secretCreate <- secret.Labels["simpleswe.dev/task-id"]
		<-releaseCreate
		return false, nil, nil
	})
	createDone := make(chan error, 1)
	go func() {
		_, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "serialize cancellation"})
		createDone <- err
	}()
	taskID := <-secretCreate
	cancelDone := make(chan error, 1)
	go func() { cancelDone <- fixture.controller.Cancel(fixture.ctx, taskID) }()
	select {
	case err := <-cancelDone:
		close(releaseCreate)
		<-createDone
		t.Fatalf("Cancel returned before same-task resource creation released: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseCreate)
	if err := <-createDone; err != nil {
		t.Fatalf("CreateTask(): %v", err)
	}
	if err := <-cancelDone; err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobs.Name(taskID, 1), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("Job exists after accepted cancellation: %v", err)
	}
	if got := getTask(t, fixture, taskID); !got.CancellationRequested {
		t.Fatalf("cancellation intent not durable: %#v", got)
	}
}

func TestReconcileReloadsTerminalTaskBeforeCreatingFromStaleSnapshot(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "terminal beats stale reconcile", "terminal-stale-reconcile")
	stale := getTask(t, fixture, created.ID)
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: "worker_failed", TaskID: created.ID, Message: "terminal"}); err != nil {
		t.Fatalf("terminal worker event: %v", err)
	}
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get terminal Job: %v", err)
	}
	secretName := taskSecretNameForTest(job)
	if err := fixture.kube.BatchV1().Jobs(workerNamespace).Delete(fixture.ctx, jobName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete terminal Job: %v", err)
	}
	if err := fixture.kube.CoreV1().Secrets(workerNamespace).Delete(fixture.ctx, secretName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete terminal Secret: %v", err)
	}
	before := len(createResources(fixture.kube.Actions()))
	if err := fixture.controller.(*Controller).reconcileTask(fixture.ctx, stale); err != nil {
		t.Fatalf("reconcile stale terminal snapshot: %v", err)
	}
	if after := len(createResources(fixture.kube.Actions())); after != before {
		t.Fatalf("stale reconcile created %d resources after terminal event", after-before)
	}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("stale reconcile recreated terminal Job: %v", err)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil || attempt.LogsExhausted {
		t.Fatalf("missing Job closed logs while matching Pod remained: %#v, %v", attempt, err)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, podName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete terminal Pod: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("reconcile terminal attempt after Pod deletion: %v", err)
	}
	attempt, err = fixture.store.GetAttempt(fixture.ctx, created.ID, created.CurrentAttemptID)
	if err != nil || !attempt.LogsExhausted {
		t.Fatalf("missing Job and Pod did not close terminal log barrier: %#v, %v", attempt, err)
	}
	if retried, err := fixture.controller.Retry(fixture.ctx, created.ID); err != nil || retried.Number != 2 {
		t.Fatalf("retry after missing worker_failed resources = %#v, %v", retried, err)
	}
}

func taskSecretNameForTest(job *batchv1.Job) string {
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "task-secret" && volume.Secret != nil {
			return volume.Secret.SecretName
		}
	}
	return job.Name + "-task"
}
