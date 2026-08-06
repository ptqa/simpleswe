package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

const fullCommitSHA = "0123456789abcdef0123456789abcdef01234567"

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
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}, Message: "passed"})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	fixture.pullRequests.failFind = 1
	fixture.pullRequests.findErr = errors.New("temporary Bitbucket outage")
	branch := protocol.Event{Type: protocol.EventBranchPushed, TaskID: created.ID, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, branch); err == nil {
		t.Fatal("transient provider failure was not reported")
	}
	if got := getTask(t, fixture, created.ID).State; got != task.CREATING_PR {
		t.Fatalf("provider failure state = %q; want resumable CREATING_PR", got)
	}
	pr, err := fixture.store.GetPullRequest(fixture.ctx, created.CurrentAttemptID)
	if err != nil || pr.State != "creating" {
		t.Fatalf("PR reservation = %#v, %v; want creating", pr, err)
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
	if got := getTask(t, fixture, created.ID).State; got != task.CREATING_PR {
		t.Fatalf("log EOF failed resumable PR state: %q", got)
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
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: first.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: first.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: first.ID})
	fixture.pullRequests.blocked = make(chan struct{}, 1)
	fixture.pullRequests.release = make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: first.ID, Branch: "simpleswe/" + first.ID + "-a1", CommitSHA: fullCommitSHA})
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
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "validate SHA", "validate-sha")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test"}})
	for _, sha := range []string{"abc123", "0123456789ABCDEF0123456789ABCDEF01234567"} {
		err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: created.ID, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: sha})
		if err == nil {
			t.Errorf("malformed SHA %q was accepted", sha)
		}
	}
	if _, err := fixture.store.GetGitResult(fixture.ctx, created.CurrentAttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("malformed SHA persisted Git result: %v", err)
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
	restarted, err := New(fixture.store, fixture.kube, changed, fixture.notifier, fixture.pullRequests)
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
	control, err := New(fixture.store, fixture.kube, fixture.config, fixture.notifier, fixture.pullRequests)
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

func TestPendingNotificationsContinueAfterOneFailure(t *testing.T) {
	fixture := newFixture(t)
	var taskIDs []string
	for i := range 2 {
		record, err := fixture.store.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: fmt.Sprintf("notify %d", i)})
		if err != nil {
			t.Fatalf("create notification task: %v", err)
		}
		attempt, _ := fixture.store.CurrentAttempt(fixture.ctx, record.ID)
		if _, err := fixture.store.ReservePullRequest(fixture.ctx, attempt.ID, record.Prompt, fmt.Sprintf("work/%d", i), "main"); err != nil {
			t.Fatalf("reserve pull request: %v", err)
		}
		if err := fixture.store.CompletePullRequest(fixture.ctx, attempt.ID, i+1, fmt.Sprintf("%s/%d", pullRequestURL, i)); err != nil {
			t.Fatalf("complete pull request: %v", err)
		}
		taskIDs = append(taskIDs, record.ID)
	}
	fixture.notifier.errors = map[string]error{taskIDs[1]: errors.New("temporary notification failure")}
	if err := fixture.controller.(*Controller).NotifyPendingPullRequests(fixture.ctx); err == nil {
		t.Fatal("notification failure was not returned")
	}
	if len(fixture.notifier.calls) != 2 {
		t.Fatalf("notification calls = %#v, want both pending tasks attempted", fixture.notifier.calls)
	}
}

func TestTaskLockWaitHonorsCallerDeadline(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "lock deadline", "lock-deadline")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	fixture.pullRequests.blocked = make(chan struct{}, 1)
	fixture.pullRequests.release = make(chan struct{})
	branchDone := make(chan error, 1)
	go func() {
		branchDone <- fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: created.ID, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
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

func TestNotifierCallIsBoundedAndReleasesTaskLock(t *testing.T) {
	fixture := newFixture(t)
	control := fixture.controller.(*Controller)
	control.providerTimeout = 25 * time.Millisecond
	fixture.notifier.blocked = make(chan struct{}, 1)
	fixture.notifier.release = make(chan struct{})
	created := createRunningTask(t, fixture, "bounded notifier", "bounded-notifier")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test", "./..."}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	done := make(chan error, 1)
	go func() {
		done <- fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventBranchPushed, TaskID: created.ID, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	}()
	select {
	case <-fixture.notifier.blocked:
	case <-time.After(time.Second):
		t.Fatal("notifier was not called")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("branch event after notifier timeout: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		close(fixture.notifier.release)
		<-done
		t.Fatal("notifier call was not bounded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := fixture.controller.Cancel(ctx, created.ID); err != nil {
		t.Fatalf("task lock remained held after notifier timeout: %v", err)
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

	if err := fixture.controller.Cancel(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	event := protocol.Event{Type: protocol.EventBranchPushed, TaskID: created.ID, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, event); err != nil {
		t.Fatalf("queued branch_pushed after accepted cancellation: %v", err)
	}
	if _, err := fixture.store.GetGitResult(fixture.ctx, created.CurrentAttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled branch event persisted Git result: %v", err)
	}
	if _, err := fixture.store.GetPullRequest(fixture.ctx, created.CurrentAttemptID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled branch event persisted pull request: %v", err)
	}
	if len(fixture.pullRequests.calls) != 0 || len(fixture.notifier.calls) != 0 {
		t.Fatalf("cancelled branch event called provider/notifier: PR=%d Slack=%d", len(fixture.pullRequests.calls), len(fixture.notifier.calls))
	}
	if got := getTask(t, fixture, created.ID); !got.CancellationRequested || got.State != task.VALIDATING {
		t.Fatalf("cancelled branch event changed outcome owner: %#v", got)
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
}

func taskSecretNameForTest(job *batchv1.Job) string {
	for _, volume := range job.Spec.Template.Spec.Volumes {
		if volume.Name == "task-secret" && volume.Secret != nil {
			return volume.Secret.SecretName
		}
	}
	return job.Name + "-task"
}
