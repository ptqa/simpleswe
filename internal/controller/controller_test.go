package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestConfiguredProjectsAreSafeAndNamed(t *testing.T) {
	controller := &Controller{config: config.Config{Repositories: config.RepositoryConfigs{
		{Name: "widget", CloneURL: "https://example.com/widget.git"},
		{CloneURL: "https://example.com/unnamed.git"},
	}}}

	got := controller.ConfiguredProjects()
	want := []store.ConfiguredProject{
		{Name: "widget", Repository: "https://example.com/widget.git"},
		{Name: "https://example.com/unnamed.git", Repository: "https://example.com/unnamed.git"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ConfiguredProjects() = %#v, want %#v", got, want)
	}
}

func TestCreateTaskPersistsQueuesAndCreatesSecretBeforeOneJob(t *testing.T) {
	fixture := newFixture(t)
	params := store.CreateTaskParams{
		Repository:     repositoryURL,
		Prompt:         "fix the flaky test",
		IdempotencyKey: "request-123",
	}

	created, err := fixture.controller.CreateTask(fixture.ctx, params)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	persisted := getTask(t, fixture, created.ID)
	if persisted.Repository != params.Repository || persisted.Prompt != params.Prompt {
		t.Fatalf("persisted task = %#v; want repository and prompt from request", persisted)
	}
	if persisted.State != task.JOB_PENDING {
		t.Fatalf("persisted state = %q; want %q after queuing one Job", persisted.State, task.JOB_PENDING)
	}
	assertEventPath(t, fixture, created.ID, []task.State{
		task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING,
	})

	creates := createResources(fixture.kube.Actions())
	if !reflect.DeepEqual(creates, []string{"secrets", "jobs"}) {
		t.Fatalf("Kubernetes create order = %v; want task Secret before exactly one Job", creates)
	}
	jobList, err := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list Jobs: %v", err)
	}
	secretList, err := fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list Secrets: %v", err)
	}
	if len(jobList.Items) != 1 || len(secretList.Items) != 1 {
		t.Fatalf("created Jobs/Secrets = %d/%d; want 1/1", len(jobList.Items), len(secretList.Items))
	}
	job := jobList.Items[0]
	if job.Name != jobs.Name(created.ID, 1) {
		t.Errorf("Job name = %q; want deterministic %q", job.Name, jobs.Name(created.ID, 1))
	}
	if got := job.Labels["simpleswe.dev/attempt"]; got != "1" {
		t.Errorf("attempt label = %q; want 1", got)
	}
	if got := job.Labels["simpleswe.dev/attempt-id"]; got != created.CurrentAttemptID {
		t.Errorf("attempt ID label = %q; want %q", got, created.CurrentAttemptID)
	}
	if got := job.Spec.Template.Labels["simpleswe.dev/attempt-id"]; got != created.CurrentAttemptID {
		t.Errorf("Pod attempt ID label = %q; want %q", got, created.CurrentAttemptID)
	}
	if got := job.Spec.Template.Spec.Containers[0].Image; got != workerImage {
		t.Errorf("worker image = %q; want repository image %q", got, workerImage)
	}

	replayed, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{
		Repository:     "https://bitbucket.example/ignored/other.git",
		Prompt:         "different replay payload",
		IdempotencyKey: params.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask(replayed request) error = %v", err)
	}
	if replayed.ID != created.ID {
		t.Fatalf("replayed event task ID = %q; want %q", replayed.ID, created.ID)
	}
	if got := createResources(fixture.kube.Actions()); !reflect.DeepEqual(got, creates) {
		t.Fatalf("replayed event created more resources: before %v, after %v", creates, got)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Number != 1 {
		t.Fatalf("replayed event attempts = %#v; want only attempt 1", attempts)
	}
}

func TestCreateTaskIdempotencyReplayDoesNotCreateAnotherJobOrAttempt(t *testing.T) {
	fixture := newFixture(t)
	params := store.CreateTaskParams{
		Repository:     repositoryURL,
		Prompt:         "generic create",
		IdempotencyKey: "request-123",
	}
	created, err := fixture.controller.CreateTask(fixture.ctx, params)
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	creates := createResources(fixture.kube.Actions())

	replayed, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{
		Repository:     params.Repository,
		Prompt:         "payload ignored on replay",
		IdempotencyKey: params.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask(idempotency replay) error = %v", err)
	}
	if replayed.ID != created.ID || replayed.Prompt != params.Prompt {
		t.Fatalf("replayed task = %#v, want original %#v", replayed, created)
	}
	if got := createResources(fixture.kube.Actions()); !reflect.DeepEqual(got, creates) {
		t.Fatalf("idempotency replay created more resources: before %v, after %v", creates, got)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts: %v", err)
	}
	if len(attempts) != 1 || attempts[0].Number != 1 {
		t.Fatalf("idempotency replay attempts = %#v; want only attempt 1", attempts)
	}
}

func TestCancelRecordsIntentBeforeForegroundDeletingOnlyActiveJob(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "cancel safely", "cancel-event")
	activeName := jobs.Name(created.ID, 1)
	historical := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name:      jobs.Name(created.ID, 99),
		Namespace: workerNamespace,
		Labels: map[string]string{
			"simpleswe.dev/task-id": created.ID,
			"simpleswe.dev/attempt": "99",
		},
	}}
	unrelated := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "unrelated-a1", Namespace: workerNamespace}}
	for _, job := range []*batchv1.Job{historical, unrelated} {
		if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Create(fixture.ctx, job, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed Job %q: %v", job.Name, err)
		}
	}

	deleteObserved := false
	fixture.kube.PrependReactor("delete", "jobs", func(action k8stesting.Action) (bool, runtime.Object, error) {
		deleteAction := action.(k8stesting.DeleteAction)
		got := getTask(t, fixture, created.ID)
		if !got.CancellationRequested {
			t.Error("Kubernetes delete happened before cancellation intent was durable")
		}
		if got.State == task.CANCELLED {
			t.Error("task transitioned to CANCELLED before Kubernetes deletion")
		}
		if deleteAction.GetNamespace() != workerNamespace || deleteAction.GetName() != activeName {
			t.Errorf("deleted %s/%s; want active Job %s/%s", deleteAction.GetNamespace(), deleteAction.GetName(), workerNamespace, activeName)
		}
		deleteObserved = true
		return false, nil, nil
	})

	if err := fixture.controller.Cancel(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if !deleteObserved {
		t.Fatal("Cancel() did not delete the active Job")
	}
	got := getTask(t, fixture, created.ID)
	if !got.CancellationRequested || got.State == task.CANCELLED {
		t.Fatalf("cancellation after delete = %#v; want durable pending intent", got)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() cancellation: %v", err)
	}
	got = getTask(t, fixture, created.ID)
	if got.CancellationRequested || got.State != task.CANCELLED {
		t.Fatalf("reconciled cancellation = %#v; want CANCELLED after Job and Pods are absent", got)
	}
	for _, name := range []string{historical.Name, unrelated.Name} {
		if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, name, metav1.GetOptions{}); err != nil {
			t.Errorf("non-active Job %q was deleted: %v", name, err)
		}
	}

	deletes := deleteActions(fixture.kube.Actions())
	if len(deletes) != 1 {
		t.Fatalf("Job deletes = %d; want exactly 1", len(deletes))
	}
	options := deletes[0].GetDeleteOptions()
	if options.PropagationPolicy == nil || *options.PropagationPolicy != metav1.DeletePropagationForeground {
		t.Fatalf("delete propagation = %v; want Foreground", options.PropagationPolicy)
	}
}

func TestCancellationWaitsForOwnedPodDeletion(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "cancel running", "cancel-running")
	if err := fixture.controller.Cancel(fixture.ctx, created.ID); err != nil {
		t.Fatalf("Cancel(): %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() with Pod: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State == task.CANCELLED || !got.CancellationRequested {
		t.Fatalf("cancellation with owned Pod = %#v; want pending", got)
	}
	if err := fixture.kube.CoreV1().Pods(workerNamespace).Delete(fixture.ctx, "worker-pod-a1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete worker Pod: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() without Pod: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.CANCELLED || got.CancellationRequested {
		t.Fatalf("completed cancellation = %#v; want CANCELLED", got)
	}
}

func TestRetryFromFailedCreatesImmutableAttemptTwoAndDistinctJob(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "retry this task", "retry-event")
	transition(t, fixture, created.ID, task.JOB_PENDING, task.FAILED, "attempt 1 failed", "kubernetes")
	exhaustAttemptLogs(t, fixture, created.ID, created.CurrentAttemptID)

	before, err := fixture.store.ListAttempts(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts before retry: %v", err)
	}
	if len(before) != 1 {
		t.Fatalf("attempts before retry = %d; want 1", len(before))
	}

	retried, err := fixture.controller.Retry(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if retried.Number != 2 || !retried.Immutable {
		t.Fatalf("retry attempt = %#v; want immutable attempt 2", retried)
	}
	if retried.State != task.JOB_PENDING || getTask(t, fixture, created.ID).State != task.JOB_PENDING {
		t.Fatalf("retry did not progress attempt 2 to job_pending: attempt=%q task=%q", retried.State, getTask(t, fixture, created.ID).State)
	}
	after, err := fixture.store.ListAttempts(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("list attempts after retry: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("attempts after retry = %d; want 2", len(after))
	}
	if !reflect.DeepEqual(after[0], before[0]) {
		t.Fatalf("attempt 1 changed from %#v to %#v", before[0], after[0])
	}

	jobList, err := fixture.kube.BatchV1().Jobs(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list retry Jobs: %v", err)
	}
	if len(jobList.Items) != 2 {
		t.Fatalf("retry Jobs = %d; want one per attempt", len(jobList.Items))
	}
	wantNames := map[string]string{
		jobs.Name(created.ID, 1): "1",
		jobs.Name(created.ID, 2): "2",
	}
	for _, job := range jobList.Items {
		attempt, ok := wantNames[job.Name]
		if !ok {
			t.Errorf("unexpected retry Job %q", job.Name)
			continue
		}
		if job.Labels["simpleswe.dev/attempt"] != attempt {
			t.Errorf("Job %q attempt label = %q; want %q", job.Name, job.Labels["simpleswe.dev/attempt"], attempt)
		}
		if job.Labels["simpleswe.dev/attempt-id"] == "" {
			t.Errorf("Job %q has no attempt ID label", job.Name)
		}
	}
	secrets, err := fixture.kube.CoreV1().Secrets(workerNamespace).List(fixture.ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list retry Secrets: %v", err)
	}
	branches := map[string]bool{}
	for _, secret := range secrets.Items {
		var manifest protocol.TaskManifest
		if err := json.Unmarshal(secret.Data["task.json"], &manifest); err != nil {
			t.Fatalf("decode retry manifest: %v", err)
		}
		branches[manifest.TaskBranch] = true
	}
	if !branches["simpleswe/"+created.ID+"-a1"] || !branches["simpleswe/"+created.ID+"-a2"] {
		t.Fatalf("retry branches = %v; want unique attempt suffixes", branches)
	}
}

func TestRetryWithKeyIsIdempotentAfterAttemptTwoProgresses(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "retry once", "retry-once")
	transition(t, fixture, created.ID, task.JOB_PENDING, task.FAILED, "attempt failed", "kubernetes")
	exhaustAttemptLogs(t, fixture, created.ID, created.CurrentAttemptID)
	first, err := fixture.controller.RetryWithKey(fixture.ctx, created.ID, "action-123")
	if err != nil {
		t.Fatalf("RetryWithKey(): %v", err)
	}
	second, err := fixture.controller.RetryWithKey(fixture.ctx, created.ID, "action-123")
	if err != nil {
		t.Fatalf("RetryWithKey(replay): %v", err)
	}
	if first.ID != second.ID || first.Number != 2 || second.State != task.JOB_PENDING {
		t.Fatalf("idempotent attempts = %#v / %#v; want progressed attempt 2", first, second)
	}
	attempts, err := fixture.store.ListAttempts(fixture.ctx, created.ID)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts = %#v, %v; want exactly two", attempts, err)
	}
}

func TestReconcileRecoversLabelledJobLifecycleExplicitly(t *testing.T) {
	tests := []struct {
		name      string
		status    batchv1.JobStatus
		pod       *corev1.Pod
		wantState task.State
		wantWord  string
	}{
		{
			name:      "running",
			status:    batchv1.JobStatus{Active: 1},
			wantState: task.RUNNING,
			wantWord:  "running",
		},
		{
			name: "failed",
			status: batchv1.JobStatus{Failed: 1, Conditions: []batchv1.JobCondition{{
				Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded",
			}}},
			pod:       failedPod("failed-worker-pod", 23),
			wantState: task.FAILED,
			wantWord:  "failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			created := createTask(t, fixture, "reconcile "+test.name, "reconcile-"+test.name)
			jobName := jobs.Name(created.ID, 1)
			job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get worker Job: %v", err)
			}
			job.Status = test.status
			job.UID = "reconcile-job-uid"
			if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("update worker Job status: %v", err)
			}
			if test.pod != nil {
				test.pod.Namespace = workerNamespace
				test.pod.Labels = copyLabels(job.Labels)
				controller := true
				test.pod.OwnerReferences = []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}}
				if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Create(fixture.ctx, test.pod, metav1.CreateOptions{}); err != nil {
					t.Fatalf("create failed worker Pod: %v", err)
				}
			}
			before := listEvents(t, fixture, created.ID)
			if test.name == "failed" {
				if err := fixture.store.MarkLogsExhausted(fixture.ctx, created.ID, created.CurrentAttemptID); err != nil {
					t.Fatalf("mark failed Job logs exhausted: %v", err)
				}
			}

			if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			got := getTask(t, fixture, created.ID)
			if got.State != test.wantState {
				t.Fatalf("reconciled state = %q; want %q", got.State, test.wantState)
			}
			after := listEvents(t, fixture, created.ID)
			if len(after) <= len(before) {
				t.Fatal("Reconcile() observed the Job but recorded no recovery event")
			}
			recovered := after[len(before):]
			for _, event := range recovered {
				if event.Trigger != "kubernetes" {
					t.Errorf("recovery trigger = %q; want kubernetes", event.Trigger)
				}
				if err := (task.Machine{}).Transition(event.FromState, event.ToState); err != nil {
					t.Errorf("recovery silently skipped lifecycle %q -> %q: %v", event.FromState, event.ToState, err)
				}
			}
			details := eventText(recovered)
			for _, want := range []string{"recovery", test.wantWord, jobName} {
				if !strings.Contains(strings.ToLower(details), strings.ToLower(want)) {
					t.Errorf("recovery events %q do not contain %q", details, want)
				}
			}
			if test.name == "failed" {
				assertFailureDetails(t, recovered[len(recovered)-1], "kubernetes", jobName, test.pod.Name, 23)
			}
		})
	}
}

func TestCompletedJobWaitsForLogsThenFailsWithoutReportedPullRequest(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "must produce a branch", "complete-without-branch")
	jobName := jobs.Name(created.ID, 1)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	job.UID = "completed-job"
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("complete Job: %v", err)
	}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "completed-pod", Namespace: workerNamespace, Labels: copyLabels(job.Labels), OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}}}}
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Create(fixture.ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create Pod: %v", err)
	}
	before := listEvents(t, fixture, created.ID)
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() before logs: %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.JOB_PENDING {
		t.Fatalf("completed Job before logs fabricated state %q; want recoverable %q", got, task.JOB_PENDING)
	}
	if len(listEvents(t, fixture, created.ID)) != len(before) {
		t.Fatal("completed Job fabricated lifecycle events before log collection")
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, pod.Name); err != nil {
		t.Fatalf("WorkerLogsExhausted(): %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.FAILED {
		t.Fatalf("completed Job without branch state = %q; want FAILED", got)
	}
	events := listEvents(t, fixture, created.ID)
	if !strings.Contains(events[len(events)-1].Reason, "OpenCode did not report a pull request") {
		t.Fatalf("completion failure reason = %q; want planned missing-report message", events[len(events)-1].Reason)
	}
}

func TestHandleWorkerEventUsesTypedEventsAndRecordsValidationFailure(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "record worker failure", "worker-failure")
	jobName := jobs.Name(created.ID, 1)
	podName := "worker-pod-a1"

	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{
		Type: "log", TaskID: created.ID, Message: "agent_started",
	}); err == nil {
		t.Log("unsupported typed event was ignored")
	}
	if got := getTask(t, fixture, created.ID).State; got != task.RUNNING {
		t.Fatalf("raw message content changed state to %q; want %q", got, task.RUNNING)
	}

	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: created.ID})
	if got := getTask(t, fixture, created.ID).State; got != task.AGENT_RUNNING {
		t.Fatalf("agent_started state = %q; want %q", got, task.AGENT_RUNNING)
	}
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: created.ID})
	if got := getTask(t, fixture, created.ID).State; got != task.VALIDATING {
		t.Fatalf("validation_started state = %q; want %q", got, task.VALIDATING)
	}
	handleEvent(t, fixture, jobName, podName, protocol.Event{
		Type: "validation_failed", TaskID: created.ID, ExitCode: 17, Message: "go test failed",
	})
	if got := getTask(t, fixture, created.ID).State; got != task.FAILED {
		t.Fatalf("validation_failed state = %q; want %q", got, task.FAILED)
	}
	events := listEvents(t, fixture, created.ID)
	assertFailureDetails(t, events[len(events)-1], "validation", jobName, podName, 17)
	runs, err := fixture.store.ListValidationRuns(fixture.ctx, created.CurrentAttemptID)
	if err != nil {
		t.Fatalf("ListValidationRuns(): %v", err)
	}
	if len(runs) != 1 || runs[0].State != "failed" || !strings.Contains(runs[0].Error, "go test failed") {
		t.Fatalf("validation runs = %#v; want durable failure", runs)
	}
}

func TestWorkerFailureEventRecordsExplicitFailure(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "record worker crash", "worker-crash")
	jobName := jobs.Name(created.ID, 1)
	handleEvent(t, fixture, jobName, "worker-pod-a1", protocol.Event{
		Type: "worker_failed", TaskID: created.ID, Message: "agent process exited", ExitCode: 9,
	})
	if got := getTask(t, fixture, created.ID).State; got != task.FAILED {
		t.Fatalf("worker failure state = %q; want FAILED", got)
	}
	events := listEvents(t, fixture, created.ID)
	assertFailureDetails(t, events[len(events)-1], "worker", jobName, "worker-pod-a1", 9)
}

func TestWorkerFailureEventPersistsTrustedReportReason(t *testing.T) {
	for i, message := range []string{"OpenCode did not report a pull request", "OpenCode reported failure: could not reproduce"} {
		t.Run(message, func(t *testing.T) {
			fixture := newFixture(t)
			created := createRunningTask(t, fixture, "fix it", fmt.Sprintf("trusted-worker-failure-%d", i))
			attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
			if err != nil {
				t.Fatal(err)
			}
			event := protocol.Event{Type: protocol.EventWorkerFailed, TaskID: created.ID, Message: message, ExitCode: 1}
			if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobs.Name(created.ID, attempt.Number), "worker-pod-a1", event); err != nil {
				t.Fatalf("HandleWorkerEvent: %v", err)
			}
			events := listEvents(t, fixture, created.ID)
			if reason := events[len(events)-1].Reason; !strings.Contains(reason, message) {
				t.Fatalf("durable failure reason = %q, want report message %q", reason, message)
			}
		})
	}
}

func TestPullRequestReadyVerifiesReportedPullRequestAtMostOnce(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "open a pull request", "pull-request-ready")
	jobName := jobs.Name(created.ID, 1)
	podName := "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "agent_started", TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_started", TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_result", TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "validation_succeeded", TaskID: created.ID})
	before := listEvents(t, fixture, created.ID)
	branch := "simpleswe/" + created.ID + "-a1"
	fixture.pullRequests.getResult = &forge.PullRequestState{Number: 42, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: "acme", SourceRepository: "widget", SourceBranch: branch, DestinationBranch: "main", HeadSHA: fullCommitSHA}
	event := protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: branch, CommitSHA: fullCommitSHA}
	handleEvent(t, fixture, jobName, podName, event)

	if got := getTask(t, fixture, created.ID).State; got != task.PR_OPEN {
		t.Fatalf("pull_request_ready state = %q; want %q", got, task.PR_OPEN)
	}
	after := listEvents(t, fixture, created.ID)
	wantPath := []task.State{task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR, task.PR_OPEN}
	assertEventSlicePath(t, after[len(before):], wantPath)
	if fixture.pullRequests.getCalls != 1 || fixture.pullRequests.getNumbers[0] != 42 {
		t.Fatalf("provider get calls = %d numbers=%v; want 1 [42]", fixture.pullRequests.getCalls, fixture.pullRequests.getNumbers)
	}
	durable, err := fixture.store.GetPullRequest(fixture.ctx, created.CurrentAttemptID)
	if err != nil || durable.Number != 42 || durable.URL != pullRequestURL || durable.Title != "Provider title" || durable.HeadBranch != branch || durable.BaseBranch != "main" {
		t.Fatalf("durable provider pull request = %#v, %v", durable, err)
	}
	runs, err := fixture.store.ListValidationRuns(fixture.ctx, created.CurrentAttemptID)
	if err != nil || len(runs) != 1 || runs[0].State != "succeeded" {
		t.Fatalf("successful validation runs = %#v, %v", runs, err)
	}

	// Replay after reconstructing the controller to require durable, not merely
	// in-memory, idempotency.
	restarted, err := New(fixture.store, fixture.kube, fixture.config, fixture.pullRequests)
	if err != nil {
		t.Fatalf("restart controller: %v", err)
	}
	if err := restarted.HandleWorkerEvent(fixture.ctx, jobName, podName, event); err != nil {
		t.Fatalf("replay pull_request_ready: %v", err)
	}
	if fixture.pullRequests.getCalls != 1 {
		t.Fatalf("replayed ready event repeated provider inspection: get=%d", fixture.pullRequests.getCalls)
	}
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get completed Job: %v", err)
	}
	job.Status.Active = 0
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("complete Job: %v", err)
	}
	if err := restarted.WorkerLogsExhausted(fixture.ctx, jobName, podName); err != nil {
		t.Fatalf("WorkerLogsExhausted(): %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.PR_OPEN {
		t.Fatalf("completed Job with durable PR fabricated final state %q; want PR_OPEN", got)
	}
}

func TestWorkerEventRejectsPodWithoutCurrentAttemptAndJobUID(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "reject spoofed pod", "spoofed-pod")
	jobName := jobs.Name(created.ID, 1)
	pod, err := fixture.kube.CoreV1().Pods(workerNamespace).Get(fixture.ctx, "worker-pod-a1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker Pod: %v", err)
	}
	pod.Labels["simpleswe.dev/attempt-id"] = "wrong-attempt"
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Update(fixture.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update worker Pod: %v", err)
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, pod.Name, protocol.Event{Type: "agent_started", TaskID: created.ID}); err == nil {
		t.Fatal("worker event from wrong attempt Pod was accepted")
	}
	if got := getTask(t, fixture, created.ID).State; got != task.RUNNING {
		t.Fatalf("spoofed worker event changed state to %q", got)
	}
}

func TestReconcileRecreatesManuallyDeletedActiveResources(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "recover resources", "recover-resources")
	jobName := jobs.Name(created.ID, 1)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	secretName := job.Spec.Template.Spec.Volumes[0].Secret.SecretName
	if err := fixture.kube.BatchV1().Jobs(workerNamespace).Delete(fixture.ctx, jobName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Job: %v", err)
	}
	if err := fixture.kube.CoreV1().Secrets(workerNamespace).Delete(fixture.ctx, secretName, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Secret: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile(): %v", err)
	}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{}); err != nil {
		t.Fatalf("recovered Job: %v", err)
	}
	if _, err := fixture.kube.CoreV1().Secrets(workerNamespace).Get(fixture.ctx, secretName, metav1.GetOptions{}); err != nil {
		t.Fatalf("recovered Secret: %v", err)
	}
	events := listEvents(t, fixture, created.ID)
	if !strings.Contains(eventText(events), "recovery recreated") {
		t.Fatalf("events do not explicitly record resource recovery: %s", eventText(events))
	}
}

func assertFailureDetails(t *testing.T, event store.TransitionEvent, stage, job, pod string, exitCode int) {
	t.Helper()
	details := eventText([]store.TransitionEvent{event})
	for _, want := range []string{
		"stage=" + stage,
		"job=" + job,
		"pod=" + pod,
		fmt.Sprintf("exit_code=%d", exitCode),
	} {
		if !strings.Contains(details, want) {
			t.Errorf("failure event %q does not contain %q", details, want)
		}
	}
}
