package runtime

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

type controllerWithoutLogEvents struct{}

func (controllerWithoutLogEvents) CreateTask(context.Context, store.CreateTaskParams) (store.Task, error) {
	return store.Task{}, nil
}
func (controllerWithoutLogEvents) Cancel(context.Context, string) error { return nil }
func (controllerWithoutLogEvents) Retry(context.Context, string) (store.Attempt, error) {
	return store.Attempt{}, nil
}
func (controllerWithoutLogEvents) Reconcile(context.Context) error { return nil }
func (controllerWithoutLogEvents) HandleWorkerEvent(context.Context, string, string, protocol.Event) error {
	return nil
}

func TestNewRuntimeValidatesDependenciesAndAppliesDefaults(t *testing.T) {
	db, _, _, _ := backendStore(t)
	client := fake.NewSimpleClientset()
	controller := newFakeController(db)
	backend := NewBackend(db, controller)

	for name, create := range map[string]func() (*Runtime, error){
		"nil client": func() (*Runtime, error) {
			return NewRuntime(nil, db, controller, backend, Options{Namespace: "workers"})
		},
		"nil store": func() (*Runtime, error) {
			return NewRuntime(client, nil, controller, backend, Options{Namespace: "workers"})
		},
		"nil controller": func() (*Runtime, error) { return NewRuntime(client, db, nil, backend, Options{Namespace: "workers"}) },
		"nil backend": func() (*Runtime, error) {
			return NewRuntime(client, db, controller, nil, Options{Namespace: "workers"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := create(); err == nil {
				t.Fatal("NewRuntime accepted a nil dependency")
			}
		})
	}
	if _, err := NewRuntime(client, db, controllerWithoutLogEvents{}, backend, Options{Namespace: "workers"}); err == nil {
		t.Fatal("NewRuntime accepted a controller that cannot record exhausted logs")
	}
	if _, err := NewRuntime(client, db, controller, backend, Options{}); err == nil {
		t.Fatal("NewRuntime accepted an empty namespace")
	}
	if _, err := NewRuntime(client, db, controller, backend, Options{Namespace: "workers", MaxLogBytes: 1}); err == nil {
		t.Fatal("NewRuntime accepted a byte quota smaller than the truncation marker")
	}
	if _, err := NewRuntime(client, db, controller, backend, Options{Namespace: "workers", SecretRetention: -time.Second}); err == nil {
		t.Fatal("NewRuntime accepted negative Secret retention")
	}

	r, err := NewRuntime(client, db, controller, backend, Options{Namespace: "workers"})
	if err != nil {
		t.Fatalf("NewRuntime defaults: %v", err)
	}
	if r.options.LogChunkBytes != 64<<10 || r.options.MaxLogBytes != 64<<20 || r.options.PodLogs == nil || r.options.Logger == nil || r.options.Clock == nil || r.options.RecoveryInterval != 30*time.Second {
		t.Fatalf("runtime defaults = %#v", r.options)
	}
	var nilContext context.Context
	if err := r.Run(nilContext); err == nil {
		t.Fatal("Run accepted a nil context")
	}
}

func TestRuntimeObservesDetailedJobAndPodState(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	r, err := NewRuntime(fake.NewSimpleClientset(), db, controller, NewBackend(db, controller), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	started := metav1.NewTime(time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC))
	completed := metav1.NewTime(started.Add(time.Minute))
	job := runtimeJob("workers", "job-1", taskRecord.ID, attempt.ID, "job-uid")
	job.Status.StartTime = &started
	job.Status.CompletionTime = &completed
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobComplete, Status: corev1.ConditionFalse},
		{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "failed", LastTransitionTime: completed},
	}
	job.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "task-secret", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "custom-secret"}}}}
	r.observeJob(context.Background(), job)

	pod := managedPod("workers", "pod-1", job.Name, taskRecord.ID, "1", attempt.ID, job.UID)
	pod.Status.Phase = corev1.PodPhase("Unexpected")
	pod.Status.Reason, pod.Status.Message = "Evicted", "node pressure"
	pod.Status.StartTime = &started
	pod.Spec.NodeName = "node-1"
	pod.Spec.Containers = []corev1.Container{{Name: "sidecar", Image: "sidecar:v1"}, {Name: "worker", Image: "worker:v2"}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{
		{Name: "waiting", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{}}},
		{Name: "running", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		{Name: "worker", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{FinishedAt: completed}}},
	}
	r.observePod(context.Background(), pod)

	storedJob, storedPod, err := db.AttemptKubernetes(context.Background(), attempt.ID)
	if err != nil {
		t.Fatalf("AttemptKubernetes: %v", err)
	}
	if storedJob.State != "failed" || storedJob.Reason != "BackoffLimitExceeded" || storedJob.SecretName != "" || storedJob.CompletedAt == nil {
		t.Fatalf("stored Job = %#v", storedJob)
	}
	if storedPod.State != "unknown" || storedPod.Node != "node-1" || storedPod.Image != "worker:v2" || storedPod.CompletedAt == nil || !strings.Contains(storedPod.ContainerStates, `"worker":"terminated"`) {
		t.Fatalf("stored Pod = %#v", storedPod)
	}
	if got := taskSecretName(job); got != "custom-secret" {
		t.Fatalf("taskSecretName = %q, want custom-secret", got)
	}
}

func TestCollectPodLogsRejectsInvalidOwnershipBoundaries(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	job := runtimeJob("workers", "job-1", taskRecord.ID, attempt.ID, "job-uid")
	badJob := runtimeJob("workers", "bad-job", taskRecord.ID, attempt.ID, "bad-job-uid")
	badJob.Labels["simpleswe.dev/task-id"] = "another-task"
	r, err := NewRuntime(fake.NewSimpleClientset(job, badJob), db, controller, NewBackend(db, controller), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	valid := managedPod("workers", "pod", job.Name, taskRecord.ID, "1", attempt.ID, job.UID)
	withoutOwner := valid.DeepCopy()
	withoutOwner.OwnerReferences = nil
	missingJob := managedPod("workers", "missing", "missing-job", taskRecord.ID, "1", attempt.ID, "missing-job-uid")
	badLabels := managedPod("workers", "bad-labels", badJob.Name, taskRecord.ID, "1", attempt.ID, badJob.UID)

	tests := []struct {
		name string
		pod  *corev1.Pod
	}{
		{name: "nil", pod: nil},
		{name: "empty UID", pod: &corev1.Pod{}},
		{name: "missing attempt labels", pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "uid"}}},
		{name: "mismatched attempt ID", pod: managedPod("workers", "wrong-attempt", job.Name, taskRecord.ID, "1", "wrong", job.UID)},
		{name: "no controlling Job", pod: withoutOwner},
		{name: "missing controlling Job", pod: missingJob},
		{name: "mismatched ownership labels", pod: badLabels},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := r.CollectPodLogs(context.Background(), test.pod); err == nil {
				t.Fatal("CollectPodLogs accepted invalid ownership")
			}
		})
	}
}

func TestLogsTerminalUsesPodJobAndDurableAttemptFallbacks(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	job := runtimeJob("workers", "job-1", taskRecord.ID, attempt.ID, "job-uid")
	job.Status.Conditions = nil
	job.Status.Active = 1
	pod := managedPod("workers", "pod-1", job.Name, taskRecord.ID, "1", attempt.ID, job.UID)
	pod.Status.Phase = corev1.PodRunning
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "worker", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}}}
	client := fake.NewSimpleClientset(job, pod)
	r, err := NewRuntime(client, db, controller, NewBackend(db, controller), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}

	terminal, err := r.logsTerminal(context.Background(), "workers", pod.Name, "worker", job.Name, taskRecord.ID, attempt.ID)
	if err != nil || !terminal {
		t.Fatalf("terminated container = %v, %v; want true", terminal, err)
	}
	pod.Status.ContainerStatuses = nil
	if _, err := client.CoreV1().Pods("workers").Update(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update Pod: %v", err)
	}
	terminal, err = r.logsTerminal(context.Background(), "workers", pod.Name, "worker", job.Name, taskRecord.ID, attempt.ID)
	if err != nil || terminal {
		t.Fatalf("active Pod and Job = %v, %v; want false", terminal, err)
	}
	if err := client.CoreV1().Pods("workers").Delete(context.Background(), pod.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Pod: %v", err)
	}
	job.Status.Active = 0
	job.Status.Failed = 1
	if _, err := client.BatchV1().Jobs("workers").Update(context.Background(), job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update Job: %v", err)
	}
	terminal, err = r.logsTerminal(context.Background(), "workers", pod.Name, "worker", job.Name, taskRecord.ID, attempt.ID)
	if err != nil || !terminal {
		t.Fatalf("failed Job = %v, %v; want true", terminal, err)
	}
	if err := client.BatchV1().Jobs("workers").Delete(context.Background(), job.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete Job: %v", err)
	}
	terminal, err = r.logsTerminal(context.Background(), "workers", pod.Name, "worker", job.Name, taskRecord.ID, attempt.ID)
	if err != nil || terminal {
		t.Fatalf("queued durable attempt = %v, %v; want false", terminal, err)
	}
	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.FAILED, store.TransitionParams{Reason: "failed", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	terminal, err = r.logsTerminal(context.Background(), "workers", pod.Name, "worker", job.Name, taskRecord.ID, attempt.ID)
	if err != nil || !terminal {
		t.Fatalf("failed durable attempt = %v, %v; want true", terminal, err)
	}

	apiErr := errors.New("Pod API unavailable")
	errorClient := fake.NewSimpleClientset()
	errorClient.PrependReactor("get", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) { return true, nil, apiErr })
	errorRuntime, _ := NewRuntime(errorClient, db, controller, NewBackend(db, controller), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if _, err := errorRuntime.logsTerminal(context.Background(), "workers", "pod", "worker", "job", taskRecord.ID, attempt.ID); !errors.Is(err, apiErr) {
		t.Fatalf("Pod API error = %v, want %v", err, apiErr)
	}
}

func TestWatchLoopsRetryListAndWatchErrors(t *testing.T) {
	db, _, _, _ := backendStore(t)
	controller := newFakeController(db)
	client := fake.NewSimpleClientset()
	jobStream, podStream := watch.NewFake(), watch.NewFake()
	var mu sync.Mutex
	jobLists, jobWatches, podLists, podWatches := 0, 0, 0, 0
	client.PrependReactor("list", "jobs", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		jobLists++
		if jobLists == 1 {
			return true, nil, errors.New("temporary Job list failure")
		}
		return true, &batchv1.JobList{}, nil
	})
	client.PrependWatchReactor("jobs", func(k8stesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		jobWatches++
		if jobWatches == 1 {
			return true, nil, errors.New("temporary Job watch failure")
		}
		return true, jobStream, nil
	})
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		podLists++
		if podLists == 1 {
			return true, nil, errors.New("temporary Pod list failure")
		}
		return true, &corev1.PodList{}, nil
	})
	client.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		podWatches++
		if podWatches == 1 {
			return true, nil, errors.New("temporary Pod watch failure")
		}
		return true, podStream, nil
	})
	r, err := NewRuntime(client, db, controller, NewBackend(db, controller), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	jobsDone, podsDone := make(chan error, 1), make(chan error, 1)
	go func() { jobsDone <- r.watchJobs(ctx) }()
	go func() { podsDone <- r.watchPods(ctx) }()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return jobLists >= 3 && jobWatches >= 2 && podLists >= 3 && podWatches >= 2
	}, "watch retries after list and watch errors")
	cancel()
	if err := <-jobsDone; err != nil {
		t.Errorf("watchJobs shutdown: %v", err)
	}
	if err := <-podsDone; err != nil {
		t.Errorf("watchPods shutdown: %v", err)
	}
}

func TestWatchConsumersFilterEventsAndHandleDeletion(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	r, err := NewRuntime(fake.NewSimpleClientset(), db, controller, NewBackend(db, controller), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	ctx := context.Background()
	jobs := watch.NewFake()
	jobDone := make(chan struct{})
	go func() { r.consumeJobWatch(ctx, jobs); close(jobDone) }()
	jobs.Action(watch.Bookmark, &batchv1.Job{})
	jobs.Add(&corev1.Pod{})
	jobs.Add(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/managed-by": "other"}}})
	jobs.Delete(runtimeJob("workers", "job-1", taskRecord.ID, attempt.ID, "job-uid"))
	waitSignal(t, controller.reconcileC, "managed deleted Job reconciliation")
	jobs.Error(&metav1.Status{Status: metav1.StatusFailure})
	select {
	case <-jobDone:
	case <-time.After(time.Second):
		t.Fatal("Job consumer did not stop on watch error")
	}

	pods := watch.NewFake()
	podDone := make(chan struct{})
	go func() { r.consumePodWatch(ctx, pods); close(podDone) }()
	pods.Action(watch.Bookmark, &corev1.Pod{})
	pods.Add(&batchv1.Job{})
	pods.Add(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app.kubernetes.io/managed-by": "other"}}})
	pods.Error(&metav1.Status{Status: metav1.StatusFailure})
	select {
	case <-podDone:
	case <-time.After(time.Second):
		t.Fatal("Pod consumer did not stop on watch error")
	}
}

func TestLogProtocolValidationAndBoundedReads(t *testing.T) {
	validCommit := strings.Repeat("a", 40)
	tests := []struct {
		name    string
		event   protocol.Event
		wantErr bool
	}{
		{name: "empty task", event: protocol.Event{Type: protocol.EventAgentStarted}, wantErr: true},
		{name: "agent started", event: protocol.Event{Type: protocol.EventAgentStarted, TaskID: "task"}},
		{name: "worker failed", event: protocol.Event{Type: "worker_failed", TaskID: "task"}},
		{name: "validation command required", event: protocol.Event{Type: protocol.EventValidationResult, TaskID: "task"}, wantErr: true},
		{name: "validation command", event: protocol.Event{Type: protocol.EventValidationFailed, TaskID: "task", Command: []string{"go", "test"}}},
		{name: "branch", event: protocol.Event{Type: protocol.EventBranchPushed, TaskID: "task", Branch: "branch", CommitSHA: validCommit}},
		{name: "bad branch commit", event: protocol.Event{Type: protocol.EventBranchPushed, TaskID: "task", Branch: "branch", CommitSHA: "short"}, wantErr: true},
		{name: "unsupported", event: protocol.Event{Type: "mystery", TaskID: "task"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateDurableWorkerEvent(test.event)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateDurableWorkerEvent error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}

	line, oversized, err := readBoundedLine(bufio.NewReaderSize(strings.NewReader("123456789\n"), 4), 5)
	if err != nil || !oversized || string(line) != "12345" {
		t.Fatalf("readBoundedLine = %q, %v, %v; want bounded oversized line", line, oversized, err)
	}
	line, oversized, err = readBoundedLine(bufio.NewReader(strings.NewReader("last")), 10)
	if !errors.Is(err, io.EOF) || oversized || string(line) != "last" {
		t.Fatalf("readBoundedLine EOF = %q, %v, %v", line, oversized, err)
	}
}

func TestDrainWorkerEventsPreservesFailedDispatchForRetry(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	event, err := protocol.EncodeEvent(protocol.Event{Type: protocol.EventAgentStarted, TaskID: taskRecord.ID})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	_, err = db.AppendPodLog(context.Background(), store.AppendPodLogParams{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, PodUID: "pod-uid", JobName: "job", PodName: "pod",
		Content: []byte(event), WorkerEventID: "event-1", WorkerEvent: event,
	}, 1<<20, 64)
	if err != nil {
		t.Fatalf("append durable event: %v", err)
	}
	failing := &workerEventErrorController{fakeController: newFakeController(db), err: errors.New("controller unavailable")}
	r, err := NewRuntime(fake.NewSimpleClientset(), db, failing, NewBackend(db, failing), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := r.drainWorkerEvents(context.Background(), "pod-uid"); err == nil || !strings.Contains(err.Error(), "controller unavailable") {
		t.Fatalf("failed dispatch error = %v", err)
	}
	pending, err := db.ListPendingWorkerEvents(context.Background(), "pod-uid")
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending events after failure = %#v, %v", pending, err)
	}

	success := newFakeController(db)
	r, _ = NewRuntime(fake.NewSimpleClientset(), db, success, NewBackend(db, success), Options{Namespace: "workers", PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err := r.drainWorkerEvents(context.Background(), "pod-uid"); err != nil {
		t.Fatalf("retry durable dispatch: %v", err)
	}
	pending, err = db.ListPendingWorkerEvents(context.Background(), "pod-uid")
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending events after retry = %#v, %v", pending, err)
	}
}

type workerEventErrorController struct {
	*fakeController
	err error
}

func (c *workerEventErrorController) HandleWorkerEvent(context.Context, string, string, protocol.Event) error {
	return c.err
}

func TestCleanupOwnershipAndCancellationGuards(t *testing.T) {
	cleanup := store.SecretCleanup{TaskID: "task", AttemptID: "attempt", AttemptNumber: 2, Namespace: "workers", SecretName: "secret"}
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": "task",
		"simpleswe.dev/attempt": "2", "simpleswe.dev/attempt-id": "attempt",
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "secret", Namespace: "workers", Labels: labels}}
	if !validTaskSecret(secret, cleanup) {
		t.Fatal("valid task Secret was rejected")
	}
	for name, candidate := range map[string]*corev1.Secret{
		"nil":         nil,
		"wrong name":  {ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "workers", Labels: labels}},
		"wrong label": {ObjectMeta: metav1.ObjectMeta{Name: "secret", Namespace: "workers", Labels: map[string]string{}}},
	} {
		t.Run(name, func(t *testing.T) {
			if validTaskSecret(candidate, cleanup) {
				t.Fatal("invalid task Secret was accepted")
			}
		})
	}

	r := &Runtime{cleanups: make(map[string]*cleanupRun)}
	ctx, cancel := context.WithCancel(context.Background())
	r.cleanups["attempt"] = &cleanupRun{jobUID: "job-uid", cancel: cancel}
	r.cancelSecretCleanup("attempt", "job-uid")
	select {
	case <-ctx.Done():
		t.Fatal("matching active cleanup was cancelled")
	default:
	}
	r.cancelSecretCleanup("attempt", "other-job")
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("stale cleanup was not cancelled")
	}
	r.cancelSecretCleanup("missing", "")
}
