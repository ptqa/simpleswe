package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/simpleswe/simpleswe/internal/config"
	parentcontroller "github.com/simpleswe/simpleswe/internal/controller"
	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

type cleanupNotifier struct{}

func (cleanupNotifier) PostPullRequest(context.Context, string, string) error { return nil }

type cleanupPullRequests struct{}

func (cleanupPullRequests) CreatePullRequest(context.Context, string, string, bitbucket.CreatePullRequestRequest) (bitbucket.PullRequest, error) {
	return bitbucket.PullRequest{}, errors.New("unexpected pull request creation")
}

func (cleanupPullRequests) FindPullRequest(context.Context, string, string, string, string) (bitbucket.PullRequest, bool, error) {
	return bitbucket.PullRequest{}, false, nil
}

func TestRunWatchesJobsAndPodsRelistsFromResourceVersionAndStopsWithContext(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	logs := &fakePodLogs{
		content: map[string][]string{"worker-a1": {fmt.Sprintf("@@simpleswe:{\"type\":\"agent_started\",\"task_id\":%q}\n", taskRecord.ID)}},
		opens:   make(chan string, 4),
	}
	client := fake.NewSimpleClientset()
	firstJobs, secondJobs := watch.NewFake(), watch.NewFake()
	firstPods, secondPods := watch.NewFake(), watch.NewFake()
	var mu sync.Mutex
	jobLists := 0
	jobWatches := 0
	podLists := 0
	podWatches := 0
	var jobWatchVersions []string
	var podWatchVersions []string

	client.PrependReactor("list", "jobs", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		jobLists++
		return true, &batchv1.JobList{ListMeta: metav1.ListMeta{ResourceVersion: fmt.Sprintf("%d0", jobLists)}}, nil
	})
	client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		defer mu.Unlock()
		podLists++
		return true, &corev1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: fmt.Sprintf("%d00", podLists)}}, nil
	})
	client.PrependWatchReactor("jobs", func(action k8stesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		jobWatches++
		jobWatchVersions = append(jobWatchVersions, action.(k8stesting.WatchAction).GetWatchRestrictions().ResourceVersion)
		if jobWatches == 1 {
			return true, firstJobs, nil
		}
		return true, secondJobs, nil
	})
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		mu.Lock()
		defer mu.Unlock()
		podWatches++
		podWatchVersions = append(podWatchVersions, action.(k8stesting.WatchAction).GetWatchRestrictions().ResourceVersion)
		if podWatches == 1 {
			return true, firstPods, nil
		}
		return true, secondPods, nil
	})

	runtime, err := NewRuntime(client, db, controller, backend, Options{
		Namespace:       namespace,
		LogChunkBytes:   64,
		SecretRetention: time.Hour,
		PodLogs:         logs,
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(ctx) }()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return jobWatches == 1 && podWatches == 1
	}, "initial Job and Pod watches")

	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      "task-job-a1",
		UID:       types.UID("watch-job-a1"),
		Labels: map[string]string{
			"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID,
			"simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID,
		},
	}}
	if _, err := client.BatchV1().Jobs(namespace).Create(context.Background(), job.DeepCopy(), metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed watched Job: %v", err)
	}
	firstJobs.Add(job)
	waitSignal(t, controller.reconcileC, "reconciliation after Job add")
	firstJobs.Modify(job.DeepCopy())
	waitSignal(t, controller.reconcileC, "reconciliation after Job update")

	pod := managedPod(namespace, "worker-a1", job.Name, taskRecord.ID, "1", attempt.ID, job.UID)
	firstPods.Add(pod)
	select {
	case opened := <-logs.opens:
		if opened != pod.Name {
			t.Fatalf("opened Pod = %q, want %q", opened, pod.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Pod discovery did not start log collection")
	}
	waitSignal(t, controller.eventC, "worker event from discovered Pod")

	firstJobs.Stop()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return jobLists >= 2 && jobWatches >= 2
	}, "Job relist and replacement watch")
	mu.Lock()
	versions := append([]string(nil), jobWatchVersions...)
	mu.Unlock()
	if len(versions) < 2 || versions[0] != "10" || versions[1] != "20" {
		t.Fatalf("Job watch resource versions = %v, want [10 20] from each preceding list", versions)
	}
	firstPods.Stop()
	waitFor(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return podLists >= 2 && podWatches >= 2
	}, "Pod relist and replacement watch")
	mu.Lock()
	versions = append([]string(nil), podWatchVersions...)
	mu.Unlock()
	if len(versions) < 2 || versions[0] != "100" || versions[1] != "200" {
		t.Fatalf("Pod watch resource versions = %v, want [100 200] from each preceding list", versions)
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() after context cancellation = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop its watch goroutines after context cancellation")
	}
}

func TestCompletedJobDeletesTaskSecretOnlyAfterRetention(t *testing.T) {
	const namespace = "simpleswe-workers"
	retention := 2 * time.Hour
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	clock := newManualClock(time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC))
	jobs := watch.NewFake()
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      "task-job-a1",
		UID:       types.UID("cleanup-job-uid"),
		Labels: map[string]string{
			"app.kubernetes.io/managed-by": "simpleswe",
			"simpleswe.dev/task-id":        taskRecord.ID,
			"simpleswe.dev/attempt":        "1",
			"simpleswe.dev/attempt-id":     attempt.ID,
		},
	}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{
		Namespace: namespace,
		Name:      job.Name + "-task",
		Labels:    job.Labels,
	}}
	client := fake.NewSimpleClientset(secret)
	watchReady := make(chan struct{}, 1)
	client.PrependWatchReactor("jobs", func(k8stesting.Action) (bool, watch.Interface, error) {
		select {
		case watchReady <- struct{}{}:
		default:
		}
		return true, jobs, nil
	})
	deleted := make(chan string, 1)
	client.PrependReactor("delete", "secrets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleted <- action.(k8stesting.DeleteAction).GetName()
		return false, nil, nil
	})

	runtime, err := NewRuntime(client, db, controller, backend, Options{
		Namespace:       namespace,
		LogChunkBytes:   64,
		SecretRetention: retention,
		PodLogs:         &fakePodLogs{content: map[string][]string{}},
		Clock:           clock,
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runDone := make(chan error, 1)
	go func() { runDone <- runtime.Run(ctx) }()
	waitSignal(t, watchReady, "Job watch")

	job.Status.Active = 1
	jobs.Add(job.DeepCopy())
	waitSignal(t, controller.reconcileC, "active Job reconciliation")
	clock.Step(24 * time.Hour)
	assertNoDelete(t, deleted, "active Job")

	job.Status.Active = 0
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.FAILED, store.TransitionParams{Reason: "terminal completion", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	jobs.Modify(job.DeepCopy())
	waitSignal(t, controller.reconcileC, "completed Job reconciliation")
	waitSignal(t, clock.added, "retention timer")
	clock.Step(retention - time.Second)
	assertNoDelete(t, deleted, "before retention elapsed")
	clock.Step(time.Second)
	select {
	case name := <-deleted:
		if name != secret.Name {
			t.Fatalf("deleted Secret = %q, want task Secret %q", name, secret.Name)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("completed Job task Secret was not deleted after retention")
	}

	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run() shutdown error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not stop")
	}
}

func TestFailedJobSecretDeleteRetriesAndDeletedJobCleanupSurvivesRestart(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID,
		"simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID,
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "failed-job", UID: "failed-job-uid", Labels: labels}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: job.Name + "-task", Labels: labels}}
	client := fake.NewSimpleClientset(secret)
	var mu sync.Mutex
	deletes := 0
	client.PrependReactor("delete", "secrets", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		mu.Lock()
		deletes++
		current := deletes
		mu.Unlock()
		if current == 1 {
			return true, nil, errors.New("temporary API outage")
		}
		return false, nil, nil
	})
	r, err := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{Namespace: namespace, SecretRetention: 0, PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.FAILED, store.TransitionParams{Reason: "Job failed", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	r.observeJob(context.Background(), job)
	r.scheduleSecretCleanup(context.Background(), job, false)
	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return deletes >= 2 }, "retry of transient Secret deletion")
	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("failed Job task Secret still exists: %v", err)
	}
	waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return len(r.cleanups) == 0 }, "cleanup bookkeeping release")

	deletedJob := job.DeepCopy()
	deletedJob.Name = "deleted-job"
	deletedJob.UID = "deleted-job-uid"
	deletedJob.Status = batchv1.JobStatus{}
	deletedSecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: deletedJob.Name + "-task", Labels: labels}}
	if _, err := client.CoreV1().Secrets(namespace).Create(context.Background(), deletedSecret, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create deleted Job Secret: %v", err)
	}
	clock := newManualClock(time.Now().UTC())
	r1, _ := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{Namespace: namespace, SecretRetention: time.Hour, PodLogs: &fakePodLogs{content: map[string][]string{}}, Clock: clock})
	r1.observeJob(context.Background(), deletedJob)
	ctx, cancel := context.WithCancel(context.Background())
	r1.scheduleSecretCleanup(ctx, deletedJob, true)
	waitSignal(t, clock.added, "durable deleted Job cleanup timer")
	cancel()
	r1.tasks.Wait()
	r2, _ := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{Namespace: namespace, SecretRetention: 0, PodLogs: &fakePodLogs{content: map[string][]string{}}})
	r2.recoverSecretCleanups(context.Background())
	waitFor(t, func() bool {
		_, err := client.CoreV1().Secrets(namespace).Get(context.Background(), deletedSecret.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, "restart cleanup of deleted Job Secret")
}

func TestDeletedActiveJobReplacementKeepsSecretUntilReplacementAttemptTerminates(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID,
		"simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID,
	}
	oldJob := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "recreated-job", UID: "old-job-uid", Labels: labels}, Status: batchv1.JobStatus{Active: 1}}
	replacement := oldJob.DeepCopy()
	replacement.UID = "replacement-job-uid"
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: oldJob.Name + "-task", Labels: labels}}
	client := fake.NewSimpleClientset(replacement, secret)
	deleted := make(chan string, 2)
	client.PrependReactor("delete", "secrets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deleted <- action.(k8stesting.DeleteAction).GetName()
		return false, nil, nil
	})
	clock := newManualClock(time.Now().UTC())
	r, err := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{
		Namespace: namespace, SecretRetention: time.Hour, PodLogs: &fakePodLogs{content: map[string][]string{}}, Clock: clock,
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	r.observeJob(context.Background(), oldJob)
	r.observeJob(context.Background(), replacement)
	r.scheduleSecretCleanup(context.Background(), oldJob, true)
	time.Sleep(20 * time.Millisecond)
	clock.Step(time.Hour)
	select {
	case name := <-deleted:
		t.Fatalf("Secret %q deleted for stale active Job deletion", name)
	case <-time.After(50 * time.Millisecond):
	}
	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("replacement Job Secret disappeared: %v", err)
	}
	cleanups, err := db.ListSecretCleanups(context.Background())
	if err != nil || len(cleanups) != 1 || cleanups[0].EligibleAt != nil || cleanups[0].JobUID != string(replacement.UID) {
		t.Fatalf("replacement cleanup eligibility = %#v, %v; want replacement UID and ineligible", cleanups, err)
	}

	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.FAILED, store.TransitionParams{Reason: "replacement completed", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("complete replacement attempt: %v", err)
	}
	replacement.Status.Active = 0
	replacement.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	if _, err := client.BatchV1().Jobs(namespace).Update(context.Background(), replacement, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("complete replacement Job: %v", err)
	}
	r.observeJob(context.Background(), replacement)
	r.scheduleSecretCleanup(context.Background(), replacement, false)
	waitSignal(t, clock.added, "replacement cleanup timer")
	clock.Step(time.Hour)
	waitFor(t, func() bool {
		_, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, "terminal replacement Secret cleanup")
}

func TestSecretCleanupNeverDeletesMismatchedCredentialSecret(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	labels := map[string]string{"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID, "simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "job", UID: "job-uid", Labels: labels}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}}}
	credentialLabels := map[string]string{"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID, "simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": "different-attempt"}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: job.Name + "-task", Labels: credentialLabels}}
	client := fake.NewSimpleClientset(secret)
	r, _ := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{Namespace: namespace, PodLogs: &fakePodLogs{content: map[string][]string{}}})
	r.observeJob(context.Background(), job)
	r.scheduleSecretCleanup(context.Background(), job, false)
	waitFor(t, func() bool { r.mu.Lock(); defer r.mu.Unlock(); return len(r.cleanups) == 0 }, "mismatched Secret cleanup refusal")
	if _, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("credential Secret was deleted: %v", err)
	}
}

func TestRestartCleanupDiscoversCancelledAttempt(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	labels := map[string]string{"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID, "simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "cancelled-job", UID: "cancelled-job-uid", Labels: labels}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: job.Name + "-task", Labels: labels}}
	client := fake.NewSimpleClientset(secret)
	r, _ := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{Namespace: namespace, PodLogs: &fakePodLogs{content: map[string][]string{}}})
	r.observeJob(context.Background(), job)
	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.CANCELLED, store.TransitionParams{Reason: "cancelled", Trigger: "controller"}); err != nil {
		t.Fatalf("cancel attempt: %v", err)
	}
	r.recoverSecretCleanups(context.Background())
	waitFor(t, func() bool {
		_, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, "cancelled attempt Secret cleanup")
}

func TestRestartCleanupRecoversSecretOnlyFailedAttempt(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	labels := map[string]string{"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID, "simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "secret-only-task", Labels: labels}}
	if err := db.RegisterSecretCleanup(context.Background(), store.SecretCleanup{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, AttemptNumber: 1, Namespace: namespace, JobName: "missing-job", SecretName: secret.Name,
	}); err != nil {
		t.Fatalf("register Secret-only cleanup: %v", err)
	}
	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.FAILED, store.TransitionParams{Reason: "Job creation failed", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("fail Secret-only attempt: %v", err)
	}
	client := fake.NewSimpleClientset(secret)
	r, err := NewRuntime(client, db, newFakeController(db), NewBackend(db, newFakeController(db)), Options{Namespace: namespace, SecretRetention: 0, PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("restart runtime: %v", err)
	}
	r.recoverSecretCleanups(context.Background())
	waitFor(t, func() bool {
		_, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, "restart cleanup of Secret-only failed attempt")
}

func TestSecretCleanupFinalCheckUsesDurableGenerationAndSecretUID(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, taskRecord, attempt, _ := backendStore(t)
	labels := map[string]string{"app.kubernetes.io/managed-by": "simpleswe", "simpleswe.dev/task-id": taskRecord.ID, "simpleswe.dev/attempt": "1", "simpleswe.dev/attempt-id": attempt.ID}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "terminal-job", UID: "terminal-job-uid", Labels: labels}, Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}}}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: job.Name + "-task", UID: "terminal-secret-uid", Labels: labels}}
	client := fake.NewSimpleClientset(secret)
	controller := newFakeController(db)
	r, err := NewRuntime(client, db, controller, NewBackend(db, controller), Options{Namespace: namespace, SecretRetention: 0, PodLogs: &fakePodLogs{content: map[string][]string{}}})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	if err := db.Transition(context.Background(), taskRecord.ID, task.QUEUED, task.FAILED, store.TransitionParams{Reason: "terminal", Trigger: "kubernetes"}); err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	controllerClient := fake.NewSimpleClientset()
	actualController, err := parentcontroller.New(db, controllerClient, config.Config{
		Controller: config.ControllerConfig{Namespace: namespace, Deadline: time.Minute},
		Worker:     config.WorkerConfig{Command: "opencode", BranchPrefix: "simpleswe/"},
		Repositories: config.RepositoryConfigs{{
			Name: "widget", CloneURL: taskRecord.Repository, DefaultBranch: "main", Worker: config.WorkerConfig{Image: "worker:test"},
		}},
	}, cleanupNotifier{}, cleanupPullRequests{})
	if err != nil {
		t.Fatalf("new actual controller: %v", err)
	}
	r.observeJob(context.Background(), job)
	enteredDelete := make(chan metav1.DeleteOptions, 1)
	releaseDelete := make(chan struct{})
	createdResource := make(chan string, 1)
	controllerClient.PrependReactor("create", "*", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		createdResource <- action.GetResource().Resource
		return false, nil, nil
	})
	client.PrependReactor("delete", "secrets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		enteredDelete <- action.(k8stesting.DeleteAction).GetDeleteOptions()
		<-releaseDelete
		return false, nil, nil
	})
	r.scheduleSecretCleanup(context.Background(), job, false)
	options := <-enteredDelete
	cleanup, err := db.GetSecretCleanup(context.Background(), attempt.ID)
	if err != nil || cleanup.Generation <= 0 || cleanup.SecretUID != string(secret.UID) {
		close(releaseDelete)
		t.Fatalf("durable cleanup identity = %#v, %v", cleanup, err)
	}
	if options.Preconditions == nil || options.Preconditions.UID == nil || *options.Preconditions.UID != secret.UID {
		close(releaseDelete)
		t.Fatalf("Secret delete preconditions = %#v, want UID %q", options.Preconditions, secret.UID)
	}
	if err := actualController.Reconcile(context.Background()); err != nil {
		close(releaseDelete)
		t.Fatalf("terminal reconcile while cleanup paused: %v", err)
	}
	select {
	case resource := <-createdResource:
		close(releaseDelete)
		t.Fatalf("terminal reconcile/create admitted while cleanup paused: %s", resource)
	default:
	}
	close(releaseDelete)
	waitFor(t, func() bool {
		_, err := client.CoreV1().Secrets(namespace).Get(context.Background(), secret.Name, metav1.GetOptions{})
		return apierrors.IsNotFound(err)
	}, "UID-preconditioned terminal Secret cleanup")
}

type transientRecoveryController struct {
	*fakeController
	mu       sync.Mutex
	attempts int
}

func (c *transientRecoveryController) Reconcile(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts++
	switch c.attempts {
	case 1:
		return errors.New("transient Job creation failure")
	case 2:
		return errors.New("transient pull-request failure")
	}
	return nil
}

func TestRunRetriesTransientJobPullRequestAndNotificationWithoutWatchActivity(t *testing.T) {
	const namespace = "simpleswe-workers"
	db, _, _, _ := backendStore(t)
	controller := &transientRecoveryController{fakeController: newFakeController(db)}
	client := fake.NewSimpleClientset()
	var notificationMu sync.Mutex
	notifications := 0
	r, err := NewRuntime(client, db, controller, NewBackend(db, controller), Options{
		Namespace: namespace, PodLogs: &fakePodLogs{content: map[string][]string{}}, RecoveryInterval: 10 * time.Millisecond,
		NotifyPendingPullRequests: func(context.Context) error {
			notificationMu.Lock()
			defer notificationMu.Unlock()
			notifications++
			if notifications == 1 {
				return errors.New("transient notification failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	waitFor(t, func() bool {
		controller.mu.Lock()
		reconciles := controller.attempts
		controller.mu.Unlock()
		notificationMu.Lock()
		notifies := notifications
		notificationMu.Unlock()
		return reconciles >= 3 && notifies >= 2
	}, "autonomous recovery retries without watch events")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("recovery loop did not stop with context")
	}
}

func waitFor(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", description)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertNoDelete(t *testing.T, deleted <-chan string, stage string) {
	t.Helper()
	select {
	case name := <-deleted:
		t.Fatalf("Secret %q deleted %s", name, stage)
	default:
	}
}
