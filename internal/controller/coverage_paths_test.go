package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestNewRejectsInvalidDependenciesAndConfiguration(t *testing.T) {
	fixture := newFixture(t)
	valid := fixture.config
	tests := []struct {
		name string
		call func() error
		want string
	}{
		{"nil store", func() error {
			_, err := New(nil, fixture.kube, valid, fixture.pullRequests)
			return err
		}, "Store is nil"},
		{"nil Kubernetes", func() error {
			_, err := New(fixture.store, nil, valid, fixture.pullRequests)
			return err
		}, "Kubernetes client is nil"},
		{"nil pull requests", func() error { _, err := New(fixture.store, fixture.kube, valid, nil); return err }, "PullRequestInspector is nil"},
		{"empty namespace", func() error {
			cfg := valid
			cfg.Controller.Namespace = " "
			_, err := New(fixture.store, fixture.kube, cfg, fixture.pullRequests)
			return err
		}, "namespace is empty"},
		{"invalid deadline", func() error {
			cfg := valid
			cfg.Controller.Deadline = 0
			_, err := New(fixture.store, fixture.kube, cfg, fixture.pullRequests)
			return err
		}, "deadline must be positive"},
		{"duplicate repository", func() error {
			cfg := valid
			cfg.Repositories = append(cfg.Repositories, cfg.Repositories[0])
			_, err := New(fixture.store, fixture.kube, cfg, fixture.pullRequests)
			return err
		}, "duplicate repository"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.call(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("New() error = %v; want %q", err, test.want)
			}
		})
	}
}

func TestJobConfigMapsResourcesSchedulingMountsAndEnvironment(t *testing.T) {
	fixture := newFixture(t)
	control := fixture.controller.(*Controller)
	seconds := int64(30)
	size := config.Quantity("1Gi")
	repository := fixture.config.Repositories[0]
	repository.Credentials.SecretName = "repository-creds"
	repository.Bitbucket.CredentialsSecret = "bitbucket-creds"
	repository.Worker = config.WorkerConfig{
		Image:    workerImage,
		Deadline: config.Duration(3 * time.Minute),
		Resources: config.ResourceRequirements{
			Requests: config.ResourceList{"cpu": "250m"},
			Limits:   config.ResourceList{"memory": "2Gi"},
		},
		Tolerations: []config.Toleration{{Key: "dedicated", Operator: "Equal", Value: "swe", Effect: "NoSchedule", TolerationSeconds: &seconds}},
		Affinity:    map[string]any{"nodeAffinity": map[string]any{}},
		Mounts: []config.Mount{
			{Name: "secret", MountPath: "/secret", Secret: &config.SecretMount{SecretName: "extra-secret"}},
			{Name: "config", MountPath: "/config", ConfigMap: &config.ConfigMapMount{Name: "extra-config"}},
			{Name: "scratch", MountPath: "/scratch", EmptyDir: &config.EmptyDir{Medium: "Memory", SizeLimit: &size}},
		},
		MountedSecrets:    []config.NamedMount{{Name: "legacy-secret", MountPath: "/legacy-secret"}},
		MountedConfigMaps: []config.NamedMount{{Name: "legacy-config", MountPath: "/legacy-config"}},
		Env: []config.Env{
			{Name: "PLAIN", Value: "value"},
			{Name: "FROM_SECRET", Secret: &config.KeyRef{Name: "env-secret", Key: "token"}},
			{Name: "FROM_CONFIG", ConfigMap: &config.KeyRef{Name: "env-config", Key: "setting"}},
		},
	}
	repository.Git.SSHSecret = "git-ssh"
	repository.OpenCode.ConfigSecret = "opencode-config"

	got, err := control.jobConfig(repository)
	if err != nil {
		t.Fatalf("jobConfig(): %v", err)
	}
	if got.Deadline != 3*time.Minute || got.Resources.Requests.Cpu().Cmp(resource.MustParse("250m")) != 0 || got.Resources.Limits.Memory().Cmp(resource.MustParse("2Gi")) != 0 {
		t.Fatalf("resource configuration = %#v", got)
	}
	if len(got.Tolerations) != 1 || got.Affinity == nil || len(got.CredentialSecrets) != 6 || len(got.ConfigMaps) != 2 || len(got.EmptyDirs) != 1 {
		t.Fatalf("scheduling and mounts = %#v", got)
	}
	wantEnv := map[string]bool{"PLAIN": false, "FROM_SECRET": false, "FROM_CONFIG": false, "GIT_SSH_COMMAND": false, "GIT_TERMINAL_PROMPT": false, "SIMPLESWE_SECRET_PATHS": false}
	for _, value := range got.Env {
		if _, ok := wantEnv[value.Name]; ok {
			wantEnv[value.Name] = true
		}
	}
	for name, found := range wantEnv {
		if !found {
			t.Errorf("environment variable %q was not mapped", name)
		}
	}
}

func TestConfigurationHelpersRejectInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		input config.ResourceRequirements
		want  string
	}{
		{"request", config.ResourceRequirements{Requests: config.ResourceList{"cpu": "not-a-quantity"}}, "parse resource request"},
		{"limit", config.ResourceRequirements{Limits: config.ResourceList{"memory": "not-a-quantity"}}, "parse resource limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := resourceRequirements(test.input); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("resourceRequirements() error = %v; want %q", err, test.want)
			}
		})
	}
	if _, err := affinityFromMap(map[string]any{"nodeAffinity": "invalid"}); err == nil || !strings.Contains(err.Error(), "decode worker affinity") {
		t.Fatalf("affinityFromMap() error = %v", err)
	}
	if _, err := affinityFromMap(map[string]any{"invalid": make(chan int)}); err == nil || !strings.Contains(err.Error(), "marshal worker affinity") {
		t.Fatalf("affinityFromMap(unmarshalable) error = %v", err)
	}
}

func TestReconcileCreatesResourcesFromDurableReceivedIntent(t *testing.T) {
	fixture := newFixture(t)
	created, err := fixture.store.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: repositoryURL, Prompt: "recover received"})
	if err != nil {
		t.Fatalf("CreateTask store intent: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile(): %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.JOB_PENDING {
		t.Fatalf("recovered state = %q; want JOB_PENDING", got.State)
	}
	var transitions []store.TransitionEvent
	for _, event := range listEvents(t, fixture, created.ID) {
		if event.FromState != event.ToState {
			transitions = append(transitions, event)
		}
	}
	assertEventSlicePath(t, transitions, []task.State{task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING})
}

func TestReconcileTerminalJobWithoutPodExhaustsLogsAndFails(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "terminal without pod", "terminal-without-pod")
	jobName := jobs.Name(created.ID, 1)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	job.Status.Failed = 1
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("fail Job: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile(): %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.FAILED {
		t.Fatalf("terminal Job state = %q; want FAILED", got.State)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil || !attempt.LogsExhausted {
		t.Fatalf("terminal attempt = %#v, %v; want logs exhausted", attempt, err)
	}
}

func TestWorkerEventAuthorizationAndStateErrors(t *testing.T) {
	fixture := newFixture(t)
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, "job", "pod", protocol.Event{}); err == nil || !strings.Contains(err.Error(), "task ID is empty") {
		t.Fatalf("empty task event error = %v", err)
	}
	created := createRunningTask(t, fixture, "worker boundaries", "worker-boundaries")
	control := fixture.controller.(*Controller)
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, "missing-job", podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID}); err == nil || !strings.Contains(err.Error(), "get worker Job") {
		t.Fatalf("missing Job error = %v", err)
	}
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	uid := job.UID
	job.UID = ""
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Update(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("clear Job UID: %v", err)
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID}); err == nil || !strings.Contains(err.Error(), "has no UID") {
		t.Fatalf("empty Job UID error = %v", err)
	}
	job.UID = uid
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Update(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("restore Job UID: %v", err)
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, "missing-pod", protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID}); err == nil || !strings.Contains(err.Error(), "get worker Pod") {
		t.Fatalf("missing Pod error = %v", err)
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID}); err == nil || !strings.Contains(err.Error(), "validation_succeeded") {
		t.Fatalf("out-of-order validation error = %v", err)
	}
	agentStarted := protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID}
	queueDurableWorkerEvent(t, fixture, "event-1", "pod-uid", jobName, podName, agentStarted)
	if err := control.HandleWorkerEventOnce(fixture.ctx, "event-1", jobName, podName, agentStarted); err != nil {
		t.Fatalf("HandleWorkerEventOnce(): %v", err)
	}
	validationStarted := protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test"}}
	queueDurableWorkerEvent(t, fixture, "event-2", "pod-uid", jobName, podName, validationStarted)
	if err := control.HandleWorkerEventOnce(fixture.ctx, "event-2", jobName, podName, validationStarted); err != nil {
		t.Fatalf("validation start: %v", err)
	}
	if err := control.HandleWorkerEventOnce(fixture.ctx, "event-2", jobName, podName, validationStarted); err != nil {
		t.Fatalf("validation start replay: %v", err)
	}
}

func TestBranchPushedValidationErrors(t *testing.T) {
	for _, test := range []struct {
		name   string
		branch string
		want   string
	}{
		{"empty", "", "does not match expected branch"},
		{"wrong attempt", "simpleswe/wrong-a1", "does not match expected branch"},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t)
			created := createRunningTask(t, fixture, "branch validation", "branch-validation-"+test.name)
			jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
			err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: test.branch, CommitSHA: fullCommitSHA})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("pull_request_ready error = %v; want %q", err, test.want)
			}
		})
	}
}

func TestPermanentPullRequestFailureIsDurablyTerminal(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "permanent provider failure", "permanent-provider")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventAgentStarted, TaskID: created.ID})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventPullRequestPublished, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationStarted, TaskID: created.ID, Command: []string{"go", "test"}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationResult, TaskID: created.ID, Command: []string{"go", "test"}})
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: protocol.EventValidationSucceeded, TaskID: created.ID})
	fixture.pullRequests.getErr = &bitbucket.HTTPError{StatusCode: 400, Status: "400 Bad Request", Message: "invalid branch"}
	err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: protocol.EventPullRequestReady, TaskID: created.ID, PullRequestNumber: 42, Branch: "simpleswe/" + created.ID + "-a1", CommitSHA: fullCommitSHA})
	if err == nil || !strings.Contains(err.Error(), "invalid branch") {
		t.Fatalf("pull_request_ready error = %v; want provider failure", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.FAILED {
		t.Fatalf("permanent provider state = %q; want FAILED", got.State)
	}
	if candidate, getErr := fixture.store.GetPullRequest(fixture.ctx, created.CurrentAttemptID); getErr != nil || candidate.State != "reported" || candidate.URL != "" {
		t.Fatalf("permanent mismatch changed durable candidate: %#v, %v", candidate, getErr)
	}
}

func TestWorkerLogsExhaustedRejectsUnknownAndMalformedJobs(t *testing.T) {
	fixture := newFixture(t)
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, "missing", "pod"); err == nil || !strings.Contains(err.Error(), "get exhausted worker Job") {
		t.Fatalf("missing Job error = %v", err)
	}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "malformed", Namespace: workerNamespace}}
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Create(context.Background(), job, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create malformed Job: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, job.Name, "pod"); err == nil || !strings.Contains(err.Error(), "missing task attempt labels") {
		t.Fatalf("malformed Job error = %v", err)
	}
}

func TestWorkerLogsExhaustedRejectsMismatchedResources(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "exhaustion provenance", "exhaustion-provenance")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	job.Labels["simpleswe.dev/attempt-id"] = "wrong"
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Update(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update Job labels: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("wrong Job labels error = %v; want conflict", err)
	}
	job.Labels["simpleswe.dev/attempt-id"] = created.CurrentAttemptID
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Update(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("restore Job labels: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, "missing-pod"); err == nil || !strings.Contains(err.Error(), "get exhausted worker Pod") {
		t.Fatalf("missing Pod error = %v", err)
	}
	pod, err := fixture.kube.CoreV1().Pods(workerNamespace).Get(fixture.ctx, podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Pod: %v", err)
	}
	pod.Labels["simpleswe.dev/attempt-id"] = "wrong"
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Update(fixture.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update Pod labels: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("wrong Pod labels error = %v; want conflict", err)
	}
	pod.Labels["simpleswe.dev/attempt-id"] = created.CurrentAttemptID
	pod.OwnerReferences = nil
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Update(fixture.ctx, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("remove Pod ownership: %v", err)
	}
	if err := fixture.controller.WorkerLogsExhausted(fixture.ctx, jobName, podName); err == nil || !strings.Contains(err.Error(), "is not owned") {
		t.Fatalf("unowned Pod error = %v", err)
	}
}

func TestControllerRejectsUnknownAndTerminalLifecycleCommands(t *testing.T) {
	fixture := newFixture(t)
	control := fixture.controller.(*Controller)
	if _, err := control.CreateTask(fixture.ctx, store.CreateTaskParams{Repository: "unknown", Prompt: "reject"}); err == nil || !strings.Contains(err.Error(), "unknown configured repository") {
		t.Fatalf("unknown repository error = %v", err)
	}
	if err := control.Cancel(fixture.ctx, "missing-task"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Cancel(missing) = %v; want not found", err)
	}
	created := createTask(t, fixture, "terminal cancellation", "terminal-cancellation")
	transition(t, fixture, created.ID, task.JOB_PENDING, task.FAILED, "failed", "kubernetes")
	if err := control.Cancel(fixture.ctx, created.ID); err == nil || !strings.Contains(err.Error(), "cannot be cancelled") {
		t.Fatalf("Cancel(terminal) = %v", err)
	}
	if _, err := control.Retry(fixture.ctx, "missing-task"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Retry(missing) = %v; want not found", err)
	}
}

func TestResourceIntentAndRecoveryGuards(t *testing.T) {
	fixture := newFixture(t)
	control := fixture.controller.(*Controller)
	created := createTask(t, fixture, "resource guards", "resource-guards")
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	if _, _, err := control.currentResourceIntent(fixture.ctx, created.ID, "stale-attempt"); !errors.Is(err, errResourceCreationBlocked) {
		t.Fatalf("stale resource intent = %v; want blocked", err)
	}
	if err := control.completeAttemptResources(fixture.ctx, created.ID, attempt.ID, "already complete"); err != nil {
		t.Fatalf("completed resource replay: %v", err)
	}
	if err := control.recoverTo(fixture.ctx, getTask(t, fixture, created.ID), task.JOB_PENDING, "already at target"); err != nil {
		t.Fatalf("recoverTo(already at target): %v", err)
	}
}

func TestReconcileCancellationRejectsForeignJob(t *testing.T) {
	fixture := newFixture(t)
	created := createTask(t, fixture, "foreign cancellation job", "foreign-cancellation-job")
	if err := fixture.store.RequestCancellation(fixture.ctx, created.ID); err != nil {
		t.Fatalf("request cancellation: %v", err)
	}
	jobName := jobs.Name(created.ID, 1)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Job: %v", err)
	}
	job.Labels["simpleswe.dev/task-id"] = "another-task"
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).Update(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("change Job ownership: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("Reconcile() error = %v; want ownership conflict", err)
	}
	if got := getTask(t, fixture, created.ID); !got.CancellationRequested || got.State == task.CANCELLED {
		t.Fatalf("foreign Job cancellation changed outcome: %#v", got)
	}
}

func TestWorkerEventStateErrorsDoNotMutateTask(t *testing.T) {
	fixture := newFixture(t)
	created := createRunningTask(t, fixture, "worker event ordering", "worker-event-ordering")
	jobName, podName := jobs.Name(created.ID, 1), "worker-pod-a1"
	for _, eventType := range []string{protocol.EventValidationResult, protocol.EventValidationFailed} {
		err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: eventType, TaskID: created.ID})
		if err == nil || !strings.Contains(err.Error(), eventType) {
			t.Errorf("%s error = %v", eventType, err)
		}
	}
	handleEvent(t, fixture, jobName, podName, protocol.Event{Type: "worker_failed", TaskID: created.ID, Message: "failed"})
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, protocol.Event{Type: "worker_failed", TaskID: created.ID, Message: "replayed"}); err != nil {
		t.Fatalf("terminal worker_failed replay: %v", err)
	}
	if got := getTask(t, fixture, created.ID); got.State != task.FAILED {
		t.Fatalf("worker event state = %q; want FAILED", got.State)
	}
}

func TestManifestParsingErrors(t *testing.T) {
	if _, err := attemptManifest(store.Attempt{ID: "missing"}); err == nil || !strings.Contains(err.Error(), "no immutable task manifest") {
		t.Fatalf("missing manifest error = %v", err)
	}
	if _, err := attemptManifest(store.Attempt{ID: "malformed", ManifestJSON: []byte("{")}); err == nil || !strings.Contains(err.Error(), "decode immutable manifest") {
		t.Fatalf("malformed manifest error = %v", err)
	}
	if _, err := attemptManifest(store.Attempt{ID: "invalid", ManifestJSON: []byte(`{}`)}); err == nil || !strings.Contains(err.Error(), "validate immutable manifest") {
		t.Fatalf("invalid manifest error = %v", err)
	}
}

func TestReconciliationAndWorkerHelpersCoverEdgeCases(t *testing.T) {
	controllerRef := true
	notController := false
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{
		{Kind: "ReplicaSet", Name: "worker", Controller: &controllerRef},
		{Kind: "Job", Name: "worker", Controller: &notController},
	}}}
	if podOwnedByJob(pod, "worker", "") {
		t.Fatal("podOwnedByJob accepted non-controller ownership")
	}
	if got := stateIndex(task.FAILED); got != -1 {
		t.Fatalf("stateIndex(FAILED) = %d; want -1", got)
	}
	if got := failureMessage("worker", "job", "pod", nil, -1, errors.New(" ")); strings.Contains(got, "error=") {
		t.Fatalf("failureMessage() included blank error: %q", got)
	}
}
