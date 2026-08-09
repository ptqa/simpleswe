package controller

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/kubernetes/jobs"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

const (
	repositoryURL   = "https://bitbucket.example/acme/widget.git"
	workerNamespace = "simpleswe-workers"
	workerImage     = "registry.example/simpleswe/widget-worker:v7"
	pullRequestURL  = "https://bitbucket.org/acme/widget/pull-requests/42"
)

// Controller contract under test:
//
//	New(*store.Store, kubernetes.Interface, config.Config, PullRequestInspector) (*Controller, error)
//	(*Controller).CreateTask(context.Context, store.CreateTaskParams) (store.Task, error)
//	(*Controller).Cancel(context.Context, string) error
//	(*Controller).Retry(context.Context, string) (store.Attempt, error)
//	(*Controller).Reconcile(context.Context) error
//	(*Controller).HandleWorkerEvent(context.Context, jobName, podName string, protocol.Event) error
//
// PullRequestInspector stays narrow at its external boundary.
type controllerContract interface {
	CreateTask(context.Context, store.CreateTaskParams) (store.Task, error)
	Cancel(context.Context, string) error
	Retry(context.Context, string) (store.Attempt, error)
	RetryWithKey(context.Context, string, string) (store.Attempt, error)
	Reconcile(context.Context) error
	HandleWorkerEvent(context.Context, string, string, protocol.Event) error
	WorkerLogsExhausted(context.Context, string, string) error
}

type fixture struct {
	ctx          context.Context
	databasePath string
	store        *store.Store
	kube         *fake.Clientset
	config       config.Config
	pullRequests *fakePullRequestCreator
	controller   controllerContract
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	databasePath := filepath.Join(t.TempDir(), "controller.sqlite")
	db, err := store.Open(context.Background(), databasePath)
	if err != nil {
		t.Fatalf("open Store: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close Store: %v", err)
		}
	})

	kube := fake.NewSimpleClientset()
	cfg := config.Config{
		Controller: config.ControllerConfig{
			Namespace: workerNamespace,
			Deadline:  10 * time.Minute,
		},
		Worker:    config.WorkerConfig{Command: "opencode", BranchPrefix: "simpleswe/"},
		Bitbucket: config.BitbucketConfig{BaseURL: "https://api.bitbucket.org"},
		GitHub:    config.GitHubConfig{BaseURL: "https://api.github.com"},
		Repositories: config.RepositoryConfigs{{
			Name:          "widget",
			CloneURL:      repositoryURL,
			DefaultBranch: "main",
			Worker:        config.WorkerConfig{Image: workerImage},
			Bitbucket:     config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "widget", CredentialsSecret: "bitbucket-widget"},
		}},
	}
	pullRequests := new(fakePullRequestCreator)
	controller, err := New(db, kube, cfg, pullRequests)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return &fixture{
		ctx:          context.Background(),
		databasePath: databasePath,
		store:        db,
		kube:         kube,
		config:       cfg,
		pullRequests: pullRequests,
		controller:   controller,
	}
}

type fakePullRequestCreator struct {
	mu         sync.Mutex
	getResult  *forge.PullRequestState
	getErr     error
	getCalls   int
	getTargets []forge.Target
	getNumbers []int
	blocked    chan struct{}
	release    chan struct{}
}

func (f *fakePullRequestCreator) GetPullRequest(_ context.Context, target forge.Target, number int) (forge.PullRequestState, error) {
	f.mu.Lock()
	f.getCalls++
	f.getTargets = append(f.getTargets, target)
	f.getNumbers = append(f.getNumbers, number)
	if f.getErr != nil {
		err := f.getErr
		f.mu.Unlock()
		return forge.PullRequestState{}, err
	}
	if f.getResult != nil {
		result := *f.getResult
		blocked, release := f.blocked, f.release
		f.mu.Unlock()
		if blocked != nil {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-release
		}
		return result, nil
	}
	result := forge.PullRequestState{Number: number, State: "open", HTMLURL: pullRequestURL, Title: "Provider title", SourceOwner: target.Owner, SourceRepository: target.Repository, HeadSHA: fullCommitSHA}
	f.mu.Unlock()
	return result, nil
}

var _ PullRequestInspector = (*fakePullRequestCreator)(nil)

func createTask(t *testing.T, fixture *fixture, prompt, idempotencyKey string) store.Task {
	t.Helper()
	created, err := fixture.controller.CreateTask(fixture.ctx, store.CreateTaskParams{
		Repository:     repositoryURL,
		Prompt:         prompt,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	return created
}

func createRunningTask(t *testing.T, fixture *fixture, prompt, idempotencyKey string) store.Task {
	t.Helper()
	created := createTask(t, fixture, prompt, idempotencyKey)
	jobName := jobs.Name(created.ID, 1)
	job, err := fixture.kube.BatchV1().Jobs(workerNamespace).Get(fixture.ctx, jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get worker Job: %v", err)
	}
	job.UID = types.UID("job-uid-" + created.ID)
	job.Status.Active = 1
	if _, err := fixture.kube.BatchV1().Jobs(workerNamespace).UpdateStatus(fixture.ctx, job, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("mark worker Job running: %v", err)
	}
	if err := fixture.controller.Reconcile(fixture.ctx); err != nil {
		t.Fatalf("Reconcile() running Job: %v", err)
	}
	if got := getTask(t, fixture, created.ID).State; got != task.RUNNING {
		t.Fatalf("running Job state = %q; want %q", got, task.RUNNING)
	}
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	controller := true
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: "worker-pod-a1", Namespace: workerNamespace, Labels: copyLabels(job.Labels),
		OwnerReferences: []metav1.OwnerReference{{Kind: "Job", Name: job.Name, UID: job.UID, Controller: &controller}},
	}}
	pod.Labels["simpleswe.dev/attempt-id"] = attempt.ID
	if _, err := fixture.kube.CoreV1().Pods(workerNamespace).Create(fixture.ctx, pod, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create worker Pod: %v", err)
	}
	return created
}

func getTask(t *testing.T, fixture *fixture, taskID string) store.Task {
	t.Helper()
	got, err := fixture.store.GetTask(fixture.ctx, taskID)
	if err != nil {
		t.Fatalf("GetTask(%q): %v", taskID, err)
	}
	return got
}

func exhaustAttemptLogs(t *testing.T, fixture *fixture, taskID, attemptID string) {
	t.Helper()
	if err := fixture.store.MarkLogsExhausted(fixture.ctx, taskID, attemptID); err != nil {
		t.Fatalf("mark attempt logs exhausted: %v", err)
	}
}

func listEvents(t *testing.T, fixture *fixture, taskID string) []store.TransitionEvent {
	t.Helper()
	events, err := fixture.store.ListEvents(fixture.ctx, taskID)
	if err != nil {
		t.Fatalf("ListEvents(%q): %v", taskID, err)
	}
	return events
}

func transition(t *testing.T, fixture *fixture, taskID string, from, to task.State, reason, trigger string) {
	t.Helper()
	if err := fixture.store.Transition(fixture.ctx, taskID, from, to, store.TransitionParams{Reason: reason, Trigger: trigger}); err != nil {
		t.Fatalf("transition %q -> %q: %v", from, to, err)
	}
}

func handleEvent(t *testing.T, fixture *fixture, jobName, podName string, event protocol.Event) {
	t.Helper()
	if event.Type == protocol.EventPullRequestReady {
		attempt, err := fixture.store.CurrentAttempt(fixture.ctx, event.TaskID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.GetGitResult(fixture.ctx, attempt.ID); errors.Is(err, store.ErrNotFound) {
			published := event
			published.Type = protocol.EventPullRequestPublished
			if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, published); err != nil {
				t.Fatalf("HandleWorkerEvent(%q) error = %v", published.Type, err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if event.Type == protocol.EventPullRequestReady && fixture.pullRequests.getResult == nil {
		fixture.pullRequests.getResult = &forge.PullRequestState{
			Number: event.PullRequestNumber, State: "open", HTMLURL: pullRequestURL, Title: "Provider title",
			SourceOwner: "acme", SourceRepository: "widget", SourceBranch: event.Branch, DestinationBranch: "main", HeadSHA: event.CommitSHA,
		}
	}
	if err := fixture.controller.HandleWorkerEvent(fixture.ctx, jobName, podName, event); err != nil {
		t.Fatalf("HandleWorkerEvent(%q) error = %v", event.Type, err)
	}
}

func queueDurableWorkerEvent(t *testing.T, fixture *fixture, id, podUID, jobName, podName string, event protocol.Event) {
	t.Helper()
	attempt, err := fixture.store.CurrentAttempt(fixture.ctx, event.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	content, err := protocol.EncodeEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.AppendPodLog(fixture.ctx, store.AppendPodLogParams{
		TaskID: event.TaskID, AttemptID: attempt.ID, PodUID: podUID, JobName: jobName, PodName: podName,
		Content: []byte(content), WorkerEventID: id, WorkerEvent: content,
	}, 1<<20, 64<<10); err != nil {
		t.Fatal(err)
	}
}

func assertEventPath(t *testing.T, fixture *fixture, taskID string, states []task.State) {
	t.Helper()
	assertEventSlicePath(t, listEvents(t, fixture, taskID), states)
}

func assertEventSlicePath(t *testing.T, events []store.TransitionEvent, states []task.State) {
	t.Helper()
	if len(events) != len(states)-1 {
		t.Fatalf("transition events = %d; want %d for path %v", len(events), len(states)-1, states)
	}
	for i, event := range events {
		if event.FromState != states[i] || event.ToState != states[i+1] {
			t.Errorf("event %d = %q -> %q; want %q -> %q", i, event.FromState, event.ToState, states[i], states[i+1])
		}
	}
}

func createResources(actions []k8stesting.Action) []string {
	var resources []string
	for _, action := range actions {
		if action.GetVerb() == "create" && (action.GetResource().Resource == "secrets" || action.GetResource().Resource == "jobs") {
			resources = append(resources, action.GetResource().Resource)
		}
	}
	return resources
}

func deleteActions(actions []k8stesting.Action) []k8stesting.DeleteAction {
	var deletes []k8stesting.DeleteAction
	for _, action := range actions {
		if action.GetVerb() == "delete" && action.GetResource().Resource == "jobs" {
			deletes = append(deletes, action.(k8stesting.DeleteAction))
		}
	}
	return deletes
}

func failedPod(name string, exitCode int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			Name: "worker",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
				ExitCode: exitCode,
			}},
		}}},
	}
}

func copyLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func eventText(events []store.TransitionEvent) string {
	var values []string
	for _, event := range events {
		values = append(values, event.Reason, event.ResourceIdentity, event.Metadata, event.Error)
	}
	return strings.Join(values, " ")
}
