package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"

	"github.com/simpleswe/simpleswe/internal/api"
	"github.com/simpleswe/simpleswe/internal/client"
	runtime "github.com/simpleswe/simpleswe/internal/controller/runtime"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestServerClientAndTUIShareTaskContract(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "contract.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	ctx := context.Background()
	record, err := db.CreateTask(ctx, store.CreateTaskParams{Repository: "https://example.test/acme/widget.git", Prompt: "fix it"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.Transition(ctx, record.ID, task.RECEIVED, task.QUEUED, store.TransitionParams{Reason: "accepted", Trigger: "api"}); err != nil {
		t.Fatalf("queue task: %v", err)
	}
	record, err = db.GetTask(ctx, record.ID)
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	attempt, _ := db.CurrentAttempt(ctx, record.ID)
	if err := db.ObserveKubernetesJob(ctx, store.KubernetesJobObservation{TaskID: record.ID, AttemptID: attempt.ID, Namespace: "workers", Name: "job-1", UID: "job-uid", State: "running", Reason: "Active"}); err != nil {
		t.Fatalf("observe Job: %v", err)
	}
	if err := db.ObserveKubernetesPod(ctx, store.KubernetesPodObservation{TaskID: record.ID, AttemptID: attempt.ID, Namespace: "workers", Name: "pod-1", UID: "pod-uid", State: "running", Reason: "Running", Node: "node-1", Image: "worker:v1", ContainerStates: `{"worker":"running"}`}); err != nil {
		t.Fatalf("observe Pod: %v", err)
	}

	server := httptest.NewServer(api.NewHandler(runtime.NewBackend(db, contractController{store: db})))
	t.Cleanup(server.Close)
	c := client.New(server.URL, server.Client())

	list, err := c.ListTasks(ctx, client.ListOptions{State: "queued", Limit: 100})
	if err != nil {
		t.Fatalf("strict ListTasks decode: %v", err)
	}
	if len(list.Tasks) != 1 || list.Tasks[0].ID != record.ID {
		t.Fatalf("ListTasks() = %#v", list)
	}
	taskModel, err := c.ShowTask(ctx, record.ID)
	if err != nil {
		t.Fatalf("strict ShowTask decode: %v", err)
	}
	attempts, err := c.ListAttempts(ctx, record.ID, client.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("strict ListAttempts decode: %v", err)
	}
	events, err := c.ListEvents(ctx, record.ID, client.ListOptions{Limit: 100})
	if err != nil {
		t.Fatalf("strict ListEvents decode: %v", err)
	}
	if len(attempts.Attempts) != 1 || attempts.Attempts[0].ID != record.CurrentAttemptID {
		t.Fatalf("ListAttempts() = %#v", attempts)
	}
	if len(events.Events) != 1 || events.Events[0].ID == "" || events.Events[0].FromState != "received" || events.Events[0].ToState != "queued" {
		t.Fatalf("ListEvents() = %#v", events)
	}

	model := NewModel(10)
	model.RefreshTasks(list.Tasks)
	model.SetDetail(TaskDetail{Task: taskModel, Attempts: attempts.Attempts, Events: events.Events})
	if got := model.Detail(); got.Task.ID != record.ID || len(got.Attempts) != 1 || len(got.Events) != 1 {
		t.Fatalf("TUI detail = %#v", got)
	}
	detail := model.Detail()
	if detail.Task.KubernetesJob.ResourceIdentity.Name != "job-1" || detail.Task.KubernetesPod.ResourceIdentity.Name != "pod-1" || detail.Attempts[0].KubernetesPod.ResourceIdentity.Namespace != "workers" {
		t.Fatalf("TUI Kubernetes panes did not receive observed resources: %#v", detail)
	}
	app := &application{model: model, options: Options{Namespace: "fallback"}}
	if pod, namespace := app.selectedPod(); pod != "pod-1" || namespace != "workers" {
		t.Fatalf("shell Pod resolution = %q/%q, want workers/pod-1", namespace, pod)
	}

	assertWireNamesAndTypes(t, server.URL, record.ID)
}

func assertWireNamesAndTypes(t *testing.T, baseURL, taskID string) {
	t.Helper()
	response, err := http.Get(baseURL + "/v1/tasks/" + url.PathEscape(taskID))
	if err != nil {
		t.Fatalf("GET task: %v", err)
	}
	defer response.Body.Close()
	var taskEnvelope struct {
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(response.Body).Decode(&taskEnvelope); err != nil {
		t.Fatalf("decode task: %v", err)
	}
	if _, ok := taskEnvelope.Data["cancellation_requested"].(bool); !ok {
		t.Errorf("cancellation_requested = %#v, want boolean", taskEnvelope.Data["cancellation_requested"])
	}
	for _, wrong := range []string{"attempts", "job", "pod"} {
		if _, exists := taskEnvelope.Data[wrong]; exists {
			t.Errorf("task unexpectedly contains %q: %#v", wrong, taskEnvelope.Data)
		}
	}

	for _, check := range []struct {
		path       string
		collection string
		id         string
		wrongID    string
	}{
		{path: "/v1/tasks/" + url.PathEscape(taskID) + "/attempts", collection: "attempts", id: "attempt_id", wrongID: "id"},
		{path: "/v1/tasks/" + url.PathEscape(taskID) + "/events", collection: "events", id: "event_id", wrongID: "id"},
	} {
		response, err := http.Get(baseURL + check.path)
		if err != nil {
			t.Fatalf("GET %s: %v", check.path, err)
		}
		defer response.Body.Close()
		var envelope struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
			t.Fatalf("decode %s: %v", check.path, err)
		}
		var items []map[string]any
		if err := json.Unmarshal(envelope.Data[check.collection], &items); err != nil || len(items) != 1 {
			t.Fatalf("%s collection = %#v, error = %v", check.path, items, err)
		}
		if _, ok := items[0][check.id].(string); !ok {
			t.Errorf("%s %s = %#v, want string", check.path, check.id, items[0][check.id])
		}
		if _, exists := items[0][check.wrongID]; exists {
			t.Errorf("%s unexpectedly contains %q: %#v", check.path, check.wrongID, items[0])
		}
	}
}

type contractController struct{ store *store.Store }

func (c contractController) CreateTask(ctx context.Context, params store.CreateTaskParams) (store.Task, error) {
	return c.store.CreateTask(ctx, params)
}
func (contractController) Cancel(context.Context, string) error { return nil }
func (c contractController) Retry(ctx context.Context, taskID string) (store.Attempt, error) {
	return c.store.CurrentAttempt(ctx, taskID)
}
func (contractController) Reconcile(context.Context) error { return nil }
func (contractController) HandleWorkerEvent(context.Context, string, string, protocol.Event) error {
	return nil
}
