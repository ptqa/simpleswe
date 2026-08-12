package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/api"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

func TestBackendListProjectsReturnsConfiguredProjects(t *testing.T) {
	controller := &fakeController{projects: []store.ConfiguredProject{
		{Name: "widget", Repository: "https://example.com/widget.git"},
	}}

	payload, err := NewBackend(nil, controller).ListProjects(context.Background())
	if err != nil {
		t.Fatalf("ListProjects() error = %v", err)
	}
	var got struct {
		Projects []struct {
			Name       string `json:"name"`
			Repository string `json:"repository"`
		} `json:"projects"`
	}
	if err := json.Unmarshal(payload, &got); err != nil {
		t.Fatalf("decode ListProjects() = %q: %v", payload, err)
	}
	want := []struct {
		Name       string `json:"name"`
		Repository string `json:"repository"`
	}{{Name: "widget", Repository: "https://example.com/widget.git"}}
	if len(got.Projects) != len(want) || got.Projects[0] != want[0] {
		t.Fatalf("ListProjects() = %#v, want %#v", got.Projects, want)
	}
}

func TestBackendFollowCatchesAppendAfterSnapshotAndClosesAtExhaustion(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	backend := NewBackend(db, newFakeController(db))
	initial, updates, err := backend.GetLogs(context.Background(), taskRecord.ID, true, attempt.ID, 100)
	if err != nil || initial != "" {
		t.Fatalf("GetLogs = %q, %v", initial, err)
	}
	var want strings.Builder
	for i := range 200 {
		line := fmt.Sprintf("raced line %03d\n", i)
		want.WriteString(line)
		if err := db.AppendLogChunk(context.Background(), taskRecord.ID, attempt.ID, []byte(line)); err != nil {
			t.Fatalf("append raced line %d: %v", i, err)
		}
	}
	var got strings.Builder
	deadline := time.After(2 * time.Second)
	for got.Len() < want.Len() {
		select {
		case update := <-updates:
			got.WriteString(update)
		case <-deadline:
			t.Fatalf("follow caught %d of %d bytes", got.Len(), want.Len())
		}
	}
	if got.String() != want.String() {
		t.Fatalf("follow dropped or reordered updates")
	}
	if err := db.MarkLogsExhausted(context.Background(), taskRecord.ID, attempt.ID); err != nil {
		t.Fatalf("mark exhausted: %v", err)
	}
	select {
	case _, open := <-updates:
		if open {
			t.Fatal("follow remained open after attempt logs exhausted")
		}
	case <-time.After(time.Second):
		t.Fatal("follow did not close after exhaustion")
	}
}

func TestBackendServesStoreAndControllerModelsAsOpenAPIJSON(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	controller := newFakeController(db)
	backend := NewBackend(db, controller)
	server := httptest.NewServer(api.NewHandler(backend))
	t.Cleanup(server.Close)

	tests := []struct {
		name       string
		method     string
		path       string
		status     int
		assertData func(*testing.T, map[string]any)
	}{
		{
			name: "health", method: http.MethodGet, path: "/v1/health", status: http.StatusOK,
			assertData: func(t *testing.T, data map[string]any) {
				if data["status"] != "ok" || data["service"] != "simpleswe" || data["checked_at"] == "" {
					t.Fatalf("health data = %#v", data)
				}
				if _, ok := data["dependencies"].([]any); !ok {
					t.Fatalf("health dependencies = %#v, want array", data["dependencies"])
				}
			},
		},
		{
			name: "list", method: http.MethodGet, path: "/v1/tasks?state=queued&limit=10", status: http.StatusOK,
			assertData: func(t *testing.T, data map[string]any) {
				tasks, ok := data["tasks"].([]any)
				if !ok || len(tasks) != 1 {
					t.Fatalf("tasks = %#v, want one task", data["tasks"])
				}
				assertOpenAPITask(t, tasks[0].(map[string]any), taskRecord.ID, attempt.ID)
				if data["next_cursor"] != "" {
					t.Fatalf("next_cursor = %#v, want empty", data["next_cursor"])
				}
			},
		},
		{
			name: "show", method: http.MethodGet, path: "/v1/tasks/" + taskRecord.ID, status: http.StatusOK,
			assertData: func(t *testing.T, data map[string]any) {
				assertOpenAPITask(t, data, taskRecord.ID, attempt.ID)
			},
		},
		{
			name: "cancel", method: http.MethodPost, path: "/v1/tasks/" + taskRecord.ID + "/cancel", status: http.StatusAccepted,
			assertData: func(t *testing.T, data map[string]any) {
				assertOpenAPITask(t, data, taskRecord.ID, attempt.ID)
			},
		},
		{
			name: "retry", method: http.MethodPost, path: "/v1/tasks/" + taskRecord.ID + "/retry", status: http.StatusAccepted,
			assertData: func(t *testing.T, data map[string]any) {
				assertOpenAPITask(t, data, taskRecord.ID, attempt.ID)
			},
		},
		{
			name: "attempts", method: http.MethodGet, path: "/v1/tasks/" + taskRecord.ID + "/attempts", status: http.StatusOK,
			assertData: func(t *testing.T, data map[string]any) {
				attempts, ok := data["attempts"].([]any)
				if !ok || len(attempts) != 1 {
					t.Fatalf("attempts = %#v, want one attempt", data["attempts"])
				}
				got := attempts[0].(map[string]any)
				if got["attempt_id"] != attempt.ID || got["task_id"] != taskRecord.ID || got["state"] != "queued" || got["immutable"] != true {
					t.Fatalf("attempt JSON = %#v", got)
				}
				assertResultDefaults(t, got)
			},
		},
		{
			name: "events", method: http.MethodGet, path: "/v1/tasks/" + taskRecord.ID + "/events", status: http.StatusOK,
			assertData: func(t *testing.T, data map[string]any) {
				events, ok := data["events"].([]any)
				if !ok || len(events) != 1 {
					t.Fatalf("events = %#v, want one event", data["events"])
				}
				got := events[0].(map[string]any)
				if got["task_id"] != taskRecord.ID || got["attempt_id"] != attempt.ID || got["from_state"] != "received" || got["to_state"] != "queued" || got["trigger"] != "api" {
					t.Fatalf("event JSON = %#v", got)
				}
				if _, ok := got["resource_identity"]; ok {
					t.Fatalf("resource_identity = %#v, want omitted when empty", got["resource_identity"])
				}
				if _, ok := got["metadata"].(map[string]any); !ok {
					t.Fatalf("metadata = %#v, want object", got["metadata"])
				}
			},
		},
		{
			name: "pull request", method: http.MethodGet, path: "/v1/tasks/" + taskRecord.ID + "/pull-request", status: http.StatusOK,
			assertData: func(t *testing.T, data map[string]any) {
				if data["state"] != "created" || data["number"] != float64(42) || data["url"] != "https://bitbucket.example/acme/widget/pull-requests/42" {
					t.Fatalf("pull request JSON = %#v", data)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, err := http.NewRequest(test.method, server.URL+test.path, nil)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				body, _ := io.ReadAll(response.Body)
				t.Fatalf("status = %d, want %d; body = %s", response.StatusCode, test.status, body)
			}
			if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
				t.Fatalf("Content-Type = %q, want application/json", response.Header.Get("Content-Type"))
			}
			var envelope struct {
				Data map[string]any `json:"data"`
			}
			if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			test.assertData(t, envelope.Data)
		})
	}

	cancelled, retried, _, _ := controller.snapshot()
	if len(cancelled) != 1 || cancelled[0] != taskRecord.ID {
		t.Fatalf("Cancel calls = %v, want %q", cancelled, taskRecord.ID)
	}
	if len(retried) != 1 || retried[0] != taskRecord.ID {
		t.Fatalf("Retry calls = %v, want %q", retried, taskRecord.ID)
	}
}

func TestBackendReturnsAPINotFoundForMissingStoreModels(t *testing.T) {
	db, _, _, _ := backendStore(t)
	server := httptest.NewServer(api.NewHandler(NewBackend(db, newFakeController(db))))
	t.Cleanup(server.Close)

	response, err := server.Client().Get(server.URL + "/v1/tasks/missing")
	if err != nil {
		t.Fatalf("request missing task: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("status = %d, want 404; body = %s", response.StatusCode, body)
	}
}

func TestBackendServesPersistedLogsThroughHTTPHandler(t *testing.T) {
	db, taskRecord, _, _ := backendStore(t)
	backend := NewBackend(db, newFakeController(db))
	server := httptest.NewServer(api.NewHandler(backend))
	t.Cleanup(server.Close)

	response, err := server.Client().Get(server.URL + "/v1/tasks/" + taskRecord.ID + "/logs?tail_lines=2")
	if err != nil {
		t.Fatalf("request logs: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/plain") {
		t.Fatalf("logs response status/content type = %d/%q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read logs: %v", err)
	}
	if len(body) != 0 {
		t.Fatalf("empty persisted logs response = %q", body)
	}
}

func TestAPITaskStatePreservesExplicitLifecycleState(t *testing.T) {
	states := []task.State{
		task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING, task.RUNNING,
		task.AGENT_RUNNING, task.VALIDATING, task.COMMITTING, task.PUSHING, task.CREATING_PR,
		task.PR_OPEN, task.WAITING_CI, task.WAITING_REVIEW, task.READY, task.FAILED, task.CANCELLED,
	}
	for _, state := range states {
		if got := apiTaskState(state); got != string(state) {
			t.Errorf("apiTaskState(%q) = %q, want %q", state, got, state)
		}
	}
	if got := apiState(store.Task{State: task.RUNNING, CancellationRequested: true}); got != "running" {
		t.Fatalf("apiState(cancellation requested) = %q, want durable state running", got)
	}
}

func backendStore(t *testing.T) (*store.Store, store.Task, store.Attempt, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime.sqlite")
	db, err := store.Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	created, err := db.CreateTask(ctx, store.CreateTaskParams{
		Repository: "https://bitbucket.example/acme/widget.git",
		Prompt:     "fix the flaky test",
	})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.QUEUED, store.TransitionParams{Reason: "accepted", Trigger: "api"}); err != nil {
		t.Fatalf("queue task: %v", err)
	}
	created, err = db.GetTask(ctx, created.ID)
	if err != nil {
		t.Fatalf("reload task: %v", err)
	}
	attempt, err := db.CurrentAttempt(ctx, created.ID)
	if err != nil {
		t.Fatalf("current attempt: %v", err)
	}
	fixtureDB, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture database: %v", err)
	}
	if _, err := fixtureDB.ExecContext(ctx, `INSERT INTO pull_requests (attempt_id, state, number, url, title, head_branch, base_branch, error) VALUES (?, 'open', 42, 'https://bitbucket.example/acme/widget/pull-requests/42', 'Fix the flaky test', 'simpleswe/fix', 'main', '')`, attempt.ID); err != nil {
		_ = fixtureDB.Close()
		t.Fatalf("insert historical pull request fixture: %v", err)
	}
	if err := fixtureDB.Close(); err != nil {
		t.Fatalf("close fixture database: %v", err)
	}
	return db, created, attempt, path
}

func assertOpenAPITask(t *testing.T, got map[string]any, taskID, attemptID string) {
	t.Helper()
	if got["task_id"] != taskID || got["current_attempt_id"] != attemptID || got["state"] != "queued" {
		t.Fatalf("task identity/state JSON = %#v", got)
	}
	if cancellationRequested, ok := got["cancellation_requested"].(bool); !ok || cancellationRequested {
		t.Errorf("cancellation_requested = %#v, want false boolean", got["cancellation_requested"])
	}
	for _, field := range []string{"repository", "prompt", "created_at", "updated_at"} {
		if got[field] == "" || got[field] == nil {
			t.Errorf("task field %q = %#v, want non-empty", field, got[field])
		}
	}
	assertResultDefaults(t, got)
}

func assertResultDefaults(t *testing.T, got map[string]any) {
	t.Helper()
	if _, ok := got["validation_runs"].([]any); !ok {
		t.Errorf("validation_runs = %#v, want array", got["validation_runs"])
	}
	git, ok := got["git_result"].(map[string]any)
	if !ok || git["state"] != "not_run" {
		t.Errorf("git_result = %#v, want not_run object", got["git_result"])
	}
	pullRequest, ok := got["pull_request"].(map[string]any)
	if !ok || pullRequest["state"] != "created" {
		t.Errorf("pull_request = %#v, want created object", got["pull_request"])
	}
}
