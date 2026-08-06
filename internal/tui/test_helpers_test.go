package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
)

type testConsole struct {
	mu   sync.Mutex
	in   *strings.Reader
	out  bytes.Buffer
	cols int
	rows int
}

func newTestConsole(cols, rows int, input string) *testConsole {
	return &testConsole{in: strings.NewReader("\x1b[?1;2c" + input), cols: cols, rows: rows}
}

func (c *testConsole) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.in.Len() == 0 {
		return 0, io.EOF
	}
	n, _ := c.in.Read(p)
	return n, nil
}

func (c *testConsole) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, _ := c.out.Write(p)
	return n, nil
}

func (*testConsole) Fd() uintptr   { return 0 }
func (*testConsole) SetRaw() error { return nil }
func (*testConsole) Reset() error  { return nil }
func (c *testConsole) Size() (int, int, int, int, error) {
	return c.cols, c.rows, 0, 0, nil
}
func (*testConsole) Close() error { return nil }

func (c *testConsole) output() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.out.String()
}

func (c *testConsole) resetOutput() {
	c.mu.Lock()
	c.out.Reset()
	c.mu.Unlock()
}

func newTestVaxis(t *testing.T, cols, rows int) (*vaxis.Vaxis, *testConsole) {
	t.Helper()
	console := newTestConsole(cols, rows, "")
	vx, err := vaxis.New(vaxis.Options{DisableMouse: true, NoSignals: true, WithConsole: console})
	if err != nil {
		t.Fatalf("vaxis.New(): %v", err)
	}
	t.Cleanup(vx.Close)
	console.resetOutput()
	return vx, console
}

type controllerFixture struct {
	task      Task
	attempt   Attempt
	event     Event
	failRetry atomic.Bool
	failTasks atomic.Bool
	server    *httptest.Server
}

func newControllerFixture(t *testing.T) *controllerFixture {
	t.Helper()
	now := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
	fixture := &controllerFixture{
		task: Task{
			ID: "task-1", Repository: "https://example.test/acme/widget.git", Prompt: "fix it",
			State: "running", CreatedAt: now, UpdatedAt: now.Add(time.Minute), CurrentAttemptID: "attempt-1",
			KubernetesPod: Pod{State: "running", ResourceIdentity: client.ResourceIdentity{Name: "pod-task", Namespace: "task-ns"}},
		},
		attempt: Attempt{
			ID: "attempt-1", TaskID: "task-1", Number: 1, State: "running", CreatedAt: now,
			KubernetesJob: Job{State: "running", ResourceIdentity: client.ResourceIdentity{Name: "job-1", Namespace: "workers"}},
			KubernetesPod: Pod{State: "running", ResourceIdentity: client.ResourceIdentity{Name: "pod-1", Namespace: "workers"}},
		},
		event: Event{ID: "event-1", TaskID: "task-1", AttemptID: "attempt-1", OccurredAt: now, FromState: "queued", ToState: "running", Reason: "started"},
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(fixture.serveHTTP))
	t.Cleanup(fixture.server.Close)
	return fixture
}

func (f *controllerFixture) client() *client.Client {
	return client.New(f.server.URL, f.server.Client())
}

func (f *controllerFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks":
		if f.failTasks.Load() {
			http.Error(w, `{"error":{"code":"unavailable","message":"offline"}}`, http.StatusServiceUnavailable)
			return
		}
		writeData(w, client.TaskList{Tasks: []Task{f.task}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1":
		writeData(w, f.task)
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1/attempts":
		writeData(w, client.AttemptList{Attempts: []Attempt{f.attempt}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1/events":
		writeData(w, client.EventList{Events: []Event{f.event}})
	case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1/logs":
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first line\n\ndata: second\tline\n\n")
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/task-1/retry":
		if f.failRetry.Load() {
			http.Error(w, `{"error":{"code":"conflict","message":"cannot retry"}}`, http.StatusConflict)
			return
		}
		retried := f.task
		retried.State = "queued"
		retried.CurrentAttemptID = "attempt-2"
		writeData(w, retried)
	case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/task-1/cancel":
		cancelled := f.task
		cancelled.State = "cancelled"
		writeData(w, cancelled)
	default:
		http.NotFound(w, r)
	}
}

func writeData[T any](w io.Writer, value T) {
	envelope := struct {
		Data T `json:"data"`
	}{Data: value}
	if err := json.NewEncoder(w).Encode(envelope); err != nil {
		panic(err)
	}
}

func newTestApplication(t *testing.T, api *client.Client) *application {
	t.Helper()
	options := (Options{
		Namespace: "default", RefreshInterval: time.Hour, RequestTimeout: time.Second,
		LogCapacity: 4, TaskLimit: 10, Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard,
	}).withDefaults()
	app := newApplication(context.Background(), nil, api, options)
	t.Cleanup(app.stop)
	return app
}

func receive[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for asynchronous TUI result")
		var zero T
		return zero
	}
}
