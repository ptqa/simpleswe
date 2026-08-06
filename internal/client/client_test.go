package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

const clientTaskJSON = `{"task_id":"task-1","repository":"https://bitbucket.example/acme/widget","prompt":"fix the bug","state":"queued","created_at":"2026-08-06T00:00:00Z","updated_at":"2026-08-06T00:00:00Z","current_attempt_id":"attempt-1","cancellation_requested":false,"kubernetes_job":{"state":"running","resource_identity":{"kind":"Job","name":"job-1"}},"kubernetes_pod":{"state":"running","resource_identity":{"kind":"Pod","name":"pod-1"}},"validation_runs":[],"git_result":{"state":"not_run"},"pull_request":{"state":"not_created"}}`

func TestClientCallsControllerEndpointsAndDecodesDataEnvelopes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks":
			if got := r.URL.Query().Get("state"); got != "queued" {
				t.Errorf("list state = %q, want queued", got)
			}
			if got := r.URL.Query().Get("limit"); got != "10" {
				t.Errorf("list limit = %q, want 10", got)
			}
			if got := r.URL.Query().Get("cursor"); got != "next" {
				t.Errorf("list cursor = %q, want next", got)
			}
			_, _ = fmt.Fprintf(w, `{"data":{"tasks":[%s],"next_cursor":"after"}}`, clientTaskJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks":
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read create body: %v", err)
			}
			if got := string(body); got != `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix the bug"}` {
				t.Errorf("create body = %s", got)
			}
			_, _ = fmt.Fprintf(w, `{"data":%s}`, clientTaskJSON)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1":
			_, _ = fmt.Fprintf(w, `{"data":%s}`, clientTaskJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/task-1/cancel":
			_, _ = fmt.Fprintf(w, `{"data":%s}`, clientTaskJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/task-1/retry":
			_, _ = fmt.Fprintf(w, `{"data":%s}`, clientTaskJSON)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1/events":
			if got := r.URL.Query().Get("limit"); got != "10" {
				t.Errorf("events limit = %q, want 10", got)
			}
			_, _ = io.WriteString(w, `{"data":{"events":[{"event_id":"event-1","task_id":"task-1","attempt_id":"attempt-1","occurred_at":"2026-08-06T00:00:00Z","from_state":"queued","to_state":"running","reason":"worker started","trigger":"controller","resource_identity":{"kind":"Pod","name":"pod-1"},"metadata":{"source":"watch"},"error":{"code":"warning","message":"recoverable"}}],"next_cursor":""}}`)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1/attempts":
			if got := r.URL.Query().Get("limit"); got != "10" {
				t.Errorf("attempts limit = %q, want 10", got)
			}
			_, _ = io.WriteString(w, `{"data":{"attempts":[{"attempt_id":"attempt-1","task_id":"task-1","number":1,"immutable":true,"state":"queued","created_at":"2026-08-06T00:00:00Z","validation_runs":[],"git_result":{"state":"not_run"},"pull_request":{"state":"not_created"}}],"next_cursor":""}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	c := New(server.URL, server.Client())
	ctx := context.Background()

	list, err := c.ListTasks(ctx, ListOptions{State: "queued", Limit: 10, Cursor: "next"})
	if err != nil {
		t.Fatalf("ListTasks() error = %v", err)
	}
	if list.NextCursor != "after" || len(list.Tasks) != 1 || list.Tasks[0].ID != "task-1" {
		t.Fatalf("ListTasks() = %#v, want one task and cursor after", list)
	}

	show, err := c.ShowTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("ShowTask() error = %v", err)
	}
	if show.ID != "task-1" || show.CurrentAttemptID != "attempt-1" || show.KubernetesJob.ResourceIdentity.Name != "job-1" || show.KubernetesPod.ResourceIdentity.Name != "pod-1" {
		t.Fatalf("ShowTask() = %#v", show)
	}

	created, err := c.CreateTask(ctx, CreateTaskRequest{
		Repository: "https://bitbucket.example/acme/widget",
		Prompt:     "fix the bug",
	})
	if err != nil {
		t.Fatalf("CreateTask() error = %v", err)
	}
	if created.ID != "task-1" {
		t.Fatalf("CreateTask() = %#v", created)
	}

	for name, call := range map[string]func() (Task, error){
		"cancel": func() (Task, error) { return c.CancelTask(ctx, "task-1") },
		"retry":  func() (Task, error) { return c.RetryTask(ctx, "task-1") },
	} {
		got, err := call()
		if err != nil {
			t.Fatalf("%s() error = %v", name, err)
		}
		if got.ID != "task-1" {
			t.Errorf("%s() = %#v", name, got)
		}
	}

	events, err := c.ListEvents(ctx, "task-1", ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListEvents() error = %v", err)
	}
	if len(events.Events) != 1 || events.Events[0].ToState != "running" || events.Events[0].ResourceIdentity.Name != "pod-1" || events.Events[0].Metadata["source"] != "watch" || events.Events[0].Error.Code != "warning" {
		t.Fatalf("ListEvents() = %#v", events)
	}

	attempts, err := c.ListAttempts(ctx, "task-1", ListOptions{Limit: 10})
	if err != nil {
		t.Fatalf("ListAttempts() error = %v", err)
	}
	if len(attempts.Attempts) != 1 || attempts.Attempts[0].ID != "attempt-1" {
		t.Fatalf("ListAttempts() = %#v", attempts)
	}
}

func TestClientDecodesControllerAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"error":{"code":"not_found","message":"resource not found","details":{"task_id":"missing"}}}`)
	}))
	defer server.Close()

	_, err := New(server.URL, server.Client()).ShowTask(context.Background(), "missing")
	if err == nil {
		t.Fatal("ShowTask() error = nil, want API error")
	}
	for _, want := range []string{"not_found", "resource not found"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("ShowTask() error = %q, want it to contain %q", err, want)
		}
	}
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Details["task_id"] != "missing" {
		t.Fatalf("ShowTask() API error = %#v, want structured details", apiError)
	}
}

func TestClientPropagatesContextCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := New(server.URL, server.Client()).ShowTask(ctx, "task-1")
		errCh <- err
	}()
	<-started
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ShowTask() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ShowTask() did not return after context cancellation")
	}
}

func TestClientStreamsSSEIncrementally(t *testing.T) {
	firstSeen := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/tasks/task-1/logs" || r.URL.Query().Get("follow") != "true" || r.URL.Query().Get("attempt_id") != "attempt-1" || r.URL.Query().Get("tail_lines") != "2" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("test server does not support flushing")
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: first line\n\n")
		flusher.Flush()
		<-release
		_, _ = io.WriteString(w, "data: second line\n\n")
		flusher.Flush()
	}))
	defer server.Close()

	var lines []string
	var mu sync.Mutex
	errCh := make(chan error, 1)
	go func() {
		errCh <- New(server.URL, server.Client()).StreamLogs(
			context.Background(),
			"task-1",
			LogOptions{Follow: true, AttemptID: "attempt-1", TailLines: 2},
			func(line string) error {
				mu.Lock()
				lines = append(lines, line)
				mu.Unlock()
				if line == "first line" {
					once.Do(func() { close(firstSeen) })
				}
				return nil
			},
		)
	}()

	select {
	case <-firstSeen:
	case <-time.After(time.Second):
		t.Fatal("StreamLogs() did not deliver the first event before the second event was released")
	}
	close(release)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("StreamLogs() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StreamLogs() did not finish")
	}
	mu.Lock()
	got := append([]string(nil), lines...)
	mu.Unlock()
	if want := []string{"first line", "second line"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("StreamLogs() lines = %#v, want %#v", got, want)
	}
}

func TestPortForwardCommandUsesArgumentsWithoutShellInterpolation(t *testing.T) {
	cmd := PortForwardCommand(context.Background(), PortForwardOptions{
		KubeContext: "dev context; echo should-not-run",
		Namespace:   "simpleswe",
		Service:     "controller",
		LocalPort:   18080,
		RemotePort:  8080,
	})
	if cmd == nil {
		t.Fatal("PortForwardCommand() returned nil")
	}
	want := []string{
		"kubectl",
		"--context", "dev context; echo should-not-run",
		"--namespace", "simpleswe",
		"port-forward",
		"service/controller",
		"18080:8080",
	}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Fatalf("PortForwardCommand() args = %#v, want %#v", cmd.Args, want)
	}
	if cmd.Path == "sh" || cmd.Path == "/bin/sh" {
		t.Fatalf("PortForwardCommand() uses a shell: %q", cmd.Path)
	}
}
