package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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

func TestClientRejectsInvalidInputsAndUsesDefaultHTTPClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":{"task_id":"task-1"}}`)
	}))
	defer server.Close()

	if _, err := New(server.URL, nil).ShowTask(context.Background(), "task-1"); err != nil {
		t.Fatalf("New() with nil HTTP client error = %v", err)
	}
	if _, err := New("%gh", server.Client()).ShowTask(context.Background(), "task-1"); err == nil || !strings.Contains(err.Error(), "client URL is invalid") {
		t.Fatalf("invalid URL error = %v, want invalid client URL", err)
	}
	var nilContext context.Context
	if _, err := New(server.URL, server.Client()).ShowTask(nilContext, "task-1"); err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("nil context error = %v, want nil context", err)
	}
	var nilClient *Client
	if _, err := nilClient.ShowTask(context.Background(), "task-1"); err == nil || !strings.Contains(err.Error(), "client URL is invalid") {
		t.Fatalf("nil client error = %v, want invalid client URL", err)
	}
}

type failingClientReader struct{}

func (failingClientReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func TestClientRejectsMalformedAndOversizedResponses(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{name: "malformed envelope", body: []byte(`{"data":`)},
		{name: "missing data", body: []byte(`{}`)},
		{name: "malformed data", body: []byte(`{"data":{`)},
		{name: "unknown response field", body: []byte(`{"data":{"unknown":true}}`)},
		{name: "multiple JSON values", body: []byte(`{"data":{}} {}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var target struct{}
			if err := decodeData(bytes.NewReader(tt.body), &target); err == nil {
				t.Fatal("decodeData() error = nil, want error")
			}
		})
	}
	if err := decodeData(failingClientReader{}, &struct{}{}); err == nil || !strings.Contains(err.Error(), "read response") {
		t.Fatalf("decodeData() read error = %v, want read response error", err)
	}
	if err := decodeData(strings.NewReader(strings.Repeat("x", maxJSONBody+1)), &struct{}{}); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("decodeData() oversized error = %v, want size error", err)
	}
}

func TestClientDecodesHTTPErrorVariants(t *testing.T) {
	tests := []struct {
		name string
		body io.Reader
	}{
		{name: "malformed", body: strings.NewReader(`{"error":`)},
		{name: "missing code", body: strings.NewReader(`{"error":{"message":"failed"}}`)},
		{name: "missing message", body: strings.NewReader(`{"error":{"code":"failed"}}`)},
		{name: "oversized", body: strings.NewReader(strings.Repeat("x", maxJSONBody+1))},
		{name: "read failure", body: failingClientReader{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := decodeHTTPError(&http.Response{StatusCode: http.StatusBadRequest, Status: "400 Bad Request", Body: io.NopCloser(tt.body)})
			if err == nil {
				t.Fatal("decodeHTTPError() error = nil, want error")
			}
		})
	}
	response := &http.Response{
		StatusCode: http.StatusConflict,
		Status:     "409 Conflict",
		Body: io.NopCloser(strings.NewReader(
			`{"error":{"code":"conflict","message":"already exists","details":{"task_id":"task-1"}}}`,
		)),
	}
	err := decodeHTTPError(response)
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Status != http.StatusConflict || apiError.Details["task_id"] != "task-1" {
		t.Fatalf("decodeHTTPError() = %#v, want structured API error", err)
	}
}

func TestClientStrictDecodingRejectsTrailingValues(t *testing.T) {
	var target struct{}
	tests := []struct {
		name    string
		content string
	}{
		{name: "unknown field", content: `{"unknown":true}`},
		{name: "extra value", content: `{} {}`},
		{name: "invalid trailing value", content: `{} nope`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := decodeStrict([]byte(tt.content), &target); err == nil {
				t.Fatal("decodeStrict() error = nil, want error")
			}
		})
	}
}

func TestClientStreamLogsRejectsNilCallback(t *testing.T) {
	if err := New("http://example.com", nil).StreamLogs(context.Background(), "task-1", LogOptions{}, nil); err == nil || err.Error() != "log callback is nil" {
		t.Fatalf("StreamLogs() error = %v, want nil callback error", err)
	}
}

func TestClientStreamLogsReturnsCallbackError(t *testing.T) {
	wantErr := errors.New("callback failed")
	server := newLogServer(t, http.StatusOK, "data: line\n\n")
	err := New(server.URL, server.Client()).StreamLogs(context.Background(), "task-1", LogOptions{}, func(string) error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("StreamLogs() error = %v, want %v", err, wantErr)
	}
}

func TestClientStreamLogsSkipsNonDataLines(t *testing.T) {
	server := newLogServer(t, http.StatusOK, "event: log\n: comment\ndata: line\n\n")
	err := New(server.URL, server.Client()).StreamLogs(context.Background(), "task-1", LogOptions{}, func(line string) error {
		if line != "line" {
			return fmt.Errorf("line = %q", line)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("StreamLogs() error = %v, want nil", err)
	}
}

func TestClientStreamLogsReturnsScannerError(t *testing.T) {
	server := newLogServer(t, http.StatusOK, strings.Repeat("x", maxSSELine+1)+"\n")
	err := New(server.URL, server.Client()).StreamLogs(context.Background(), "task-1", LogOptions{}, func(string) error {
		return nil
	})
	if err == nil {
		t.Fatal("StreamLogs() error = nil, want scanner error")
	}
}

func TestClientStreamLogsDecodesHTTPError(t *testing.T) {
	server := newLogServer(t, http.StatusNotFound, `{"error":{"code":"not_found","message":"missing"}}`)
	err := New(server.URL, server.Client()).StreamLogs(context.Background(), "task-1", LogOptions{}, func(string) error {
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "not_found") {
		t.Fatalf("StreamLogs() error = %v, want not_found", err)
	}
}

func newLogServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return server
}

func TestAPIErrorStringHandlesNilAndStructuredErrors(t *testing.T) {
	var nilError *APIError
	if got := nilError.Error(); got != "" {
		t.Fatalf("nil APIError.Error() = %q, want empty", got)
	}
	if got := (&APIError{Status: http.StatusBadRequest, Code: "invalid", Message: "bad request"}).Error(); !strings.Contains(got, "invalid") || !strings.Contains(got, "bad request") {
		t.Fatalf("APIError.Error() = %q, want code and message", got)
	}
}

func TestPortForwardLifecycle(t *testing.T) {
	installFakeKubectl(t, "ready")
	forward, err := StartPortForward(context.Background(), PortForwardOptions{Namespace: "simpleswe", Service: "controller", LocalPort: 18080, RemotePort: 8080})
	if err != nil {
		t.Fatalf("StartPortForward() error = %v", err)
	}
	if err := forward.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady() error = %v, want nil", err)
	}
	select {
	case <-forward.Ready():
	default:
		t.Fatal("Ready() channel is not closed")
	}
	if err := forward.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := forward.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
	if err := forward.Wait(); err == nil {
		t.Fatal("Wait() error = nil, want killed process error")
	}
}

func TestPortForwardReportsEarlyExitAndContextErrors(t *testing.T) {
	installFakeKubectl(t, "exit")
	forward, err := StartPortForward(context.Background(), PortForwardOptions{})
	if err != nil {
		t.Fatalf("StartPortForward() error = %v", err)
	}
	if err := forward.WaitReady(context.Background()); err == nil {
		t.Fatal("WaitReady() error = nil, want early process error")
	}
	if err := forward.Wait(); err == nil {
		t.Fatal("Wait() error = nil, want process error")
	}
	if err := forward.Close(); err == nil {
		t.Fatal("Close() error = nil after early process exit")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := StartPortForward(ctx, PortForwardOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("StartPortForward() canceled error = %v, want context.Canceled", err)
	}
	var nilContext context.Context
	if _, err := StartPortForward(nilContext, PortForwardOptions{}); err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("StartPortForward() nil context error = %v, want nil context", err)
	}
}

func TestPortForwardReportsStartFailureAndWaitReadyCancellation(t *testing.T) {
	path := t.TempDir()
	t.Setenv("PATH", path)
	if _, err := StartPortForward(context.Background(), PortForwardOptions{}); err == nil || !strings.Contains(err.Error(), "start kubectl port-forward") {
		t.Fatalf("StartPortForward() start error = %v, want start error", err)
	}

	forward := &PortForward{ready: make(chan struct{})}
	var nilContext context.Context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := forward.WaitReady(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReady() canceled error = %v, want context.Canceled", err)
	}
	if err := forward.WaitReady(nilContext); err == nil || !strings.Contains(err.Error(), "context is nil") {
		t.Fatalf("WaitReady() nil context error = %v, want nil context", err)
	}
}

func installFakeKubectl(t *testing.T, mode string) {
	t.Helper()
	directory := t.TempDir()
	script := fmt.Sprintf("#!/bin/sh\nexec %q -test.run=TestPortForwardHelper\n", os.Args[0])
	path := filepath.Join(directory, "kubectl")
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake kubectl: %v", err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("SIMPLESWE_FAKE_KUBECTL_MODE", mode)
}

func TestPortForwardHelper(_ *testing.T) {
	switch os.Getenv("SIMPLESWE_FAKE_KUBECTL_MODE") {
	case "ready":
		fmt.Fprintln(os.Stdout, "not forwarding yet")
		fmt.Fprintln(os.Stdout, "Forwarding from 127.0.0.1:18080 -> 8080")
		select {}
	case "exit":
		fmt.Fprintln(os.Stdout, "kubectl failed")
		os.Exit(2)
	}
}
