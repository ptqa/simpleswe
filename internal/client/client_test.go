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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
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
			if got := string(body); got != `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix the bug","idempotency_key":"generic-request-1"}` {
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
		Repository:     "https://bitbucket.example/acme/widget",
		Prompt:         "fix the bug",
		IdempotencyKey: "generic-request-1",
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

func TestClientWaitTaskReturnsWhenPullRequestURLExists(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tasks/task-1" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, waitTaskResponse("running", "https://bitbucket.example/acme/widget/pull-requests/1"))
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := New(server.URL, server.Client()).WaitTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("WaitTask() error = %v", err)
	}
	if got.State != "running" || got.PullRequest.URL == "" {
		t.Fatalf("WaitTask() = %#v, want non-terminal task with pull-request URL", got)
	}
	if calls != 1 {
		t.Fatalf("WaitTask() ShowTask calls = %d, want 1", calls)
	}
}

func TestClientWaitTaskReturnsForTerminalStates(t *testing.T) {
	for _, state := range []string{"failed", "cancelled", "ready"} {
		t.Run(state, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Method != http.MethodGet || r.URL.Path != "/v1/tasks/task-1" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, waitTaskResponse(state, ""))
			}))
			t.Cleanup(server.Close)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			got, err := New(server.URL, server.Client()).WaitTask(ctx, "task-1")
			if err != nil {
				t.Fatalf("WaitTask() error = %v", err)
			}
			if got.ID != "task-1" || got.State != state {
				t.Fatalf("WaitTask() = %#v, want task-1 in %s state", got, state)
			}
			if calls != 1 {
				t.Fatalf("WaitTask() ShowTask calls = %d, want 1", calls)
			}
		})
	}
}

func TestClientWaitTaskRetriesTransientErrorsThenReturnsReady(t *testing.T) {
	var calls int
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		switch calls {
		case 1:
			return nil, errors.New("connection reset")
		case 2:
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Status:     "503 Service Unavailable",
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("temporarily unavailable")),
				Request:    request,
			}, nil
		default:
			return waitTaskHTTPResponse(request, waitTaskResponse("ready", "")), nil
		}
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := New("http://controller", httpClient).WaitTask(ctx, "task-1")
	if err != nil || got.State != "ready" || calls != 3 {
		t.Fatalf("WaitTask() = %#v, %v after %d calls; want ready after transport and HTTP 503 retries", got, err, calls)
	}
}

func TestClientWaitTaskPropagatesAPI4xxImmediately(t *testing.T) {
	var calls int
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Status:     "404 Not Found",
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"not_found","message":"missing task"}}`)),
			Request:    request,
		}, nil
	})}

	_, err := New("http://controller", httpClient).WaitTask(context.Background(), "task-1")
	var apiError *APIError
	if !errors.As(err, &apiError) || apiError.Status != http.StatusNotFound || calls != 1 {
		t.Fatalf("WaitTask() error = %#v after %d calls, want immediate 404 API error", err, calls)
	}
}

func TestClientWaitTaskPropagatesContextCancellation(t *testing.T) {
	t.Run("during ShowTask", func(t *testing.T) {
		started := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
		}))
		t.Cleanup(server.Close)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := New(server.URL, server.Client()).WaitTask(ctx, "task-1")
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("WaitTask() did not call ShowTask")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "wait for task") {
				t.Fatalf("WaitTask() error = %v, want wrapped context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("WaitTask() did not return after context cancellation")
		}
	})

	t.Run("between polls", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		calls := 0
		httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			calls++
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Header:     make(http.Header),
				Body: cancelOnCloseBody{
					Reader: strings.NewReader(waitTaskResponse("queued", "")),
					Cancel: cancel,
				},
				Request: request,
			}, nil
		})}

		_, err := New("http://controller", httpClient).WaitTask(ctx, "task-1")
		if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "wait for task") {
			t.Fatalf("WaitTask() error = %v, want wrapped context.Canceled", err)
		}
		if calls != 1 {
			t.Fatalf("WaitTask() ShowTask calls = %d, want one completed poll before cancellation", calls)
		}
	})
}

func TestClientWaitTaskUsesFixedIntervalWithoutBusyLooping(t *testing.T) {
	const minimumPollInterval = 900 * time.Millisecond
	var calls int
	var pollTimes []time.Time
	httpClient := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.Path != "/v1/tasks/task-1" {
			return nil, fmt.Errorf("request = %s %s", request.Method, request.URL.Path)
		}
		calls++
		pollTimes = append(pollTimes, time.Now())
		state := "queued"
		if calls == 2 {
			state = "ready"
		}
		return waitTaskHTTPResponse(request, waitTaskResponse(state, "")), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	got, err := New("http://controller", httpClient).WaitTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("WaitTask() error = %v", err)
	}
	if got.State != "ready" || calls != 2 {
		t.Fatalf("WaitTask() = %#v after %d polls, want ready after 2 polls", got, calls)
	}
	for i := 1; i < len(pollTimes); i++ {
		if gap := pollTimes[i].Sub(pollTimes[i-1]); gap < minimumPollInterval {
			t.Fatalf("poll interval = %v, want at least %v", gap, minimumPollInterval)
		}
	}
}

func waitTaskResponse(state, pullRequestURL string) string {
	return fmt.Sprintf(`{"data":{"task_id":"task-1","state":%q,"pull_request":{"state":"not_created","url":%q}}}`, state, pullRequestURL)
}

func waitTaskHTTPResponse(request *http.Request, body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type cancelOnCloseBody struct {
	io.Reader
	Cancel context.CancelFunc
}

func (b cancelOnCloseBody) Close() error {
	b.Cancel()
	return nil
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

func TestPortForwardConfigHonorsSelectedKubeContext(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config")
	config := `apiVersion: v1
kind: Config
current-context: default
clusters:
- name: default
  cluster:
    server: https://default.example
- name: selected
  cluster:
    server: https://selected.example
contexts:
- name: default
  context:
    cluster: default
    namespace: context-namespace
- name: selected
  context:
    cluster: selected
    namespace: wrong-namespace
users: []
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	t.Setenv("KUBECONFIG", configPath)

	got, err := loadPortForwardConfig(PortForwardOptions{KubeContext: "selected"})
	if err != nil {
		t.Fatalf("loadPortForwardConfig() error = %v", err)
	}
	if got.Host != "https://selected.example" {
		t.Fatalf("selected kubeconfig host = %q, want selected context", got.Host)
	}
}

func TestPortForwardConfigSupportsOIDCAuthProvider(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config")
	config := `apiVersion: v1
kind: Config
current-context: default
clusters:
- name: default
  cluster:
    server: https://default.example
- name: selected
  cluster:
    server: https://selected.example
contexts:
- name: default
  context:
    cluster: default
    user: oidc
- name: selected
  context:
    cluster: selected
    namespace: kubeconfig-namespace
    user: oidc
users:
- name: oidc
  user:
    auth-provider:
      name: oidc
      config:
        client-id: simpleswe
        idp-issuer-url: https://issuer.example
`
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatalf("write kubeconfig: %v", err)
	}
	t.Setenv("KUBECONFIG", configPath)

	got, err := loadPortForwardConfig(PortForwardOptions{KubeContext: "selected", Namespace: "explicit-namespace"})
	if err != nil {
		if strings.Contains(err.Error(), `no Auth Provider found for name "oidc"`) {
			t.Fatalf("OIDC auth provider was not registered: %v", err)
		}
		t.Fatalf("loadPortForwardConfig() error = %v", err)
	}
	if got.Host != "https://selected.example" {
		t.Fatalf("selected kubeconfig host = %q, want selected context", got.Host)
	}
	if _, err := rest.TransportFor(got); err != nil {
		if strings.Contains(err.Error(), `no Auth Provider found for name "oidc"`) {
			t.Fatalf("OIDC auth provider was not registered while building transport: %v", err)
		}
		t.Fatalf("rest.TransportFor() error = %v", err)
	}
}

func TestResolvePortForwardTargetUsesExplicitNamespaceAndService(t *testing.T) {
	service := testPortForwardService("team-a", "controller", map[string]string{"app": "controller"}, corev1.ServicePort{
		Name: "http", Port: 80, TargetPort: intstr.FromInt(8080),
	})
	otherNamespace := testReadyPortForwardPod("other", "controller", map[string]string{"app": "controller"}, 8080, "http")
	selected := testReadyPortForwardPod("team-a", "z-controller", map[string]string{"app": "controller"}, 8080, "http")
	first := testReadyPortForwardPod("team-a", "a-controller", map[string]string{"app": "controller"}, 8080, "http")
	pending := testReadyPortForwardPod("team-a", "pending", map[string]string{"app": "controller"}, 8080, "http")
	pending.Status.Phase = corev1.PodPending
	notReady := testReadyPortForwardPod("team-a", "not-ready", map[string]string{"app": "controller"}, 8080, "http")
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse
	terminating := testReadyPortForwardPod("team-a", "terminating", map[string]string{"app": "controller"}, 8080, "http")
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	unrelated := testReadyPortForwardPod("team-a", "unrelated", map[string]string{"app": "other"}, 8080, "http")
	kube := fake.NewSimpleClientset(service, otherNamespace, selected, first, pending, notReady, terminating, unrelated)

	got, err := resolvePortForwardTarget(context.Background(), kube, PortForwardOptions{
		Namespace: "team-a", Service: "controller", RemotePort: 80,
	})
	if err != nil {
		t.Fatalf("resolvePortForwardTarget() error = %v", err)
	}
	if got.PodName != "a-controller" || got.Port != 8080 {
		t.Fatalf("resolved target = %#v, want deterministic ready pod a-controller:8080", got)
	}
}

func TestResolvePortForwardTargetMatchesKubectlServicePortSemantics(t *testing.T) {
	tests := []struct {
		name          string
		targetPort    intstr.IntOrString
		clusterIP     string
		containerPort int32
		containerName string
		initPort      int32
		initName      string
		wantPort      int
	}{
		{name: "integer target", targetPort: intstr.FromInt(8080), wantPort: 8080},
		{name: "zero target uses service port", targetPort: intstr.FromInt(0), wantPort: 80},
		{name: "headless service uses service port", targetPort: intstr.FromInt(8080), clusterIP: corev1.ClusterIPNone, wantPort: 80},
		{name: "named regular container port", targetPort: intstr.FromString("metrics"), containerPort: 9090, containerName: "metrics", wantPort: 9090},
		{name: "named init container port", targetPort: intstr.FromString("setup"), initPort: 9091, initName: "setup", wantPort: 9091},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := testPortForwardService("team-a", "controller", map[string]string{"app": "controller"}, corev1.ServicePort{
				Name: "http", Port: 80, TargetPort: tt.targetPort,
			})
			service.Spec.ClusterIP = tt.clusterIP
			pod := testReadyPortForwardPod("team-a", "controller-pod", map[string]string{"app": "controller"}, tt.containerPort, tt.containerName)
			if tt.initPort != 0 {
				pod.Spec.InitContainers = []corev1.Container{{Name: "setup", Ports: []corev1.ContainerPort{{Name: tt.initName, ContainerPort: tt.initPort}}}}
			}
			got, err := resolvePortForwardTarget(context.Background(), fake.NewSimpleClientset(service, pod), PortForwardOptions{
				Namespace: "team-a", Service: "controller", RemotePort: 80,
			})
			if err != nil {
				t.Fatalf("resolvePortForwardTarget() error = %v", err)
			}
			if got.PodName != pod.Name || got.Port != tt.wantPort {
				t.Fatalf("resolved target = %#v, want %s:%d", got, pod.Name, tt.wantPort)
			}
		})
	}
}

func TestResolvePortForwardTargetReportsActionableErrors(t *testing.T) {
	ready := testReadyPortForwardPod("team-a", "controller-pod", map[string]string{"app": "controller"}, 8080, "http")
	tests := []struct {
		name         string
		objects      []runtime.Object
		remotePort   int
		wantMessages []string
	}{
		{
			name:         "missing service",
			objects:      []runtime.Object{ready},
			remotePort:   80,
			wantMessages: []string{"service", "controller", "team-a"},
		},
		{
			name:         "empty selector",
			objects:      []runtime.Object{testPortForwardService("team-a", "controller", nil, corev1.ServicePort{Port: 80, TargetPort: intstr.FromInt(8080)})},
			remotePort:   80,
			wantMessages: []string{"selector", "controller"},
		},
		{
			name: "missing service port",
			objects: []runtime.Object{
				testPortForwardService("team-a", "controller", map[string]string{"app": "controller"}, corev1.ServicePort{Port: 81, TargetPort: intstr.FromInt(8080)}),
				ready,
			},
			remotePort:   80,
			wantMessages: []string{"port", "80", "controller"},
		},
		{
			name: "non-TCP service port",
			objects: []runtime.Object{
				testPortForwardService("team-a", "controller", map[string]string{"app": "controller"}, corev1.ServicePort{Port: 80, Protocol: corev1.ProtocolUDP}),
				ready,
			},
			remotePort:   80,
			wantMessages: []string{"unsupported", "UDP", "controller"},
		},
		{
			name: "unresolved named target",
			objects: []runtime.Object{
				testPortForwardService("team-a", "controller", map[string]string{"app": "controller"}, corev1.ServicePort{Port: 80, TargetPort: intstr.FromString("metrics")}),
				ready,
			},
			remotePort:   80,
			wantMessages: []string{"target", "metrics", "controller"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := resolvePortForwardTarget(context.Background(), fake.NewSimpleClientset(tt.objects...), PortForwardOptions{
				Namespace: "team-a", Service: "controller", RemotePort: tt.remotePort,
			})
			if err == nil {
				t.Fatal("resolvePortForwardTarget() error = nil, want actionable error")
			}
			for _, want := range tt.wantMessages {
				if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(want)) {
					t.Errorf("error = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}

func TestResolvePortForwardTargetTimesOutWaitingForReadyPod(t *testing.T) {
	service := testPortForwardService("team-a", "controller", map[string]string{"app": "controller"}, corev1.ServicePort{
		Port: 80, TargetPort: intstr.FromInt(8080),
	})
	pending := testReadyPortForwardPod("team-a", "pending", map[string]string{"app": "controller"}, 8080, "http")
	pending.Status.Phase = corev1.PodPending
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := resolvePortForwardTarget(ctx, fake.NewSimpleClientset(service, pending), PortForwardOptions{
		KubeContext: "production", Namespace: "team-a", Service: "controller", RemotePort: 80,
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("resolvePortForwardTarget() error = %v, want context deadline exceeded", err)
	}
	for _, want := range []string{"production", "team-a/controller", "waiting for a running, ready pod"} {
		if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(want)) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
}

func TestPortForwardLifecycleUsesActualBoundPortAndIsIdempotent(t *testing.T) {
	fakeForwarder := newFakePortForwarder(49152, nil)
	forward := newPortForward(context.Background(), fakeForwarder, fakeForwarder.ready)
	close(fakeForwarder.ready)
	if err := forward.WaitReady(context.Background()); err != nil {
		t.Fatalf("WaitReady() error = %v, want nil", err)
	}
	if got := forward.LocalPort(); got != 49152 {
		t.Fatalf("LocalPort() = %d, want actual bound port 49152", got)
	}
	if err := forward.Close(); err != nil {
		t.Fatalf("Close() error = %v, want nil", err)
	}
	if err := forward.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
	if err := forward.Wait(); err != nil {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
	select {
	case <-fakeForwarder.stopped:
	case <-time.After(time.Second):
		t.Fatal("Close() did not stop forwarding")
	}
}

func TestPortForwardContextCancellationStopsForwarding(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fakeForwarder := newFakePortForwarder(49153, nil)
	forward := newPortForward(ctx, fakeForwarder, fakeForwarder.ready)
	close(fakeForwarder.ready)
	cancel()
	select {
	case <-fakeForwarder.stopped:
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not stop forwarding")
	}
	if err := forward.Wait(); err != nil {
		t.Fatalf("Wait() after context cancellation = %v, want nil", err)
	}
}

func TestPortForwardReportsEarlyForwardingFailure(t *testing.T) {
	wantErr := errors.New("dial pod controller-pod: connection refused")
	fakeForwarder := newFakePortForwarder(0, wantErr)
	forward := newPortForward(context.Background(), fakeForwarder, fakeForwarder.ready)
	if err := forward.WaitReady(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("WaitReady() error = %v, want %v", err, wantErr)
	}
	if err := forward.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("Wait() error = %v, want %v", err, wantErr)
	}
}

func testPortForwardService(namespace, name string, selector map[string]string, port corev1.ServicePort) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}, Spec: corev1.ServiceSpec{
		Selector: selector, Ports: []corev1.ServicePort{port},
	}}
}

func testReadyPortForwardPod(namespace, name string, labels map[string]string, port int32, portName string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels}, Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "controller", Ports: []corev1.ContainerPort{{Name: portName, ContainerPort: port}}}},
	}, Status: corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
	}}
}

type fakePortForwarder struct {
	ready      chan struct{}
	stop       chan struct{}
	stopped    chan struct{}
	closeOnce  sync.Once
	localPort  int
	forwardErr error
}

func newFakePortForwarder(localPort int, forwardErr error) *fakePortForwarder {
	return &fakePortForwarder{ready: make(chan struct{}), stop: make(chan struct{}), stopped: make(chan struct{}), localPort: localPort, forwardErr: forwardErr}
}

func (f *fakePortForwarder) ForwardPorts() error {
	defer close(f.stopped)
	if f.forwardErr != nil {
		return f.forwardErr
	}
	<-f.stop
	return nil
}

func (f *fakePortForwarder) LocalPort() (int, error) {
	return f.localPort, nil
}

func (f *fakePortForwarder) Close() {
	f.closeOnce.Do(func() { close(f.stop) })
}
