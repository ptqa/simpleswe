package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

type fakeHandlerDependency struct {
	mu         sync.Mutex
	calls      []string
	createBody []byte
}

func (f *fakeHandlerDependency) Health(context.Context) ([]byte, error) {
	f.record("health")
	return []byte(`{"status":"ok","service":"simpleswe","checked_at":"2026-08-06T00:00:00Z","dependencies":[]}`), nil
}

func (f *fakeHandlerDependency) CreateTask(_ context.Context, body []byte) ([]byte, error) {
	f.mu.Lock()
	f.calls = append(f.calls, "create")
	f.createBody = append([]byte(nil), body...)
	f.mu.Unlock()
	return []byte(taskJSON), nil
}

func (f *fakeHandlerDependency) ListTasks(context.Context, url.Values) ([]byte, error) {
	f.record("list")
	return []byte(`{"tasks":[` + taskJSON + `],"next_cursor":""}`), nil
}

func (f *fakeHandlerDependency) GetTask(context.Context, string) ([]byte, error) {
	f.record("show")
	return []byte(taskJSON), nil
}

func (f *fakeHandlerDependency) CancelTask(context.Context, string) ([]byte, error) {
	f.record("cancel")
	return []byte(taskJSON), nil
}

func (f *fakeHandlerDependency) RetryTask(context.Context, string) ([]byte, error) {
	f.record("retry")
	return []byte(taskJSON), nil
}

func (f *fakeHandlerDependency) ListAttempts(context.Context, string, url.Values) ([]byte, error) {
	f.record("attempts")
	return []byte(`{"attempts":[],"next_cursor":""}`), nil
}

func (f *fakeHandlerDependency) ListEvents(context.Context, string, url.Values) ([]byte, error) {
	f.record("events")
	return []byte(`{"events":[],"next_cursor":""}`), nil
}

func (f *fakeHandlerDependency) GetLogs(context.Context, string, bool, string, int) (string, <-chan string, error) {
	f.record("logs")
	updates := make(chan string, 1)
	updates <- "live line"
	close(updates)
	return "initial line\nsecond line\n", updates, nil
}

func (f *fakeHandlerDependency) GetPullRequest(context.Context, string) ([]byte, error) {
	f.record("pull-request")
	return []byte(`{"state":"not_created"}`), nil
}

func (f *fakeHandlerDependency) record(call string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, call)
}

func (f *fakeHandlerDependency) wasCalled(call string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, got := range f.calls {
		if got == call {
			return true
		}
	}
	return false
}

func (f *fakeHandlerDependency) lastCreateBody() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]byte(nil), f.createBody...)
}

const taskJSON = `{"task_id":"task-1","repository":"https://bitbucket.example/acme/widget","prompt":"fix the bug","state":"queued","created_at":"2026-08-06T00:00:00Z","updated_at":"2026-08-06T00:00:00Z","cancellation_requested":false,"validation_runs":[],"git_result":{"state":"not_run"},"pull_request":{"state":"not_created"}}`

func TestHandlerAcceptsAndForwardsIdempotencyKey(t *testing.T) {
	dependency := new(fakeHandlerDependency)
	body := `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","idempotency_key":"` + strings.Repeat("k", 256) + `"}`
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(body))

	NewHandler(dependency).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if got := string(dependency.lastCreateBody()); got != body {
		t.Fatalf("forwarded create body = %q, want %q", got, body)
	}
}

func TestHandlerRejectsInvalidCreateIdempotencyKeys(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "257 characters",
			body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","idempotency_key":"` + strings.Repeat("k", 257) + `"}`,
		},
		{
			name: "explicit null",
			body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","idempotency_key":null}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dependency := new(fakeHandlerDependency)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(test.body))

			NewHandler(dependency).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			var envelope struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if envelope.Error.Code != "invalid_request" {
				t.Fatalf("error code = %q, want invalid_request", envelope.Error.Code)
			}
			if dependency.wasCalled("create") {
				t.Fatal("invalid create request reached dependency")
			}
		})
	}
}

func TestHandlerRejectsSlackCreateFieldsAsUnknown(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "slack origin", body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","slack_origin":{"workspace_id":"T1","channel_id":"C1","message_ts":"1.2"}}`},
		{name: "slack event ID", body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","slack_event_id":"event-1"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			dependency := new(fakeHandlerDependency)
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/v1/tasks", strings.NewReader(test.body))

			NewHandler(dependency).ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if dependency.wasCalled("create") {
				t.Fatal("unknown Slack create field reached dependency")
			}
		})
	}
}

func TestHandlerExposesOpenAPITaskRoutes(t *testing.T) {
	dependency := new(fakeHandlerDependency)
	server := httptest.NewServer(NewHandler(dependency))
	defer server.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
		wantCall   string
		wantJSON   bool
	}{
		{name: "health", method: http.MethodGet, path: "/v1/health", wantStatus: http.StatusOK, wantCall: "health", wantJSON: true},
		{name: "create", method: http.MethodPost, path: "/v1/tasks", body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix the bug"}`, wantStatus: http.StatusCreated, wantCall: "create", wantJSON: true},
		{name: "list", method: http.MethodGet, path: "/v1/tasks?state=queued&limit=10&cursor=next", wantStatus: http.StatusOK, wantCall: "list", wantJSON: true},
		{name: "show", method: http.MethodGet, path: "/v1/tasks/task-1", wantStatus: http.StatusOK, wantCall: "show", wantJSON: true},
		{name: "cancel", method: http.MethodPost, path: "/v1/tasks/task-1/cancel", wantStatus: http.StatusAccepted, wantCall: "cancel", wantJSON: true},
		{name: "retry", method: http.MethodPost, path: "/v1/tasks/task-1/retry", wantStatus: http.StatusAccepted, wantCall: "retry", wantJSON: true},
		{name: "attempts", method: http.MethodGet, path: "/v1/tasks/task-1/attempts", wantStatus: http.StatusOK, wantCall: "attempts", wantJSON: true},
		{name: "events", method: http.MethodGet, path: "/v1/tasks/task-1/events", wantStatus: http.StatusOK, wantCall: "events", wantJSON: true},
		{name: "logs", method: http.MethodGet, path: "/v1/tasks/task-1/logs", wantStatus: http.StatusOK, wantCall: "logs"},
		{name: "pull request", method: http.MethodGet, path: "/v1/tasks/task-1/pull-request", wantStatus: http.StatusOK, wantCall: "pull-request", wantJSON: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			request, err := http.NewRequest(tt.method, server.URL+tt.path, body)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tt.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}

			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, tt.wantStatus)
			}
			if tt.wantJSON {
				assertJSONData(t, response)
			} else {
				if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/plain") {
					t.Fatalf("Content-Type = %q, want text/plain", got)
				}
				if body, err := io.ReadAll(response.Body); err != nil || !strings.Contains(string(body), "initial line") {
					t.Fatalf("logs body = %q, read error = %v", body, err)
				}
			}
			if !dependency.wasCalled(tt.wantCall) {
				t.Fatalf("dependency operation %q was not called", tt.wantCall)
			}
		})
	}
}

func TestHandlerReturnsJSONErrorsAndHandlesMethods(t *testing.T) {
	server := httptest.NewServer(NewHandler(new(fakeHandlerDependency)))
	defer server.Close()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{name: "malformed JSON", method: http.MethodPost, path: "/v1/tasks", body: `{"repository":`, status: http.StatusBadRequest},
		{name: "health method", method: http.MethodPost, path: "/v1/health", status: http.StatusMethodNotAllowed},
		{name: "tasks method", method: http.MethodPatch, path: "/v1/tasks", status: http.StatusMethodNotAllowed},
		{name: "task method", method: http.MethodDelete, path: "/v1/tasks/task-1", status: http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			request, err := http.NewRequest(tt.method, server.URL+tt.path, body)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tt.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}

			response, err := server.Client().Do(request)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != tt.status {
				t.Fatalf("status = %d, want %d", response.StatusCode, tt.status)
			}
			assertJSONError(t, response)
		})
	}
}

func TestHandlerLimitsCreateRequestBody(t *testing.T) {
	server := httptest.NewServer(NewHandler(new(fakeHandlerDependency)))
	defer server.Close()

	request, err := http.NewRequest(
		http.MethodPost,
		server.URL+"/v1/tasks",
		strings.NewReader(`{"repository":"https://bitbucket.example/acme/widget","prompt":"`+strings.Repeat("x", 2<<20)+`"}`),
	)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
	assertJSONError(t, response)
}

func TestHandlerFramesFollowLogsAsSSE(t *testing.T) {
	server := httptest.NewServer(NewHandler(new(fakeHandlerDependency)))
	defer server.Close()

	response, err := server.Client().Get(server.URL + "/v1/tasks/task-1/logs?follow=true&attempt_id=attempt-1&tail_lines=2")
	if err != nil {
		t.Fatalf("request logs: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read event stream: %v", err)
	}
	stream := string(body)
	for _, want := range []string{
		"data: initial line\n\n",
		"data: second line\n\n",
		"data: live line\n\n",
	} {
		if !strings.Contains(stream, want) {
			t.Errorf("SSE stream %q does not contain %q", stream, want)
		}
	}
}

func TestHandlerAcceptsEveryLifecycleStateFilter(t *testing.T) {
	server := httptest.NewServer(NewHandler(new(fakeHandlerDependency)))
	defer server.Close()
	states := []string{
		"received", "queued", "creating_job", "job_pending", "running", "agent_running",
		"validating", "committing", "pushing", "creating_pr", "pr_open", "waiting_ci",
		"waiting_review", "ready", "failed", "cancelled",
	}
	for _, state := range states {
		response, err := server.Client().Get(server.URL + "/v1/tasks?state=" + state)
		if err != nil {
			t.Fatalf("GET state %q: %v", state, err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Errorf("state %q status = %d, want 200", state, response.StatusCode)
		}
	}
}

func assertJSONData(t *testing.T, response *http.Response) map[string]any {
	t.Helper()
	if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var envelope map[string]any
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode JSON response: %v", err)
	}
	data, ok := envelope["data"].(map[string]any)
	if !ok {
		t.Fatalf("JSON response data = %#v, want object", envelope["data"])
	}
	return data
}

func assertJSONError(t *testing.T, response *http.Response) {
	t.Helper()
	if got := response.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	var envelope struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatalf("decode JSON error: %v", err)
	}
	if envelope.Error.Code == "" || envelope.Error.Message == "" {
		t.Fatalf("JSON error = %#v, want non-empty code and message", envelope.Error)
	}
}

type errorHandlerDependency struct {
	err error
}

func (f errorHandlerDependency) Health(context.Context) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) CreateTask(context.Context, []byte) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) ListTasks(context.Context, url.Values) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) GetTask(context.Context, string) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) CancelTask(context.Context, string) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) RetryTask(context.Context, string) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) ListAttempts(context.Context, string, url.Values) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) ListEvents(context.Context, string, url.Values) ([]byte, error) {
	return nil, f.err
}

func (f errorHandlerDependency) GetLogs(context.Context, string, bool, string, int) (string, <-chan string, error) {
	return "", nil, f.err
}

func (f errorHandlerDependency) GetPullRequest(context.Context, string) ([]byte, error) {
	return nil, f.err
}

func TestHandlerMapsDependencyErrors(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
	}{
		{name: "not found", err: ErrNotFound, wantStatus: http.StatusNotFound, wantCode: "not_found"},
		{name: "conflict", err: ErrConflict, wantStatus: http.StatusConflict, wantCode: "conflict"},
		{name: "invalid", err: ErrInvalid, wantStatus: http.StatusUnprocessableEntity, wantCode: "validation_error"},
		{name: "unexpected", err: errors.New("database unavailable"), wantStatus: http.StatusInternalServerError, wantCode: "internal_error"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/v1/tasks/task-1", nil)
			NewHandler(errorHandlerDependency{err: tt.err}).ServeHTTP(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()
			if response.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, tt.wantStatus)
			}
			var envelope struct {
				Error struct {
					Code string `json:"code"`
				} `json:"error"`
			}
			if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
				t.Fatalf("decode error response: %v", err)
			}
			if envelope.Error.Code != tt.wantCode {
				t.Fatalf("error code = %q, want %q", envelope.Error.Code, tt.wantCode)
			}
		})
	}
}

func TestHandlerReturnsHealthUnavailableWhenDependencyFails(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	NewHandler(errorHandlerDependency{err: errors.New("unavailable")}).ServeHTTP(recorder, request)
	response := recorder.Result()
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	assertJSONError(t, response)
}

func TestHandlerRejectsUnsupportedMethodsAndUnknownRoutes(t *testing.T) {
	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "cancel", method: http.MethodGet, path: "/v1/tasks/task-1/cancel"},
		{name: "retry", method: http.MethodGet, path: "/v1/tasks/task-1/retry"},
		{name: "attempts", method: http.MethodPost, path: "/v1/tasks/task-1/attempts"},
		{name: "events", method: http.MethodPost, path: "/v1/tasks/task-1/events"},
		{name: "logs", method: http.MethodPost, path: "/v1/tasks/task-1/logs"},
		{name: "pull request", method: http.MethodPost, path: "/v1/tasks/task-1/pull-request"},
		{name: "unknown", method: http.MethodGet, path: "/unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.path, nil)
			NewHandler(new(fakeHandlerDependency)).ServeHTTP(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()
			if response.StatusCode != http.StatusMethodNotAllowed && response.StatusCode != http.StatusNotFound {
				t.Fatalf("status = %d, want method not allowed or not found", response.StatusCode)
			}
			assertJSONError(t, response)
		})
	}
}

func TestHandlerRejectsInvalidQueriesAndCreateBodies(t *testing.T) {
	validCreate := `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix the bug"}`
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "invalid limit", method: http.MethodGet, path: "/v1/tasks?limit=101"},
		{name: "non-numeric limit", method: http.MethodGet, path: "/v1/tasks?limit=nope"},
		{name: "empty cursor", method: http.MethodGet, path: "/v1/tasks?cursor="},
		{name: "repeated cursor", method: http.MethodGet, path: "/v1/tasks?cursor=a&cursor=b"},
		{name: "invalid state", method: http.MethodGet, path: "/v1/tasks?state=unknown"},
		{name: "attempts query", method: http.MethodGet, path: "/v1/tasks/task-1/attempts?limit=0"},
		{name: "invalid follow", method: http.MethodGet, path: "/v1/tasks/task-1/logs?follow=maybe"},
		{name: "repeated attempt", method: http.MethodGet, path: "/v1/tasks/task-1/logs?attempt_id=a&attempt_id=b"},
		{name: "invalid tail", method: http.MethodGet, path: "/v1/tasks/task-1/logs?tail_lines=-1"},
		{name: "missing fields", method: http.MethodPost, path: "/v1/tasks", body: `{}`},
		{name: "unknown field", method: http.MethodPost, path: "/v1/tasks", body: validCreate[:len(validCreate)-1] + `,"unknown":true}`},
		{name: "multiple values", method: http.MethodPost, path: "/v1/tasks", body: validCreate + ` {}`},
		{name: "inline credentials", method: http.MethodPost, path: "/v1/tasks", body: `{"repository":"https://user:pass@bitbucket.example/acme/widget","prompt":"fix"}`},
		{name: "empty idempotency key", method: http.MethodPost, path: "/v1/tasks", body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","idempotency_key":""}`},
		{name: "whitespace idempotency key", method: http.MethodPost, path: "/v1/tasks", body: `{"repository":"https://bitbucket.example/acme/widget","prompt":"fix","idempotency_key":" \t"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var body io.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.path, body)
			NewHandler(new(fakeHandlerDependency)).ServeHTTP(recorder, request)
			response := recorder.Result()
			defer response.Body.Close()
			if response.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusBadRequest)
			}
			assertJSONError(t, response)
		})
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("read failed")
}

func TestDecodeCreateBodyReportsReadErrors(t *testing.T) {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/tasks", failingReader{})
	if _, ok := decodeCreateBody(recorder, request); ok {
		t.Fatal("decodeCreateBody() ok = true, want false")
	}
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
}

func TestTaskIDValidation(t *testing.T) {
	tests := []struct {
		name string
		id   string
		ok   bool
	}{
		{name: "valid", id: "task_1.v2-okay", ok: true},
		{name: "empty", id: ""},
		{name: "too long", id: strings.Repeat("a", 129)},
		{name: "invalid character", id: "task/1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/", nil)
			request.SetPathValue("taskID", tt.id)
			got, ok := taskID(recorder, request)
			if ok != tt.ok || (ok && got != tt.id) {
				t.Fatalf("taskID() = %q, %t, want %q, %t", got, ok, tt.id, tt.ok)
			}
		})
	}
}

func TestSSEStopsOnContextAndHandlesWriterFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
	serveSSE(recorder, request, "first\r\nsecond\r", make(chan string))
	if got := recorder.Body.String(); !strings.Contains(got, "data: first\n\ndata: second\n\n") {
		t.Fatalf("SSE body = %q, want normalized initial lines", got)
	}

	failing := &flushWriter{err: errors.New("write failed")}
	serveSSE(failing, httptest.NewRequest(http.MethodGet, "/", nil), "line", nil)
	if failing.status != http.StatusOK {
		t.Fatalf("status = %d, want %d", failing.status, http.StatusOK)
	}

	noFlush := &responseWriterWithoutFlush{}
	serveSSE(noFlush, httptest.NewRequest(http.MethodGet, "/", nil), "line", nil)
	if noFlush.status != http.StatusInternalServerError {
		t.Fatalf("non-flusher status = %d, want %d", noFlush.status, http.StatusInternalServerError)
	}
}

type flushWriter struct {
	err    error
	status int
}

func (w *flushWriter) Header() http.Header { return http.Header{} }

func (w *flushWriter) WriteHeader(status int) { w.status = status }

func (w *flushWriter) Write([]byte) (int, error) { return 0, w.err }

func (w *flushWriter) Flush() {}

type responseWriterWithoutFlush struct {
	status int
}

func (w *responseWriterWithoutFlush) Header() http.Header { return http.Header{} }

func (w *responseWriterWithoutFlush) WriteHeader(status int) { w.status = status }

func (w *responseWriterWithoutFlush) Write(p []byte) (int, error) { return len(p), nil }

func TestWriteDataRejectsInvalidJSON(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeData(recorder, http.StatusOK, []byte(`{"broken"`))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
}

func TestWriteErrorIgnoresWriterErrors(t *testing.T) {
	writer := &flushWriter{err: errors.New("write failed")}
	writeError(writer, http.StatusBadRequest, "bad", "bad request")
}
