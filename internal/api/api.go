package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

const maxCreateBody = 1 << 20

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
	ErrInvalid  = errors.New("invalid request")
)

type dependency interface {
	Health(context.Context) ([]byte, error)
	CreateTask(context.Context, []byte) ([]byte, error)
	ListTasks(context.Context, url.Values) ([]byte, error)
	GetTask(context.Context, string) ([]byte, error)
	CancelTask(context.Context, string) ([]byte, error)
	RetryTask(context.Context, string) ([]byte, error)
	ListAttempts(context.Context, string, url.Values) ([]byte, error)
	ListEvents(context.Context, string, url.Values) ([]byte, error)
	GetLogs(context.Context, string, bool, string, int) (string, <-chan string, error)
	GetPullRequest(context.Context, string) ([]byte, error)
}

type projectsDependency interface {
	ListProjects(context.Context) ([]byte, error)
}

type Handler struct {
	dependency dependency
	mux        *http.ServeMux
}

func NewHandler(dependency dependency) *Handler {
	handler := &Handler{dependency: dependency}
	handler.mux = http.NewServeMux()
	handler.mux.HandleFunc("/v1/health", handler.health)
	handler.mux.HandleFunc("/v1/projects", handler.projects)
	handler.mux.HandleFunc("/v1/tasks", handler.tasks)
	handler.mux.HandleFunc("/v1/tasks/{taskID}", handler.task)
	handler.mux.HandleFunc("/v1/tasks/{taskID}/cancel", handler.cancel)
	handler.mux.HandleFunc("/v1/tasks/{taskID}/retry", handler.retry)
	handler.mux.HandleFunc("/v1/tasks/{taskID}/attempts", handler.attempts)
	handler.mux.HandleFunc("/v1/tasks/{taskID}/events", handler.events)
	handler.mux.HandleFunc("/v1/tasks/{taskID}/logs", handler.logs)
	handler.mux.HandleFunc("/v1/tasks/{taskID}/pull-request", handler.pullRequest)
	handler.mux.HandleFunc("/", handler.notFound)
	return handler
}

func (h *Handler) projects(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	provider, ok := h.dependency.(projectsDependency)
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
		return
	}
	payload, err := provider.ListProjects(r.Context())
	if err != nil {
		writeDependencyError(w, err)
		return
	}
	writeData(w, http.StatusOK, payload)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	payload, err := h.dependency.Health(r.Context())
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "service_unavailable", "service unavailable")
		return
	}
	writeData(w, http.StatusOK, payload)
}

func (h *Handler) tasks(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if err := validateListQuery(r.URL.Query()); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
			return
		}
		payload, err := h.dependency.ListTasks(r.Context(), r.URL.Query())
		if err != nil {
			writeDependencyError(w, err)
			return
		}
		writeData(w, http.StatusOK, payload)
	case http.MethodPost:
		body, ok := decodeCreateBody(w, r)
		if !ok {
			return
		}
		payload, err := h.dependency.CreateTask(r.Context(), body)
		if err != nil {
			writeDependencyError(w, err)
			return
		}
		writeData(w, http.StatusCreated, payload)
	default:
		methodNotAllowed(w, http.MethodGet, http.MethodPost)
	}
}

func (h *Handler) task(w http.ResponseWriter, r *http.Request) {
	id, ok := taskID(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	payload, err := h.dependency.GetTask(r.Context(), id)
	if err != nil {
		writeDependencyError(w, err)
		return
	}
	writeData(w, http.StatusOK, payload)
}

func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	h.taskAction(w, r, http.MethodPost, http.StatusAccepted, h.dependency.CancelTask)
}

func (h *Handler) retry(w http.ResponseWriter, r *http.Request) {
	h.taskAction(w, r, http.MethodPost, http.StatusAccepted, h.dependency.RetryTask)
}

func (h *Handler) taskAction(w http.ResponseWriter, r *http.Request, method string, status int, action func(context.Context, string) ([]byte, error)) {
	if r.Method != method {
		methodNotAllowed(w, method)
		return
	}
	id, ok := taskID(w, r)
	if !ok {
		return
	}
	payload, err := action(r.Context(), id)
	if err != nil {
		writeDependencyError(w, err)
		return
	}
	writeData(w, status, payload)
}

func (h *Handler) attempts(w http.ResponseWriter, r *http.Request) {
	h.listTaskData(w, r, h.dependency.ListAttempts)
}

func (h *Handler) events(w http.ResponseWriter, r *http.Request) {
	h.listTaskData(w, r, h.dependency.ListEvents)
}

func (h *Handler) listTaskData(w http.ResponseWriter, r *http.Request, list func(context.Context, string, url.Values) ([]byte, error)) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	id, ok := taskID(w, r)
	if !ok {
		return
	}
	if err := validateListQuery(r.URL.Query()); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	payload, err := list(r.Context(), id, r.URL.Query())
	if err != nil {
		writeDependencyError(w, err)
		return
	}
	writeData(w, http.StatusOK, payload)
}

func (h *Handler) logs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	id, ok := taskID(w, r)
	if !ok {
		return
	}
	follow, attemptID, tailLines, err := parseLogQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", err.Error())
		return
	}
	initial, updates, err := h.dependency.GetLogs(r.Context(), id, follow, attemptID, tailLines)
	if err != nil {
		writeDependencyError(w, err)
		return
	}
	if !follow {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		// #nosec G705 -- logs are served as non-sniffable plain text, not HTML.
		_, _ = io.WriteString(w, initial)
		return
	}
	serveSSE(w, r, initial, updates)
}

func (h *Handler) pullRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	id, ok := taskID(w, r)
	if !ok {
		return
	}
	payload, err := h.dependency.GetPullRequest(r.Context(), id)
	if err != nil {
		writeDependencyError(w, err)
		return
	}
	writeData(w, http.StatusOK, payload)
}

func (h *Handler) notFound(w http.ResponseWriter, _ *http.Request) {
	writeError(w, http.StatusNotFound, "not_found", "resource not found")
}

func decodeCreateBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxCreateBody+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid request body")
		return nil, false
	}
	if len(body) > maxCreateBody {
		writeError(w, http.StatusBadRequest, "request_too_large", "request body is too large")
		return nil, false
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var request struct {
		Repository     string  `json:"repository"`
		Prompt         string  `json:"prompt"`
		PRTitle        *string `json:"pr_title"`
		IdempotencyKey *string `json:"idempotency_key"`
	}
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return nil, false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must contain one JSON value")
		return nil, false
	}
	var properties map[string]json.RawMessage
	if err := json.Unmarshal(body, &properties); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return nil, false
	}
	idempotencyKeyJSON, idempotencyKeyPresent := properties["idempotency_key"]
	prTitleJSON, prTitlePresent := properties["pr_title"]
	if strings.TrimSpace(request.Repository) == "" || strings.TrimSpace(request.Prompt) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "repository and prompt are required")
		return nil, false
	}
	if repository, err := url.Parse(request.Repository); err == nil && repository.User != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "repository must not contain inline credentials")
		return nil, false
	}
	if idempotencyKeyPresent && bytes.Equal(bytes.TrimSpace(idempotencyKeyJSON), []byte("null")) {
		writeError(w, http.StatusBadRequest, "invalid_request", "idempotency_key must not be null")
		return nil, false
	}
	if request.IdempotencyKey != nil && strings.TrimSpace(*request.IdempotencyKey) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "idempotency_key must not be empty")
		return nil, false
	}
	if request.IdempotencyKey != nil && utf8.RuneCountInString(*request.IdempotencyKey) > 256 {
		writeError(w, http.StatusBadRequest, "invalid_request", "idempotency_key must not exceed 256 characters")
		return nil, false
	}
	if prTitlePresent && bytes.Equal(bytes.TrimSpace(prTitleJSON), []byte("null")) {
		writeError(w, http.StatusBadRequest, "invalid_request", "pr_title must not be null")
		return nil, false
	}
	if request.PRTitle != nil && strings.TrimSpace(*request.PRTitle) == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "pr_title must not be empty")
		return nil, false
	}
	if request.PRTitle != nil && utf8.RuneCountInString(*request.PRTitle) > 256 {
		writeError(w, http.StatusBadRequest, "invalid_request", "pr_title must not exceed 256 characters")
		return nil, false
	}
	return body, true
}

func taskID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("taskID")
	if id == "" || len(id) > 128 {
		writeError(w, http.StatusBadRequest, "invalid_task_id", "task ID is invalid")
		return "", false
	}
	for _, char := range id {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') && (char < '0' || char > '9') && char != '-' && char != '_' && char != '.' {
			writeError(w, http.StatusBadRequest, "invalid_task_id", "task ID is invalid")
			return "", false
		}
	}
	return id, true
}

func validateListQuery(query url.Values) error {
	if raw := query.Get("limit"); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 100 {
			return errors.New("limit must be between 1 and 100")
		}
	}
	if cursor, ok := query["cursor"]; ok && (len(cursor) != 1 || cursor[0] == "") {
		return errors.New("cursor must not be empty")
	}
	for _, state := range query["state"] {
		switch state {
		case "received", "queued", "creating_job", "job_pending", "running", "agent_running", "validating", "committing", "pushing", "creating_pr", "pr_open", "waiting_ci", "waiting_review", "ready", "failed", "cancelled":
		default:
			return errors.New("state is invalid")
		}
	}
	return nil
}

func parseLogQuery(query url.Values) (bool, string, int, error) {
	follow := false
	if raw := query.Get("follow"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return false, "", 0, errors.New("follow must be a boolean")
		}
		follow = parsed
	}
	attemptID := query.Get("attempt_id")
	if values, ok := query["attempt_id"]; ok && (len(values) != 1 || attemptID == "") {
		return false, "", 0, errors.New("attempt_id must not be empty")
	}
	tailLines := 200
	if raw := query.Get("tail_lines"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 || parsed > 10000 {
			return false, "", 0, errors.New("tail_lines must be between 0 and 10000")
		}
		tailLines = parsed
	}
	return follow, attemptID, tailLines, nil
}

func serveSSE(w http.ResponseWriter, r *http.Request, initial string, updates <-chan string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if !writeSSELines(w, flusher, initial) {
		return
	}
	if updates == nil {
		return
	}
	for {
		select {
		case <-r.Context().Done():
			return
		case update, open := <-updates:
			if !open {
				return
			}
			if !writeSSELines(w, flusher, update) {
				return
			}
		}
	}
}

func writeSSELines(w io.Writer, flusher http.Flusher, text string) bool {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	lines := strings.Split(text, "\n")
	if len(lines) > 1 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	for _, line := range lines {
		if _, err := io.WriteString(w, "data: "+line+"\n\n"); err != nil {
			return false
		}
		flusher.Flush()
	}
	return true
}

func writeData(w http.ResponseWriter, status int, payload []byte) {
	if !json.Valid(payload) {
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(`{"data":`))
	// #nosec G705 -- payload is validated JSON in a non-sniffable JSON response.
	_, _ = w.Write(payload)
	_, _ = w.Write([]byte("}\n"))
}

func writeDependencyError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, ErrConflict):
		writeError(w, http.StatusConflict, "conflict", "requested transition is not valid")
	case errors.Is(err, ErrInvalid):
		writeError(w, http.StatusUnprocessableEntity, "validation_error", "request failed validation")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func methodNotAllowed(w http.ResponseWriter, methods ...string) {
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{Error: struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}{Code: code, Message: message}}); err != nil {
		return
	}
}
