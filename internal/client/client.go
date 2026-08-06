package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxJSONBody = 1 << 20
	maxSSELine  = 1 << 20
)

// Client is the HTTP client for the local controller API.
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// New returns a client for baseURL. Invalid URLs are reported by the first
// operation rather than making construction awkward for callers.
func New(baseURL string, httpClient *http.Client) *Client {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil {
		parsed = nil
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{baseURL: parsed, httpClient: httpClient}
}

type ListOptions struct {
	State  string
	Limit  int
	Cursor string
}

type CreateTaskRequest struct {
	Repository   string        `json:"repository"`
	Prompt       string        `json:"prompt"`
	SlackOrigin  *SlackDetails `json:"slack_origin,omitempty"`
	SlackEventID string        `json:"slack_event_id,omitempty"`
}

type TaskList struct {
	Tasks      []Task `json:"tasks"`
	NextCursor string `json:"next_cursor"`
}

type EventList struct {
	Events     []Event `json:"events"`
	NextCursor string  `json:"next_cursor"`
}

type AttemptList struct {
	Attempts   []Attempt `json:"attempts"`
	NextCursor string    `json:"next_cursor"`
}

type Task struct {
	ID                    string          `json:"task_id"`
	Repository            string          `json:"repository"`
	Prompt                string          `json:"prompt"`
	State                 string          `json:"state"`
	CreatedAt             time.Time       `json:"created_at"`
	UpdatedAt             time.Time       `json:"updated_at"`
	CurrentAttemptID      string          `json:"current_attempt_id"`
	CancellationRequested bool            `json:"cancellation_requested"`
	ValidationRuns        []ValidationRun `json:"validation_runs"`
	GitResult             GitResult       `json:"git_result"`
	PullRequest           PullRequest     `json:"pull_request"`
	KubernetesJob         KubernetesJob   `json:"kubernetes_job"`
	KubernetesPod         KubernetesPod   `json:"kubernetes_pod"`
	SlackEventID          string          `json:"slack_event_id"`
	SlackOrigin           SlackDetails    `json:"slack_origin"`
}

type Attempt struct {
	ID             string          `json:"attempt_id"`
	TaskID         string          `json:"task_id"`
	Number         int             `json:"number"`
	Immutable      bool            `json:"immutable"`
	State          string          `json:"state"`
	CreatedAt      time.Time       `json:"created_at"`
	StartedAt      *time.Time      `json:"started_at"`
	CompletedAt    *time.Time      `json:"completed_at"`
	KubernetesJob  KubernetesJob   `json:"kubernetes_job"`
	KubernetesPod  KubernetesPod   `json:"kubernetes_pod"`
	ValidationRuns []ValidationRun `json:"validation_runs"`
	GitResult      GitResult       `json:"git_result"`
	PullRequest    PullRequest     `json:"pull_request"`
}

type Event struct {
	ID               string            `json:"event_id"`
	TaskID           string            `json:"task_id"`
	AttemptID        string            `json:"attempt_id"`
	OccurredAt       time.Time         `json:"occurred_at"`
	FromState        string            `json:"from_state"`
	ToState          string            `json:"to_state"`
	Reason           string            `json:"reason"`
	Trigger          string            `json:"trigger"`
	ResourceIdentity *ResourceIdentity `json:"resource_identity"`
	Metadata         map[string]any    `json:"metadata"`
	Error            *Error            `json:"error"`
}

type ValidationRun struct {
	ID          string     `json:"run_id"`
	Name        string     `json:"name"`
	State       string     `json:"state"`
	Summary     string     `json:"summary"`
	Error       *Error     `json:"error"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

type GitResult struct {
	State     string `json:"state"`
	Branch    string `json:"branch"`
	CommitSHA string `json:"commit_sha"`
	Error     *Error `json:"error"`
}

type PullRequest struct {
	State      string `json:"state"`
	Number     int    `json:"number"`
	URL        string `json:"url"`
	Title      string `json:"title"`
	HeadBranch string `json:"head_branch"`
	BaseBranch string `json:"base_branch"`
	Error      *Error `json:"error"`
}

type ResourceIdentity struct {
	APIVersion string `json:"api_version"`
	Kind       string `json:"kind"`
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
}

type Error struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details"`
}

type KubernetesJob struct {
	State            string           `json:"state"`
	ResourceIdentity ResourceIdentity `json:"resource_identity"`
	Reason           string           `json:"reason"`
	Message          string           `json:"message"`
	StartedAt        *time.Time       `json:"started_at"`
	CompletedAt      *time.Time       `json:"completed_at"`
}

type KubernetesPod struct {
	State            string           `json:"state"`
	ResourceIdentity ResourceIdentity `json:"resource_identity"`
	Reason           string           `json:"reason"`
	Message          string           `json:"message"`
	StartedAt        *time.Time       `json:"started_at"`
	CompletedAt      *time.Time       `json:"completed_at"`
}

type SlackDetails struct {
	WorkspaceID string `json:"workspace_id"`
	ChannelID   string `json:"channel_id"`
	MessageTS   string `json:"message_ts"`
	ThreadTS    string `json:"thread_ts"`
	UserID      string `json:"user_id"`
}

type LogOptions struct {
	Follow    bool
	AttemptID string
	TailLines int
}

func (c *Client) ListTasks(ctx context.Context, options ListOptions) (TaskList, error) {
	var result TaskList
	err := c.getJSON(ctx, "/v1/tasks", options.values(), &result)
	return result, err
}

func (c *Client) ShowTask(ctx context.Context, taskID string) (Task, error) {
	var result Task
	err := c.getJSON(ctx, "/v1/tasks/"+url.PathEscape(taskID), nil, &result)
	return result, err
}

func (c *Client) CreateTask(ctx context.Context, request CreateTaskRequest) (Task, error) {
	var result Task
	err := c.doJSON(ctx, http.MethodPost, "/v1/tasks", nil, request, &result)
	return result, err
}

func (c *Client) CancelTask(ctx context.Context, taskID string) (Task, error) {
	var result Task
	err := c.doJSON(ctx, http.MethodPost, "/v1/tasks/"+url.PathEscape(taskID)+"/cancel", nil, nil, &result)
	return result, err
}

func (c *Client) RetryTask(ctx context.Context, taskID string) (Task, error) {
	var result Task
	err := c.doJSON(ctx, http.MethodPost, "/v1/tasks/"+url.PathEscape(taskID)+"/retry", nil, nil, &result)
	return result, err
}

func (c *Client) ListEvents(ctx context.Context, taskID string, options ListOptions) (EventList, error) {
	var result EventList
	err := c.getJSON(ctx, "/v1/tasks/"+url.PathEscape(taskID)+"/events", options.values(), &result)
	return result, err
}

func (c *Client) ListAttempts(ctx context.Context, taskID string, options ListOptions) (AttemptList, error) {
	var result AttemptList
	err := c.getJSON(ctx, "/v1/tasks/"+url.PathEscape(taskID)+"/attempts", options.values(), &result)
	return result, err
}

// StreamLogs delivers each SSE data line to onLine as soon as it is read.
// It deliberately does not return or retain the complete log stream.
func (c *Client) StreamLogs(ctx context.Context, taskID string, options LogOptions, onLine func(string) error) (resultErr error) {
	if onLine == nil {
		return errors.New("log callback is nil")
	}
	query := url.Values{}
	if options.Follow {
		query.Set("follow", "true")
	}
	if options.AttemptID != "" {
		query.Set("attempt_id", options.AttemptID)
	}
	if options.TailLines != 0 {
		query.Set("tail_lines", strconv.Itoa(options.TailLines))
	}
	response, err := c.request(ctx, http.MethodGet, "/v1/tasks/"+url.PathEscape(taskID)+"/logs", query, nil)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return decodeHTTPError(response)
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 64*1024), maxSSELine)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		line = strings.TrimPrefix(line, "data:")
		line = strings.TrimPrefix(line, " ")
		if err := onLine(line); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (o ListOptions) values() url.Values {
	query := url.Values{}
	if o.State != "" {
		query.Set("state", o.State)
	}
	if o.Limit != 0 {
		query.Set("limit", strconv.Itoa(o.Limit))
	}
	if o.Cursor != "" {
		query.Set("cursor", o.Cursor)
	}
	return query
}

func (c *Client) getJSON(ctx context.Context, path string, query url.Values, target any) error {
	return c.doJSON(ctx, http.MethodGet, path, query, nil, target)
}

func (c *Client) doJSON(ctx context.Context, method, path string, query url.Values, body any, target any) (resultErr error) {
	var encoded []byte
	var err error
	if body != nil {
		encoded, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	var requestBody io.Reader
	if body != nil {
		requestBody = bytes.NewReader(encoded)
	}
	response, err := c.request(ctx, method, path, query, requestBody)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, response.Body.Close()) }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return decodeHTTPError(response)
	}
	return decodeData(response.Body, target)
}

func (c *Client) request(ctx context.Context, method, path string, query url.Values, body io.Reader) (*http.Response, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if c == nil || c.baseURL == nil {
		return nil, errors.New("client URL is invalid")
	}
	requestURL := *c.baseURL
	requestURL.Path = strings.TrimRight(requestURL.Path, "/") + path
	requestURL.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), body)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	return response, nil
}

type APIError struct {
	Status  int
	Code    string
	Message string
	Details map[string]any
}

func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("controller API error (%d) %s: %s", e.Status, e.Code, e.Message)
}

func decodeData(body io.Reader, target any) error {
	contents, err := readBounded(body)
	if err != nil {
		return err
	}
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := decodeStrict(contents, &envelope); err != nil {
		return fmt.Errorf("decode response envelope: %w", err)
	}
	if len(envelope.Data) == 0 {
		return errors.New("response envelope has no data")
	}
	if err := decodeStrict(envelope.Data, target); err != nil {
		return fmt.Errorf("decode response data: %w", err)
	}
	return nil
}

func decodeHTTPError(response *http.Response) error {
	contents, readErr := readBounded(response.Body)
	if readErr != nil {
		return fmt.Errorf("read controller error: %w", readErr)
	}
	var envelope struct {
		Error Error `json:"error"`
	}
	if err := decodeStrict(contents, &envelope); err != nil {
		return fmt.Errorf("controller returned %s: decode error: %w", response.Status, err)
	}
	if envelope.Error.Code == "" || envelope.Error.Message == "" {
		return fmt.Errorf("controller returned %s: error code and message are required", response.Status)
	}
	return &APIError{Status: response.StatusCode, Code: envelope.Error.Code, Message: envelope.Error.Message, Details: envelope.Error.Details}
}

func readBounded(body io.Reader) ([]byte, error) {
	contents, err := io.ReadAll(io.LimitReader(body, maxJSONBody+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(contents) > maxJSONBody {
		return nil, errors.New("response body exceeds limit")
	}
	return contents, nil
}

func decodeStrict(contents []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("response contains more than one JSON value")
		}
		return err
	}
	return nil
}

type PortForwardOptions struct {
	KubeContext string
	Namespace   string
	Service     string
	LocalPort   int
	RemotePort  int
}

// PortForwardCommand builds the kubectl process without invoking a shell.
func PortForwardCommand(ctx context.Context, options PortForwardOptions) *exec.Cmd {
	args := make([]string, 0, 8)
	if options.KubeContext != "" {
		args = append(args, "--context", options.KubeContext)
	}
	args = append(args, "--namespace", options.Namespace, "port-forward", "service/"+options.Service, strconv.Itoa(options.LocalPort)+":"+strconv.Itoa(options.RemotePort))
	// #nosec G204 -- the executable is fixed and arguments are passed without a shell.
	return exec.CommandContext(ctx, "kubectl", args...)
}

// PortForward is the minimal lifecycle wrapper for a kubectl port-forward.
type PortForward struct {
	cmd       *exec.Cmd
	ready     chan struct{}
	done      chan struct{}
	readyOnce sync.Once
	stopOnce  sync.Once
	readyErr  error
	waitErr   error
}

// StartPortForward starts kubectl and marks readiness when kubectl reports a
// forwarding address. If kubectl exits first, readiness is released with the
// process error.
func StartPortForward(ctx context.Context, options PortForwardOptions) (*PortForward, error) {
	if ctx == nil {
		return nil, errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cmd := PortForwardCommand(ctx, options)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("capture kubectl stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("capture kubectl stderr: %w", err)
	}
	forward := &PortForward{cmd: cmd, ready: make(chan struct{}), done: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start kubectl port-forward: %w", err)
	}

	var readers sync.WaitGroup
	readers.Add(2)
	go forward.scanOutput(stdout, &readers)
	go forward.scanOutput(stderr, &readers)
	go forward.wait(&readers)
	go func() {
		select {
		case <-ctx.Done():
			_ = forward.stop()
		case <-forward.done:
		}
	}()
	return forward, nil
}

func (p *PortForward) scanOutput(reader io.Reader, readers *sync.WaitGroup) {
	defer readers.Done()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4*1024), 64*1024)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "Forwarding from") {
			p.markReady(nil)
		}
	}
}

func (p *PortForward) wait(readers *sync.WaitGroup) {
	err := p.cmd.Wait()
	readers.Wait()
	if err != nil {
		p.markReady(err)
	} else {
		p.markReady(errors.New("kubectl port-forward exited before readiness"))
	}
	p.waitErr = err
	close(p.done)
}

func (p *PortForward) markReady(err error) {
	p.readyOnce.Do(func() {
		p.readyErr = err
		close(p.ready)
	})
}

// Ready is closed when forwarding is ready or the process has exited.
func (p *PortForward) Ready() <-chan struct{} { return p.ready }

// WaitReady waits for readiness and returns an early process failure.
func (p *PortForward) WaitReady(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	select {
	case <-p.ready:
		return p.readyErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Wait returns the kubectl process result.
func (p *PortForward) Wait() error {
	<-p.done
	return p.waitErr
}

// Close terminates kubectl and waits for its exit.
func (p *PortForward) Close() error {
	return p.stop()
}

func (p *PortForward) stop() error {
	p.stopOnce.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Kill()
		}
	})
	return p.Wait()
}
