package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/api"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

type backendErrorController struct {
	*fakeController
	createErr    error
	createParams store.CreateTaskParams
	cancelErr    error
	retryErr     error
}

func (c *backendErrorController) CreateTask(ctx context.Context, params store.CreateTaskParams) (store.Task, error) {
	c.createParams = params
	if c.createErr != nil {
		return store.Task{}, c.createErr
	}
	return c.fakeController.CreateTask(ctx, params)
}

func (c *backendErrorController) Cancel(ctx context.Context, taskID string) error {
	if c.cancelErr != nil {
		return c.cancelErr
	}
	return c.fakeController.Cancel(ctx, taskID)
}

func (c *backendErrorController) Retry(ctx context.Context, taskID string) (store.Attempt, error) {
	if c.retryErr != nil {
		return store.Attempt{}, c.retryErr
	}
	return c.fakeController.Retry(ctx, taskID)
}

func TestBackendCreatePaginationAndErrorMapping(t *testing.T) {
	db, taskRecord, _, _ := backendStore(t)
	controller := &backendErrorController{fakeController: newFakeController(db)}
	backend := NewBackend(db, controller)

	payload, err := backend.CreateTask(context.Background(), []byte(`{"repository":"repo","prompt":"work","slack_event_id":"event-1","slack_origin":{"channel_id":"channel-1"}}`))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	var created map[string]any
	if err := json.Unmarshal(payload, &created); err != nil {
		t.Fatalf("decode created task: %v", err)
	}
	if created["repository"] != "repo" || created["slack_event_id"] != "event-1" {
		t.Fatalf("created task = %#v", created)
	}
	if controller.createParams.SlackEventID != "event-1" {
		t.Fatalf("create params = %#v, want Slack event ID", controller.createParams)
	}
	if origin, ok := created["slack_origin"].(map[string]any); !ok || origin["channel_id"] != "channel-1" {
		t.Fatalf("created Slack origin = %#v", created["slack_origin"])
	}
	if _, err := backend.CreateTask(context.Background(), []byte(`{"repository":"repo","prompt":"generic work","idempotency_key":"generic-1"}`)); err != nil {
		t.Fatalf("CreateTask with generic key: %v", err)
	}
	if controller.createParams.IdempotencyKey != "generic-1" || controller.createParams.SlackEventID != "" {
		t.Fatalf("generic create params = %#v", controller.createParams)
	}
	if _, err := backend.CreateTask(context.Background(), []byte("{")); !errors.Is(err, api.ErrInvalid) {
		t.Fatalf("malformed CreateTask error = %v, want ErrInvalid", err)
	}

	controller.createErr = errors.New("unknown configured repository repo")
	if _, err := backend.CreateTask(context.Background(), []byte(`{"repository":"repo"}`)); !errors.Is(err, api.ErrInvalid) {
		t.Fatalf("unknown repository error = %v, want ErrInvalid", err)
	}
	controller.createErr = store.ErrConflict
	if _, err := backend.CreateTask(context.Background(), []byte(`{}`)); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("create conflict error = %v, want ErrConflict", err)
	}
	controller.createErr = nil
	controller.cancelErr = errors.New("state requires cancellation")
	if _, err := backend.CancelTask(context.Background(), taskRecord.ID); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("cancel state error = %v, want ErrConflict", err)
	}
	controller.retryErr = store.ErrNotFound
	if _, err := backend.RetryTask(context.Background(), taskRecord.ID); !errors.Is(err, api.ErrNotFound) {
		t.Fatalf("retry missing error = %v, want ErrNotFound", err)
	}

	listed, err := backend.ListTasks(context.Background(), url.Values{"limit": {"1"}})
	if err != nil {
		t.Fatalf("ListTasks page: %v", err)
	}
	var pageResult struct {
		Tasks      []map[string]any `json:"tasks"`
		NextCursor string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(listed, &pageResult); err != nil {
		t.Fatalf("decode task page: %v", err)
	}
	if len(pageResult.Tasks) != 1 || pageResult.NextCursor != "1" {
		t.Fatalf("task page = %#v", pageResult)
	}
	listed, err = backend.ListTasks(context.Background(), url.Values{"state": {"missing"}, "cursor": {"99"}})
	if err != nil {
		t.Fatalf("ListTasks empty page: %v", err)
	}
	if err := json.Unmarshal(listed, &pageResult); err != nil || len(pageResult.Tasks) != 0 {
		t.Fatalf("empty task page = %#v, %v", pageResult, err)
	}

	for _, query := range []url.Values{{"limit": {"0"}}, {"limit": {"101"}}, {"cursor": {"-1"}}, {"cursor": {"nope"}}} {
		if _, _, err := page(query); !errors.Is(err, api.ErrInvalid) {
			t.Errorf("page(%v) error = %v, want ErrInvalid", query, err)
		}
	}
	if _, _, err := page(url.Values{"limit": {"10"}, "cursor": {"2"}}); err != nil {
		t.Fatalf("valid page: %v", err)
	}

	if _, err := backend.ListAttempts(context.Background(), "missing", nil); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("missing attempts error = %v, want ErrNotFound", err)
	}
	if _, err := backend.ListEvents(context.Background(), "missing", nil); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("missing events error = %v, want ErrNotFound", err)
	}
	if _, _, err := backend.GetLogs(context.Background(), "missing", false, "", 10); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("missing logs error = %v, want ErrNotFound", err)
	}
	if _, err := backend.GetPullRequest(context.Background(), "missing"); !errors.Is(err, api.ErrNotFound) {
		t.Errorf("missing pull request error = %v, want ErrNotFound", err)
	}
}

func TestBackendMapsDurableResultsEventsAndKubernetesResources(t *testing.T) {
	db, taskRecord, attempt, _ := backendStore(t)
	ctx := context.Background()
	from := task.QUEUED
	for _, to := range []task.State{task.CREATING_JOB, task.JOB_PENDING, task.RUNNING, task.AGENT_RUNNING, task.VALIDATING} {
		params := store.TransitionParams{Reason: "advance", Trigger: "controller"}
		if to == task.VALIDATING {
			params.Validation = &store.ValidationTransition{Name: "go test ./...", State: "running", Summary: "started"}
		}
		if err := db.Transition(ctx, taskRecord.ID, from, to, params); err != nil {
			t.Fatalf("transition %s -> %s: %v", from, to, err)
		}
		from = to
	}
	if err := db.RecordValidationResult(ctx, taskRecord.ID, attempt.ID, "go test ./...", "package failed", 1); err != nil {
		t.Fatalf("record validation: %v", err)
	}
	if err := db.RecordGitResult(ctx, store.GitResult{AttemptID: attempt.ID, State: "failed", Branch: "simpleswe/fix", CommitSHA: strings.Repeat("a", 40), Error: "push rejected"}); err != nil {
		t.Fatalf("record Git result: %v", err)
	}
	started := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	completed := started.Add(time.Minute)
	if err := db.ObserveKubernetesJob(ctx, store.KubernetesJobObservation{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, AttemptNumber: 1, Namespace: "workers", Name: "job-1", UID: "job-uid",
		State: "failed", Reason: "BackoffLimitExceeded", Message: "worker failed", StartedAt: &started, CompletedAt: &completed,
	}); err != nil {
		t.Fatalf("observe Job: %v", err)
	}
	if err := db.ObserveKubernetesPod(ctx, store.KubernetesPodObservation{
		TaskID: taskRecord.ID, AttemptID: attempt.ID, Namespace: "workers", Name: "pod-1", UID: "pod-uid",
		State: "failed", Reason: "Error", Message: "exit 1", StartedAt: &started, CompletedAt: &completed,
	}); err != nil {
		t.Fatalf("observe Pod: %v", err)
	}

	payload, err := NewBackend(db, newFakeController(db)).GetTask(ctx, taskRecord.ID)
	if err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	var model map[string]any
	if err := json.Unmarshal(payload, &model); err != nil {
		t.Fatalf("decode task model: %v", err)
	}
	runs, ok := model["validation_runs"].([]any)
	if !ok || len(runs) != 1 {
		t.Fatalf("validation runs = %#v", model["validation_runs"])
	}
	run := runs[0].(map[string]any)
	if run["state"] != "failed" || run["summary"] != "package failed" || run["error"].(map[string]any)["code"] != "validation_failed" {
		t.Fatalf("validation model = %#v", run)
	}
	git := model["git_result"].(map[string]any)
	if git["state"] != "failed" || git["branch"] != "simpleswe/fix" || git["commit_sha"] != strings.Repeat("a", 40) || git["error"].(map[string]any)["code"] != "git_failed" {
		t.Fatalf("Git model = %#v", git)
	}
	for _, field := range []string{"kubernetes_job", "kubernetes_pod"} {
		resource, ok := model[field].(map[string]any)
		if !ok || resource["reason"] == "" || resource["message"] == "" || resource["started_at"] == nil || resource["completed_at"] == nil {
			t.Errorf("%s model = %#v", field, model[field])
		}
	}

	event := eventModel(store.TransitionEvent{
		ID: "event-1", TaskID: taskRecord.ID, AttemptID: attempt.ID, FromState: task.RUNNING, ToState: task.FAILED,
		ResourceIdentity: `{"kind":"Pod","name":"pod-1"}`, Metadata: `{"retry":true}`, Error: `{"code":"worker_failed"}`,
	})
	if event["resource_identity"].(map[string]any)["kind"] != "Pod" || event["error"].(map[string]any)["code"] != "worker_failed" {
		t.Fatalf("event model = %#v", event)
	}
}
