package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/simpleswe/simpleswe/internal/api"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

// Controller is the narrow controller surface used by the HTTP backend and
// Kubernetes runtime.
type Controller interface {
	CreateTask(context.Context, store.CreateTaskParams) (store.Task, error)
	Cancel(context.Context, string) error
	Retry(context.Context, string) (store.Attempt, error)
	Reconcile(context.Context) error
	HandleWorkerEvent(context.Context, string, string, protocol.Event) error
}

// Backend adapts durable controller/store models to the HTTP API dependency.
type Backend struct {
	store      *store.Store
	controller Controller
}

type configuredProjectProvider interface {
	ConfiguredProjects() []store.ConfiguredProject
}

func NewBackend(db *store.Store, controller Controller) *Backend {
	return &Backend{store: db, controller: controller}
}

func (b *Backend) Health(ctx context.Context) ([]byte, error) {
	status := "ok"
	dependencyStatus := "ok"
	message := ""
	if _, err := b.store.Pragmas(ctx); err != nil {
		status, dependencyStatus, message = "degraded", "unavailable", err.Error()
	}
	return marshal(map[string]any{
		"status":     status,
		"service":    "simpleswe",
		"checked_at": time.Now().UTC(),
		"dependencies": []map[string]string{{
			"name": "store", "status": dependencyStatus, "message": message,
		}},
	})
}

func (b *Backend) ListProjects(context.Context) ([]byte, error) {
	provider, ok := b.controller.(configuredProjectProvider)
	if !ok {
		return marshal(map[string]any{"projects": []any{}})
	}
	projects := provider.ConfiguredProjects()
	items := make([]map[string]string, 0, len(projects))
	for _, project := range projects {
		items = append(items, map[string]string{"name": project.Name, "repository": project.Repository})
	}
	return marshal(map[string]any{"projects": items})
}

func (b *Backend) CreateTask(ctx context.Context, body []byte) ([]byte, error) {
	var request struct {
		Repository     string `json:"repository"`
		Prompt         string `json:"prompt"`
		PRTitle        string `json:"pr_title"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, api.ErrInvalid
	}
	created, err := b.controller.CreateTask(ctx, store.CreateTaskParams{
		Repository: request.Repository, Prompt: request.Prompt, PRTitle: request.PRTitle, IdempotencyKey: request.IdempotencyKey,
	})
	if err != nil {
		return nil, mapCreateError(err)
	}
	return b.taskPayload(ctx, created)
}

func (b *Backend) ListTasks(ctx context.Context, query url.Values) ([]byte, error) {
	all, err := b.store.ListTasks(ctx)
	if err != nil {
		return nil, err
	}
	filtered := make([]store.Task, 0, len(all))
	states := query["state"]
	for _, item := range all {
		if len(states) == 0 || contains(states, apiState(item)) {
			filtered = append(filtered, item)
		}
	}
	offset, limit, err := page(query)
	if err != nil {
		return nil, err
	}
	if offset > len(filtered) {
		offset = len(filtered)
	}
	end := min(offset+limit, len(filtered))
	items := make([]any, 0, end-offset)
	for _, item := range filtered[offset:end] {
		model, err := b.taskModel(ctx, item)
		if err != nil {
			return nil, err
		}
		items = append(items, model)
	}
	next := ""
	if end < len(filtered) {
		next = strconv.Itoa(end)
	}
	return marshal(map[string]any{"tasks": items, "next_cursor": next})
}

func (b *Backend) GetTask(ctx context.Context, taskID string) ([]byte, error) {
	record, err := b.store.GetTask(ctx, taskID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	return b.taskPayload(ctx, record)
}

func (b *Backend) CancelTask(ctx context.Context, taskID string) ([]byte, error) {
	if err := b.controller.Cancel(ctx, taskID); err != nil {
		return nil, mapActionError(err)
	}
	return b.GetTask(ctx, taskID)
}

func (b *Backend) RetryTask(ctx context.Context, taskID string) ([]byte, error) {
	if _, err := b.controller.Retry(ctx, taskID); err != nil {
		return nil, mapActionError(err)
	}
	return b.GetTask(ctx, taskID)
}

func (b *Backend) ListAttempts(ctx context.Context, taskID string, query url.Values) ([]byte, error) {
	if _, err := b.store.GetTask(ctx, taskID); err != nil {
		return nil, mapStoreError(err)
	}
	attempts, err := b.store.ListAttempts(ctx, taskID)
	if err != nil {
		return nil, err
	}
	offset, limit, err := page(query)
	if err != nil {
		return nil, err
	}
	if offset > len(attempts) {
		offset = len(attempts)
	}
	end := min(offset+limit, len(attempts))
	items := make([]any, 0, end-offset)
	for _, attempt := range attempts[offset:end] {
		model, err := b.attemptModel(ctx, attempt)
		if err != nil {
			return nil, err
		}
		items = append(items, model)
	}
	next := ""
	if end < len(attempts) {
		next = strconv.Itoa(end)
	}
	return marshal(map[string]any{"attempts": items, "next_cursor": next})
}

func (b *Backend) ListEvents(ctx context.Context, taskID string, query url.Values) ([]byte, error) {
	if _, err := b.store.GetTask(ctx, taskID); err != nil {
		return nil, mapStoreError(err)
	}
	events, err := b.store.ListEvents(ctx, taskID)
	if err != nil {
		return nil, err
	}
	offset, limit, err := page(query)
	if err != nil {
		return nil, err
	}
	if offset > len(events) {
		offset = len(events)
	}
	end := min(offset+limit, len(events))
	items := make([]any, 0, end-offset)
	for _, event := range events[offset:end] {
		items = append(items, eventModel(event))
	}
	next := ""
	if end < len(events) {
		next = strconv.Itoa(end)
	}
	return marshal(map[string]any{"events": items, "next_cursor": next})
}

func (b *Backend) GetLogs(ctx context.Context, taskID string, follow bool, attemptID string, tailLines int) (string, <-chan string, error) {
	attempt, err := b.resolveAttempt(ctx, taskID, attemptID)
	if err != nil {
		return "", nil, mapStoreError(err)
	}
	initial, cursor, err := b.store.ReadLogTailCursor(ctx, taskID, attempt.ID, tailLines)
	if err != nil {
		return "", nil, mapStoreError(err)
	}
	if !follow {
		return initial, nil, nil
	}
	return initial, b.subscribe(ctx, taskID, attempt.ID, cursor), nil
}

func (b *Backend) GetPullRequest(ctx context.Context, taskID string) ([]byte, error) {
	attempt, err := b.resolveAttempt(ctx, taskID, "")
	if err != nil {
		return nil, mapStoreError(err)
	}
	result, err := b.pullRequestModel(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	return marshal(result)
}

func (b *Backend) taskPayload(ctx context.Context, record store.Task) ([]byte, error) {
	model, err := b.taskModel(ctx, record)
	if err != nil {
		return nil, err
	}
	return marshal(model)
}

func (b *Backend) taskModel(ctx context.Context, record store.Task) (map[string]any, error) {
	attempt, err := b.store.CurrentAttempt(ctx, record.ID)
	if err != nil {
		return nil, mapStoreError(err)
	}
	results, err := b.resultModels(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	model := map[string]any{
		"task_id": record.ID, "repository": record.Repository, "prompt": record.Prompt,
		"state": apiState(record), "created_at": record.CreatedAt, "updated_at": record.UpdatedAt,
		"current_attempt_id": record.CurrentAttemptID, "cancellation_requested": record.CancellationRequested,
		"validation_runs": results.validation,
		"git_result":      results.git, "pull_request": results.pullRequest,
	}
	if record.PRTitle != "" {
		model["pr_title"] = record.PRTitle
	}
	job, pod, err := b.kubernetesModels(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if job != nil {
		model["kubernetes_job"] = job
	}
	if pod != nil {
		model["kubernetes_pod"] = pod
	}
	return model, nil
}

func (b *Backend) attemptModel(ctx context.Context, attempt store.Attempt) (map[string]any, error) {
	results, err := b.resultModels(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	model := map[string]any{
		"attempt_id": attempt.ID, "task_id": attempt.TaskID, "number": attempt.Number,
		"immutable": attempt.Immutable, "state": apiTaskState(attempt.State), "created_at": attempt.CreatedAt,
		"validation_runs": results.validation, "git_result": results.git, "pull_request": results.pullRequest,
	}
	job, pod, err := b.kubernetesModels(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if job != nil {
		model["kubernetes_job"] = job
	}
	if pod != nil {
		model["kubernetes_pod"] = pod
	}
	return model, nil
}

func (b *Backend) kubernetesModels(ctx context.Context, attemptID string) (map[string]any, map[string]any, error) {
	job, pod, err := b.store.AttemptKubernetes(ctx, attemptID)
	if err != nil {
		return nil, nil, err
	}
	var jobModel map[string]any
	if job.Name != "" {
		jobModel = resourceModel("Job", job.APIVersion, job.Namespace, job.Name, job.UID, job.State, job.Reason, job.Message, job.StartedAt, job.CompletedAt)
	}
	var podModel map[string]any
	if pod.Name != "" {
		podModel = resourceModel("Pod", pod.APIVersion, pod.Namespace, pod.Name, pod.UID, pod.State, pod.Reason, pod.Message, pod.StartedAt, pod.CompletedAt)
	}
	return jobModel, podModel, nil
}

func resourceModel(kind, apiVersion, namespace, name, uid, state, reason, message string, startedAt, completedAt *time.Time) map[string]any {
	model := map[string]any{"state": state, "resource_identity": map[string]any{"api_version": apiVersion, "kind": kind, "namespace": namespace, "name": name, "uid": uid}}
	if reason != "" {
		model["reason"] = reason
	}
	if message != "" {
		model["message"] = message
	}
	if startedAt != nil {
		model["started_at"] = startedAt
	}
	if completedAt != nil {
		model["completed_at"] = completedAt
	}
	return model
}

type resultSet struct {
	validation  []any
	git         map[string]any
	pullRequest map[string]any
}

func (b *Backend) resultModels(ctx context.Context, attemptID string) (resultSet, error) {
	runs, err := b.store.ListValidationRuns(ctx, attemptID)
	if err != nil {
		return resultSet{}, err
	}
	validation := make([]any, 0, len(runs))
	for _, run := range runs {
		model := map[string]any{"run_id": run.ID, "name": run.Name, "state": run.State, "created_at": run.CreatedAt}
		if run.Summary != "" {
			model["summary"] = run.Summary
		}
		if run.Error != "" {
			model["error"] = errorModel("validation_failed", run.Error)
		}
		validation = append(validation, model)
	}
	git := map[string]any{"state": "not_run"}
	if durable, err := b.store.GetGitResult(ctx, attemptID); err == nil {
		state := durable.State
		if state == "pushed" {
			state = "succeeded"
		} else if state == "candidate" {
			state = "running"
		}
		git = map[string]any{"state": state}
		if durable.Branch != "" {
			git["branch"] = durable.Branch
		}
		if durable.CommitSHA != "" {
			git["commit_sha"] = durable.CommitSHA
		}
		if durable.Error != "" {
			git["error"] = errorModel("git_failed", durable.Error)
		}
	} else if !errors.Is(err, store.ErrNotFound) {
		return resultSet{}, err
	}
	pullRequest, err := b.pullRequestModel(ctx, attemptID)
	if err != nil {
		return resultSet{}, err
	}
	return resultSet{validation: validation, git: git, pullRequest: pullRequest}, nil
}

func (b *Backend) pullRequestModel(ctx context.Context, attemptID string) (map[string]any, error) {
	durable, err := b.store.GetPullRequest(ctx, attemptID)
	if errors.Is(err, store.ErrNotFound) {
		return map[string]any{"state": "not_created"}, nil
	}
	if err != nil {
		return nil, err
	}
	state := durable.State
	if state == "open" {
		state = "created"
	} else if state == "reported" {
		state = "creating"
	}
	model := map[string]any{"state": state}
	if durable.Number > 0 {
		model["number"] = durable.Number
	}
	for key, value := range map[string]string{
		"url": durable.URL, "title": durable.Title, "head_branch": durable.HeadBranch, "base_branch": durable.BaseBranch,
	} {
		if value != "" {
			model[key] = value
		}
	}
	if durable.Error != "" {
		model["error"] = errorModel("pull_request_failed", durable.Error)
	}
	return model, nil
}

func (b *Backend) resolveAttempt(ctx context.Context, taskID, attemptID string) (store.Attempt, error) {
	if attemptID == "" {
		return b.store.CurrentAttempt(ctx, taskID)
	}
	return b.store.GetAttempt(ctx, taskID, attemptID)
}

func (b *Backend) subscribe(ctx context.Context, taskID, attemptID string, cursor int64) <-chan string {
	updates := make(chan string, 1)
	go func() {
		defer close(updates)
		for {
			chunks, err := b.store.ReadLogChunksAfter(ctx, taskID, attemptID, cursor, 64)
			if err == nil {
				for _, chunk := range chunks {
					select {
					case updates <- chunk.Content:
						cursor = chunk.Sequence
					case <-ctx.Done():
						return
					}
				}
				if len(chunks) > 0 {
					continue
				}
				complete, completeErr := b.store.AttemptFollowComplete(ctx, taskID, attemptID)
				if completeErr == nil && complete {
					return
				}
			}
			timer := time.NewTimer(25 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return updates
}

func eventModel(event store.TransitionEvent) map[string]any {
	model := map[string]any{
		"event_id": event.ID, "task_id": event.TaskID, "attempt_id": event.AttemptID,
		"occurred_at": event.OccurredAt, "from_state": apiTaskState(event.FromState),
		"to_state": apiTaskState(event.ToState), "reason": event.Reason, "trigger": event.Trigger,
		"metadata": jsonObject(event.Metadata),
	}
	if resource := jsonObject(event.ResourceIdentity); len(resource) > 0 {
		model["resource_identity"] = resource
	}
	if eventError := jsonObject(event.Error); len(eventError) > 0 {
		model["error"] = eventError
	}
	return model
}

func jsonObject(value string) map[string]any {
	result := make(map[string]any)
	_ = json.Unmarshal([]byte(value), &result)
	return result
}

func apiState(record store.Task) string {
	return apiTaskState(record.State)
}

func apiTaskState(state task.State) string {
	return string(state)
}

func page(query url.Values) (int, int, error) {
	limit := 50
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			return 0, 0, api.ErrInvalid
		}
		limit = parsed
	}
	offset := 0
	if raw := query.Get("cursor"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return 0, 0, api.ErrInvalid
		}
		offset = parsed
	}
	return offset, limit, nil
}

func contains(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func marshal(value any) ([]byte, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal API payload: %w", err)
	}
	return payload, nil
}

func errorModel(code, message string) map[string]any {
	return map[string]any{"code": code, "message": message}
}

func mapStoreError(err error) error {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return fmt.Errorf("%w: %w", api.ErrNotFound, err)
	case errors.Is(err, store.ErrConflict):
		return fmt.Errorf("%w: %w", api.ErrConflict, err)
	default:
		return err
	}
}

func mapCreateError(err error) error {
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
		return mapStoreError(err)
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "unknown configured repository") || strings.Contains(message, " is empty") {
		return fmt.Errorf("%w: %w", api.ErrInvalid, err)
	}
	return err
}

func mapActionError(err error) error {
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, store.ErrConflict) {
		return mapStoreError(err)
	}
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "requires") || strings.Contains(message, "state") || strings.Contains(message, "terminal") || strings.Contains(message, "cancellation") {
		return fmt.Errorf("%w: %w", api.ErrConflict, err)
	}
	return err
}
