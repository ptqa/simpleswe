package store

import (
	"context"
	"errors"
	"testing"

	"github.com/simpleswe/simpleswe/internal/task"
)

func TestValidationLifecyclePersistsOrderedIdempotentResults(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "validate"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	advanceStoreTask(t, db, created.ID, task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING, task.RUNNING, task.AGENT_RUNNING)
	if err := db.Transition(ctx, created.ID, task.AGENT_RUNNING, task.VALIDATING, TransitionParams{
		Reason: "agent completed", Trigger: "controller",
		Validation: &ValidationTransition{Name: "go test ./...", State: "running", Summary: "tests started", EventID: "start-1"},
	}); err != nil {
		t.Fatalf("start validation during transition: %v", err)
	}
	runs, err := db.ListValidationRuns(ctx, created.CurrentAttemptID)
	if err != nil || len(runs) != 1 || runs[0].Sequence != 1 || runs[0].State != "running" || runs[0].Name != "go test ./..." {
		t.Fatalf("initial validation runs = %#v, %v", runs, err)
	}
	if err := db.RecordValidationStarted(ctx, created.ID, created.CurrentAttemptID, "golangci-lint", "lint started"); !errors.Is(err, ErrConflict) {
		t.Fatalf("start validation while one runs = %v, want ErrConflict", err)
	}
	if err := db.RecordValidationResult(ctx, created.ID, created.CurrentAttemptID, "wrong command", "ok", 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("mismatched validation result = %v, want ErrConflict", err)
	}
	if err := db.RecordValidationResultOnce(ctx, created.ID, created.CurrentAttemptID, "go test ./...", "all tests passed", 0, "result-1"); err != nil {
		t.Fatalf("complete first validation: %v", err)
	}
	if err := db.RecordValidationResultOnce(ctx, created.ID, "wrong-attempt", "ignored", "ignored", 1, "result-1"); err != nil {
		t.Fatalf("replay first result event: %v", err)
	}
	if err := db.RecordValidationStartedOnce(ctx, created.ID, created.CurrentAttemptID, "golangci-lint", "lint started", "start-2"); err != nil {
		t.Fatalf("start second validation: %v", err)
	}
	if err := db.RecordValidationStartedOnce(ctx, "missing", "wrong-attempt", "ignored", "ignored", "start-2"); err != nil {
		t.Fatalf("replay second start event: %v", err)
	}
	if err := db.RecordValidationResultOnce(ctx, created.ID, created.CurrentAttemptID, "golangci-lint", "lint failed", 2, "result-2"); err != nil {
		t.Fatalf("complete failed validation: %v", err)
	}
	if err := db.RecordValidationFailureDetail(ctx, created.CurrentAttemptID, "golangci-lint", "specific lint output", 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("failure detail with wrong exit = %v, want ErrConflict", err)
	}
	if err := db.RecordValidationFailureDetail(ctx, created.CurrentAttemptID, "golangci-lint", "specific lint output", 2); err != nil {
		t.Fatalf("record failure detail: %v", err)
	}
	if err := db.MarkValidationComplete(ctx, created.ID, created.CurrentAttemptID, "unknown"); err == nil {
		t.Fatal("aggregate validation accepted an invalid state")
	}
	if err := db.MarkValidationComplete(ctx, created.ID, created.CurrentAttemptID, "failed"); err != nil {
		t.Fatalf("mark aggregate validation failed: %v", err)
	}
	runs, err = db.ListValidationRuns(ctx, created.CurrentAttemptID)
	if err != nil || len(runs) != 2 {
		t.Fatalf("completed validation runs = %#v, %v", runs, err)
	}
	if runs[0].Sequence != 1 || runs[0].State != "succeeded" || runs[0].ExitCode != 0 || runs[0].Summary != "all tests passed" {
		t.Fatalf("first validation run = %#v", runs[0])
	}
	if runs[1].Sequence != 2 || runs[1].State != "failed" || runs[1].ExitCode != 2 || runs[1].Summary != "specific lint output" || runs[1].Error != "specific lint output" {
		t.Fatalf("second validation run = %#v", runs[1])
	}
	attempt, err := db.CurrentAttempt(ctx, created.ID)
	if err != nil || attempt.ValidationState != "failed" {
		t.Fatalf("aggregate validation state = %#v, %v", attempt, err)
	}
	if err := db.Transition(ctx, created.ID, task.VALIDATING, task.COMMITTING, TransitionParams{
		Reason: "results accepted", Trigger: "controller",
		Validation: &ValidationTransition{State: "succeeded", Summary: "aggregate complete"},
	}); err != nil {
		t.Fatalf("complete validation transition: %v", err)
	}
	if err := db.RecordValidationStarted(ctx, created.ID, created.CurrentAttemptID, "late", "late"); !errors.Is(err, ErrConflict) {
		t.Fatalf("start validation outside validating = %v, want ErrConflict", err)
	}
	if err := db.RecordObservation(ctx, created.ID, "reconciled durable validation", "system"); err != nil {
		t.Fatalf("record observation: %v", err)
	}
	events, err := db.ListEvents(ctx, created.ID)
	if err != nil || events[len(events)-1].Reason != "reconciled durable validation" || events[len(events)-1].FromState != task.COMMITTING || events[len(events)-1].ToState != task.COMMITTING {
		t.Fatalf("observation event = %#v, %v", events, err)
	}
}

func TestValidationLifecycleRejectsIncompleteAndInvalidEvents(t *testing.T) {
	ctx := context.Background()
	db := openTestStore(t)
	if err := db.RecordObservation(ctx, "missing", "", "system"); err == nil {
		t.Fatal("observation accepted an empty reason")
	}
	if err := db.RecordObservation(ctx, "missing", "reason", "invalid"); err == nil {
		t.Fatal("observation accepted an invalid trigger")
	}
	if err := db.RecordObservation(ctx, "missing", "reason", "system"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("observation for missing task = %v, want ErrNotFound", err)
	}

	created, err := db.CreateTask(ctx, CreateTaskParams{Repository: "repo", Prompt: "validate"})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.QUEUED, TransitionParams{Reason: "", Trigger: "system"}); err == nil {
		t.Fatal("transition accepted an empty reason")
	}
	if err := db.Transition(ctx, created.ID, task.RECEIVED, task.QUEUED, TransitionParams{Reason: "queue", Trigger: "invalid"}); err == nil {
		t.Fatal("transition accepted an invalid trigger")
	}
	if err := db.Transition(ctx, "missing", task.RECEIVED, task.QUEUED, TransitionParams{Reason: "queue", Trigger: "system"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("transition missing task = %v, want ErrNotFound", err)
	}
	if err := db.Transition(ctx, created.ID, task.QUEUED, task.CREATING_JOB, TransitionParams{Reason: "wrong source", Trigger: "system"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("transition with stale source = %v, want ErrConflict", err)
	}
	advanceStoreTask(t, db, created.ID, task.RECEIVED, task.QUEUED, task.CREATING_JOB, task.JOB_PENDING, task.RUNNING, task.AGENT_RUNNING)
	if err := db.Transition(ctx, created.ID, task.AGENT_RUNNING, task.VALIDATING, TransitionParams{
		Reason: "validate", Trigger: "system", Validation: &ValidationTransition{State: "running"},
	}); err == nil {
		t.Fatal("validation transition accepted an empty command")
	}
	if err := db.Transition(ctx, created.ID, task.AGENT_RUNNING, task.VALIDATING, TransitionParams{
		Reason: "validate", Trigger: "system", Validation: &ValidationTransition{Name: "test", State: "invalid"},
	}); err == nil {
		t.Fatal("validation transition accepted an invalid validation state")
	}
	if err := db.Transition(ctx, created.ID, task.AGENT_RUNNING, task.VALIDATING, TransitionParams{Reason: "validate", Trigger: "system"}); err != nil {
		t.Fatalf("enter validating: %v", err)
	}
	if err := db.RecordValidationResult(ctx, created.ID, created.CurrentAttemptID, "test", "none", 0); !errors.Is(err, ErrConflict) {
		t.Fatalf("result without running command = %v, want ErrConflict", err)
	}
	if err := db.MarkValidationComplete(ctx, created.ID, created.CurrentAttemptID, "succeeded"); !errors.Is(err, ErrConflict) {
		t.Fatalf("aggregate without runs = %v, want ErrConflict", err)
	}
	if err := db.Transition(ctx, created.ID, task.VALIDATING, task.COMMITTING, TransitionParams{
		Reason: "invalid finish", Trigger: "system", Validation: &ValidationTransition{State: "failed"},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("failed completion without running command = %v, want ErrConflict", err)
	}
	if err := db.RecordValidationStartedOnce(ctx, "missing", "missing", "test", "start", "new-event"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("validation start for missing task = %v, want ErrNotFound", err)
	}
	if err := db.RecordValidationResultOnce(ctx, "missing", "missing", "test", "result", 0, "new-result"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("validation result for missing task = %v, want ErrNotFound", err)
	}
}

func advanceStoreTask(t *testing.T, db *Store, taskID string, states ...task.State) {
	t.Helper()
	for i := 1; i < len(states); i++ {
		if err := db.Transition(context.Background(), taskID, states[i-1], states[i], TransitionParams{Reason: "advance", Trigger: "system"}); err != nil {
			t.Fatalf("transition %q -> %q: %v", states[i-1], states[i], err)
		}
	}
}
