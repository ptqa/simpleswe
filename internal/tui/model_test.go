package tui

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestModelRefreshPreservesSelectedTask(t *testing.T) {
	model := NewModel(3)
	model.RefreshTasks([]Task{
		{ID: "task-1", State: "running"},
		{ID: "task-2", State: "queued"},
	})
	model.SetSelectedTask("task-2")

	model.RefreshTasks([]Task{
		{ID: "task-3", State: "failed"},
		{ID: "task-2", State: "running"},
	})

	if got := model.SelectedTaskID(); got != "task-2" {
		t.Fatalf("SelectedTaskID() = %q, want task-2", got)
	}
}

func TestModelLogBufferIsBounded(t *testing.T) {
	model := NewModel(3)
	for i := 1; i <= 5; i++ {
		model.AppendLog("line-" + string(rune('0'+i)))
	}

	if got, want := model.Logs(), []string{"line-3", "line-4", "line-5"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Logs() = %#v, want %#v", got, want)
	}
}

func TestModelTracksExplicitConnectivityLostAndRestoredStates(t *testing.T) {
	model := NewModel(3)
	model.SetConnectivity(ConnectivityLost)
	if got := model.Connectivity(); got != ConnectivityLost {
		t.Fatalf("Connectivity() after loss = %q, want %q", got, ConnectivityLost)
	}

	model.SetConnectivity(ConnectivityRestored)
	if got := model.Connectivity(); got != ConnectivityRestored {
		t.Fatalf("Connectivity() after restore = %q, want %q", got, ConnectivityRestored)
	}
}

func TestModelMapsActionKeys(t *testing.T) {
	tests := map[rune]Action{
		'\r':   ActionDetails,
		'\n':   ActionDetails,
		'l':    ActionLogs,
		'e':    ActionEvents,
		'j':    ActionNext,
		'k':    ActionPrevious,
		'g':    ActionFirst,
		'G':    ActionLast,
		'd':    ActionJob,
		'p':    ActionPod,
		's':    ActionShell,
		'r':    ActionRetry,
		'\x04': ActionCancel,
		'R':    ActionRefresh,
		'/':    ActionSearch,
		't':    ActionTheme,
		'?':    ActionHelp,
		'h':    ActionBackOrQuit,
		'q':    ActionBackOrQuit,
	}
	model := NewModel(3)
	for key, want := range tests {
		if got := model.ActionForKey(key); got != want {
			t.Errorf("ActionForKey(%q) = %q, want %q", key, got, want)
		}
	}
	if got := model.ActionForKey('z'); got != ActionNone {
		t.Fatalf("ActionForKey(z) = %q, want %q", got, ActionNone)
	}
}

func TestModelSelectionCopiesAndEmptyStates(t *testing.T) {
	model := NewModel(-1)
	model.AppendLog("discarded")
	if model.Logs() != nil {
		t.Fatalf("negative-capacity logs = %#v, want nil", model.Logs())
	}

	tasks := []Task{{ID: "task-1"}, {ID: "task-2"}}
	model.RefreshTasks(tasks)
	tasks[0].ID = "mutated"
	gotTasks := model.Tasks()
	gotTasks[0].ID = "also-mutated"
	if got := model.Tasks()[0].ID; got != "task-1" {
		t.Fatalf("Tasks did not copy input/output: %q", got)
	}
	if selected, err := model.SelectedTask(); err != nil || selected.ID != "task-1" {
		t.Fatalf("SelectedTask() = %#v, %v", selected, err)
	}
	model.SetSelectedTask("missing")
	if _, err := model.SelectedTask(); !errors.Is(err, errNoSelectedTask) {
		t.Fatalf("SelectedTask missing error = %v", err)
	}
	model.RefreshTasks([]Task{{ID: "task-3"}})
	if got := model.SelectedTaskID(); got != "task-3" {
		t.Fatalf("replacement selection = %q", got)
	}
	model.RefreshTasks(nil)
	if got := model.SelectedTaskID(); got != "" {
		t.Fatalf("empty selection = %q", got)
	}
}

func TestModelConnectivityMessagesAndLogReset(t *testing.T) {
	model := NewModel(2)
	for state, want := range map[ConnectivityState]string{
		ConnectivityUnknown:   "",
		ConnectivityConnected: "controller connected",
		ConnectivityLost:      "controller connection lost",
		ConnectivityRestored:  "controller connection restored",
	} {
		model.SetConnectivity(state)
		if got := model.ConnectivityMessage(); got != want {
			t.Errorf("ConnectivityMessage(%q) = %q, want %q", state, got, want)
		}
	}
	model.SetConnectivity(ConnectivityLost, "custom")
	if got := model.ConnectivityMessage(); got != "custom" {
		t.Fatalf("custom connectivity message = %q", got)
	}
	model.AppendLog("one")
	model.AppendLog("two")
	model.ResetLogs()
	if got := model.Logs(); got != nil {
		t.Fatalf("reset logs = %#v", got)
	}
}

func TestModelDetailIsCopied(t *testing.T) {
	detail := TaskDetail{
		Task:     Task{ID: "task", ValidationRuns: []ValidationRun{{ID: "run"}}},
		Attempts: []Attempt{{ID: "attempt"}}, Events: []Event{{ID: "event"}},
	}
	model := NewModel(1)
	model.SetDetail(detail)
	detail.Attempts[0].ID = "changed"
	detail.Events[0].ID = "changed"
	detail.Task.ValidationRuns[0].ID = "changed"
	got := model.Detail()
	got.Attempts[0].ID = "output-changed"
	got.Events[0].ID = "output-changed"
	got.Task.ValidationRuns[0].ID = "output-changed"
	again := model.Detail()
	if again.Attempts[0].ID != "attempt" || again.Events[0].ID != "event" || again.Task.ValidationRuns[0].ID != "run" {
		t.Fatalf("detail aliases caller data: %#v", again)
	}
}

func TestOptionsBoundsTaskLimitToAPIContract(t *testing.T) {
	if got := (Options{}).withDefaults().TaskLimit; got != 100 {
		t.Fatalf("default TaskLimit = %d, want 100", got)
	}
	if got := (Options{TaskLimit: 101}).withDefaults().TaskLimit; got != 100 {
		t.Fatalf("bounded TaskLimit = %d, want 100", got)
	}
}

func TestModelDetailContainsTaskAttemptAndEventFieldsForViews(t *testing.T) {
	created := time.Date(2026, time.August, 6, 0, 0, 0, 0, time.UTC)
	task := Task{
		ID:               "task-1",
		Repository:       "https://bitbucket.example/acme/widget",
		Prompt:           "fix the bug",
		State:            "running",
		CreatedAt:        created,
		UpdatedAt:        created.Add(time.Minute),
		CurrentAttemptID: "attempt-2",
	}
	attempt := Attempt{
		ID:        "attempt-2",
		TaskID:    "task-1",
		Number:    2,
		State:     "running",
		CreatedAt: created,
		Immutable: true,
	}
	event := Event{
		ID:         "event-1",
		TaskID:     "task-1",
		AttemptID:  "attempt-2",
		OccurredAt: created,
		FromState:  "queued",
		ToState:    "running",
		Reason:     "worker started",
		Trigger:    "controller",
	}

	model := NewModel(3)
	model.SetDetail(TaskDetail{Task: task, Attempts: []Attempt{attempt}, Events: []Event{event}})
	got := model.Detail()
	if got.Task.ID != task.ID || got.Task.Repository != task.Repository || got.Task.Prompt != task.Prompt || got.Task.State != task.State || got.Task.CurrentAttemptID != task.CurrentAttemptID {
		t.Fatalf("detail task = %#v, want task view fields preserved", got.Task)
	}
	if !reflect.DeepEqual(got.Attempts, []Attempt{attempt}) {
		t.Fatalf("detail attempts = %#v, want %#v", got.Attempts, []Attempt{attempt})
	}
	if !reflect.DeepEqual(got.Events, []Event{event}) {
		t.Fatalf("detail events = %#v, want %#v", got.Events, []Event{event})
	}
}
