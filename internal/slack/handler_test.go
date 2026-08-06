package slack

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

var (
	_ TaskController = (*fakeTaskController)(nil)
	_ Messenger      = (*fakeMessenger)(nil)
)

const acceptedResponse = "Task: swe-123\nRepository: workspace/repository\nState: received\nAttempt: 1"

func TestHandleAppMentionRunPassesEventIDAndPreservesOrigin(t *testing.T) {
	controller := newFakeTaskController()
	messenger := newFakeMessenger()
	handler := NewHandler(controller, messenger)
	event := Event{
		Kind:    "app_mention",
		EventID: "Ev-unchanged-123",
		Text:    "<@Ubot> run workspace/repository fix the flaky test",
		Origin: protocol.SlackOrigin{
			WorkspaceID: "workspace-1",
			ChannelID:   "channel-1",
			MessageTS:   "1712345678.000100",
			ThreadTS:    "1712345678.000001",
			UserID:      "user-1",
		},
	}

	if err := handler.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("handle app mention: %v", err)
	}
	if len(controller.createParams) != 1 {
		t.Fatalf("CreateTask calls = %d; want 1", len(controller.createParams))
	}
	gotParams := controller.createParams[0]
	wantParams := store.CreateTaskParams{
		Repository:   "workspace/repository",
		Prompt:       "fix the flaky test",
		SlackEventID: "Ev-unchanged-123",
	}
	if !reflect.DeepEqual(gotParams, wantParams) {
		t.Fatalf("CreateTask params = %#v, want %#v", gotParams, wantParams)
	}

	assertOneMessage(t, messenger, event.Origin, acceptedResponse)
}

func TestHandleSlashRunUsesMessageTSWhenNotAlreadyThreaded(t *testing.T) {
	controller := newFakeTaskController()
	messenger := newFakeMessenger()
	handler := NewHandler(controller, messenger)
	event := Event{
		Kind:    "slash_command",
		EventID: "slash-run-1",
		Text:    "run workspace/repository fix the flaky test",
		Origin: protocol.SlackOrigin{
			WorkspaceID: "workspace-1",
			ChannelID:   "channel-1",
			MessageTS:   "1712345678.000200",
			UserID:      "user-1",
		},
	}

	if err := handler.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("handle slash run: %v", err)
	}
	wantOrigin := event.Origin
	wantOrigin.ThreadTS = event.Origin.MessageTS
	assertOneMessage(t, messenger, wantOrigin, acceptedResponse)
}

func TestHandleSlashTaskCommands(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		check func(*testing.T, *fakeTaskController)
	}{
		{
			name: "status",
			text: "status swe-123",
			check: func(t *testing.T, controller *fakeTaskController) {
				t.Helper()
				if !reflect.DeepEqual(controller.getIDs, []string{"swe-123"}) {
					t.Fatalf("GetTask IDs = %#v; want [swe-123]", controller.getIDs)
				}
			},
		},
		{
			name: "cancel",
			text: "cancel swe-123",
			check: func(t *testing.T, controller *fakeTaskController) {
				t.Helper()
				if !reflect.DeepEqual(controller.cancelIDs, []string{"swe-123"}) {
					t.Fatalf("RequestCancellation IDs = %#v; want [swe-123]", controller.cancelIDs)
				}
			},
		},
		{
			name: "retry",
			text: "retry swe-123",
			check: func(t *testing.T, controller *fakeTaskController) {
				t.Helper()
				if !reflect.DeepEqual(controller.retryIDs, []string{"swe-123"}) {
					t.Fatalf("RetryTaskWithKey IDs = %#v; want [swe-123]", controller.retryIDs)
				}
				if !reflect.DeepEqual(controller.retryKeys, []string{"slash-retry"}) {
					t.Fatalf("RetryTaskWithKey keys = %#v; want [slash-retry]", controller.retryKeys)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := newFakeTaskController()
			messenger := newFakeMessenger()
			handler := NewHandler(controller, messenger)
			event := Event{
				Kind:    "slash_command",
				EventID: "slash-" + tt.name,
				Text:    tt.text,
				Origin: protocol.SlackOrigin{
					ChannelID: "channel-1",
					MessageTS: "1712345678.000300",
					UserID:    "user-1",
				},
			}
			if err := handler.HandleEvent(context.Background(), event); err != nil {
				t.Fatalf("handle %s: %v", tt.name, err)
			}
			if len(messenger.messages) != 1 {
				t.Fatalf("messages = %d; want 1", len(messenger.messages))
			}
			if messenger.messages[0].Text != acceptedResponse {
				t.Fatalf("%s response = %q; want %q", tt.name, messenger.messages[0].Text, acceptedResponse)
			}
			tt.check(t, controller)
		})
	}
}

func TestHandlerReplayCanPostDuplicateResponseAfterSideEffectWindow(t *testing.T) {
	controller := newFakeTaskController()
	messenger := newFakeMessenger()
	handler := NewHandler(controller, messenger)
	event := Event{
		Kind:    "slash_command",
		EventID: "duplicate-event-1",
		Text:    "run workspace/repository fix the flaky test",
		Origin: protocol.SlackOrigin{
			ChannelID: "channel-1",
			MessageTS: "1712345678.000400",
			UserID:    "user-1",
		},
	}

	for i := 0; i < 2; i++ {
		if err := handler.HandleEvent(context.Background(), event); err != nil {
			t.Fatalf("handle duplicate event %d: %v", i+1, err)
		}
	}
	if len(controller.createParams) != 2 {
		t.Fatalf("CreateTask calls = %d; want 2 idempotent attempts", len(controller.createParams))
	}
	if controller.createParams[0].SlackEventID != controller.createParams[1].SlackEventID ||
		controller.createParams[0].SlackEventID != event.EventID {
		t.Fatalf("duplicate event IDs = %#v; want unchanged %q", controller.createParams, event.EventID)
	}
	// The durable transport normally suppresses handled events. If the process
	// crashes after posting but before marking handled, replay can post again;
	// task creation remains safe because SlackEventID is the durable key.
	if len(messenger.messages) != 2 {
		t.Fatalf("direct handler replay posted %d task responses; want 2 at-least-once deliveries", len(messenger.messages))
	}
}

func TestMalformedCommandsReturnUsageWithoutCreatingTask(t *testing.T) {
	for _, text := range []string{
		"",
		"<@Ubot>",
		"run workspace/repository",
		"status",
		"cancel swe-123 now",
		"retry",
		"deploy workspace/repository now",
	} {
		t.Run(text, func(t *testing.T) {
			controller := newFakeTaskController()
			messenger := newFakeMessenger()
			handler := NewHandler(controller, messenger)
			event := Event{
				Kind:    "slash_command",
				EventID: "malformed-1",
				Text:    text,
				Origin:  protocol.SlackOrigin{ChannelID: "channel-1", MessageTS: "1712345678.000500"},
			}

			if err := handler.HandleEvent(context.Background(), event); err != nil {
				t.Fatalf("handle malformed command: %v", err)
			}
			if len(controller.createParams) != 0 {
				t.Fatalf("CreateTask calls = %d; want 0", len(controller.createParams))
			}
			if len(messenger.messages) != 1 || !strings.Contains(strings.ToLower(messenger.messages[0].Text), "usage") {
				t.Fatalf("malformed command response = %#v; want usage", messenger.messages)
			}
		})
	}
}

func TestTaskErrorIncludesInspectAndRetryCommands(t *testing.T) {
	controller := newFakeTaskController()
	controller.retryErr = errors.New("task is still running")
	messenger := newFakeMessenger()
	handler := NewHandler(controller, messenger)
	event := Event{
		Kind:    "slash_command",
		EventID: "retry-error-1",
		Text:    "retry swe-123",
		Origin:  protocol.SlackOrigin{ChannelID: "channel-1", MessageTS: "1712345678.000600"},
	}

	if err := handler.HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("handle retry error: %v", err)
	}
	if len(messenger.messages) != 1 {
		t.Fatalf("messages = %d; want 1 concise error", len(messenger.messages))
	}
	response := messenger.messages[0].Text
	for _, want := range []string{"swe-123", "status swe-123", "retry swe-123", "task is still running"} {
		if !strings.Contains(response, want) {
			t.Errorf("retry error response %q does not contain %q", response, want)
		}
	}
}

func TestWorkerLinesPostOnlyMeaningfulTransitions(t *testing.T) {
	controller := newFakeTaskController()
	messenger := newFakeMessenger()
	handler := NewHandler(controller, messenger)
	origin := protocol.SlackOrigin{ChannelID: "channel-1", ThreadTS: "1712345678.000700", UserID: "user-1"}

	for _, line := range []string{
		"plain worker output",
		"INFO downloading dependencies",
		"@@simpleswe:{\"type\":\"log\",\"task_id\":\"swe-123\",\"message\":\"arbitrary log\"}",
	} {
		if err := handler.HandleWorkerLine(context.Background(), origin, line); err != nil {
			t.Fatalf("handle arbitrary worker line %q: %v", line, err)
		}
	}
	if len(messenger.messages) != 0 {
		t.Fatalf("arbitrary worker lines posted %#v; want no messages", messenger.messages)
	}

	transition, err := protocol.EncodeEvent(protocol.Event{
		Type:    "transition",
		TaskID:  "swe-123",
		Message: "entered running",
	})
	if err != nil {
		t.Fatalf("encode transition: %v", err)
	}
	if err := handler.HandleWorkerLine(context.Background(), origin, transition); err != nil {
		t.Fatalf("handle transition: %v", err)
	}
	if len(messenger.messages) != 1 || !strings.Contains(messenger.messages[0].Text, "entered running") {
		t.Fatalf("transition response = %#v; want one meaningful update", messenger.messages)
	}
}

func assertOneMessage(t *testing.T, messenger *fakeMessenger, wantOrigin protocol.SlackOrigin, wantText string) {
	t.Helper()
	if len(messenger.messages) != 1 {
		t.Fatalf("messages = %d; want 1", len(messenger.messages))
	}
	message := messenger.messages[0]
	if !reflect.DeepEqual(message.Origin, wantOrigin) {
		t.Errorf("message origin = %#v, want %#v", message.Origin, wantOrigin)
	}
	if message.Text != wantText {
		t.Errorf("message text = %q, want %q", message.Text, wantText)
	}
}

type fakeTaskController struct {
	task         store.Task
	attempts     []store.Attempt
	createParams []store.CreateTaskParams
	getIDs       []string
	cancelIDs    []string
	retryIDs     []string
	retryKeys    []string
	createErr    error
	getErr       error
	cancelErr    error
	retryErr     error
}

func newFakeTaskController() *fakeTaskController {
	return &fakeTaskController{
		task: store.Task{
			ID:         "swe-123",
			Repository: "workspace/repository",
			Prompt:     "fix the flaky test",
			State:      task.RECEIVED,
		},
		attempts: []store.Attempt{{ID: "attempt-1", TaskID: "swe-123", Number: 1, State: task.RECEIVED}},
	}
}

func (f *fakeTaskController) CreateTask(_ context.Context, params store.CreateTaskParams) (store.Task, error) {
	f.createParams = append(f.createParams, params)
	if f.createErr != nil {
		return store.Task{}, f.createErr
	}
	return f.task, nil
}

func (f *fakeTaskController) GetTask(_ context.Context, taskID string) (store.Task, error) {
	f.getIDs = append(f.getIDs, taskID)
	if f.getErr != nil {
		return store.Task{}, f.getErr
	}
	return f.task, nil
}

func (f *fakeTaskController) ListAttempts(_ context.Context, taskID string) ([]store.Attempt, error) {
	if taskID != f.task.ID {
		return nil, errors.New("unexpected task ID")
	}
	return append([]store.Attempt(nil), f.attempts...), nil
}

func (f *fakeTaskController) RequestCancellation(_ context.Context, taskID string) error {
	f.cancelIDs = append(f.cancelIDs, taskID)
	return f.cancelErr
}

func (f *fakeTaskController) RetryTaskWithKey(_ context.Context, taskID, key string) (store.Attempt, error) {
	f.retryIDs = append(f.retryIDs, taskID)
	f.retryKeys = append(f.retryKeys, key)
	if f.retryErr != nil {
		return store.Attempt{}, f.retryErr
	}
	return store.Attempt{ID: "attempt-2", TaskID: taskID, Number: 2, State: f.task.State}, nil
}

type fakeMessenger struct {
	messages []postedMessage
}

type postedMessage struct {
	Origin protocol.SlackOrigin
	Text   string
}

func newFakeMessenger() *fakeMessenger {
	return &fakeMessenger{}
}

func (f *fakeMessenger) PostMessage(_ context.Context, origin protocol.SlackOrigin, text string) error {
	f.messages = append(f.messages, postedMessage{Origin: origin, Text: text})
	return nil
}
