package slack

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestHandleEventRejectsInvalidEnvelopeAndDependencies(t *testing.T) {
	valid := Event{
		Kind: EventKindSlashCommand, EventID: "event-1", Text: "status swe-123",
		Origin: protocol.SlackOrigin{ChannelID: "channel-1"},
	}
	tests := []struct {
		name    string
		ctx     context.Context
		event   Event
		handler *Handler
		want    string
	}{
		{"nil context", nil, valid, NewHandler(newFakeTaskController(), newFakeMessenger()), "context is nil"},
		{"empty event ID", context.Background(), func() Event { e := valid; e.EventID = " "; return e }(), NewHandler(newFakeTaskController(), newFakeMessenger()), "event ID is empty"},
		{"empty channel", context.Background(), func() Event { e := valid; e.Origin.ChannelID = " "; return e }(), NewHandler(newFakeTaskController(), newFakeMessenger()), "channel ID is empty"},
		{"mention without timestamp", context.Background(), func() Event { e := valid; e.Kind = EventKindAppMention; return e }(), NewHandler(newFakeTaskController(), newFakeMessenger()), "message timestamp is empty"},
		{"unsupported kind", context.Background(), func() Event { e := valid; e.Kind = "reaction"; e.Origin.MessageTS = "123.45"; return e }(), NewHandler(newFakeTaskController(), newFakeMessenger()), "unsupported Slack event kind"},
		{"nil controller", context.Background(), valid, NewHandler(nil, newFakeMessenger()), "task controller is nil"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.handler.HandleEvent(test.ctx, test.event)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("HandleEvent error = %v; want containing %q", err, test.want)
			}
		})
	}
}

func TestHandleEventReportsControllerAndMessengerFailures(t *testing.T) {
	controller := newFakeTaskController()
	messenger := newFakeMessenger()
	handler := NewHandler(controller, messenger)
	event := Event{
		Kind: EventKindSlashCommand, EventID: "event-1",
		Origin: protocol.SlackOrigin{ChannelID: "channel-1"},
	}

	for _, test := range []struct {
		name string
		text string
		set  func()
	}{
		{"create", "run acme/repo fix it", func() { controller.createErr = errors.New("create failed") }},
		{"status", "status swe-123", func() { controller.getErr = errors.New("lookup failed") }},
		{"cancel", "cancel swe-123", func() { controller.cancelErr = errors.New("cancel failed") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			*controller = *newFakeTaskController()
			messenger.messages = nil
			test.set()
			event.Text = test.text
			if err := handler.HandleEvent(context.Background(), event); err != nil {
				t.Fatalf("HandleEvent returned posting error: %v", err)
			}
			if len(messenger.messages) != 1 || !strings.Contains(messenger.messages[0].Text, "failed") {
				t.Fatalf("failure response = %#v; want one safe failure", messenger.messages)
			}
		})
	}

	wantErr := errors.New("chat unavailable")
	err := NewHandler(controller, failingMessenger{err: wantErr}).HandleEvent(context.Background(), Event{
		Kind: EventKindSlashCommand, EventID: "event-2", Text: "not-a-command",
		Origin: protocol.SlackOrigin{ChannelID: "channel-1"},
	})
	if !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "post Slack message") {
		t.Fatalf("messenger error = %v; want wrapped chat error", err)
	}

	err = NewHandler(controller, nil).HandleEvent(context.Background(), Event{
		Kind: EventKindSlashCommand, EventID: "event-3", Text: "not-a-command",
		Origin: protocol.SlackOrigin{ChannelID: "channel-1"},
	})
	if err == nil || !strings.Contains(err.Error(), "messenger is nil") {
		t.Fatalf("nil messenger error = %v", err)
	}
}

func TestRunUsesOriginAwareController(t *testing.T) {
	controller := &originController{fakeTaskController: newFakeTaskController()}
	messenger := newFakeMessenger()
	origin := protocol.SlackOrigin{ChannelID: "channel-1", MessageTS: "123.45"}
	event := Event{Kind: EventKindAppMention, EventID: "event-1", Text: "run acme/repo fix it", Origin: origin}

	if err := NewHandler(controller, messenger).HandleEvent(context.Background(), event); err != nil {
		t.Fatalf("HandleEvent: %v", err)
	}
	if len(controller.origins) != 1 || controller.origins[0].ThreadTS != origin.MessageTS {
		t.Fatalf("origins = %#v; want message timestamp promoted to thread", controller.origins)
	}
}

func TestCurrentAttemptSelectionAndRepositoryRedaction(t *testing.T) {
	controller := newFakeTaskController()
	handler := NewHandler(controller, newFakeMessenger())
	controller.task.CurrentAttemptID = "attempt-1"
	controller.attempts = []store.Attempt{{ID: "old", Number: 1}, {ID: "attempt-1", Number: 2}}
	if got, err := handler.currentAttemptNumber(context.Background(), controller.task, nil); err != nil || got != 2 {
		t.Fatalf("current attempt = %d, %v; want 2", got, err)
	}

	controller.task.CurrentAttemptID = "missing"
	if got, err := handler.currentAttemptNumber(context.Background(), controller.task, nil); err != nil || got != 2 {
		t.Fatalf("latest attempt = %d, %v; want 2", got, err)
	}
	controller.attempts = nil
	if got, err := handler.currentAttemptNumber(context.Background(), controller.task, &store.Attempt{Number: 3}); err != nil || got != 3 {
		t.Fatalf("preferred attempt = %d, %v; want 3", got, err)
	}
	if _, err := handler.currentAttemptNumber(context.Background(), controller.task, nil); err == nil {
		t.Fatal("task without attempts returned nil error")
	}

	got := displayRepository("https://user:token@example.com/acme/repo\n")
	if strings.Contains(got, "token") || got != "https://<redacted>@example.com/acme/repo" {
		t.Fatalf("displayRepository = %q; want credentials removed", got)
	}
}

func TestWorkerLineValidationFallbackAndRedaction(t *testing.T) {
	handler := NewHandler(newFakeTaskController(), newFakeMessenger())
	validOrigin := protocol.SlackOrigin{ChannelID: "channel-1", ThreadTS: "123.45"}
	for _, test := range []struct {
		name   string
		ctx    context.Context
		origin protocol.SlackOrigin
		line   string
		want   string
	}{
		{"nil context", nil, validOrigin, "plain", "context is nil"},
		{"empty channel", context.Background(), protocol.SlackOrigin{ThreadTS: "123.45"}, "plain", "channel ID is empty"},
		{"empty thread", context.Background(), protocol.SlackOrigin{ChannelID: "channel-1"}, "plain", "thread timestamp is empty"},
		{"malformed event", context.Background(), validOrigin, "@@simpleswe:{", "parse worker line"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := handler.HandleWorkerLine(test.ctx, test.origin, test.line)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("HandleWorkerLine error = %v; want containing %q", err, test.want)
			}
		})
	}

	messenger := newFakeMessenger()
	handler = NewHandler(newFakeTaskController(), messenger)
	line := `@@simpleswe:{"type":"validation_failed","task_id":"swe-123","message":"token=xoxb-secret\nfailed"}`
	if err := handler.HandleWorkerLine(context.Background(), validOrigin, line); err != nil {
		t.Fatalf("HandleWorkerLine: %v", err)
	}
	if len(messenger.messages) != 1 || strings.Contains(messenger.messages[0].Text, "xoxb-secret") || strings.Contains(messenger.messages[0].Text, "\n") {
		t.Fatalf("transition message = %#v; want one redacted single-line message", messenger.messages)
	}

	if got := safeTransitionText(protocol.Event{Type: "agent_started", TaskID: "swe-123"}); got != "Task swe-123: agent started" {
		t.Fatalf("fallback transition text = %q", got)
	}
	if got := safeError(nil); got != "operation failed" {
		t.Fatalf("safeError(nil) = %q", got)
	}
	if got := safeText(strings.Repeat("a", 513)); len(got) != 515 || !strings.HasSuffix(got, "...") {
		t.Fatalf("truncated safe text length = %d; want 515", len(got))
	}
}

type failingMessenger struct{ err error }

func (f failingMessenger) PostMessage(context.Context, protocol.SlackOrigin, string) error {
	return f.err
}

type originController struct {
	*fakeTaskController
	origins []protocol.SlackOrigin
}

func (c *originController) CreateTaskWithOrigin(ctx context.Context, params store.CreateTaskParams, origin protocol.SlackOrigin) (store.Task, error) {
	c.origins = append(c.origins, origin)
	return c.CreateTask(ctx, params)
}
