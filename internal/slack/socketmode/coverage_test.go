package socketmode

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	slackapi "github.com/slack-go/slack"
	slacksocket "github.com/slack-go/slack/socketmode"

	internalslack "github.com/simpleswe/simpleswe/internal/slack"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestMessengerValidatesInputsAndWrapsAPIErrors(t *testing.T) {
	validOrigin := protocol.SlackOrigin{ChannelID: "channel-1", MessageTS: "123.45"}
	for _, test := range []struct {
		name      string
		messenger internalslack.Messenger
		ctx       context.Context
		origin    protocol.SlackOrigin
		text      string
		want      string
	}{
		{"nil context", NewMessenger(&errorChatAPI{}), nil, validOrigin, "hello", "context is nil"},
		{"nil API", NewMessenger(nil), context.Background(), validOrigin, "hello", "chat API is nil"},
		{"empty channel", NewMessenger(&errorChatAPI{}), context.Background(), protocol.SlackOrigin{}, "hello", "channel ID is empty"},
		{"empty text", NewMessenger(&errorChatAPI{}), context.Background(), validOrigin, " ", "message text is empty"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.messenger.PostMessage(test.ctx, test.origin, test.text)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PostMessage error = %v; want containing %q", err, test.want)
			}
		})
	}

	wantErr := errors.New("token=xoxb-secret upstream unavailable")
	err := NewMessenger(&errorChatAPI{err: wantErr}).PostMessage(context.Background(), validOrigin, "hello")
	if !errors.Is(err, wantErr) || strings.Contains(err.Error(), "xoxb-secret") {
		t.Fatalf("API error = %v; want wrapped and redacted", err)
	}

	api := &errorChatAPI{}
	if err := NewMessenger(api).PostMessage(context.Background(), validOrigin, "hello"); err != nil {
		t.Fatalf("PostMessage: %v", err)
	}
	if api.request.ThreadTS != validOrigin.MessageTS {
		t.Fatalf("thread timestamp = %q; want fallback %q", api.request.ThreadTS, validOrigin.MessageTS)
	}
}

func TestRunValidatesTransportDependencies(t *testing.T) {
	validSocket := newFakeSocket()
	validInbox := newFakeInbox()
	validHandler := &recordingHandler{}
	for _, test := range []struct {
		name      string
		transport *Transport
		ctx       context.Context
		want      string
	}{
		{"nil transport", nil, context.Background(), "transport is nil"},
		{"nil context", NewTransport(validSocket, validInbox, validHandler, nil), nil, "context is nil"},
		{"nil socket", NewTransport(nil, validInbox, validHandler, nil), context.Background(), "socket is nil"},
		{"nil inbox", NewTransport(validSocket, nil, validHandler, nil), context.Background(), "inbox is nil"},
		{"nil handler", NewTransport(validSocket, validInbox, nil, nil), context.Background(), "handler is nil"},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.transport.Run(test.ctx)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Run error = %v; want containing %q", err, test.want)
			}
		})
	}
}

func TestHandleEnvelopePersistenceAndStatusErrors(t *testing.T) {
	wantErr := errors.New("token=xoxb-secret database unavailable")
	socket := newOpenFakeSocket()
	transport := NewTransport(socket, putErrorInbox{fakeInbox: newFakeInbox(), err: wantErr}, &recordingHandler{}, slog.Default())
	err := transport.handleEnvelope(context.Background(), appMentionEnvelope())
	if !errors.Is(err, wantErr) || strings.Contains(err.Error(), "xoxb-secret") {
		t.Fatalf("persistence error = %v; want wrapped and redacted", err)
	}
	if socket.ackCount() != 0 {
		t.Fatal("envelope was acknowledged after persistence failure")
	}

	socket = newOpenFakeSocket()
	transport = NewTransport(socket, statusInbox{fakeInbox: newFakeInbox(), status: "mystery"}, &recordingHandler{}, slog.Default())
	err = transport.handleEnvelope(context.Background(), appMentionEnvelope())
	if err == nil || !strings.Contains(err.Error(), "invalid status") || socket.ackCount() != 1 {
		t.Fatalf("invalid status result = %v, acks=%d; want error after ack", err, socket.ackCount())
	}
}

func TestWorkerStorageErrorsAndCancellation(t *testing.T) {
	wantErr := errors.New("storage unavailable")
	baseEvent := store.SlackInboxEvent{EventID: "event-1", Kind: internalslack.EventKindSlashCommand}

	transport := NewTransport(newOpenFakeSocket(), listErrorInbox{fakeInbox: newFakeInbox(), err: wantErr}, &recordingHandler{}, slog.Default())
	if err := transport.Run(context.Background()); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "list pending") {
		t.Fatalf("list error = %v; want wrapped storage error", err)
	}

	transport = NewTransport(newOpenFakeSocket(), startErrorInbox{fakeInbox: newFakeInbox(), err: wantErr}, &recordingHandler{}, slog.Default())
	if err := transport.process(context.Background(), baseEvent); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "start Slack inbox attempt") {
		t.Fatalf("start error = %v; want wrapped storage error", err)
	}

	handlerErr := errors.New("handler failed")
	transport = NewTransport(newOpenFakeSocket(), recordErrorInbox{fakeInbox: newFakeInbox(), err: wantErr}, handlerFunc(func(context.Context, internalslack.Event) error { return handlerErr }), slog.Default())
	err := transport.process(context.Background(), baseEvent)
	if !errors.Is(err, handlerErr) || !errors.Is(err, wantErr) {
		t.Fatalf("record error = %v; want joined handler and storage errors", err)
	}

	transport = NewTransport(newOpenFakeSocket(), markErrorInbox{fakeInbox: newFakeInbox(), err: wantErr}, &recordingHandler{}, slog.Default())
	if err := transport.process(context.Background(), baseEvent); !errors.Is(err, wantErr) || !strings.Contains(err.Error(), "mark Slack event handled") {
		t.Fatalf("mark error = %v; want wrapped storage error", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transport = NewTransport(newOpenFakeSocket(), newFakeInbox(), handlerFunc(func(context.Context, internalslack.Event) error { return handlerErr }), slog.Default())
	if err := transport.process(ctx, baseEvent); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled handler result = %v; want context canceled", err)
	}
	transport = NewTransport(newOpenFakeSocket(), newFakeInbox(), &recordingHandler{}, slog.Default())
	if err := transport.process(ctx, baseEvent); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled successful handler result = %v; want context canceled", err)
	}
}

func TestNormalizationFallbacksAndSafeErrors(t *testing.T) {
	event, supported, err := normalize(Envelope{
		ID: "", Type: "slash_commands",
		Payload: []byte(`{"request_id":"request-1","trigger_id":"trigger-1","channel_id":"channel-1","ts":"123.45"}`),
	})
	if err != nil || !supported || event.EventID != "request-1" || event.Origin.MessageTS != "123.45" {
		t.Fatalf("normalized slash event = %#v, supported=%t, err=%v", event, supported, err)
	}
	if _, _, err := normalize(Envelope{Type: "slash_commands", Payload: []byte(`{`)}); err == nil {
		t.Fatal("malformed slash command returned nil error")
	}
	if _, supported, err := normalize(Envelope{Type: "unknown"}); err != nil || supported {
		t.Fatalf("unknown envelope supported=%t, err=%v; want ignored", supported, err)
	}

	if got := retryDelay(0, time.Second, time.Minute); got != 0 {
		t.Fatalf("retryDelay(0) = %v; want zero", got)
	}
	if got := retryDelay(1, 0, 0); got != defaultRetryInitialDelay {
		t.Fatalf("retryDelay defaults = %v; want %v", got, defaultRetryInitialDelay)
	}
	if got := retryDelay(1, time.Minute, time.Second); got != time.Second {
		t.Fatalf("retryDelay capped initial = %v; want 1s", got)
	}

	wantErr := errors.New("token=xoxb-secret\nbad")
	wrapped := wrapSafeError("operation: ", wantErr)
	if !errors.Is(wrapped, wantErr) || strings.Contains(wrapped.Error(), "xoxb-secret") || strings.Contains(wrapped.Error(), "\n") {
		t.Fatalf("safe wrapped error = %q; want redacted single line", wrapped)
	}
	if got := wrapSafeError(" empty: ", nil).Error(); got != "empty:" {
		t.Fatalf("nil wrapped error = %q", got)
	}
	if got := safeText(strings.Repeat("a", 513)); len(got) != 515 || !strings.HasSuffix(got, "...") {
		t.Fatalf("truncated safe text length = %d; want 515", len(got))
	}
}

func TestSDKWrappersValidateAndPost(t *testing.T) {
	var nilSocket *SDKSocket
	if err := nilSocket.Ack(context.Background(), "envelope-1"); err == nil {
		t.Fatal("nil SDK socket Ack returned nil error")
	}
	if err := nilSocket.Run(context.Background()); err == nil {
		t.Fatal("nil SDK socket Run returned nil error")
	}
	if err := (SDKChatAPI{}).PostMessage(context.Background(), PostMessageRequest{}); err == nil {
		t.Fatal("nil SDK chat client returned nil error")
	}

	requests := make(chan map[string]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if err := request.ParseForm(); err != nil {
			t.Errorf("parse Slack request: %v", err)
		}
		requests <- map[string]string{
			"channel": request.Form.Get("channel"), "text": request.Form.Get("text"),
			"thread_ts": request.Form.Get("thread_ts"), "unfurl_links": request.Form.Get("unfurl_links"),
			"unfurl_media": request.Form.Get("unfurl_media"),
		}
		response.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(response, `{"ok":true,"channel":"channel-1","ts":"123.46"}`)
	}))
	defer server.Close()
	client := slackapi.New("xoxb-test", slackapi.OptionAPIURL(server.URL+"/"))
	api := SDKChatAPI{Client: client}
	request := PostMessageRequest{Channel: "channel-1", ThreadTS: "123.45", Text: "hello"}
	if err := api.PostMessage(context.Background(), request); err != nil {
		t.Fatalf("SDK PostMessage: %v", err)
	}
	got := <-requests
	if got["channel"] != request.Channel || got["text"] != request.Text || got["thread_ts"] != request.ThreadTS || got["unfurl_links"] != "false" || got["unfurl_media"] != "false" {
		t.Fatalf("Slack Web API form = %#v", got)
	}

	clientWithoutThread := slackapi.New("xoxb-test", slackapi.OptionAPIURL(server.URL+"/"))
	if err := (SDKChatAPI{Client: clientWithoutThread}).PostMessage(context.Background(), PostMessageRequest{Channel: "channel-1", Text: "hello"}); err != nil {
		t.Fatalf("SDK PostMessage without thread: %v", err)
	}
	if got := <-requests; got["thread_ts"] != "" {
		t.Fatalf("thread_ts = %q; want omitted", got["thread_ts"])
	}
}

func TestSDKSocketCanceledLifecycleClosesEvents(t *testing.T) {
	client := slacksocket.New(slackapi.New("xapp-test"))
	socket := NewSDKSocket(client)
	if socket.Events() == nil {
		t.Fatal("SDK socket events channel is nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := socket.Run(ctx); err != nil {
		t.Fatalf("Run canceled SDK socket: %v", err)
	}
	if _, open := <-socket.Events(); open {
		t.Fatal("SDK socket events channel remained open after Run")
	}
}

type errorChatAPI struct {
	err     error
	request PostMessageRequest
}

func (a *errorChatAPI) PostMessage(_ context.Context, request PostMessageRequest) error {
	a.request = request
	return a.err
}

type handlerFunc func(context.Context, internalslack.Event) error

func (f handlerFunc) HandleEvent(ctx context.Context, event internalslack.Event) error {
	return f(ctx, event)
}

type putErrorInbox struct {
	*fakeInbox
	err error
}

func (i putErrorInbox) PutSlackInboxEvent(context.Context, store.SlackInboxEvent) (store.SlackInboxEvent, error) {
	return store.SlackInboxEvent{}, i.err
}

type statusInbox struct {
	*fakeInbox
	status string
}

func (i statusInbox) PutSlackInboxEvent(ctx context.Context, event store.SlackInboxEvent) (store.SlackInboxEvent, error) {
	stored, err := i.fakeInbox.PutSlackInboxEvent(ctx, event)
	stored.Status = i.status
	return stored, err
}

type listErrorInbox struct {
	*fakeInbox
	err error
}

func (i listErrorInbox) ListPendingSlackInboxEvents(context.Context) ([]store.SlackInboxEvent, error) {
	return nil, i.err
}

type startErrorInbox struct {
	*fakeInbox
	err error
}

func (i startErrorInbox) StartSlackInboxAttempt(context.Context, string) error { return i.err }

type recordErrorInbox struct {
	*fakeInbox
	err error
}

func (i recordErrorInbox) RecordSlackInboxError(context.Context, string, error) error { return i.err }

type markErrorInbox struct {
	*fakeInbox
	err error
}

func (i markErrorInbox) MarkSlackInboxHandled(context.Context, string) error { return i.err }
