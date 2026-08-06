package socketmode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/slack"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

var _ Socket = (*fakeSocket)(nil)
var _ EventHandler = (*recordingHandler)(nil)
var _ Inbox = (*fakeInbox)(nil)

func TestRunPersistsBeforeAckAndAcksBeforeSlowHandler(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	socket := newFakeSocket(appMentionEnvelope())
	inbox := newFakeInbox()
	inbox.ackCount = socket.ackCount
	handler := &recordingHandler{
		started:    started,
		release:    release,
		ackCount:   socket.ackCount,
		ackAtStart: make(chan int, 1),
	}
	run := NewTransport(socket, inbox, handler, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	done := make(chan error, 1)
	go func() { done <- run.Run(context.Background()) }()

	select {
	case <-socket.acked:
	case <-time.After(time.Second):
		t.Fatal("transport did not acknowledge the envelope before the handler completed")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler was not called")
	}
	if got := <-handler.ackAtStart; got != 1 {
		t.Fatalf("ack count when handler started = %d; want 1", got)
	}
	if got := socket.ackCount(); got != 1 {
		t.Fatalf("ack count while handler is blocked = %d; want 1", got)
	}
	if got := inbox.callsSnapshot(); !reflect.DeepEqual(got, []string{"put", "start"}) {
		// handled cannot occur until the blocked handler is released.
		t.Fatalf("inbox calls while handler blocked = %#v; want put, start", got)
	}
	if !inbox.putBeforeAck {
		t.Fatal("supported event was acknowledged before durable insert")
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("run transport: %v", err)
	}
	if got := socket.ackCount(); got != 1 {
		t.Fatalf("ack count after handler completed = %d; want 1", got)
	}

	want := slack.Event{
		Kind:    slack.EventKindAppMention,
		EventID: "Ev-app-1",
		Text:    "<@Ubot> run acme/repo fix flaky test",
		Origin: protocol.SlackOrigin{
			WorkspaceID: "T-app-1",
			ChannelID:   "C-app-1",
			MessageTS:   "1712345678.000001",
			ThreadTS:    "1712345678.000000",
			UserID:      "U-app-1",
		},
	}
	assertHandledEvent(t, handler, want)
}

func TestRunNormalizesSlashCommandWithRequestAndTriggerIDs(t *testing.T) {
	handler := &recordingHandler{}
	socket := newFakeSocket(Envelope{
		ID:      "request-slash-1",
		Type:    "slash_commands",
		Payload: json.RawMessage(`{"team_id":"T-slash-1","channel_id":"C-slash-1","user_id":"U-slash-1","command":"/swe","text":"status swe-123","trigger_id":"trigger-slash-1","message_ts":"1712345678.000002","thread_ts":"1712345678.000001"}`),
	})

	if err := NewTransport(socket, newFakeInbox(), handler, slog.Default()).Run(context.Background()); err != nil {
		t.Fatalf("run transport: %v", err)
	}
	if got := socket.ackCount(); got != 1 {
		t.Fatalf("ack count = %d; want 1", got)
	}

	// Slash commands have no Events API event_id; the Socket Mode envelope ID
	// is the request ID used by Handler for replay-safe task creation.
	want := slack.Event{
		Kind:    slack.EventKindSlashCommand,
		EventID: "request-slash-1",
		Text:    "status swe-123",
		Origin: protocol.SlackOrigin{
			WorkspaceID: "T-slash-1",
			ChannelID:   "C-slash-1",
			MessageTS:   "1712345678.000002",
			ThreadTS:    "1712345678.000001",
			UserID:      "U-slash-1",
		},
	}
	assertHandledEvent(t, handler, want)
}

func TestRunAcknowledgesUnsupportedEventAndIgnoresIt(t *testing.T) {
	handler := &recordingHandler{}
	socket := newFakeSocket(Envelope{
		ID:      "envelope-message-1",
		Type:    "events_api",
		Payload: json.RawMessage(`{"event_id":"Ev-message-1","event":{"type":"message","team":"T-1","channel":"C-1","ts":"1712345678.000003","user":"U-1","text":"not a command"}}`),
	})

	inbox := newFakeInbox()
	if err := NewTransport(socket, inbox, handler, slog.Default()).Run(context.Background()); err != nil {
		t.Fatalf("run unsupported event: %v", err)
	}
	if got := socket.ackCount(); got != 1 {
		t.Fatalf("ack count = %d; want 1", got)
	}
	if got := handler.eventsSnapshot(); len(got) != 0 {
		t.Fatalf("handler events = %#v; want none", got)
	}
	if got := inbox.callsSnapshot(); len(got) != 0 {
		t.Fatalf("unsupported event inbox calls = %#v; want none", got)
	}
}

func TestRunAcknowledgementFailureDoesNotStopIntake(t *testing.T) {
	socket := newFakeSocket(appMentionEnvelope(), Envelope{
		ID:      "envelope-status-2",
		Type:    "slash_commands",
		Payload: json.RawMessage(`{"team_id":"T1","channel_id":"C1","user_id":"U1","text":"status swe-123"}`),
	})
	socket.ackErr = errors.New("socket write failed")
	handler := &recordingHandler{}
	var logs bytes.Buffer

	if err := NewTransport(socket, newFakeInbox(), handler, slog.New(slog.NewTextHandler(&logs, nil))).Run(context.Background()); err != nil {
		t.Fatalf("run after acknowledgement failure: %v", err)
	}
	if got := socket.ackCount(); got != 2 {
		t.Fatalf("ack attempts = %d; want 2", got)
	}
	if got := handler.eventsSnapshot(); len(got) != 2 {
		t.Fatalf("handled events = %d; want both events", len(got))
	}
	if !strings.Contains(logs.String(), "acknowledgement failed") {
		t.Fatalf("log output %q does not report acknowledgement failure", logs.String())
	}
}

func TestRunRecordsAndLogsHandlerErrorWithoutStopping(t *testing.T) {
	wantErr := errors.New("handler exploded")
	handler := &recordingHandler{errs: []error{wantErr, nil}}
	socket := newFakeSocket(appMentionEnvelope(), Envelope{
		ID:      "envelope-status-2",
		Type:    "slash_commands",
		Payload: json.RawMessage(`{"team_id":"T1","channel_id":"C1","user_id":"U1","text":"status swe-123"}`),
	})
	inbox := newFakeInbox()
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))

	err := NewTransport(socket, inbox, handler, logger).Run(context.Background())
	if err != nil {
		t.Fatalf("run error = %v; want handler failure isolated", err)
	}
	if got := socket.ackCount(); got != 2 {
		t.Fatalf("ack count = %d; want 2", got)
	}
	for _, want := range []string{"handler exploded", "Ev-app-1"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("log output %q does not contain %q", logs.String(), want)
		}
	}
	stored := inbox.eventSnapshot("Ev-app-1")
	if stored.Status != store.SlackInboxPending || stored.Attempts != 1 || stored.LastError != wantErr.Error() {
		t.Fatalf("failed inbox event = %#v; want pending attempt with error", stored)
	}
	if stored := inbox.eventSnapshot("envelope-status-2"); stored.Status != store.SlackInboxHandled || stored.Attempts != 1 {
		t.Fatalf("event after handler failure = %#v; want handled", stored)
	}
	if got := handler.eventsSnapshot(); len(got) != 2 {
		t.Fatalf("handler calls = %d; want failure followed by next event", len(got))
	}
}

func TestRunRetriesPendingEventWithBoundedBackoff(t *testing.T) {
	handler := &recordingHandler{errs: []error{errors.New("temporary failure"), nil}}
	socket := newOpenFakeSocket(appMentionEnvelope())
	inbox := newFakeInbox()
	transport := NewTransport(socket, inbox, handler, slog.Default())
	transport.retryInitialDelay = 20 * time.Millisecond
	transport.retryMaxDelay = 40 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx) }()

	eventually(t, time.Second, func() bool {
		stored := inbox.eventSnapshot("Ev-app-1")
		return stored.Status == store.SlackInboxHandled && stored.Attempts == 2
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v; want context canceled", err)
	}
	times := handler.callTimesSnapshot()
	if len(times) != 2 {
		t.Fatalf("handler call times = %v; want two attempts", times)
	}
	if elapsed := times[1].Sub(times[0]); elapsed < 15*time.Millisecond {
		t.Fatalf("retry elapsed = %v; want backoff without a busy loop", elapsed)
	}
	stored := inbox.eventSnapshot("Ev-app-1")
	if stored.LastError != "" || stored.UpdatedAt.Before(stored.CreatedAt) {
		t.Fatalf("handled retry metadata = %#v", stored)
	}
}

func TestRunContinuesIntakeWhileHandlerIsSlow(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	socket := newOpenFakeSocket(appMentionEnvelope(), Envelope{
		ID:      "envelope-status-2",
		Type:    "slash_commands",
		Payload: json.RawMessage(`{"team_id":"T1","channel_id":"C1","user_id":"U1","text":"status swe-123"}`),
	})
	inbox := newFakeInbox()
	handler := &recordingHandler{started: started, release: release}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewTransport(socket, inbox, handler, slog.Default()).Run(ctx) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first handler did not start")
	}
	eventually(t, time.Second, func() bool { return socket.ackCount() == 2 })
	if got := len(handler.eventsSnapshot()); got != 1 {
		t.Fatalf("concurrent handler calls while first is blocked = %d; want one single-flight worker", got)
	}
	puts := 0
	for _, call := range inbox.callsSnapshot() {
		if call == "put" {
			puts++
		}
	}
	if puts != 2 {
		t.Fatalf("persisted envelopes while first handler is blocked = %d; want 2", puts)
	}

	close(release)
	eventually(t, time.Second, func() bool { return len(handler.eventsSnapshot()) == 2 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run error = %v; want context canceled", err)
	}
}

func TestRunRejectsMalformedSupportedEnvelopeAndProcessesFollowingEvent(t *testing.T) {
	malformed := Envelope{ID: "bad-envelope-1", Type: "events_api", Payload: json.RawMessage(`{"event":`)}
	valid := Envelope{
		ID:      "valid-envelope-2",
		Type:    "slash_commands",
		Payload: json.RawMessage(`{"team_id":"T1","channel_id":"C1","user_id":"U1","text":"status swe-123"}`),
	}
	socket := newFakeSocket(malformed, valid)
	inbox := newFakeInbox()
	handler := &recordingHandler{}
	var logs bytes.Buffer

	if err := NewTransport(socket, inbox, handler, slog.New(slog.NewTextHandler(&logs, nil))).Run(context.Background()); err != nil {
		t.Fatalf("run malformed then valid envelopes: %v", err)
	}
	if got := socket.acksSnapshot(); !reflect.DeepEqual(got, []string{"bad-envelope-1", "valid-envelope-2"}) {
		t.Fatalf("acknowledgements = %#v; want each envelope once", got)
	}
	rejected := inbox.eventSnapshot("bad-envelope-1")
	if rejected.Status != store.SlackInboxRejected || rejected.Attempts != 1 || rejected.LastError == "" || rejected.UpdatedAt.IsZero() {
		t.Fatalf("rejected event = %#v; want durable rejection metadata", rejected)
	}
	if !strings.Contains(logs.String(), "normalization failed") {
		t.Fatalf("log output %q does not report malformed envelope", logs.String())
	}
	got := handler.eventsSnapshot()
	if len(got) != 1 || got[0].EventID != "valid-envelope-2" {
		t.Fatalf("handled events = %#v; want following valid event", got)
	}
}

func TestRunContinuesWhenMalformedEnvelopeCannotBeRecorded(t *testing.T) {
	socket := newFakeSocket(
		Envelope{ID: "bad-envelope-1", Type: "events_api", Payload: json.RawMessage(`{"event":`)},
		Envelope{ID: "valid-envelope-2", Type: "slash_commands", Payload: json.RawMessage(`{"text":"status swe-123"}`)},
	)
	inbox := newFakeInbox()
	inbox.rejectErr = errors.New("rejection store unavailable")
	handler := &recordingHandler{}
	var logs bytes.Buffer

	if err := NewTransport(socket, inbox, handler, slog.New(slog.NewTextHandler(&logs, nil))).Run(context.Background()); err != nil {
		t.Fatalf("run after rejection persistence failure: %v", err)
	}
	if got := socket.ackCount(); got != 2 {
		t.Fatalf("ack count = %d; want malformed and valid envelopes acknowledged", got)
	}
	if got := handler.eventsSnapshot(); len(got) != 1 || got[0].EventID != "valid-envelope-2" {
		t.Fatalf("handled events = %#v; want following valid event", got)
	}
	if !strings.Contains(logs.String(), "rejection persistence failed") {
		t.Fatalf("log output %q does not report rejection persistence failure", logs.String())
	}
}

func TestRunContextStopsPendingRetryWait(t *testing.T) {
	handler := &recordingHandler{err: errors.New("still failing")}
	socket := newOpenFakeSocket(appMentionEnvelope())
	inbox := newFakeInbox()
	transport := NewTransport(socket, inbox, handler, slog.Default())
	transport.retryInitialDelay = time.Hour
	transport.retryMaxDelay = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- transport.Run(ctx) }()
	eventually(t, time.Second, func() bool { return inbox.eventSnapshot("Ev-app-1").Attempts == 1 })

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v; want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("transport did not stop during retry backoff")
	}
	if got := inbox.eventSnapshot("Ev-app-1").Attempts; got != 1 {
		t.Fatalf("attempts after cancellation = %d; want 1", got)
	}
}

func TestRetryDelayIsExponentialAndCapped(t *testing.T) {
	for _, test := range []struct {
		attempts int
		want     time.Duration
	}{{1, 10 * time.Millisecond}, {2, 20 * time.Millisecond}, {3, 25 * time.Millisecond}, {100, 25 * time.Millisecond}} {
		if got := retryDelay(test.attempts, 10*time.Millisecond, 25*time.Millisecond); got != test.want {
			t.Errorf("retryDelay(%d) = %v; want %v", test.attempts, got, test.want)
		}
	}
}

func TestRunReplaysPendingAtStartupAndSkipsHandledRedelivery(t *testing.T) {
	event, _, err := normalize(appMentionEnvelope())
	if err != nil {
		t.Fatalf("normalize fixture: %v", err)
	}
	inbox := newFakeInbox()
	inbox.events[event.EventID] = store.SlackInboxEvent{
		EventID: event.EventID, Kind: event.Kind, Text: event.Text, Origin: event.Origin, Status: store.SlackInboxPending,
	}
	socket := newFakeSocket(appMentionEnvelope())
	handler := &recordingHandler{}

	if err := NewTransport(socket, inbox, handler, slog.Default()).Run(context.Background()); err != nil {
		t.Fatalf("run transport: %v", err)
	}
	if got := handler.eventsSnapshot(); len(got) != 1 {
		t.Fatalf("handler calls = %d; want startup replay only", len(got))
	}
	if got := socket.ackCount(); got != 1 {
		t.Fatalf("redelivery ack count = %d; want 1", got)
	}
	if got := inbox.eventSnapshot(event.EventID); got.Status != store.SlackInboxHandled || got.Attempts != 1 {
		t.Fatalf("replayed inbox event = %#v; want handled once", got)
	}
}

func appMentionEnvelope() Envelope {
	return Envelope{
		ID:      "envelope-app-mention-1",
		Type:    "events_api",
		Payload: json.RawMessage(`{"team_id":"T-app-1","event_id":"Ev-app-1","event":{"type":"app_mention","team":"T-app-1","channel":"C-app-1","ts":"1712345678.000001","thread_ts":"1712345678.000000","user":"U-app-1","text":"<@Ubot> run acme/repo fix flaky test"}}`),
	}
}

func assertHandledEvent(t *testing.T, handler *recordingHandler, want slack.Event) {
	t.Helper()
	got := handler.eventsSnapshot()
	if len(got) != 1 {
		t.Fatalf("handled events = %d; want 1", len(got))
	}
	if got[0] != want {
		t.Fatalf("normalized event = %#v; want %#v", got[0], want)
	}
}

type fakeSocket struct {
	events chan Envelope
	acked  chan struct{}
	ackErr error

	mu   sync.Mutex
	acks []string
}

func newFakeSocket(input ...Envelope) *fakeSocket {
	events := make(chan Envelope, len(input))
	for _, event := range input {
		events <- event
	}
	close(events)
	return &fakeSocket{events: events, acked: make(chan struct{}, 8)}
}

func newOpenFakeSocket(input ...Envelope) *fakeSocket {
	events := make(chan Envelope, len(input))
	for _, event := range input {
		events <- event
	}
	return &fakeSocket{events: events, acked: make(chan struct{}, 8)}
}

func (f *fakeSocket) Events() <-chan Envelope { return f.events }

func (f *fakeSocket) Ack(_ context.Context, envelopeID string) error {
	f.mu.Lock()
	f.acks = append(f.acks, envelopeID)
	f.mu.Unlock()
	f.acked <- struct{}{}
	return f.ackErr
}

func (f *fakeSocket) ackCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.acks)
}

func (f *fakeSocket) acksSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.acks...)
}

type recordingHandler struct {
	mu        sync.Mutex
	events    []slack.Event
	callTimes []time.Time
	started   chan struct{}
	release   <-chan struct{}
	err       error
	errs      []error

	ackCount   func() int
	ackAtStart chan int
}

func (h *recordingHandler) HandleEvent(_ context.Context, event slack.Event) error {
	h.mu.Lock()
	h.events = append(h.events, event)
	h.callTimes = append(h.callTimes, time.Now())
	started := h.started
	h.started = nil
	release := h.release
	err := h.err
	if len(h.errs) > 0 {
		err = h.errs[0]
		h.errs = h.errs[1:]
	}
	ackCount := h.ackCount
	ackAtStart := h.ackAtStart
	h.mu.Unlock()

	if ackCount != nil {
		ackAtStart <- ackCount()
	}
	if started != nil {
		close(started)
	}
	if release != nil {
		<-release
	}
	return err
}

func (h *recordingHandler) callTimesSnapshot() []time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]time.Time(nil), h.callTimes...)
}

func (h *recordingHandler) eventsSnapshot() []slack.Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slack.Event(nil), h.events...)
}

type fakeInbox struct {
	mu           sync.Mutex
	events       map[string]store.SlackInboxEvent
	calls        []string
	ackCount     func() int
	putBeforeAck bool
	rejectErr    error
}

func newFakeInbox() *fakeInbox {
	return &fakeInbox{events: make(map[string]store.SlackInboxEvent)}
}

func (f *fakeInbox) PutSlackInboxEvent(_ context.Context, event store.SlackInboxEvent) (store.SlackInboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "put")
	if f.ackCount != nil {
		f.putBeforeAck = f.ackCount() == 0
	}
	if existing, ok := f.events[event.EventID]; ok {
		return existing, nil
	}
	event.Status = store.SlackInboxPending
	event.CreatedAt = time.Now()
	event.UpdatedAt = event.CreatedAt
	f.events[event.EventID] = event
	return event, nil
}

func (f *fakeInbox) ListPendingSlackInboxEvents(context.Context) ([]store.SlackInboxEvent, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var pending []store.SlackInboxEvent
	for _, event := range f.events {
		if event.Status == store.SlackInboxPending {
			pending = append(pending, event)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].CreatedAt.Equal(pending[j].CreatedAt) {
			return pending[i].EventID < pending[j].EventID
		}
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})
	return pending, nil
}

func (f *fakeInbox) StartSlackInboxAttempt(_ context.Context, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "start")
	event := f.events[eventID]
	event.Attempts++
	event.UpdatedAt = time.Now()
	f.events[eventID] = event
	return nil
}

func (f *fakeInbox) RecordSlackInboxError(_ context.Context, eventID string, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "error")
	event := f.events[eventID]
	event.LastError = cause.Error()
	event.UpdatedAt = time.Now()
	f.events[eventID] = event
	return nil
}

func (f *fakeInbox) RecordRejectedSlackInboxEvent(_ context.Context, eventID, kind string, cause error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "rejected")
	if f.rejectErr != nil {
		return f.rejectErr
	}
	now := time.Now()
	event := f.events[eventID]
	if event.CreatedAt.IsZero() {
		event.CreatedAt = now
	}
	event.EventID = eventID
	event.Kind = kind
	event.Status = store.SlackInboxRejected
	event.Attempts++
	event.LastError = cause.Error()
	event.UpdatedAt = now
	f.events[eventID] = event
	return nil
}

func (f *fakeInbox) MarkSlackInboxHandled(_ context.Context, eventID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, "handled")
	event := f.events[eventID]
	event.Status = store.SlackInboxHandled
	event.LastError = ""
	event.UpdatedAt = time.Now()
	f.events[eventID] = event
	return nil
}

func (f *fakeInbox) callsSnapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeInbox) eventSnapshot(eventID string) store.SlackInboxEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.events[eventID]
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}
