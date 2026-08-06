package socketmode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/simpleswe/simpleswe/internal/slack"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

// Socket is the small Socket Mode boundary used by Transport. A production
// adapter can translate a Slack client into these two operations.
type Socket interface {
	Events() <-chan Envelope
	Ack(context.Context, string) error
}

// Envelope is the vendor-neutral part of a Socket Mode request used here.
type Envelope struct {
	ID      string
	Type    string
	Payload json.RawMessage
}

// EventHandler is the transport-independent Slack handler contract.
type EventHandler interface {
	HandleEvent(context.Context, slack.Event) error
}

// Inbox is the durable subset of the store used by Socket Mode intake.
type Inbox interface {
	PutSlackInboxEvent(context.Context, store.SlackInboxEvent) (store.SlackInboxEvent, error)
	RecordRejectedSlackInboxEvent(context.Context, string, string, error) error
	ListPendingSlackInboxEvents(context.Context) ([]store.SlackInboxEvent, error)
	StartSlackInboxAttempt(context.Context, string) error
	RecordSlackInboxError(context.Context, string, error) error
	MarkSlackInboxHandled(context.Context, string) error
}

// Transport normalizes and persists supported Socket Mode envelopes before
// acknowledgement, then hands them to the internal Slack handler.
type Transport struct {
	socket            Socket
	inbox             Inbox
	handler           EventHandler
	logger            *slog.Logger
	retryInitialDelay time.Duration
	retryMaxDelay     time.Duration
}

const (
	defaultRetryInitialDelay = time.Second
	defaultRetryMaxDelay     = 30 * time.Second
)

// NewTransport constructs a Socket Mode transport without depending on a
// Slack SDK.
func NewTransport(socket Socket, inbox Inbox, handler EventHandler, logger *slog.Logger) *Transport {
	if logger == nil {
		logger = slog.Default()
	}
	return &Transport{
		socket: socket, inbox: inbox, handler: handler, logger: logger,
		retryInitialDelay: defaultRetryInitialDelay,
		retryMaxDelay:     defaultRetryMaxDelay,
	}
}

// Run consumes envelopes while one bounded worker processes and retries the
// durable inbox. Handler and acknowledgement errors do not stop intake.
func (t *Transport) Run(ctx context.Context) error {
	if t == nil {
		return errors.New("Slack Socket Mode transport is nil")
	}
	if ctx == nil {
		return errors.New("Slack Socket Mode context is nil")
	}
	if t.socket == nil {
		return errors.New("Slack Socket Mode socket is nil")
	}
	if t.inbox == nil {
		return errors.New("Slack inbox is nil")
	}
	if t.handler == nil {
		return errors.New("Slack event handler is nil")
	}

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	wake := make(chan struct{}, 1)
	closing := make(chan struct{})
	workerDone := make(chan error, 1)
	go func() { workerDone <- t.runWorker(workerCtx, wake, closing) }()
	wake <- struct{}{}

	for {
		select {
		case <-ctx.Done():
			cancelWorker()
			<-workerDone
			return ctx.Err()
		case err := <-workerDone:
			return err
		case envelope, ok := <-t.socket.Events():
			if !ok {
				close(closing)
				err := <-workerDone
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
			if err := t.handleEnvelope(ctx, envelope); err != nil {
				return err
			}
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}
}

func (t *Transport) handleEnvelope(ctx context.Context, envelope Envelope) error {
	event, supported, err := normalize(envelope)
	if err != nil {
		wrapped := wrapSafeError("normalize Slack envelope: ", err)
		t.logger.Error("Slack envelope normalization failed", "envelope_id", safeText(envelope.ID), "error", wrapped)
		if recordErr := t.inbox.RecordRejectedSlackInboxEvent(ctx, envelope.ID, envelope.Type, errors.New(safeText(err.Error()))); recordErr != nil {
			t.logger.Error("Slack envelope rejection persistence failed", "envelope_id", safeText(envelope.ID), "error", wrapSafeError("record rejected Slack envelope: ", recordErr))
		}
		t.ack(ctx, envelope.ID)
		return nil
	}
	if !supported {
		t.ack(ctx, envelope.ID)
		return nil
	}

	stored, err := t.inbox.PutSlackInboxEvent(ctx, store.SlackInboxEvent{
		EventID: event.EventID,
		Kind:    event.Kind,
		Text:    event.Text,
		Origin:  event.Origin,
	})
	if err != nil {
		wrapped := wrapSafeError("record Slack envelope: ", err)
		t.logger.Error("Slack envelope persistence failed", "envelope_id", safeText(envelope.ID), "error", wrapped)
		return wrapped
	}
	t.ack(ctx, envelope.ID)
	if stored.Status == store.SlackInboxHandled {
		return nil
	}
	if stored.Status != store.SlackInboxPending {
		return fmt.Errorf("Slack inbox event %q has invalid status %q", safeText(stored.EventID), safeText(stored.Status))
	}
	return nil
}

func (t *Transport) ack(ctx context.Context, envelopeID string) {
	if err := t.socket.Ack(ctx, envelopeID); err != nil {
		wrapped := wrapSafeError("ack Slack envelope: ", err)
		t.logger.Error("Slack envelope acknowledgement failed", "envelope_id", safeText(envelopeID), "error", wrapped)
	}
}

func (t *Transport) runWorker(ctx context.Context, wake <-chan struct{}, closing <-chan struct{}) error {
	draining := false
	for {
		pending, err := t.inbox.ListPendingSlackInboxEvents(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return wrapSafeError("list pending Slack events: ", err)
		}
		now := time.Now()
		var next time.Time
		processed := false
		for _, event := range pending {
			due := event.UpdatedAt.Add(retryDelay(event.Attempts, t.retryInitialDelay, t.retryMaxDelay))
			if event.Attempts == 0 || !due.After(now) {
				if err := t.process(ctx, event); err != nil {
					return err
				}
				processed = true
				break
			}
			if next.IsZero() || due.Before(next) {
				next = due
			}
		}
		if processed {
			continue
		}
		if draining {
			return nil
		}

		var timer <-chan time.Time
		var stopTimer func()
		if !next.IsZero() {
			wait := time.Until(next)
			if wait < 0 {
				wait = 0
			}
			clock := time.NewTimer(wait)
			timer = clock.C
			stopTimer = func() { clock.Stop() }
		} else {
			stopTimer = func() {}
		}
		select {
		case <-ctx.Done():
			stopTimer()
			return ctx.Err()
		case <-wake:
			stopTimer()
		case <-timer:
		case <-closing:
			stopTimer()
			draining = true
		}
	}
}

func retryDelay(attempts int, initial, maximum time.Duration) time.Duration {
	if attempts <= 0 {
		return 0
	}
	if initial <= 0 {
		initial = defaultRetryInitialDelay
	}
	if maximum <= 0 {
		maximum = defaultRetryMaxDelay
	}
	if initial >= maximum {
		return maximum
	}
	delay := initial
	for remaining := attempts - 1; remaining > 0; remaining-- {
		if delay >= maximum/2 {
			return maximum
		}
		delay *= 2
	}
	return delay
}

func (t *Transport) process(ctx context.Context, stored store.SlackInboxEvent) error {
	if err := t.inbox.StartSlackInboxAttempt(ctx, stored.EventID); err != nil {
		return wrapSafeError("start Slack inbox attempt: ", err)
	}
	event := slack.Event{Kind: stored.Kind, EventID: stored.EventID, Text: stored.Text, Origin: stored.Origin}
	if err := t.handler.HandleEvent(ctx, event); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		wrapped := wrapSafeError("handle Slack event: ", err)
		if recordErr := t.inbox.RecordSlackInboxError(ctx, stored.EventID, errors.New(safeText(err.Error()))); recordErr != nil {
			return errors.Join(wrapped, wrapSafeError("record Slack handler error: ", recordErr))
		}
		t.logger.Error("Slack event handler failed", "event_id", safeText(stored.EventID), "error", wrapped)
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	// A crash after Handler succeeds (including chat.postMessage) and before
	// this update can replay the response. Task creation and retry are keyed by
	// EventID; Slack message delivery remains intentionally at-least-once.
	if err := t.inbox.MarkSlackInboxHandled(ctx, stored.EventID); err != nil {
		return wrapSafeError("mark Slack event handled: ", err)
	}
	return nil
}

type eventsAPIPayload struct {
	TeamID  string         `json:"team_id"`
	EventID string         `json:"event_id"`
	Event   eventsAPIEvent `json:"event"`
}

type eventsAPIEvent struct {
	Type     string `json:"type"`
	Team     string `json:"team"`
	Channel  string `json:"channel"`
	TS       string `json:"ts"`
	ThreadTS string `json:"thread_ts"`
	User     string `json:"user"`
	Text     string `json:"text"`
}

type slashCommandPayload struct {
	TeamID    string `json:"team_id"`
	ChannelID string `json:"channel_id"`
	UserID    string `json:"user_id"`
	Text      string `json:"text"`
	TriggerID string `json:"trigger_id"`
	RequestID string `json:"request_id"`
	MessageTS string `json:"message_ts"`
	TS        string `json:"ts"`
	ThreadTS  string `json:"thread_ts"`
}

func normalize(envelope Envelope) (slack.Event, bool, error) {
	switch envelope.Type {
	case "events_api":
		var payload eventsAPIPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return slack.Event{}, false, err
		}
		if payload.Event.Type != slack.EventKindAppMention {
			return slack.Event{}, false, nil
		}
		return slack.Event{
			Kind:    slack.EventKindAppMention,
			EventID: firstNonEmpty(payload.EventID, envelope.ID),
			Text:    payload.Event.Text,
			Origin: slackOrigin(
				firstNonEmpty(payload.Event.Team, payload.TeamID),
				payload.Event.Channel,
				payload.Event.TS,
				payload.Event.ThreadTS,
				payload.Event.User,
			),
		}, true, nil
	case "slash_commands":
		var payload slashCommandPayload
		if err := json.Unmarshal(envelope.Payload, &payload); err != nil {
			return slack.Event{}, false, err
		}
		return slack.Event{
			Kind:    slack.EventKindSlashCommand,
			EventID: firstNonEmpty(envelope.ID, payload.RequestID, payload.TriggerID),
			Text:    payload.Text,
			Origin: slackOrigin(
				payload.TeamID,
				payload.ChannelID,
				firstNonEmpty(payload.MessageTS, payload.TS),
				payload.ThreadTS,
				payload.UserID,
			),
		}, true, nil
	default:
		return slack.Event{}, false, nil
	}
}

func slackOrigin(workspaceID, channelID, messageTS, threadTS, userID string) protocol.SlackOrigin {
	return protocol.SlackOrigin{
		WorkspaceID: workspaceID,
		ChannelID:   channelID,
		MessageTS:   messageTS,
		ThreadTS:    threadTS,
		UserID:      userID,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

type safeWrappedError struct {
	message string
	cause   error
}

func (e *safeWrappedError) Error() string { return e.message }
func (e *safeWrappedError) Unwrap() error { return e.cause }

func wrapSafeError(prefix string, err error) error {
	if err == nil {
		return errors.New(strings.TrimSpace(prefix))
	}
	return &safeWrappedError{message: prefix + safeText(err.Error()), cause: err}
}

var (
	sensitiveAssignment = regexp.MustCompile(`(?i)(\b(?:password|passwd|token|secret|api[_-]?key|authorization)\s*[:=]\s*)\S+`)
	bearerCredential    = regexp.MustCompile(`(?i)(\bbearer\s+)\S+`)
	credentialURL       = regexp.MustCompile(`(?i)(https?://)[^/\s@]+@`)
)

func safeText(text string) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
	text = bearerCredential.ReplaceAllString(text, `${1}<redacted>`)
	text = sensitiveAssignment.ReplaceAllString(text, `${1}<redacted>`)
	text = credentialURL.ReplaceAllString(text, `${1}<redacted>@`)
	text = strings.TrimSpace(text)
	if len(text) > 512 {
		return text[:512] + "..."
	}
	return text
}
