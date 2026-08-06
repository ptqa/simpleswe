package slack

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"unicode"

	"github.com/simpleswe/simpleswe/internal/slack/commands"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

const (
	// EventKindAppMention is the normalized kind for a Slack app mention.
	EventKindAppMention = "app_mention"
	// EventKindSlashCommand is the normalized kind for a Slack slash command.
	EventKindSlashCommand = "slash_command"

	usage = "Usage: run <repository> <prompt> | status <task> | cancel <task> | retry <task>"
)

// Event is the transport-independent subset of a Slack event used by Handler.
type Event struct {
	Kind    string
	EventID string
	Text    string
	Origin  protocol.SlackOrigin
}

// TaskController is the task API needed to dispatch Slack commands.
type TaskController interface {
	CreateTask(context.Context, store.CreateTaskParams) (store.Task, error)
	GetTask(context.Context, string) (store.Task, error)
	ListAttempts(context.Context, string) ([]store.Attempt, error)
	RequestCancellation(context.Context, string) error
	RetryTaskWithKey(context.Context, string, string) (store.Attempt, error)
}

type originTaskController interface {
	CreateTaskWithOrigin(context.Context, store.CreateTaskParams, protocol.SlackOrigin) (store.Task, error)
}

// Messenger posts a message to an existing Slack conversation.
type Messenger interface {
	PostMessage(context.Context, protocol.SlackOrigin, string) error
}

// Handler dispatches normalized Slack events without depending on Socket Mode.
type Handler struct {
	controller TaskController
	messenger  Messenger
}

// NewHandler constructs a transport-independent Slack handler.
func NewHandler(controller TaskController, messenger Messenger) *Handler {
	return &Handler{
		controller: controller,
		messenger:  messenger,
	}
}

// HandleEvent parses and dispatches one normalized app mention or slash command.
func (h *Handler) HandleEvent(ctx context.Context, event Event) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEventEnvelope(event); err != nil {
		return err
	}
	if event.Kind != EventKindAppMention && event.Kind != EventKindSlashCommand {
		return fmt.Errorf("unsupported Slack event kind %q", event.Kind)
	}

	command, err := commands.Parse(event.Text)
	if err != nil {
		return h.post(ctx, messageOrigin(event.Origin), usage)
	}
	if h.controller == nil {
		return errors.New("Slack task controller is nil")
	}

	switch command.Name {
	case "run":
		return h.run(ctx, event, command.Repo, command.Prompt)
	case "status":
		return h.status(ctx, event, command.TaskID)
	case "cancel":
		return h.cancel(ctx, event, command.TaskID)
	case "retry":
		return h.retry(ctx, event, command.TaskID)
	default:
		return h.post(ctx, messageOrigin(event.Origin), usage)
	}
}

func (h *Handler) run(ctx context.Context, event Event, repository, prompt string) error {
	params := store.CreateTaskParams{
		Repository:   repository,
		Prompt:       prompt,
		SlackEventID: event.EventID,
	}
	var created store.Task
	var err error
	if controller, ok := h.controller.(originTaskController); ok {
		created, err = controller.CreateTaskWithOrigin(ctx, params, messageOrigin(event.Origin))
	} else {
		created, err = h.controller.CreateTask(ctx, params)
	}
	if err != nil {
		return h.postTaskError(ctx, event, "", err)
	}
	return h.postTask(ctx, event, created, nil)
}

func (h *Handler) status(ctx context.Context, event Event, taskID string) error {
	got, err := h.controller.GetTask(ctx, taskID)
	if err != nil {
		return h.postTaskError(ctx, event, taskID, err)
	}
	return h.postTask(ctx, event, got, nil)
}

func (h *Handler) cancel(ctx context.Context, event Event, taskID string) error {
	if err := h.controller.RequestCancellation(ctx, taskID); err != nil {
		return h.postTaskError(ctx, event, taskID, err)
	}
	got, err := h.controller.GetTask(ctx, taskID)
	if err != nil {
		return h.postTaskError(ctx, event, taskID, err)
	}
	return h.postTask(ctx, event, got, nil)
}

func (h *Handler) retry(ctx context.Context, event Event, taskID string) error {
	attempt, err := h.controller.RetryTaskWithKey(ctx, taskID, event.EventID)
	if err != nil {
		return h.postTaskError(ctx, event, taskID, err)
	}
	got, err := h.controller.GetTask(ctx, taskID)
	if err != nil {
		return h.postTaskError(ctx, event, taskID, err)
	}
	return h.postTask(ctx, event, got, &attempt)
}

func (h *Handler) postTask(ctx context.Context, event Event, task store.Task, preferred *store.Attempt) error {
	attemptNumber, err := h.currentAttemptNumber(ctx, task, preferred)
	if err != nil {
		return h.postTaskError(ctx, event, task.ID, err)
	}
	text := fmt.Sprintf(
		"Task: %s\nRepository: %s\nState: %s\nAttempt: %d",
		task.ID, displayRepository(task.Repository), task.State, attemptNumber,
	)
	return h.post(ctx, messageOrigin(event.Origin), text)
}

func (h *Handler) currentAttemptNumber(ctx context.Context, task store.Task, preferred *store.Attempt) (int, error) {
	attempts, err := h.controller.ListAttempts(ctx, task.ID)
	if err != nil {
		return 0, err
	}
	if task.CurrentAttemptID != "" {
		for _, attempt := range attempts {
			if attempt.ID == task.CurrentAttemptID {
				return attempt.Number, nil
			}
		}
	}
	if len(attempts) > 0 {
		return attempts[len(attempts)-1].Number, nil
	}
	if preferred != nil && preferred.Number > 0 {
		return preferred.Number, nil
	}
	return 0, errors.New("task has no attempt")
}

func (h *Handler) postTaskError(ctx context.Context, event Event, taskID string, err error) error {
	message := "Task operation failed: " + safeError(err)
	if taskID != "" {
		message = fmt.Sprintf(
			"Task %s failed: %s\nInspect: status %s\nRetry: retry %s",
			taskID, safeError(err), taskID, taskID,
		)
	}
	return h.post(ctx, messageOrigin(event.Origin), message)
}

// HandleWorkerLine forwards only allowlisted structured transition events.
// Plain worker output remains in the log stream and is never posted to Slack.
func (h *Handler) HandleWorkerLine(ctx context.Context, origin protocol.SlackOrigin, line string) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if strings.TrimSpace(origin.ChannelID) == "" {
		return errors.New("Slack channel ID is empty")
	}
	origin = messageOrigin(origin)
	if strings.TrimSpace(origin.ThreadTS) == "" {
		return errors.New("Slack thread timestamp is empty")
	}

	parsed, err := protocol.ParseLine(line)
	if err != nil {
		return fmt.Errorf("parse worker line: %w", err)
	}
	if parsed.Event == nil || !meaningfulTransition(parsed.Event.Type) || parsed.Event.TaskID == "" {
		return nil
	}
	text := safeTransitionText(*parsed.Event)
	if text == "" {
		return nil
	}
	return h.post(ctx, origin, text)
}

func meaningfulTransition(eventType string) bool {
	switch eventType {
	case "transition", "agent_started", "validation_started", "validation_failed", "branch_pushed":
		return true
	default:
		return false
	}
}

func safeTransitionText(event protocol.Event) string {
	message := safeText(event.Message)
	if message == "" {
		message = strings.ReplaceAll(event.Type, "_", " ")
	}
	return fmt.Sprintf("Task %s: %s", event.TaskID, message)
}

func (h *Handler) post(ctx context.Context, origin protocol.SlackOrigin, text string) error {
	if h.messenger == nil {
		return errors.New("Slack messenger is nil")
	}
	if err := h.messenger.PostMessage(ctx, origin, text); err != nil {
		return fmt.Errorf("post Slack message: %w", err)
	}
	return nil
}

func validateContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	return nil
}

func validateEventEnvelope(event Event) error {
	if strings.TrimSpace(event.EventID) == "" {
		return errors.New("Slack event ID is empty")
	}
	if strings.TrimSpace(event.Origin.ChannelID) == "" {
		return errors.New("Slack channel ID is empty")
	}
	if event.Kind != EventKindSlashCommand && strings.TrimSpace(event.Origin.MessageTS) == "" {
		return errors.New("Slack message timestamp is empty")
	}
	return nil
}

func messageOrigin(origin protocol.SlackOrigin) protocol.SlackOrigin {
	if origin.ThreadTS == "" {
		origin.ThreadTS = origin.MessageTS
	}
	return origin
}

func displayRepository(repository string) string {
	repository = safeText(repository)
	parsed, err := url.Parse(repository)
	if err == nil && parsed.User != nil {
		parsed.User = nil
		return parsed.String()
	}
	return repository
}

func safeError(err error) string {
	if err == nil {
		return "operation failed"
	}
	return safeText(err.Error())
}

func safeText(text string) string {
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
	text = redactSensitiveText(text)
	text = strings.TrimSpace(text)
	if len(text) > 512 {
		return text[:512] + "..."
	}
	return text
}

var (
	sensitiveAssignment = regexp.MustCompile(`(?i)(\b(?:password|passwd|token|secret|api[_-]?key|authorization)\s*[:=]\s*)\S+`)
	bearerCredential    = regexp.MustCompile(`(?i)(\bbearer\s+)\S+`)
	credentialURL       = regexp.MustCompile(`(?i)(https?://)[^/\s@]+@`)
)

func redactSensitiveText(text string) string {
	text = bearerCredential.ReplaceAllString(text, `${1}<redacted>`)
	text = sensitiveAssignment.ReplaceAllString(text, `${1}<redacted>`)
	return credentialURL.ReplaceAllString(text, `${1}<redacted>@`)
}
