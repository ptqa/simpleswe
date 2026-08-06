package socketmode

import (
	"context"
	"errors"

	slackapi "github.com/slack-go/slack"
	slacksocket "github.com/slack-go/slack/socketmode"
)

// SDKSocket adapts slack-go's Socket Mode client to the small transport
// boundary. AckCtx writes the acknowledgement directly and does not wait for
// event handling.
type SDKSocket struct {
	client *slacksocket.Client
	events chan Envelope
}

func NewSDKSocket(client *slacksocket.Client) *SDKSocket {
	return &SDKSocket{client: client, events: make(chan Envelope)}
}

func (s *SDKSocket) Events() <-chan Envelope { return s.events }

func (s *SDKSocket) Ack(ctx context.Context, envelopeID string) error {
	if s == nil || s.client == nil {
		return errors.New("Slack Socket Mode SDK client is nil")
	}
	return s.client.AckCtx(ctx, envelopeID, nil)
}

// Run owns the slack-go connection and event bridge until context shutdown.
func (s *SDKSocket) Run(ctx context.Context) error {
	if s == nil || s.client == nil {
		return errors.New("Slack Socket Mode SDK client is nil")
	}
	bridgeCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	bridgeDone := make(chan struct{})
	go func() {
		defer close(bridgeDone)
		defer close(s.events)
		for {
			select {
			case <-bridgeCtx.Done():
				return
			case event, open := <-s.client.Events:
				if !open {
					return
				}
				if event.Request == nil || event.Request.EnvelopeID == "" {
					continue
				}
				envelope := Envelope{ID: event.Request.EnvelopeID, Type: event.Request.Type, Payload: append([]byte(nil), event.Request.Payload...)}
				select {
				case s.events <- envelope:
				case <-bridgeCtx.Done():
					return
				}
			}
		}
	}()
	err := s.client.RunContext(ctx)
	cancel()
	<-bridgeDone
	if ctx.Err() != nil {
		return nil
	}
	return err
}

type SDKChatAPI struct{ Client *slackapi.Client }

func (a SDKChatAPI) PostMessage(ctx context.Context, request PostMessageRequest) error {
	if a.Client == nil {
		return errors.New("Slack Web API client is nil")
	}
	options := []slackapi.MsgOption{
		slackapi.MsgOptionText(request.Text, false),
		slackapi.MsgOptionDisableLinkUnfurl(),
		slackapi.MsgOptionDisableMediaUnfurl(),
	}
	if request.ThreadTS != "" {
		options = append(options, slackapi.MsgOptionTS(request.ThreadTS))
	}
	_, _, err := a.Client.PostMessageContext(ctx, request.Channel, options...)
	return err
}
