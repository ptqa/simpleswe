package socketmode

import (
	"context"
	"errors"
	"strings"

	"github.com/simpleswe/simpleswe/internal/slack"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

// ChatAPI is the vendor-neutral chat.postMessage operation used by Messenger.
type ChatAPI interface {
	PostMessage(context.Context, PostMessageRequest) error
}

// PostMessageRequest contains only the safe chat.postMessage fields needed by
// the Slack handler.
type PostMessageRequest struct {
	Channel     string `json:"channel"`
	ThreadTS    string `json:"thread_ts"`
	Text        string `json:"text"`
	UnfurlLinks bool   `json:"unfurl_links"`
	UnfurlMedia bool   `json:"unfurl_media"`
}

type messenger struct {
	api ChatAPI
}

// NewMessenger adapts the vendor-neutral chat API to slack.Messenger.
func NewMessenger(api ChatAPI) slack.Messenger {
	return &messenger{api: api}
}

func (m *messenger) PostMessage(ctx context.Context, origin protocol.SlackOrigin, text string) error {
	if ctx == nil {
		return errors.New("Slack messenger context is nil")
	}
	if m == nil || m.api == nil {
		return errors.New("Slack chat API is nil")
	}
	if strings.TrimSpace(origin.ChannelID) == "" {
		return errors.New("Slack channel ID is empty")
	}
	threadTS := origin.ThreadTS
	if strings.TrimSpace(threadTS) == "" {
		threadTS = origin.MessageTS
	}
	if strings.TrimSpace(text) == "" {
		return errors.New("Slack message text is empty")
	}

	request := PostMessageRequest{
		Channel:     origin.ChannelID,
		ThreadTS:    threadTS,
		Text:        safeText(text),
		UnfurlLinks: false,
		UnfurlMedia: false,
	}
	if err := m.api.PostMessage(ctx, request); err != nil {
		return wrapSafeError("post Slack message: ", err)
	}
	return nil
}
