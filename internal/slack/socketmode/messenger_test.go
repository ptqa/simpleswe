package socketmode

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

var _ ChatAPI = (*fakeChatAPI)(nil)

func TestMessengerPostsThreadedMessageWithUnfurlsDisabledAndNoSecretContent(t *testing.T) {
	api := &fakeChatAPI{}
	messenger := NewMessenger(api)
	secret := "xoxb-test-secret"
	origin := protocol.SlackOrigin{
		ChannelID: "C-thread-1",
		ThreadTS:  "1712345678.000010",
	}

	if err := messenger.PostMessage(context.Background(), origin, "Task complete; token="+secret); err != nil {
		t.Fatalf("post message: %v", err)
	}
	if len(api.posts) != 1 {
		t.Fatalf("chat.postMessage calls = %d; want 1", len(api.posts))
	}
	post := api.posts[0]
	if post.Channel != origin.ChannelID {
		t.Errorf("channel = %q; want %q", post.Channel, origin.ChannelID)
	}
	if post.ThreadTS != origin.ThreadTS {
		t.Errorf("thread_ts = %q; want %q", post.ThreadTS, origin.ThreadTS)
	}
	if !strings.Contains(post.Text, "Task complete") {
		t.Errorf("message text = %q; want public task result", post.Text)
	}
	if post.UnfurlLinks || post.UnfurlMedia {
		t.Errorf("unfurls = links:%t media:%t; want both disabled", post.UnfurlLinks, post.UnfurlMedia)
	}
	wire, err := json.Marshal(post)
	if err != nil {
		t.Fatalf("marshal chat.postMessage request: %v", err)
	}
	if strings.Contains(string(wire), secret) {
		t.Fatalf("chat.postMessage request contains secret content: %s", wire)
	}
}

type fakeChatAPI struct {
	posts []PostMessageRequest
}

func (f *fakeChatAPI) PostMessage(_ context.Context, post PostMessageRequest) error {
	f.posts = append(f.posts, post)
	return nil
}
