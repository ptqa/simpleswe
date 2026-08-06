package run

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

type recordingMessenger struct {
	origin protocol.SlackOrigin
	text   string
}

func (m *recordingMessenger) PostMessage(_ context.Context, origin protocol.SlackOrigin, text string) error {
	m.origin, m.text = origin, text
	return nil
}

func TestPullRequestNotifierLooksUpPersistedSlackOrigin(t *testing.T) {
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "notifier.sqlite"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()
	origin := protocol.SlackOrigin{WorkspaceID: "T1", ChannelID: "C1", MessageTS: "1.2", ThreadTS: "1.1", UserID: "U1"}
	task, err := db.CreateTask(context.Background(), store.CreateTaskParams{Repository: "repo", Prompt: "fix", SlackOrigin: origin})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	messenger := new(recordingMessenger)
	notifier := &pullRequestNotifier{store: db, messenger: messenger}
	if err := notifier.PostPullRequest(context.Background(), task.ID, "https://bitbucket.example/pr/1"); err != nil {
		t.Fatalf("notify: %v", err)
	}
	if !reflect.DeepEqual(messenger.origin, origin) || messenger.text != "Pull request: https://bitbucket.example/pr/1" {
		t.Fatalf("message = %#v %q", messenger.origin, messenger.text)
	}
}

func TestCancellationReplayIsAlreadyApplied(t *testing.T) {
	tests := []struct {
		name string
		task store.Task
		want bool
	}{
		{name: "request recorded", task: store.Task{State: task.RUNNING, CancellationRequested: true}, want: true},
		{name: "cancellation completed", task: store.Task{State: task.CANCELLED}, want: true},
		{name: "new request", task: store.Task{State: task.RUNNING}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := cancellationAlreadyApplied(tt.task); got != tt.want {
				t.Fatalf("cancellationAlreadyApplied(%#v) = %t; want %t", tt.task, got, tt.want)
			}
		})
	}
}
