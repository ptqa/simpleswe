package store

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestSlackOriginIsTransactionalAndSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "origin.sqlite")
	db, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	origin := protocol.SlackOrigin{WorkspaceID: "T1", ChannelID: "C1", MessageTS: "1.2", ThreadTS: "1.1", UserID: "U1"}
	created, err := db.CreateTask(context.Background(), CreateTaskParams{Repository: "repo", Prompt: "fix", SlackEventID: "E1", SlackOrigin: origin})
	if err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	db, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer db.Close()
	got, err := db.GetTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if !reflect.DeepEqual(got.SlackOrigin, origin) {
		t.Fatalf("Slack origin = %#v, want %#v", got.SlackOrigin, origin)
	}
}
