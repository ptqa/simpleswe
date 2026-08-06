package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/client"
)

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestRunRejectsInvalidContextsAndMissingRuntimes(t *testing.T) {
	var nilContext context.Context
	if err := Run(nilContext, nil, nil, nil, nil, Dependencies{}); err == nil || err.Error() != "context is nil" {
		t.Fatalf("Run(nil context) error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(cancelled, []string{"worker"}, nil, nil, nil, Dependencies{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run(cancelled context) error = %v", err)
	}

	tests := []struct {
		args []string
		want string
	}{
		{args: []string{"controller", "--config", "config.yaml", "--database", "tasks.db"}, want: "controller runtime is not configured"},
		{args: []string{"worker"}, want: "worker runtime is not configured"},
		{args: []string{"tui", "--address", "http://controller"}, want: "TUI runtime is not configured"},
		{args: []string{"task", "list", "--address", "http://controller"}, want: "task list runtime is not configured"},
		{args: []string{"task", "show", "--address", "http://controller", "task-1"}, want: "task show runtime is not configured"},
		{args: []string{"task", "cancel", "--address", "http://controller", "task-1"}, want: "task cancel runtime is not configured"},
		{args: []string{"task", "retry", "--address", "http://controller", "task-1"}, want: "task retry runtime is not configured"},
		{args: []string{"task", "logs", "--address", "http://controller", "task-1"}, want: "task logs runtime is not configured"},
	}
	for _, test := range tests {
		err := Run(context.Background(), test.args, nil, nil, nil, Dependencies{})
		if err == nil || err.Error() != test.want {
			t.Errorf("Run(%q) error = %v, want %q", test.args, err, test.want)
		}
	}
}

func TestRunPropagatesDependencyAndCleanupErrors(t *testing.T) {
	runErr := errors.New("runtime failed")
	closeErr := errors.New("cleanup failed")
	workerErr := Run(context.Background(), []string{"worker"}, nil, nil, nil, Dependencies{
		NewWorkspace: func() (string, func() error, error) {
			return "/workspace", func() error { return closeErr }, nil
		},
		RunWorker: func(context.Context, string, string, io.Writer, io.Writer) error { return runErr },
	})
	if !errors.Is(workerErr, runErr) || !errors.Is(workerErr, closeErr) {
		t.Fatalf("Run(worker) error = %v, want runtime and cleanup errors", workerErr)
	}

	tuiErr := Run(context.Background(), []string{"tui"}, nil, nil, nil, Dependencies{
		PortForward: func(context.Context, string, string) (string, func() error, error) {
			return "http://controller", func() error { return closeErr }, nil
		},
		RunTUI: func(context.Context, string, string, string, io.Reader, io.Writer, io.Writer) error { return runErr },
	})
	if !errors.Is(tuiErr, runErr) || !errors.Is(tuiErr, closeErr) {
		t.Fatalf("Run(tui) error = %v, want runtime and cleanup errors", tuiErr)
	}

	apiErr := errors.New("API failed")
	for _, deps := range []Dependencies{
		{ListTasks: func(context.Context, string) (client.TaskList, error) { return client.TaskList{}, apiErr }},
		{ShowTask: func(context.Context, string, string) (client.Task, error) { return client.Task{}, apiErr }},
	} {
		args := []string{"task", "list", "--address", "http://controller"}
		if deps.ShowTask != nil {
			args = []string{"task", "show", "--address", "http://controller", "task-1"}
		}
		if err := Run(context.Background(), args, nil, nil, nil, deps); !errors.Is(err, apiErr) {
			t.Errorf("Run(%q) error = %v, want API error", args, err)
		}
	}
}

func TestAddressAndWorkspaceSetupFailures(t *testing.T) {
	setupErr := errors.New("setup failed")
	closed := false
	tests := []struct {
		name string
		deps Dependencies
		want string
	}{
		{name: "missing port forward", deps: Dependencies{}, want: "port-forward runtime is not configured"},
		{name: "port forward error", deps: Dependencies{PortForward: func(context.Context, string, string) (string, func() error, error) {
			return "", nil, setupErr
		}}, want: setupErr.Error()},
		{name: "empty address", deps: Dependencies{PortForward: func(context.Context, string, string) (string, func() error, error) {
			return "", func() error { closed = true; return nil }, nil
		}}, want: "port-forward returned an empty address"},
	}
	for _, test := range tests {
		deps := test.deps
		deps.RunTUI = func(context.Context, string, string, string, io.Reader, io.Writer, io.Writer) error { return nil }
		err := Run(context.Background(), []string{"tui"}, nil, nil, nil, deps)
		if err == nil || err.Error() != test.want {
			t.Errorf("%s error = %v, want %q", test.name, err, test.want)
		}
	}
	if !closed {
		t.Fatal("empty port-forward address did not close the forward")
	}

	for _, deps := range []Dependencies{
		{NewWorkspace: func() (string, func() error, error) { return "", nil, setupErr }},
		{NewWorkspace: func() (string, func() error, error) { return "", nil, nil }},
	} {
		err := Run(context.Background(), []string{"worker"}, nil, nil, nil, deps)
		if err == nil {
			t.Fatal("Run(worker) accepted invalid workspace setup")
		}
	}
}

func TestDefaultCleanupAndOutputErrors(t *testing.T) {
	workspace := filepath.Join(t.TempDir(), "workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	err := Run(context.Background(), []string{"worker", "--manifest", "task.json"}, nil, nil, nil, Dependencies{
		NewWorkspace: func() (string, func() error, error) { return workspace, nil, nil },
		RunWorker:    func(context.Context, string, string, io.Writer, io.Writer) error { return nil },
	})
	if err != nil {
		t.Fatalf("Run(worker) error = %v", err)
	}
	if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace cleanup error = %v, want removed", err)
	}

	created, cleanup, err := newWorkspace(Dependencies{})
	if err != nil {
		t.Fatalf("newWorkspace() error = %v", err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup default workspace: %v", err)
	}
	if !strings.Contains(filepath.Base(created), "simpleswe-worker-") {
		t.Fatalf("default workspace = %q", created)
	}

	writeErr := errors.New("write failed")
	if err := encodeJSON(errorWriter{err: writeErr}, map[string]string{"ok": "yes"}); !errors.Is(err, writeErr) {
		t.Fatalf("encodeJSON() error = %v, want write error", err)
	}
	var output bytes.Buffer
	deps := Dependencies{
		PortForward: func(context.Context, string, string) (string, func() error, error) {
			return "http://controller", nil, nil
		},
		ListTasks: func(context.Context, string) (client.TaskList, error) { return client.TaskList{}, nil },
	}
	if err := Run(context.Background(), []string{"task", "list"}, nil, &output, nil, deps); err != nil {
		t.Fatalf("Run(task list) with nil closer error = %v", err)
	}
}

func TestParsersRejectDuplicateAndMissingFlagValues(t *testing.T) {
	invalid := [][]string{
		{"tui", "--context", "one", "--context", "two"},
		{"tui", "--namespace", "one", "--namespace", "two"},
		{"tui", "--address", "one", "--address", "two"},
		{"tui", "--context"},
		{"tui", "--namespace", ""},
		{"tui", "--address", "--context"},
		{"worker", "--manifest", "one", "--manifest", "two"},
		{"controller", "--config", "one", "--config", "two", "--database", "db"},
		{"controller", "--config", "config", "--database", "one", "--database", "two"},
	}
	for _, args := range invalid {
		if err := Run(context.Background(), args, nil, nil, nil, Dependencies{}); err == nil {
			t.Errorf("Run(%q) error = nil, want usage error", args)
		}
	}
}
