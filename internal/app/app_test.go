package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
)

var _ func(context.Context, []string, io.Reader, io.Writer, io.Writer, Dependencies) error = Run

const (
	defaultManifest = "/run/simpleswe/task.json"
	usageRoot       = "usage: simpleswe <controller|worker|tui|task>"
	usageController = "usage: simpleswe controller --config PATH --database PATH"
	usageWorker     = "usage: simpleswe worker [--manifest PATH]"
	usageTUI        = "usage: simpleswe tui [--context NAME] [--namespace NAME] [--address URL]"
	usageTask       = "usage: simpleswe task <create|list|show|cancel|retry|logs>"
	usageTaskCreate = "usage: simpleswe task create [--context NAME] [--namespace NAME] [--address URL] REPOSITORY PROMPT"
	usageTaskList   = "usage: simpleswe task list [--context NAME] [--namespace NAME] [--address URL]"
	usageTaskShow   = "usage: simpleswe task show [--context NAME] [--namespace NAME] [--address URL] ID"
	usageTaskCancel = "usage: simpleswe task cancel [--context NAME] [--namespace NAME] [--address URL] ID"
	usageTaskRetry  = "usage: simpleswe task retry [--context NAME] [--namespace NAME] [--address URL] ID"
	usageTaskLogs   = "usage: simpleswe task logs [--context NAME] [--namespace NAME] [--address URL] ID"
)

// These tests define the binary seam: Run owns argument parsing, connection
// setup, JSON output, and cleanup; Dependencies contains only the operations
// needed to keep external processes, Kubernetes, and the network out of tests.
func TestRunDispatchesCommandsAndRuntimeConfiguration(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want []string
	}{
		{
			name: "controller",
			args: []string{"controller", "--config", "/etc/simpleswe/config.yaml", "--database", "/var/lib/simpleswe/tasks.db"},
			want: []string{"controller config=/etc/simpleswe/config.yaml database=/var/lib/simpleswe/tasks.db"},
		},
		{
			name: "worker",
			args: []string{"worker"},
			want: []string{"workspace /isolated/workspace-1", "worker manifest=" + defaultManifest + " workspace=/isolated/workspace-1", "close workspace"},
		},
		{
			name: "tui with automatic port forward",
			args: []string{"tui", "--context", "development", "--namespace", "operators"},
			want: []string{"port-forward context=development namespace=operators", "tui address=http://127.0.0.1:18080 context=development namespace=operators", "close port-forward"},
		},
		{
			name: "task list with automatic port forward defaults",
			args: []string{"task", "list"},
			want: []string{"port-forward context= namespace=simpleswe", "list address=http://127.0.0.1:18080", "close port-forward"},
		},
		{
			name: "task show with explicit address",
			args: []string{"task", "show", "--address", "https://controller.example", "task-7"},
			want: []string{"show address=https://controller.example id=task-7"},
		},
		{
			name: "task cancel with automatic port forward",
			args: []string{"task", "cancel", "--context", "staging", "--namespace", "team-a", "task-8"},
			want: []string{"port-forward context=staging namespace=team-a", "cancel address=http://127.0.0.1:18080 id=task-8", "close port-forward"},
		},
		{
			name: "task retry with explicit address",
			args: []string{"task", "retry", "--address", "http://controller:8080", "task-9"},
			want: []string{"retry address=http://controller:8080 id=task-9"},
		},
		{
			name: "task logs with automatic port forward",
			args: []string{"task", "logs", "--context", "production", "--namespace", "simpleswe-prod", "task-10"},
			want: []string{"port-forward context=production namespace=simpleswe-prod", "logs address=http://127.0.0.1:18080 id=task-10", "close port-forward"},
		},
		{
			name: "task create with hyphen-prefixed prompt",
			args: []string{"task", "create", "--context", "staging", "--namespace", "team-a", "widget", "- update the failing test"},
			want: []string{
				"port-forward context=staging namespace=team-a",
				"create address=http://127.0.0.1:18080 repository=widget prompt=- update the failing test",
				"close port-forward",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			type contextKey struct{}
			ctx := context.WithValue(context.Background(), contextKey{}, tt.name)
			stdin := strings.NewReader("input")
			var stdout, stderr bytes.Buffer
			var calls []string
			deps := recordingDependencies(t, ctx, stdin, &stdout, &stderr, &calls)

			if err := Run(ctx, tt.args, stdin, &stdout, &stderr, deps); err != nil {
				t.Fatalf("Run(%q) error = %v", tt.args, err)
			}
			if !reflect.DeepEqual(calls, tt.want) {
				t.Fatalf("Run(%q) calls = %#v, want %#v", tt.args, calls, tt.want)
			}
		})
	}
}

func TestRunReportsExactUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing command", want: usageRoot},
		{name: "unknown command", args: []string{"serve"}, want: `unknown command "serve"; ` + usageRoot},
		{name: "controller missing both paths", args: []string{"controller"}, want: usageController},
		{name: "controller missing config", args: []string{"controller", "--database", "tasks.db"}, want: usageController},
		{name: "controller missing database", args: []string{"controller", "--config", "config.yaml"}, want: usageController},
		{name: "controller empty config", args: []string{"controller", "--config", "", "--database", "tasks.db"}, want: usageController},
		{name: "controller empty database", args: []string{"controller", "--config", "config.yaml", "--database", ""}, want: usageController},
		{name: "controller positional argument", args: []string{"controller", "--config", "config.yaml", "--database", "tasks.db", "extra"}, want: usageController},
		{name: "worker positional argument", args: []string{"worker", "extra"}, want: usageWorker},
		{name: "tui positional argument", args: []string{"tui", "extra"}, want: usageTUI},
		{name: "missing task command", args: []string{"task"}, want: usageTask},
		{name: "unknown task command", args: []string{"task", "delete"}, want: `unknown task command "delete"; ` + usageTask},
		{name: "task create missing values", args: []string{"task", "create"}, want: usageTaskCreate},
		{name: "task create missing prompt", args: []string{"task", "create", "repo"}, want: usageTaskCreate},
		{name: "task create empty repository", args: []string{"task", "create", "", "prompt"}, want: usageTaskCreate},
		{name: "task create empty prompt", args: []string{"task", "create", "repo", ""}, want: usageTaskCreate},
		{name: "task create whitespace repository", args: []string{"task", "create", " \t", "prompt"}, want: usageTaskCreate},
		{name: "task create whitespace prompt", args: []string{"task", "create", "repo", " \n"}, want: usageTaskCreate},
		{name: "task create extra value", args: []string{"task", "create", "repo", "prompt", "extra"}, want: usageTaskCreate},
		{name: "task create flag after values", args: []string{"task", "create", "repo", "prompt", "--address", "http://controller"}, want: usageTaskCreate},
		{name: "task list positional argument", args: []string{"task", "list", "task-1"}, want: usageTaskList},
		{name: "task show missing ID", args: []string{"task", "show"}, want: usageTaskShow},
		{name: "task show extra ID", args: []string{"task", "show", "task-1", "task-2"}, want: usageTaskShow},
		{name: "task cancel missing ID", args: []string{"task", "cancel"}, want: usageTaskCancel},
		{name: "task cancel extra ID", args: []string{"task", "cancel", "task-1", "task-2"}, want: usageTaskCancel},
		{name: "task retry missing ID", args: []string{"task", "retry"}, want: usageTaskRetry},
		{name: "task retry extra ID", args: []string{"task", "retry", "task-1", "task-2"}, want: usageTaskRetry},
		{name: "task logs missing ID", args: []string{"task", "logs"}, want: usageTaskLogs},
		{name: "task logs extra ID", args: []string{"task", "logs", "task-1", "task-2"}, want: usageTaskLogs},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			err := Run(context.Background(), tt.args, strings.NewReader(""), &stdout, &stderr, Dependencies{})
			if err == nil || err.Error() != tt.want {
				t.Fatalf("Run(%q) error = %v, want %q", tt.args, err, tt.want)
			}
		})
	}
}

func TestWorkerUsesDefaultManifestAndCleansUniqueWorkspaces(t *testing.T) {
	var workspaces []string
	var cleaned []string
	sequence := 0
	deps := Dependencies{
		NewWorkspace: func() (string, func() error, error) {
			sequence++
			workspace := "/isolated/workspace-" + strconv.Itoa(sequence)
			return workspace, func() error {
				cleaned = append(cleaned, workspace)
				return nil
			}, nil
		},
		RunWorker: func(_ context.Context, manifest, workspace string, _, _ io.Writer) error {
			if manifest != defaultManifest {
				t.Errorf("worker manifest = %q, want %q", manifest, defaultManifest)
			}
			workspaces = append(workspaces, workspace)
			return nil
		},
	}

	for range 2 {
		if err := Run(context.Background(), []string{"worker"}, strings.NewReader(""), io.Discard, io.Discard, deps); err != nil {
			t.Fatalf("Run(worker) error = %v", err)
		}
	}
	if want := []string{"/isolated/workspace-1", "/isolated/workspace-2"}; !reflect.DeepEqual(workspaces, want) {
		t.Fatalf("worker workspaces = %#v, want %#v", workspaces, want)
	}
	if !reflect.DeepEqual(cleaned, workspaces) {
		t.Fatalf("cleaned workspaces = %#v, want %#v", cleaned, workspaces)
	}
}

func TestTaskCommandsWriteJSONAndLogsWriteLines(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		deps    Dependencies
		wantID  string
		wantKey string
		wantRaw string
	}{
		{
			name: "list",
			args: []string{"task", "list", "--address", "http://controller"},
			deps: Dependencies{ListTasks: func(context.Context, string) (client.TaskList, error) {
				return client.TaskList{Tasks: []client.Task{{ID: "task-list"}}, NextCursor: "next"}, nil
			}},
			wantID: "task-list", wantKey: "tasks",
		},
		{
			name: "create",
			args: []string{"task", "create", "--address", "http://controller", "https://bitbucket.example/acme/widget", "fix the failing test now"},
			deps: Dependencies{CreateTask: func(context.Context, string, client.CreateTaskRequest) (client.Task, error) {
				return client.Task{ID: "task-create"}, nil
			}},
			wantID: "task-create",
		},
		{
			name: "show",
			args: []string{"task", "show", "--address", "http://controller", "task-show"},
			deps: Dependencies{ShowTask: func(context.Context, string, string) (client.Task, error) {
				return client.Task{ID: "task-show"}, nil
			}},
			wantID: "task-show",
		},
		{
			name: "cancel",
			args: []string{"task", "cancel", "--address", "http://controller", "task-cancel"},
			deps: Dependencies{CancelTask: func(context.Context, string, string) (client.Task, error) {
				return client.Task{ID: "task-cancel"}, nil
			}},
			wantID: "task-cancel",
		},
		{
			name: "retry",
			args: []string{"task", "retry", "--address", "http://controller", "task-retry"},
			deps: Dependencies{RetryTask: func(context.Context, string, string) (client.Task, error) {
				return client.Task{ID: "task-retry"}, nil
			}},
			wantID: "task-retry",
		},
		{
			name: "logs",
			args: []string{"task", "logs", "--address", "http://controller", "task-logs"},
			deps: Dependencies{StreamLogs: func(_ context.Context, _, _ string, output io.Writer) error {
				_, err := io.WriteString(output, "first line\nsecond line\n")
				return err
			}},
			wantRaw: "first line\nsecond line\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout bytes.Buffer
			if err := Run(context.Background(), tt.args, strings.NewReader(""), &stdout, io.Discard, tt.deps); err != nil {
				t.Fatalf("Run(%q) error = %v", tt.args, err)
			}
			if tt.wantRaw != "" {
				if got := stdout.String(); got != tt.wantRaw {
					t.Fatalf("stdout = %q, want %q", got, tt.wantRaw)
				}
				return
			}
			if !bytes.HasSuffix(stdout.Bytes(), []byte("\n")) {
				t.Fatalf("JSON stdout is not newline terminated: %q", stdout.String())
			}
			var document map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &document); err != nil {
				t.Fatalf("stdout is not JSON: %v\n%s", err, stdout.String())
			}
			if tt.wantKey == "tasks" {
				tasks, ok := document["tasks"].([]any)
				if !ok || len(tasks) != 1 {
					t.Fatalf("list JSON = %#v, want one task", document)
				}
				task, ok := tasks[0].(map[string]any)
				if !ok || task["task_id"] != tt.wantID {
					t.Fatalf("list JSON = %#v, want task ID %q", document, tt.wantID)
				}
			} else if document["task_id"] != tt.wantID {
				t.Fatalf("task JSON = %#v, want task ID %q", document, tt.wantID)
			}
		})
	}
}

func TestRunPropagatesContextCancellation(t *testing.T) {
	tests := []struct {
		name string
		args []string
		deps func(chan struct{}) Dependencies
	}{
		{
			name: "controller runtime",
			args: []string{"controller", "--config", "config.yaml", "--database", "tasks.db"},
			deps: func(started chan struct{}) Dependencies {
				return Dependencies{RunController: func(ctx context.Context, _, _ string, _, _ io.Writer) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}}
			},
		},
		{
			name: "worker runtime",
			args: []string{"worker"},
			deps: func(started chan struct{}) Dependencies {
				return Dependencies{
					NewWorkspace: func() (string, func() error, error) { return "/isolated/worker", func() error { return nil }, nil },
					RunWorker: func(ctx context.Context, _, _ string, _, _ io.Writer) error {
						close(started)
						<-ctx.Done()
						return ctx.Err()
					},
				}
			},
		},
		{
			name: "tui runtime",
			args: []string{"tui", "--address", "http://controller"},
			deps: func(started chan struct{}) Dependencies {
				return Dependencies{RunTUI: func(ctx context.Context, _, _, _ string, _ io.Reader, _, _ io.Writer) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				}}
			},
		},
		{
			name: "local API operation",
			args: []string{"task", "list", "--address", "http://controller"},
			deps: func(started chan struct{}) Dependencies {
				return Dependencies{ListTasks: func(ctx context.Context, _ string) (client.TaskList, error) {
					close(started)
					<-ctx.Done()
					return client.TaskList{}, ctx.Err()
				}}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			started := make(chan struct{})
			done := make(chan error, 1)
			go func() {
				done <- Run(ctx, tt.args, strings.NewReader(""), io.Discard, io.Discard, tt.deps(started))
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("dependency was not called")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Run() error = %v, want context.Canceled", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Run() did not return after context cancellation")
			}
		})
	}
}

func recordingDependencies(t *testing.T, wantCtx context.Context, wantStdin io.Reader, wantStdout, wantStderr io.Writer, calls *[]string) Dependencies {
	t.Helper()
	checkContext := func(got context.Context) {
		t.Helper()
		if got != wantCtx {
			t.Errorf("dependency context differs from Run context")
		}
	}
	checkOutput := func(stdout, stderr io.Writer) {
		t.Helper()
		if stdout != wantStdout || stderr != wantStderr {
			t.Errorf("dependency output streams differ from Run streams")
		}
	}
	workspaceSequence := 0
	return Dependencies{
		RunController: func(ctx context.Context, configPath, databasePath string, stdout, stderr io.Writer) error {
			checkContext(ctx)
			checkOutput(stdout, stderr)
			*calls = append(*calls, "controller config="+configPath+" database="+databasePath)
			return nil
		},
		NewWorkspace: func() (string, func() error, error) {
			workspaceSequence++
			workspace := "/isolated/workspace-" + strconv.Itoa(workspaceSequence)
			*calls = append(*calls, "workspace "+workspace)
			return workspace, func() error {
				*calls = append(*calls, "close workspace")
				return nil
			}, nil
		},
		RunWorker: func(ctx context.Context, manifestPath, workspace string, stdout, stderr io.Writer) error {
			checkContext(ctx)
			checkOutput(stdout, stderr)
			*calls = append(*calls, "worker manifest="+manifestPath+" workspace="+workspace)
			return nil
		},
		RunTUI: func(ctx context.Context, address, kubeContext, namespace string, stdin io.Reader, stdout, stderr io.Writer) error {
			checkContext(ctx)
			checkOutput(stdout, stderr)
			if stdin != wantStdin {
				t.Errorf("TUI stdin differs from Run stdin")
			}
			*calls = append(*calls, "tui address="+address+" context="+kubeContext+" namespace="+namespace)
			return nil
		},
		PortForward: func(ctx context.Context, kubeContext, namespace string) (string, func() error, error) {
			checkContext(ctx)
			*calls = append(*calls, "port-forward context="+kubeContext+" namespace="+namespace)
			return "http://127.0.0.1:18080", func() error {
				*calls = append(*calls, "close port-forward")
				return nil
			}, nil
		},
		ListTasks: func(ctx context.Context, address string) (client.TaskList, error) {
			checkContext(ctx)
			*calls = append(*calls, "list address="+address)
			return client.TaskList{}, nil
		},
		ShowTask: func(ctx context.Context, address, id string) (client.Task, error) {
			checkContext(ctx)
			*calls = append(*calls, "show address="+address+" id="+id)
			return client.Task{ID: id}, nil
		},
		CancelTask: func(ctx context.Context, address, id string) (client.Task, error) {
			checkContext(ctx)
			*calls = append(*calls, "cancel address="+address+" id="+id)
			return client.Task{ID: id}, nil
		},
		RetryTask: func(ctx context.Context, address, id string) (client.Task, error) {
			checkContext(ctx)
			*calls = append(*calls, "retry address="+address+" id="+id)
			return client.Task{ID: id}, nil
		},
		StreamLogs: func(ctx context.Context, address, id string, stdout io.Writer) error {
			checkContext(ctx)
			if stdout != wantStdout {
				t.Errorf("log output stream differs from Run stdout")
			}
			*calls = append(*calls, "logs address="+address+" id="+id)
			return nil
		},
		CreateTask: func(ctx context.Context, address string, request client.CreateTaskRequest) (client.Task, error) {
			checkContext(ctx)
			*calls = append(*calls, "create address="+address+" repository="+request.Repository+" prompt="+request.Prompt)
			return client.Task{ID: "task-create", Repository: request.Repository, Prompt: request.Prompt}, nil
		},
	}
}
