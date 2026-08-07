package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unicode/utf8"

	"github.com/simpleswe/simpleswe/internal/client"
)

const (
	workerManifestPath = "/run/simpleswe/task.json"
	defaultNamespace   = "simpleswe"

	rootUsage       = "usage: simpleswe <controller|worker|tui|task>"
	controllerUsage = "usage: simpleswe controller --config PATH --database PATH"
	workerUsage     = "usage: simpleswe worker [--manifest PATH]"
	tuiUsage        = "usage: simpleswe tui [--context NAME] [--namespace NAME] [--address URL]"
	taskUsage       = "usage: simpleswe task <create|list|show|cancel|retry|logs|wait>"
	taskCreateUsage = "usage: simpleswe task create [--context NAME] [--namespace NAME] [--address URL] [--idempotency-key KEY] [--pr-title TITLE] REPOSITORY PROMPT"
	taskListUsage   = "usage: simpleswe task list [--context NAME] [--namespace NAME] [--address URL]"
	taskShowUsage   = "usage: simpleswe task show [--context NAME] [--namespace NAME] [--address URL] ID"
	taskWaitUsage   = "usage: simpleswe task wait [--context NAME] [--namespace NAME] [--address URL] ID"
	taskCancelUsage = "usage: simpleswe task cancel [--context NAME] [--namespace NAME] [--address URL] ID"
	taskRetryUsage  = "usage: simpleswe task retry [--context NAME] [--namespace NAME] [--address URL] ID"
	taskLogsUsage   = "usage: simpleswe task logs [--context NAME] [--namespace NAME] [--address URL] ID"
)

// Dependencies is the composition seam used by the binary entrypoint. The
// fields keep command dispatch independent from Kubernetes, external commands,
// the terminal, and the network.
type Dependencies struct {
	RunController func(context.Context, string, string, io.Writer, io.Writer) error
	NewWorkspace  func() (string, func() error, error)
	RunWorker     func(context.Context, string, string, io.Writer, io.Writer) error
	RunTUI        func(context.Context, string, string, string, io.Reader, io.Writer, io.Writer) error
	PortForward   func(context.Context, string, string) (string, func() error, error)

	CreateTask func(context.Context, string, client.CreateTaskRequest) (client.Task, error)
	ListTasks  func(context.Context, string) (client.TaskList, error)
	ShowTask   func(context.Context, string, string) (client.Task, error)
	WaitTask   func(context.Context, string, string) (client.Task, error)
	CancelTask func(context.Context, string, string) (client.Task, error)
	RetryTask  func(context.Context, string, string) (client.Task, error)
	StreamLogs func(context.Context, string, string, io.Writer) error
}

// Run dispatches one simpleswe command. It does not derive a child context so
// cancellation and values reach the selected dependency unchanged.
func Run(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, deps Dependencies) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if stdin == nil {
		stdin = strings.NewReader("")
	}
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if len(args) == 0 {
		return errors.New(rootUsage)
	}

	switch args[0] {
	case "controller":
		return runController(ctx, args[1:], stdout, stderr, deps)
	case "worker":
		return runWorker(ctx, args[1:], stdout, stderr, deps)
	case "tui":
		return runTUI(ctx, args[1:], stdin, stdout, stderr, deps)
	case "task":
		return runTask(ctx, args[1:], stdout, stderr, deps)
	default:
		return fmt.Errorf("unknown command %q; %s", args[0], rootUsage)
	}
}

func runController(ctx context.Context, args []string, stdout, stderr io.Writer, deps Dependencies) error {
	configPath, databasePath, ok := parseControllerArgs(args)
	if !ok {
		return errors.New(controllerUsage)
	}
	if deps.RunController == nil {
		return errors.New("controller runtime is not configured")
	}
	return deps.RunController(ctx, configPath, databasePath, stdout, stderr)
}

func runWorker(ctx context.Context, args []string, stdout, stderr io.Writer, deps Dependencies) (runErr error) {
	manifestPath, ok := parseWorkerArgs(args)
	if !ok {
		return errors.New(workerUsage)
	}

	workspace, closeWorkspace, err := newWorkspace(deps)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeWorkspace()) }()
	if deps.RunWorker == nil {
		return errors.New("worker runtime is not configured")
	}
	return deps.RunWorker(ctx, manifestPath, workspace, stdout, stderr)
}

func runTUI(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, deps Dependencies) (runErr error) {
	flags, _, ok := parseRuntimeArgs(args, 0, false)
	if !ok {
		return errors.New(tuiUsage)
	}
	if deps.RunTUI == nil {
		return errors.New("TUI runtime is not configured")
	}
	address, closeForward, err := resolveAddress(ctx, flags, deps)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeForward()) }()
	return deps.RunTUI(ctx, address, flags.kubeContext, flags.namespace, stdin, stdout, stderr)
}

func runTask(ctx context.Context, args []string, stdout, stderr io.Writer, deps Dependencies) (runErr error) {
	if len(args) == 0 {
		return errors.New(taskUsage)
	}
	command := args[0]
	usage, positionalCount, ok := taskCommandUsage(command)
	if !ok {
		return fmt.Errorf("unknown task command %q; %s", command, taskUsage)
	}
	flags, positional, ok := parseRuntimeArgs(args[1:], positionalCount, command == "create")
	if !ok {
		return errors.New(usage)
	}
	if command == "create" && (strings.TrimSpace(positional[0]) == "" || strings.TrimSpace(positional[1]) == "") {
		return errors.New(usage)
	}

	address, closeForward, err := resolveAddress(ctx, flags, deps)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, closeForward()) }()

	switch command {
	case "create":
		if deps.CreateTask == nil {
			return errors.New("task create runtime is not configured")
		}
		result, err := deps.CreateTask(ctx, address, client.CreateTaskRequest{
			Repository:     positional[0],
			Prompt:         positional[1],
			PRTitle:        flags.prTitle,
			IdempotencyKey: flags.idempotencyKey,
		})
		if err != nil {
			return err
		}
		return encodeJSON(stdout, result)
	case "list":
		if deps.ListTasks == nil {
			return errors.New("task list runtime is not configured")
		}
		result, err := deps.ListTasks(ctx, address)
		if err != nil {
			return err
		}
		return encodeJSON(stdout, result)
	case "show":
		return runTaskJSON(ctx, address, positional[0], stdout, deps.ShowTask, "task show runtime is not configured")
	case "wait":
		return runTaskJSON(ctx, address, positional[0], stdout, deps.WaitTask, "task wait runtime is not configured")
	case "cancel":
		return runTaskJSON(ctx, address, positional[0], stdout, deps.CancelTask, "task cancel runtime is not configured")
	case "retry":
		return runTaskJSON(ctx, address, positional[0], stdout, deps.RetryTask, "task retry runtime is not configured")
	case "logs":
		if deps.StreamLogs == nil {
			return errors.New("task logs runtime is not configured")
		}
		return deps.StreamLogs(ctx, address, positional[0], stdout)
	default:
		return errors.New(usage)
	}
}

func runTaskJSON(
	ctx context.Context,
	address, id string,
	stdout io.Writer,
	run func(context.Context, string, string) (client.Task, error),
	missing string,
) error {
	if run == nil {
		return errors.New(missing)
	}
	result, err := run(ctx, address, id)
	if err != nil {
		return err
	}
	return encodeJSON(stdout, result)
}

type runtimeFlags struct {
	kubeContext    string
	namespace      string
	address        string
	prTitle        string
	idempotencyKey string
}

type runtimeFlagState struct {
	contextSet        bool
	namespaceSet      bool
	addressSet        bool
	prTitleSet        bool
	idempotencyKeySet bool
}

func parseRuntimeArgs(args []string, positionalCount int, allowIdempotencyKey bool) (runtimeFlags, []string, bool) {
	flags := runtimeFlags{namespace: defaultNamespace}
	var state runtimeFlagState
	var positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "" {
			return runtimeFlags{}, nil, false
		}
		if len(positional) > 0 {
			positional = append(positional, arg)
			continue
		}
		if strings.HasPrefix(arg, "-") {
			next, ok := state.parse(args, i, &flags, allowIdempotencyKey)
			if !ok {
				return runtimeFlags{}, nil, false
			}
			i = next
			continue
		}

		positional = append(positional, arg)
	}
	if len(positional) != positionalCount || flags.namespace == "" {
		return runtimeFlags{}, nil, false
	}
	return flags, positional, true
}

func (state *runtimeFlagState) parse(args []string, index int, flags *runtimeFlags, allowIdempotencyKey bool) (int, bool) {
	value, next, ok := nextValue(args, index)
	if !ok {
		return index, false
	}

	switch args[index] {
	case "--context":
		if state.contextSet {
			return index, false
		}
		flags.kubeContext = value
		state.contextSet = true
	case "--namespace":
		if state.namespaceSet {
			return index, false
		}
		flags.namespace = value
		state.namespaceSet = true
	case "--address":
		if state.addressSet {
			return index, false
		}
		flags.address = value
		state.addressSet = true
	case "--idempotency-key":
		if !allowIdempotencyKey || state.idempotencyKeySet {
			return index, false
		}
		flags.idempotencyKey = value
		state.idempotencyKeySet = true
	case "--pr-title":
		if !allowIdempotencyKey || state.prTitleSet || strings.TrimSpace(value) == "" || utf8.RuneCountInString(value) > 256 {
			return index, false
		}
		flags.prTitle = value
		state.prTitleSet = true
	default:
		return index, false
	}
	return next, true
}

func parseControllerArgs(args []string) (configPath, databasePath string, ok bool) {
	for i := 0; i < len(args); i++ {
		value, next, valid := namedValue(args, i, "--config")
		if valid {
			if configPath != "" {
				return "", "", false
			}
			configPath, i = value, next
			continue
		}
		value, next, valid = namedValue(args, i, "--database")
		if valid {
			if databasePath != "" {
				return "", "", false
			}
			databasePath, i = value, next
			continue
		}
		return "", "", false
	}
	return configPath, databasePath, configPath != "" && databasePath != ""
}

func parseWorkerArgs(args []string) (string, bool) {
	manifestPath := workerManifestPath
	manifestSet := false
	for i := 0; i < len(args); i++ {
		value, next, valid := namedValue(args, i, "--manifest")
		if !valid || manifestSet {
			return "", false
		}
		manifestPath, i = value, next
		manifestSet = true
	}
	return manifestPath, true
}

func namedValue(args []string, index int, name string) (string, int, bool) {
	if index >= len(args) || args[index] != name || index+1 >= len(args) || args[index+1] == "" || strings.HasPrefix(args[index+1], "-") {
		return "", index, false
	}
	return args[index+1], index + 1, true
}

func nextValue(args []string, index int) (string, int, bool) {
	if index+1 >= len(args) || args[index+1] == "" || strings.HasPrefix(args[index+1], "-") {
		return "", index, false
	}
	return args[index+1], index + 1, true
}

func taskCommandUsage(command string) (string, int, bool) {
	switch command {
	case "create":
		return taskCreateUsage, 2, true
	case "list":
		return taskListUsage, 0, true
	case "show":
		return taskShowUsage, 1, true
	case "wait":
		return taskWaitUsage, 1, true
	case "cancel":
		return taskCancelUsage, 1, true
	case "retry":
		return taskRetryUsage, 1, true
	case "logs":
		return taskLogsUsage, 1, true
	default:
		return "", 0, false
	}
}

func resolveAddress(ctx context.Context, flags runtimeFlags, deps Dependencies) (string, func() error, error) {
	if flags.address != "" {
		return flags.address, func() error { return nil }, nil
	}
	if deps.PortForward == nil {
		return "", nil, errors.New("port-forward runtime is not configured")
	}
	address, closeForward, err := deps.PortForward(ctx, flags.kubeContext, flags.namespace)
	if err != nil {
		return "", nil, err
	}
	if address == "" {
		if closeForward != nil {
			_ = closeForward()
		}
		return "", nil, errors.New("port-forward returned an empty address")
	}
	if closeForward == nil {
		closeForward = func() error { return nil }
	}
	return address, closeForward, nil
}

func newWorkspace(deps Dependencies) (string, func() error, error) {
	if deps.NewWorkspace != nil {
		workspace, closeWorkspace, err := deps.NewWorkspace()
		if err != nil {
			return "", nil, err
		}
		if workspace == "" {
			return "", nil, errors.New("workspace path is empty")
		}
		if closeWorkspace == nil {
			closeWorkspace = func() error { return os.RemoveAll(workspace) }
		}
		return workspace, closeWorkspace, nil
	}
	workspace, err := os.MkdirTemp("", "simpleswe-worker-")
	if err != nil {
		return "", nil, fmt.Errorf("create worker workspace: %w", err)
	}
	return workspace, func() error { return os.RemoveAll(workspace) }, nil
}

func encodeJSON(output io.Writer, value any) error {
	if err := json.NewEncoder(output).Encode(value); err != nil {
		return fmt.Errorf("encode command output: %w", err)
	}
	return nil
}
