package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

var ErrNoChanges = errors.New("no changes detected")

const (
	DefaultCommandOutputLimit = 64 << 10
	OutputTruncatedMarker     = "[output truncated; showing tail]\n"
)

type CommandResult struct {
	Command  []string
	ExitCode int
	Stdout   string
	Stderr   string
}

type ValidationResult struct {
	Runs []CommandResult
}

func RequireChanges(changed bool) error {
	if !changed {
		return ErrNoChanges
	}
	return nil
}

// RunCommand executes argv directly. It never joins argv into a shell
// command, and gives each command its own process group so cancellation can
// terminate descendants when the platform supports it.
func RunCommand(ctx context.Context, argv []string) (CommandResult, error) {
	return runCommandInDir(ctx, "", argv)
}

func runCommandInDir(ctx context.Context, dir string, argv []string) (CommandResult, error) {
	return runCommandInDirWithOutput(ctx, dir, argv, nil, nil, DefaultCommandOutputLimit)
}

func runCommandInDirWithOutput(ctx context.Context, dir string, argv []string, stdoutWriter, stderrWriter io.Writer, outputLimit int) (CommandResult, error) {
	return runCommandInDirWithOutputEnv(ctx, dir, argv, stdoutWriter, stderrWriter, outputLimit, nil)
}

func runCommandInDirWithOutputEnv(ctx context.Context, dir string, argv []string, stdoutWriter, stderrWriter io.Writer, outputLimit int, environment map[string]string) (CommandResult, error) {
	command := append([]string(nil), argv...)
	result := CommandResult{Command: command, ExitCode: -1}
	if len(argv) == 0 || argv[0] == "" {
		return result, errors.New("command is empty")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}

	// #nosec G204 -- executing the configured argv directly is the worker contract; no shell is used.
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	if len(environment) > 0 {
		cmd.Env = commandEnvironment(environment)
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if outputLimit <= 0 {
		outputLimit = DefaultCommandOutputLimit
	}
	stdout := newTailBuffer(outputLimit)
	stderr := newTailBuffer(outputLimit)
	cmd.Stdout = stdout
	if stdoutWriter != nil {
		cmd.Stdout = io.MultiWriter(stdout, stdoutWriter)
	}
	cmd.Stderr = stderr
	if stderrWriter != nil {
		cmd.Stderr = io.MultiWriter(stderr, stderrWriter)
	}
	if err := cmd.Start(); err != nil {
		result.Stdout = stdout.String()
		result.Stderr = stderr.String()
		return result, err
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
		killProcessGroup(cmd.Process.Pid)
		<-done
		err = ctx.Err()
	}

	result.ExitCode = commandExitCode(cmd, err)
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	return result, err
}

func commandEnvironment(overrides map[string]string) []string {
	environment := os.Environ()
	filtered := environment[:0]
	for _, value := range environment {
		name, _, _ := strings.Cut(value, "=")
		if _, overridden := overrides[name]; !overridden {
			filtered = append(filtered, value)
		}
	}
	for name, value := range overrides {
		filtered = append(filtered, name+"="+value)
	}
	return filtered
}

type tailBuffer struct {
	limit     int
	data      []byte
	truncated bool
}

func newTailBuffer(limit int) *tailBuffer {
	return &tailBuffer{limit: limit, data: make([]byte, 0, limit)}
}

func (b *tailBuffer) Write(data []byte) (int, error) {
	written := len(data)
	if len(data) >= b.limit {
		b.data = append(b.data[:0], data[len(data)-b.limit:]...)
		b.truncated = true
		return written, nil
	}
	if overflow := len(b.data) + len(data) - b.limit; overflow > 0 {
		copy(b.data, b.data[overflow:])
		b.data = b.data[:len(b.data)-overflow]
		b.truncated = true
	}
	b.data = append(b.data, data...)
	return written, nil
}

func (b *tailBuffer) WriteByte(value byte) error {
	_, err := b.Write([]byte{value})
	return err
}

func (b *tailBuffer) String() string {
	if !b.truncated {
		return string(b.data)
	}
	keep := b.limit - len(OutputTruncatedMarker)
	if keep < 0 {
		return OutputTruncatedMarker[:b.limit]
	}
	data := b.data
	if len(data) > keep {
		data = data[len(data)-keep:]
	}
	return OutputTruncatedMarker + string(data)
}

// RunValidationLoop runs the supplied validation command(s) once initially
// and once per allowed fix attempt. Fixing is intentionally not performed
// here: callers can compose OpenCode or another editor around this primitive.
func RunValidationLoop(ctx context.Context, manifest protocol.TaskManifest, run func(context.Context, []string) (CommandResult, error)) (ValidationResult, error) {
	var result ValidationResult
	if err := protocol.ValidateManifest(manifest); err != nil {
		return result, err
	}
	if run == nil {
		return result, errors.New("validation command runner is nil")
	}

	commands := manifest.ValidationCommands
	if len(commands) == 0 {
		commands = [][]string{manifest.ValidationCommand}
	}
	var lastErr error
	for attempt := 0; attempt <= manifest.MaxFixAttempts; attempt++ {
		allPassed := true
		for _, command := range commands {
			if err := ctx.Err(); err != nil {
				return result, err
			}
			runResult, err := run(ctx, command)
			runResult.Command = append([]string(nil), command...)
			result.Runs = append(result.Runs, runResult)
			if err != nil || runResult.ExitCode != 0 {
				allPassed = false
				if err != nil {
					lastErr = err
				} else {
					lastErr = fmt.Errorf("validation command exited with code %d", runResult.ExitCode)
				}
			}
		}
		if allPassed {
			return result, nil
		}
	}

	return result, fmt.Errorf("validation failed after %d runs: %w", len(result.Runs), lastErr)
}

func killProcessGroup(pid int) {
	if pid <= 0 {
		return
	}
	if err := syscall.Kill(-pid, syscall.SIGKILL); err == nil {
		return
	}
	_ = syscall.Kill(pid, syscall.SIGKILL)
}

func commandExitCode(cmd *exec.Cmd, err error) int {
	if cmd.ProcessState != nil {
		return cmd.ProcessState.ExitCode()
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return -1
}
