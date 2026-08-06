package worker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/redaction"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

func TestRunCommandStreamsAllOutputButRetainsOnlyBoundedTail(t *testing.T) {
	tmp := t.TempDir()
	script := filepath.Join(tmp, "large-output")
	body := "#!/bin/sh\nprintf 'begin-'\ni=0\nwhile test $i -lt 200; do printf x; i=$((i + 1)); done\nprintf '%s' '-end'\nprintf 'stderr-begin-' >&2\ni=0\nwhile test $i -lt 200; do printf y >&2; i=$((i + 1)); done\nprintf '%s' '-stderr-end' >&2\n"
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("write output command: %v", err)
	}

	var stdout, stderr bytes.Buffer
	result, err := runCommandInDirWithOutput(context.Background(), "", []string{script}, &stdout, &stderr, 96)
	if err != nil {
		t.Fatalf("run output command: %v", err)
	}
	if !strings.HasPrefix(stdout.String(), "begin-") || !strings.HasSuffix(stdout.String(), "-end") {
		t.Fatalf("stdout did not stream completely: %q", stdout.String())
	}
	if !strings.HasPrefix(stderr.String(), "stderr-begin-") || !strings.HasSuffix(stderr.String(), "-stderr-end") {
		t.Fatalf("stderr did not stream completely: %q", stderr.String())
	}
	for name, retained := range map[string]string{"stdout": result.Stdout, "stderr": result.Stderr} {
		if len(retained) > 96 {
			t.Errorf("%s retained %d bytes; want at most 96", name, len(retained))
		}
		if !strings.Contains(retained, OutputTruncatedMarker) {
			t.Errorf("%s missing truncation marker: %q", name, retained)
		}
	}
	if !strings.HasSuffix(result.Stdout, "-end") || !strings.HasSuffix(result.Stderr, "-stderr-end") {
		t.Errorf("bounded results do not retain tails: %#v", result)
	}
}

func TestRunCommandRejectsEmptyCanceledAndMissingCommands(t *testing.T) {
	result, err := RunCommand(context.Background(), nil)
	if err == nil || err.Error() != "command is empty" {
		t.Fatalf("empty command error: got %v", err)
	}
	if result.ExitCode != -1 || len(result.Command) != 0 {
		t.Fatalf("empty command result: %#v", result)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err = RunCommand(ctx, []string{"does-not-run"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled command error: got %v", err)
	}
	if result.ExitCode != -1 || len(result.Command) != 1 || result.Command[0] != "does-not-run" {
		t.Fatalf("canceled command result: %#v", result)
	}

	result, err = RunCommand(context.Background(), []string{filepath.Join(t.TempDir(), "missing-command")})
	if err == nil {
		t.Fatal("missing command unexpectedly succeeded")
	}
	if result.ExitCode != -1 || len(result.Command) != 1 {
		t.Fatalf("missing command result: %#v", result)
	}
}

func TestTailBufferWriteByteAndTinyTruncation(t *testing.T) {
	buffer := newTailBuffer(2)
	if err := buffer.WriteByte('x'); err != nil {
		t.Fatalf("write byte: %v", err)
	}
	if got := buffer.String(); got != "x" {
		t.Fatalf("untruncated byte: got %q, want %q", got, "x")
	}
	if _, err := buffer.Write([]byte("yz")); err != nil {
		t.Fatalf("write overflow: %v", err)
	}
	tiny := newTailBuffer(1)
	if _, err := tiny.Write([]byte("yz")); err != nil {
		t.Fatalf("write tiny overflow: %v", err)
	}
	if got, want := tiny.String(), OutputTruncatedMarker[:1]; got != want {
		t.Fatalf("tiny truncated buffer: got %q, want %q", got, want)
	}
}

func TestLoadSecretsIncludesNamedEnvironmentValuesAndMultilineParts(t *testing.T) {
	t.Setenv("API_TOKEN", "header-line\nenvironment-secret-line\nxy")
	secrets := loadSecrets([]string{"mounted-secret"}, []string{"API_TOKEN"})
	redacted := redaction.Redact("mounted-secret environment-secret-line xy", secrets)
	if strings.Contains(redacted, "mounted-secret") || strings.Contains(redacted, "environment-secret-line") {
		t.Fatalf("loaded secret leaked after redaction: %q", redacted)
	}
	if !strings.Contains(redacted, "xy") {
		t.Fatalf("trivial multiline fragment was redacted: %q", redacted)
	}
}

func TestValidationFixPromptIsBoundedAndMarked(t *testing.T) {
	prompt := validationFixPrompt(strings.Repeat("old-output-", validationFixPromptLimit))
	if len(prompt) > validationFixPromptLimit {
		t.Fatalf("fix prompt length = %d; want at most %d", len(prompt), validationFixPromptLimit)
	}
	if !strings.Contains(prompt, OutputTruncatedMarker) {
		t.Fatalf("truncated fix prompt missing marker: %q", prompt[:min(len(prompt), 200)])
	}
	if !strings.HasSuffix(prompt, "old-output-") {
		t.Fatal("fix prompt did not retain validation tail")
	}
}

func TestReadableStreamRedactsSecretSplitAcrossBoundedChunks(t *testing.T) {
	const secret = "split-environment-secret"
	var output bytes.Buffer
	stream := &readableStream{output: &output, name: "stdout", secrets: redaction.ExpandSecrets([]string{secret})}
	first := strings.Repeat("x", streamChunkBytes-5) + secret[:8]
	second := secret[8:] + strings.Repeat("y", streamChunkBytes)
	if _, err := stream.Write([]byte(first)); err != nil {
		t.Fatalf("write first stream chunk: %v", err)
	}
	if _, err := stream.Write([]byte(second)); err != nil {
		t.Fatalf("write second stream chunk: %v", err)
	}
	if output.Len() == 0 {
		t.Fatal("long line was not streamed before flush")
	}
	if err := stream.flush(); err != nil {
		t.Fatalf("flush stream: %v", err)
	}
	if strings.Contains(output.String(), secret) || !strings.Contains(output.String(), redaction.Placeholder) {
		t.Fatalf("split secret was not redacted: %q", output.String())
	}
}

func TestReadableStreamDoesNotRetainLongSecretAtChunkStart(t *testing.T) {
	secret := strings.Repeat("secret", streamChunkBytes/6+100)
	var output bytes.Buffer
	stream := &readableStream{output: &output, name: "stdout", secrets: []string{secret}}
	data := secret + strings.Repeat("x", streamChunkBytes*3)
	if _, err := stream.Write([]byte(data)); err != nil {
		t.Fatalf("write long secret stream: %v", err)
	}
	if len(stream.pending) > streamChunkBytes+len(secret) {
		t.Fatalf("pending stream retained %d bytes; bounded maximum is %d", len(stream.pending), streamChunkBytes+len(secret))
	}
	if err := stream.flush(); err != nil {
		t.Fatalf("flush long secret stream: %v", err)
	}
	if strings.Contains(output.String(), secret) || !strings.Contains(output.String(), redaction.Placeholder) {
		t.Fatalf("long secret was not redacted: %q", output.String())
	}
}

func TestReadableStreamAndStreamingCommandPropagateOutputErrors(t *testing.T) {
	wantErr := errors.New("output unavailable")
	stream := &readableStream{output: failingWriter{err: wantErr}, name: "stdout"}
	if _, err := stream.Write([]byte("line\n")); !errors.Is(err, wantErr) {
		t.Fatalf("stream write error: got %v, want %v", err, wantErr)
	}
	if _, err := stream.Write([]byte("again\n")); !errors.Is(err, wantErr) {
		t.Fatalf("stream retained error: got %v, want %v", err, wantErr)
	}
	if err := stream.flush(); !errors.Is(err, wantErr) {
		t.Fatalf("stream flush error: got %v, want %v", err, wantErr)
	}

	_, err := runStreamingCommand(context.Background(), "", []string{"sh", "-c", "printf line"}, failingWriter{err: wantErr}, nil, 128)
	if !errors.Is(err, wantErr) {
		t.Fatalf("streaming command output error: got %v, want %v", err, wantErr)
	}
}

func TestEmitEventReportsOutputErrors(t *testing.T) {
	wantErr := errors.New("event output unavailable")
	err := emitEvent(failingWriter{err: wantErr}, nil, protocol.Event{Type: protocol.EventAgentStarted})
	if !errors.Is(err, wantErr) {
		t.Fatalf("event output error: got %v, want %v", err, wantErr)
	}
}

func TestRunValidationLoopIsBoundedAndRetainsFailures(t *testing.T) {
	manifest := protocol.TaskManifest{
		TaskID:            "task-1",
		Repository:        "repo",
		Prompt:            "fix it",
		ValidationCommand: []string{"fake-validation"},
		MaxFixAttempts:    2,
	}
	wantRuns := []CommandResult{
		{ExitCode: 7, Stdout: "first stdout", Stderr: "first stderr"},
		{ExitCode: 8, Stdout: "second stdout", Stderr: "second stderr"},
		{ExitCode: 9, Stdout: "final stdout", Stderr: "final stderr"},
	}

	var calls int
	run := func(_ context.Context, command []string) (CommandResult, error) {
		calls++
		result := wantRuns[calls-1]
		result.Command = append([]string(nil), command...)
		return result, nil
	}

	done := make(chan struct {
		result ValidationResult
		err    error
	}, 1)
	go func() {
		result, err := RunValidationLoop(context.Background(), manifest, run)
		done <- struct {
			result ValidationResult
			err    error
		}{result, err}
	}()

	var outcome struct {
		result ValidationResult
		err    error
	}
	select {
	case outcome = <-done:
	case <-time.After(time.Second):
		t.Fatal("validation loop did not stop at max_fix_attempts")
	}

	if outcome.err == nil {
		t.Fatal("all failed validations unexpectedly succeeded")
	}
	if calls != 3 {
		t.Fatalf("validation calls: got %d, want initial validation plus 2 fixes", calls)
	}
	if len(outcome.result.Runs) != len(wantRuns) {
		t.Fatalf("retained runs: got %d, want %d", len(outcome.result.Runs), len(wantRuns))
	}
	for i, want := range wantRuns {
		got := outcome.result.Runs[i]
		if got.ExitCode != want.ExitCode || got.Stdout != want.Stdout || got.Stderr != want.Stderr {
			t.Errorf("run %d: got %#v, want %#v", i, got, want)
		}
		if len(got.Command) != 1 || got.Command[0] != manifest.ValidationCommand[0] {
			t.Errorf("run %d command: got %#v, want %#v", i, got.Command, manifest.ValidationCommand)
		}
	}
}

func TestRunValidationLoopHandlesInputRunnerErrorsAndCancellation(t *testing.T) {
	manifest := protocol.TaskManifest{
		TaskID:             "task-1",
		Repository:         "repo",
		Prompt:             "fix it",
		ValidationCommands: [][]string{{"first"}, {"second"}},
	}

	if _, err := RunValidationLoop(context.Background(), protocol.TaskManifest{}, func(context.Context, []string) (CommandResult, error) {
		return CommandResult{}, nil
	}); err == nil {
		t.Fatal("invalid manifest unexpectedly succeeded")
	}
	if _, err := RunValidationLoop(context.Background(), manifest, nil); err == nil {
		t.Fatal("nil validation runner unexpectedly succeeded")
	}

	calls := 0
	result, err := RunValidationLoop(context.Background(), manifest, func(_ context.Context, command []string) (CommandResult, error) {
		calls++
		if calls == 1 {
			return CommandResult{ExitCode: 3}, nil
		}
		return CommandResult{ExitCode: 4}, errors.New("runner failed")
	})
	if err == nil || !strings.Contains(err.Error(), "runner failed") {
		t.Fatalf("runner error: got %v", err)
	}
	if len(result.Runs) != 2 || len(result.Runs[0].Command) != 1 || result.Runs[0].Command[0] != "first" {
		t.Fatalf("validation runs: %#v", result.Runs)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := RunValidationLoop(ctx, manifest, func(context.Context, []string) (CommandResult, error) {
		t.Fatal("canceled validation invoked runner")
		return CommandResult{}, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled validation error: got %v", err)
	}
}

func TestRunCommandCancellationReachesChild(t *testing.T) {
	tmp := t.TempDir()
	pidFile := filepath.Join(tmp, "child.pid")
	script := filepath.Join(tmp, "wait-forever")
	scriptBody := "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$1\"\nwhile :; do sleep 1; done\n"
	if err := os.WriteFile(script, []byte(scriptBody), 0o755); err != nil {
		t.Fatalf("write fake executable: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := RunCommand(ctx, []string{script, pidFile})
		done <- err
	}()

	pid := waitForPID(t, pidFile)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled command returned nil error")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled child command did not return")
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("child process %d is still running after cancellation", pid)
}

func TestRequireChangesReturnsExactNoChangesError(t *testing.T) {
	err := RequireChanges(false)
	if err == nil || err.Error() != "no changes detected" {
		t.Fatalf("no-changes error: got %v, want %q", err, "no changes detected")
	}
	if !errors.Is(err, ErrNoChanges) {
		t.Fatalf("no-changes error is not ErrNoChanges: %v", err)
	}
	if err := RequireChanges(true); err != nil {
		t.Fatalf("changed workspace rejected: %v", err)
	}
}

func waitForPID(t *testing.T, path string) int {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
			if err == nil {
				return pid
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("fake executable did not write PID file %s", path)
	return 0
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}
