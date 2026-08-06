package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/simpleswe/simpleswe/internal/redaction"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
)

// Runner executes one task manifest in a fresh repository workspace.
type Runner struct {
	ManifestPath    string
	WorkspaceDir    string
	Output          io.Writer
	Secrets         []string
	OutputTailBytes int
}

const (
	validationFixPromptLimit = 32 << 10
	streamChunkBytes         = 8 << 10
)

// Run clones the task repository, invokes OpenCode, validates its changes,
// and pushes one non-forced task branch.
func (r Runner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if r.ManifestPath == "" {
		return errors.New("manifest path is required")
	}
	if r.WorkspaceDir == "" {
		return errors.New("workspace directory is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	manifest, err := readManifest(r.ManifestPath)
	if err != nil {
		return err
	}
	if err := validateRunnerManifest(manifest); err != nil {
		return err
	}
	if err := requireFreshWorkspace(r.WorkspaceDir); err != nil {
		return err
	}
	r.Secrets = loadSecrets(r.Secrets, strings.Split(os.Getenv(protocol.SecretEnvNamesVariable), ","))

	output := r.Output
	if output == nil {
		output = io.Discard
	}
	output = &synchronizedWriter{writer: output}
	run := func(argv []string) (CommandResult, error) {
		return runStreamingCommand(ctx, r.WorkspaceDir, argv, output, r.Secrets, r.OutputTailBytes)
	}
	runOutsideWorkspace := func(argv []string) (CommandResult, error) {
		return runStreamingCommand(ctx, "", argv, output, r.Secrets, r.OutputTailBytes)
	}
	runWithEnvironment := func(argv []string, environment map[string]string) (CommandResult, error) {
		return runStreamingCommandWithEnvironment(ctx, r.WorkspaceDir, argv, output, r.Secrets, r.OutputTailBytes, environment)
	}

	cloneURL := manifest.CloneURL
	if cloneURL == "" {
		cloneURL = manifest.Repository
	}
	if _, err := runOutsideWorkspace([]string{"git", "clone", "--no-checkout", "--origin", "origin", "--", cloneURL, r.WorkspaceDir}); err != nil {
		return commandFailure(ctx, "clone repository", err)
	}
	baseRef := "refs/remotes/origin/" + manifest.BaseBranch
	if _, err := run([]string{"git", "fetch", "--no-tags", "origin", "+refs/heads/" + manifest.BaseBranch + ":" + baseRef}); err != nil {
		return commandFailure(ctx, "fetch base branch", err)
	}
	baseCommit, err := commandText(ctx, run, []string{"git", "rev-parse", "--verify", baseRef + "^{commit}"})
	if err != nil {
		return fmt.Errorf("resolve base branch: %w", err)
	}
	if _, err := run([]string{"git", "checkout", "-b", manifest.TaskBranch, baseCommit}); err != nil {
		return commandFailure(ctx, "check out task branch", err)
	}

	initialCommand := appendCommandArgument(manifest.OpenCodeCommand, manifest.Prompt)
	if err := emitEvent(output, r.Secrets, protocol.Event{
		Type:      protocol.EventAgentStarted,
		TaskID:    manifest.TaskID,
		Message:   "initial",
		Timestamp: time.Now().UTC(),
		Command:   initialCommand,
	}); err != nil {
		return err
	}
	if _, err := run(initialCommand); err != nil {
		return commandFailure(ctx, "run OpenCode", err)
	}
	changed, err := repositoryChanged(ctx, run)
	if err != nil {
		return err
	}
	if !changed {
		return fmt.Errorf("%w: OpenCode did not modify the repository", ErrNoChanges)
	}

	commands := manifest.ValidationCommands
	if len(commands) == 0 {
		commands = [][]string{manifest.ValidationCommand}
	}
	var lastFailure *validationFailure
	for attempt := 0; attempt <= manifest.MaxFixAttempts; attempt++ {
		failure, err := r.runValidationRound(ctx, output, manifest, commands, attempt+1, run)
		if err != nil {
			return err
		}
		if failure == nil {
			lastFailure = nil
			if err := emitEvent(output, r.Secrets, protocol.Event{
				Type: protocol.EventValidationSucceeded, TaskID: manifest.TaskID,
				Message: fmt.Sprintf("attempt %d", attempt+1), Timestamp: time.Now().UTC(),
			}); err != nil {
				return err
			}
			break
		}
		lastFailure = failure
		if attempt == manifest.MaxFixAttempts {
			break
		}

		fixPrompt := validationFixPrompt(failure.summary)
		fixCommand := appendCommandArgument(manifest.OpenCodeCommand, fixPrompt)
		if err := emitEvent(output, r.Secrets, protocol.Event{
			Type:      protocol.EventAgentStarted,
			TaskID:    manifest.TaskID,
			Message:   "validation_fix",
			Timestamp: time.Now().UTC(),
			Command:   fixCommand,
		}); err != nil {
			return err
		}
		if _, err := run(fixCommand); err != nil {
			return commandFailure(ctx, "run OpenCode validation fix", err)
		}
	}
	if lastFailure != nil {
		if err := emitEvent(output, r.Secrets, protocol.Event{
			Type: protocol.EventValidationFailed, TaskID: manifest.TaskID,
			Message: lastFailure.summary, Timestamp: time.Now().UTC(),
			Command: lastFailure.command, ExitCode: lastFailure.exitCode,
		}); err != nil {
			return err
		}
		return fmt.Errorf("validation failed after %d attempts: %s", manifest.MaxFixAttempts+1, lastFailure.summary)
	}

	head, err := commandText(ctx, run, []string{"git", "rev-parse", "--verify", "HEAD^{commit}"})
	if err != nil {
		return fmt.Errorf("resolve task branch: %w", err)
	}
	if head != baseCommit {
		return fmt.Errorf("task branch HEAD changed unexpectedly before worker commit: got %s, want %s", head, baseCommit)
	}
	if _, err := run([]string{"git", "merge-base", "--is-ancestor", baseCommit, "HEAD"}); err != nil {
		return commandFailure(ctx, "verify task branch ancestry", err)
	}
	if _, err := run([]string{"git", "add", "--all", "--", "."}); err != nil {
		return commandFailure(ctx, "stage repository changes", err)
	}
	staged, err := hasStagedChanges(ctx, run)
	if err != nil {
		return err
	}
	if !staged {
		return fmt.Errorf("%w: OpenCode did not modify the repository", ErrNoChanges)
	}
	commitMessage := "chore: complete SimpleSWE task " + manifest.TaskID
	commitEnvironment := map[string]string{
		"GIT_AUTHOR_NAME": "simpleswe", "GIT_AUTHOR_EMAIL": "simpleswe@localhost",
		"GIT_COMMITTER_NAME": "simpleswe", "GIT_COMMITTER_EMAIL": "simpleswe@localhost",
	}
	if _, err := runWithEnvironment([]string{"git", "-c", "user.name=simpleswe", "-c", "user.email=simpleswe@localhost", "commit", "-m", commitMessage}, commitEnvironment); err != nil {
		return commandFailure(ctx, "commit task changes", err)
	}
	commit, err := commandText(ctx, run, []string{"git", "rev-parse", "--verify", "HEAD^{commit}"})
	if err != nil {
		return fmt.Errorf("resolve task commit: %w", err)
	}
	if _, err := run([]string{"git", "merge-base", "--is-ancestor", baseCommit, commit}); err != nil {
		return commandFailure(ctx, "verify committed branch ancestry", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := run([]string{"git", "push", "origin", "HEAD:refs/heads/" + manifest.TaskBranch}); err != nil {
		return commandFailure(ctx, "push task branch", err)
	}
	branchEvent := protocol.Event{
		Type:      protocol.EventBranchPushed,
		TaskID:    manifest.TaskID,
		Timestamp: time.Now().UTC(),
		Branch:    manifest.TaskBranch,
		CommitSHA: commit,
	}
	if err := protocol.ValidateEvent(branchEvent, manifest.TaskBranch); err != nil {
		return err
	}
	return emitEvent(output, r.Secrets, branchEvent)
}

type validationFailure struct {
	summary  string
	command  []string
	exitCode int
}

func (r Runner) runValidationRound(
	ctx context.Context,
	output io.Writer,
	manifest protocol.TaskManifest,
	commands [][]string,
	attempt int,
	run func([]string) (CommandResult, error),
) (*validationFailure, error) {
	failures := newTailBuffer(validationFixPromptLimit)
	hasFailure := false
	var lastFailure *validationFailure
	for _, command := range commands {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := emitEvent(output, r.Secrets, protocol.Event{
			Type:      protocol.EventValidationStarted,
			TaskID:    manifest.TaskID,
			Message:   fmt.Sprintf("attempt %d", attempt),
			Timestamp: time.Now().UTC(),
			Command:   command,
		}); err != nil {
			return nil, err
		}
		result, runErr := run(command)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		passed := runErr == nil && result.ExitCode == 0
		message := fmt.Sprintf("attempt %d succeeded", attempt)
		if !passed {
			message = fmt.Sprintf("attempt %d failed", attempt)
		}
		if err := emitEvent(output, r.Secrets, protocol.Event{
			Type: protocol.EventValidationResult, TaskID: manifest.TaskID, Message: message,
			Timestamp: time.Now().UTC(), Command: command, ExitCode: result.ExitCode,
		}); err != nil {
			return nil, err
		}
		if passed {
			continue
		}
		if hasFailure {
			_ = failures.WriteByte('\n')
		}
		hasFailure = true
		_, _ = fmt.Fprintf(failures, "command: %s\nexit code: %d", strings.Join(result.Command, " "), result.ExitCode)
		if result.Stdout != "" {
			_, _ = fmt.Fprintf(failures, "\nstdout:\n%s", result.Stdout)
		}
		if result.Stderr != "" {
			_, _ = fmt.Fprintf(failures, "\nstderr:\n%s", result.Stderr)
		}
		if runErr != nil {
			_, _ = fmt.Fprintf(failures, "\nerror: %v", runErr)
		}
		lastFailure = &validationFailure{command: append([]string(nil), command...), exitCode: result.ExitCode}
	}
	if lastFailure != nil {
		lastFailure.summary = redaction.Redact(failures.String(), r.Secrets)
	}
	return lastFailure, nil
}

func loadSecrets(values, environmentNames []string) []string {
	loaded := append([]string(nil), values...)
	for _, name := range environmentNames {
		name = strings.TrimSpace(name)
		if name != "" {
			loaded = append(loaded, os.Getenv(name))
		}
	}
	return redaction.ExpandSecrets(loaded)
}

func validationFixPrompt(failure string) string {
	const header = "Validation failed. Fix the repository so all validation commands pass.\n\n"
	tail := newTailBuffer(validationFixPromptLimit - len(header))
	_, _ = tail.Write([]byte(failure))
	return header + tail.String()
}

func readManifest(path string) (protocol.TaskManifest, error) {
	// #nosec G304 -- path is the explicit manifest path supplied to the worker.
	data, err := os.ReadFile(path)
	if err != nil {
		return protocol.TaskManifest{}, fmt.Errorf("read task manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest protocol.TaskManifest
	if err := decoder.Decode(&manifest); err != nil {
		return protocol.TaskManifest{}, fmt.Errorf("decode task manifest: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return protocol.TaskManifest{}, errors.New("decode task manifest: multiple JSON values")
		}
		return protocol.TaskManifest{}, fmt.Errorf("decode task manifest: %w", err)
	}
	if err := protocol.ValidateManifest(manifest); err != nil {
		return protocol.TaskManifest{}, fmt.Errorf("validate task manifest: %w", err)
	}
	return manifest, nil
}

func validateRunnerManifest(manifest protocol.TaskManifest) error {
	if manifest.BaseBranch == "" {
		return errors.New("base_branch is required")
	}
	if manifest.TaskBranch == "" {
		return errors.New("task_branch is required")
	}
	if len(manifest.OpenCodeCommand) == 0 {
		return errors.New("opencode_command is required")
	}
	for name, branch := range map[string]string{"base_branch": manifest.BaseBranch, "task_branch": manifest.TaskBranch} {
		if strings.HasPrefix(branch, "refs/") || strings.ContainsAny(branch, " ~^:?*[\\") {
			return fmt.Errorf("%s is not a safe branch name", name)
		}
	}
	return nil
}

func requireFreshWorkspace(path string) error {
	_, err := os.Stat(path)
	if err == nil {
		return fmt.Errorf("workspace %q already exists; a fresh workspace is required", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect workspace: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("create workspace parent: %w", err)
	}
	return nil
}

func appendCommandArgument(command []string, argument string) []string {
	argv := make([]string, len(command), len(command)+1)
	copy(argv, command)
	return append(argv, argument)
}

func repositoryChanged(ctx context.Context, run func([]string) (CommandResult, error)) (bool, error) {
	result, err := run([]string{"git", "status", "--porcelain", "--untracked-files=all"})
	if err != nil {
		return false, commandFailure(ctx, "inspect repository changes", err)
	}
	return result.Stdout != "", nil
}

func hasStagedChanges(ctx context.Context, run func([]string) (CommandResult, error)) (bool, error) {
	result, err := run([]string{"git", "diff", "--cached", "--quiet", "--exit-code"})
	if err == nil {
		return false, nil
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if result.ExitCode == 1 {
		return true, nil
	}
	return false, fmt.Errorf("inspect staged changes: %w", err)
}

func commandText(ctx context.Context, run func([]string) (CommandResult, error), command []string) (string, error) {
	result, err := run(command)
	if err != nil {
		return "", commandFailure(ctx, strings.Join(command, " "), err)
	}
	return strings.TrimSpace(result.Stdout), nil
}

func commandFailure(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func runStreamingCommand(ctx context.Context, dir string, command []string, output io.Writer, secrets []string, outputLimit int) (CommandResult, error) {
	return runStreamingCommandWithEnvironment(ctx, dir, command, output, secrets, outputLimit, nil)
}

func runStreamingCommandWithEnvironment(ctx context.Context, dir string, command []string, output io.Writer, secrets []string, outputLimit int, environment map[string]string) (CommandResult, error) {
	stdout := &readableStream{output: output, name: "stdout", secrets: secrets}
	stderr := &readableStream{output: output, name: "stderr", secrets: secrets}
	result, runErr := runCommandInDirWithOutputEnv(ctx, dir, command, stdout, stderr, outputLimit, environment)
	if err := errors.Join(stdout.flush(), stderr.flush()); err != nil {
		return result, err
	}
	return result, runErr
}

type synchronizedWriter struct {
	mu     sync.Mutex
	writer io.Writer
}

func (w *synchronizedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(data)
}

type readableStream struct {
	output  io.Writer
	name    string
	secrets []string
	pending []byte
	err     error
}

func (s *readableStream) Write(data []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	written := len(data)
	s.pending = append(s.pending, data...)
	for {
		newline := bytes.IndexByte(s.pending, '\n')
		if newline >= 0 && newline <= streamChunkBytes {
			if err := s.writeLine(string(s.pending[:newline])); err != nil {
				s.err = err
				return 0, err
			}
			s.pending = s.pending[newline+1:]
			continue
		}
		maxSecretLength := 1
		for _, secret := range s.secrets {
			if len(secret) > maxSecretLength {
				maxSecretLength = len(secret)
			}
		}
		if len(s.pending) <= streamChunkBytes+maxSecretLength {
			break
		}
		chunk, consumed := nextStreamChunk(s.pending, s.secrets)
		if err := s.writeLine(chunk); err != nil {
			s.err = err
			return 0, err
		}
		s.pending = s.pending[consumed:]
	}
	return written, nil
}

func (s *readableStream) flush() error {
	if s.err != nil {
		return s.err
	}
	if len(s.pending) == 0 {
		return nil
	}
	err := s.writeLine(string(s.pending))
	s.pending = nil
	return err
}

func nextStreamChunk(data []byte, secrets []string) (string, int) {
	matchStart, matchLength := -1, 0
	for _, secret := range secrets {
		if secret == "" {
			continue
		}
		start := bytes.Index(data, []byte(secret))
		if start >= 0 && start < streamChunkBytes && (matchStart < 0 || start < matchStart || start == matchStart && len(secret) > matchLength) {
			matchStart, matchLength = start, len(secret)
		}
	}
	if matchStart > 0 {
		return string(data[:matchStart]), matchStart
	}
	if matchStart == 0 {
		return redaction.Placeholder, matchLength
	}
	return string(data[:streamChunkBytes]), streamChunkBytes
}

func (s *readableStream) writeLine(line string) error {
	if _, err := fmt.Fprintf(s.output, "[%s] %s\n", s.name, redaction.Redact(line, s.secrets)); err != nil {
		return fmt.Errorf("write worker output: %w", err)
	}
	return nil
}

func emitEvent(output io.Writer, secrets []string, event protocol.Event) error {
	event.Message = redaction.Redact(event.Message, secrets)
	event.Command = append([]string(nil), event.Command...)
	for i := range event.Command {
		event.Command[i] = redaction.Redact(event.Command[i], secrets)
	}
	line, err := protocol.EncodeEvent(event)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintln(output, line); err != nil {
		return fmt.Errorf("write worker event: %w", err)
	}
	return nil
}
