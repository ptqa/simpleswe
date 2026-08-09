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
	"unicode/utf8"

	"github.com/simpleswe/simpleswe/internal/redaction"
	"github.com/simpleswe/simpleswe/internal/worker/protocol"
	"golang.org/x/sys/unix"
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
	baseWorkflowInstructions = "You own all repository and forge actions for this task. Read the immutable task manifest from $SIMPLESWE_TASK_MANIFEST_PATH for the exact provider, repository, base branch, task branch, requested pull-request title, and any existing pull-request number. Edit the repository, commit all intended changes, commit and push the task branch, and create or update the pull request through the configured forge tooling. Write the pull-request title, body, and useful evidence yourself. For a follow-up, update only the existing pull request and its same branch. After the provider operation is complete, report the pull-request number with `simpleswe worker report --pull-request NUMBER`; report an intentional failure with `simpleswe worker report --failure REASON`."
)

// Run clones the task repository, invokes OpenCode, validates its changes,
// and verifies OpenCode's published task branch and reported pull request.
func (r Runner) Run(ctx context.Context) (runErr error) {
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
	manifestPath, err := filepath.Abs(r.ManifestPath)
	if err != nil {
		return fmt.Errorf("resolve manifest path: %w", err)
	}
	r.ManifestPath = manifestPath
	restoreIsolation, err := isolateRunnerProcess()
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, restoreIsolation()) }()

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
	resultDir, err := os.MkdirTemp(filepath.Dir(r.WorkspaceDir), ".simpleswe-worker-result-")
	if err != nil {
		return fmt.Errorf("create private worker result directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(resultDir) }()
	r.Secrets = loadSecrets(r.Secrets, strings.Split(os.Getenv(protocol.SecretEnvNamesVariable), ","))

	output := r.Output
	if output == nil {
		output = io.Discard
	}
	output = &synchronizedWriter{writer: output}
	run := func(argv []string) (CommandResult, error) {
		return runContainedStreamingCommandWithEnvironment(ctx, r.WorkspaceDir, argv, output, r.Secrets, r.OutputTailBytes, nil)
	}
	runOutsideWorkspace := func(argv []string) (CommandResult, error) {
		return runContainedStreamingCommandWithEnvironment(ctx, "", argv, output, r.Secrets, r.OutputTailBytes, nil)
	}
	cloneURL, startCommit, err := prepareRunnerWorkspace(ctx, r.WorkspaceDir, manifest, run, runOutsideWorkspace)
	if err != nil {
		return err
	}
	candidate, err := r.runAgentValidation(ctx, output, manifest, resultDir, cloneURL, startCommit, run)
	if err != nil {
		return err
	}
	return r.emitReadyCandidate(ctx, output, manifest, cloneURL, startCommit, candidate, run)
}

type runnerCommand func([]string) (CommandResult, error)

func prepareRunnerWorkspace(ctx context.Context, workspace string, manifest protocol.TaskManifest, run, runOutsideWorkspace runnerCommand) (string, string, error) {
	cloneURL := manifest.CloneURL
	if cloneURL == "" {
		cloneURL = manifest.Repository
	}
	if _, err := runOutsideWorkspace([]string{"git", "clone", "--no-checkout", "--origin", "origin", "--", cloneURL, workspace}); err != nil {
		return "", "", commandFailure(ctx, "clone repository", err)
	}
	baseRef := "refs/simpleswe/base"
	if _, err := run([]string{"git", "fetch", "--no-tags", "--", cloneURL, "+refs/heads/" + manifest.BaseBranch + ":" + baseRef}); err != nil {
		return "", "", commandFailure(ctx, "fetch base branch", err)
	}
	startCommit, err := commandText(ctx, run, []string{"git", "rev-parse", "--verify", baseRef + "^{commit}"})
	if err != nil {
		return "", "", fmt.Errorf("resolve base branch: %w", err)
	}
	if manifest.ExistingPullRequestNumber > 0 {
		followUpRef := "refs/simpleswe/follow-up"
		if _, err := run([]string{"git", "fetch", "--no-tags", "--", cloneURL, "+refs/heads/" + manifest.TaskBranch + ":" + followUpRef}); err != nil {
			return "", "", commandFailure(ctx, "fetch existing remote task branch", err)
		}
		startCommit, err = commandText(ctx, run, []string{"git", "rev-parse", "--verify", followUpRef + "^{commit}"})
		if err != nil {
			return "", "", fmt.Errorf("resolve existing remote task branch: %w", err)
		}
		if startCommit != manifest.ExistingPullRequestHeadSHA {
			return "", "", fmt.Errorf("existing remote task branch %q is %s, want immutable existing pull request head %s", manifest.TaskBranch, startCommit, manifest.ExistingPullRequestHeadSHA)
		}
	}
	if _, err := run([]string{"git", "checkout", "-b", manifest.TaskBranch, startCommit}); err != nil {
		return "", "", commandFailure(ctx, "check out task branch", err)
	}
	return cloneURL, startCommit, nil
}

func (r Runner) runAgentValidation(ctx context.Context, output io.Writer, manifest protocol.TaskManifest, resultDir, cloneURL, startCommit string, run runnerCommand) (protocol.Event, error) {
	initialCommand := append(append([]string(nil), manifest.OpenCodeCommand...), agentPrompt(manifest.Prompt))
	if err := emitEvent(output, r.Secrets, protocol.Event{
		Type: protocol.EventAgentStarted, TaskID: manifest.TaskID, Message: "initial",
		Timestamp: time.Now().UTC(), Command: initialCommand,
	}); err != nil {
		return protocol.Event{}, err
	}
	reportedPullRequest, err := r.invokeOpenCode(ctx, output, manifest, resultDir, initialCommand, manifest.ExistingPullRequestNumber)
	if err != nil {
		return protocol.Event{}, err
	}
	candidate, err := r.publishCandidate(ctx, output, manifest, cloneURL, startCommit, reportedPullRequest, run)
	if err != nil {
		return protocol.Event{}, err
	}
	commands := manifest.ValidationCommands
	if len(commands) == 0 {
		commands = [][]string{manifest.ValidationCommand}
	}
	var lastFailure *validationFailure
	for attempt := 0; attempt <= manifest.MaxFixAttempts; attempt++ {
		failure, err := r.runValidationRound(ctx, output, manifest, commands, attempt+1, run)
		if err != nil {
			return protocol.Event{}, err
		}
		if failure == nil {
			lastFailure = nil
			if err := emitEvent(output, r.Secrets, protocol.Event{
				Type: protocol.EventValidationSucceeded, TaskID: manifest.TaskID,
				Message: fmt.Sprintf("attempt %d", attempt+1), Timestamp: time.Now().UTC(),
			}); err != nil {
				return protocol.Event{}, err
			}
			break
		}
		lastFailure = failure
		if attempt == manifest.MaxFixAttempts {
			break
		}
		fixCommand := append(append([]string(nil), manifest.OpenCodeCommand...), agentPrompt(validationFixPrompt(failure.summary, reportedPullRequest)))
		if err := emitEvent(output, r.Secrets, protocol.Event{
			Type: protocol.EventAgentStarted, TaskID: manifest.TaskID, Message: "validation_fix",
			Timestamp: time.Now().UTC(), Command: fixCommand,
		}); err != nil {
			return protocol.Event{}, err
		}
		reportedPullRequest, err = r.invokeOpenCode(ctx, output, manifest, resultDir, fixCommand, reportedPullRequest)
		if err != nil {
			return protocol.Event{}, err
		}
		candidate, err = r.publishCandidate(ctx, output, manifest, cloneURL, startCommit, reportedPullRequest, run)
		if err != nil {
			return protocol.Event{}, err
		}
	}
	if lastFailure == nil {
		return candidate, nil
	}
	if err := emitEvent(output, r.Secrets, protocol.Event{
		Type: protocol.EventValidationFailed, TaskID: manifest.TaskID,
		Message: lastFailure.summary, Timestamp: time.Now().UTC(),
		Command: lastFailure.command, ExitCode: lastFailure.exitCode,
	}); err != nil {
		return protocol.Event{}, err
	}
	return protocol.Event{}, fmt.Errorf("validation failed after %d attempts: %s", manifest.MaxFixAttempts+1, lastFailure.summary)
}

func (r Runner) invokeOpenCode(ctx context.Context, output io.Writer, manifest protocol.TaskManifest, resultDir string, command []string, expectedPullRequest int) (int, error) {
	resultFile, err := os.CreateTemp(resultDir, "result-*.json")
	if err != nil {
		return 0, fmt.Errorf("reserve private OpenCode result path: %w", err)
	}
	resultPath := resultFile.Name()
	if err := errors.Join(resultFile.Close(), os.Remove(resultPath)); err != nil {
		return 0, fmt.Errorf("prepare fresh OpenCode result path: %w", err)
	}
	result, runErr := runContainedStreamingCommandWithEnvironment(ctx, r.WorkspaceDir, command, output, r.Secrets, r.OutputTailBytes, map[string]string{
		WorkerResultPathVariable: resultPath, "SIMPLESWE_TASK_MANIFEST_PATH": r.ManifestPath,
	})
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("run OpenCode canceled: %w", err)
	}
	reported, reportErr := readWorkerResult(resultPath)
	invocationErr := workerInvocationError(ctx, reported, reportErr, runErr, result, expectedPullRequest)
	if invocationErr == nil {
		return reported.PullRequestNumber, nil
	}
	message := boundedWorkerFailureMessage(invocationErr.Error(), r.Secrets)
	emitErr := emitEvent(output, nil, protocol.Event{
		Type: protocol.EventWorkerFailed, TaskID: manifest.TaskID, Message: message,
		Timestamp: time.Now().UTC(), ExitCode: result.ExitCode,
	})
	return 0, errors.Join(errors.New(message), emitErr)
}

func workerInvocationError(ctx context.Context, reported workerResult, reportErr, runErr error, result CommandResult, expectedPullRequest int) error {
	switch {
	case errors.Is(reportErr, os.ErrNotExist):
		return errors.New("OpenCode did not report a pull request")
	case reportErr != nil:
		return reportErr
	case reported.Outcome == workerOutcomeFailed:
		return fmt.Errorf("OpenCode reported failure: %s", reported.Reason)
	case expectedPullRequest > 0 && reported.PullRequestNumber != expectedPullRequest:
		return fmt.Errorf("OpenCode reported pull request %d, want pull request %d", reported.PullRequestNumber, expectedPullRequest)
	case runErr != nil:
		return commandFailure(ctx, "run OpenCode", fmt.Errorf("%w (exit code %d)", runErr, result.ExitCode))
	default:
		return nil
	}
}

func (r Runner) publishCandidate(ctx context.Context, output io.Writer, manifest protocol.TaskManifest, cloneURL, startCommit string, reportedPullRequest int, run runnerCommand) (protocol.Event, error) {
	candidate, err := verifyPublishedCandidate(ctx, manifest, cloneURL, startCommit, reportedPullRequest, "", run)
	if err != nil {
		return protocol.Event{}, err
	}
	if err := emitEvent(output, r.Secrets, candidate); err != nil {
		return protocol.Event{}, err
	}
	return candidate, nil
}

func (r Runner) emitReadyCandidate(ctx context.Context, output io.Writer, manifest protocol.TaskManifest, cloneURL, startCommit string, candidate protocol.Event, run runnerCommand) error {
	verified, err := verifyPublishedCandidate(ctx, manifest, cloneURL, startCommit, candidate.PullRequestNumber, candidate.CommitSHA, run)
	if err != nil {
		return err
	}
	verified.Type = protocol.EventPullRequestReady
	verified.Timestamp = time.Now().UTC()
	if err := protocol.ValidateEvent(verified, manifest.TaskBranch); err != nil {
		return fmt.Errorf("validate pull_request_ready event: %w", err)
	}
	return emitEvent(output, r.Secrets, verified)
}

func verifyPublishedCandidate(ctx context.Context, manifest protocol.TaskManifest, cloneURL, startCommit string, reportedPullRequest int, expectedCommit string, run runnerCommand) (protocol.Event, error) {
	dirty, err := repositoryChanged(ctx, run)
	if err != nil {
		return protocol.Event{}, err
	}
	if dirty {
		return protocol.Event{}, errors.New("worktree must be clean after successful OpenCode report and validation")
	}
	head, err := commandText(ctx, run, []string{"git", "rev-parse", "--verify", "HEAD^{commit}"})
	if err != nil {
		return protocol.Event{}, fmt.Errorf("resolve task branch: %w", err)
	}
	if _, err := run([]string{"git", "merge-base", "--is-ancestor", startCommit, "HEAD"}); err != nil {
		return protocol.Event{}, commandFailure(ctx, "verify attempt start is ancestor of task branch", err)
	}
	if head == startCommit {
		return protocol.Event{}, fmt.Errorf("%w: OpenCode did not modify the repository", ErrNoChanges)
	}
	if expectedCommit != "" && head != expectedCommit {
		return protocol.Event{}, fmt.Errorf("validation changed local HEAD from published candidate %s to %s", expectedCommit, head)
	}
	branch, err := commandText(ctx, run, []string{"git", "symbolic-ref", "--short", "HEAD"})
	if err != nil {
		return protocol.Event{}, fmt.Errorf("resolve checked-out task branch: %w", err)
	}
	if branch != manifest.TaskBranch {
		return protocol.Event{}, fmt.Errorf("checked-out branch %q does not match task branch %q", branch, manifest.TaskBranch)
	}
	if err := ctx.Err(); err != nil {
		return protocol.Event{}, fmt.Errorf("verify published task branch canceled: %w", err)
	}
	verifiedTaskRef := "refs/simpleswe/verified-task"
	if _, err := run([]string{"git", "fetch", "--no-tags", "--", cloneURL, "+refs/heads/" + manifest.TaskBranch + ":" + verifiedTaskRef}); err != nil {
		return protocol.Event{}, commandFailure(ctx, "fetch remote task branch", err)
	}
	remoteHead, err := commandText(ctx, run, []string{"git", "rev-parse", "--verify", verifiedTaskRef + "^{commit}"})
	if err != nil {
		return protocol.Event{}, fmt.Errorf("resolve remote task branch: %w", err)
	}
	if remoteHead != head {
		return protocol.Event{}, fmt.Errorf("remote task branch %q is %s, want local HEAD %s", manifest.TaskBranch, remoteHead, head)
	}
	candidate := protocol.Event{
		Type: protocol.EventPullRequestPublished, TaskID: manifest.TaskID, Timestamp: time.Now().UTC(),
		PullRequestNumber: reportedPullRequest, Branch: manifest.TaskBranch, CommitSHA: head,
	}
	if err := protocol.ValidateEvent(candidate, manifest.TaskBranch); err != nil {
		return protocol.Event{}, fmt.Errorf("validate pull_request_published event: %w", err)
	}
	return candidate, nil
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

func validationFixPrompt(failure string, pullRequestNumber int) string {
	header := fmt.Sprintf("Validation failed. Fix the repository so all validation commands pass. Commit and push the repair, update the same pull request %d, then report pull request %d.\n\n", pullRequestNumber, pullRequestNumber)
	tail := newTailBuffer(validationFixPromptLimit - len(header))
	_, _ = tail.Write([]byte(failure))
	return header + tail.String()
}

func boundedWorkerFailureMessage(message string, secrets []string) string {
	message = strings.ToValidUTF8(redaction.Redact(message, secrets), "\uFFFD")
	if len(message) <= protocol.MaxWorkerEventMessageLen {
		return message
	}
	keep := protocol.MaxWorkerEventMessageLen - len(OutputTruncatedMarker)
	start := len(message) - keep
	for start < len(message) && !utf8.RuneStart(message[start]) {
		start++
	}
	return OutputTruncatedMarker + message[start:]
}

func readWorkerResult(path string) (_ workerResult, resultErr error) {
	defer func() {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove OpenCode worker result: %w", err))
		}
	}()
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return workerResult{}, fmt.Errorf("read OpenCode worker result: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	var before unix.Stat_t
	if err := unix.Fstat(fd, &before); err != nil {
		return workerResult{}, fmt.Errorf("inspect OpenCode worker result: %w", err)
	}
	if before.Mode&unix.S_IFMT != unix.S_IFREG {
		return workerResult{}, errors.New("OpenCode worker result is not a regular file")
	}
	if int64(before.Uid) != int64(os.Geteuid()) {
		return workerResult{}, errors.New("OpenCode worker result has an unexpected owner")
	}
	if before.Nlink != 1 {
		return workerResult{}, errors.New("OpenCode worker result is not a fresh file")
	}
	if before.Size < 0 || before.Size > int64(maxWorkerResultEncodedBytes) {
		return workerResult{}, errors.New("OpenCode worker result is too large")
	}
	if err := requireWorkerResultPath(path, before); err != nil {
		return workerResult{}, err
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(maxWorkerResultEncodedBytes+1)))
	if err != nil {
		return workerResult{}, fmt.Errorf("read OpenCode worker result: %w", err)
	}
	if len(data) > maxWorkerResultEncodedBytes {
		return workerResult{}, errors.New("OpenCode worker result is too large")
	}
	var after unix.Stat_t
	if err := unix.Fstat(fd, &after); err != nil {
		return workerResult{}, fmt.Errorf("reinspect OpenCode worker result: %w", err)
	}
	if before.Dev != after.Dev || before.Ino != after.Ino || before.Size != after.Size || int64(len(data)) != after.Size {
		return workerResult{}, errors.New("OpenCode worker result changed while being read")
	}
	if err := requireWorkerResultPath(path, after); err != nil {
		return workerResult{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var result workerResult
	if err := decoder.Decode(&result); err != nil {
		return workerResult{}, fmt.Errorf("decode OpenCode worker result: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return workerResult{}, errors.New("decode OpenCode worker result: multiple JSON values")
	}
	if err := validateWorkerResult(result); err != nil {
		return workerResult{}, fmt.Errorf("validate OpenCode worker result: %w", err)
	}
	return result, nil
}

func requireWorkerResultPath(path string, opened unix.Stat_t) error {
	var current unix.Stat_t
	if err := unix.Fstatat(unix.AT_FDCWD, path, &current, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return fmt.Errorf("inspect OpenCode worker result path: %w", err)
	}
	if current.Mode&unix.S_IFMT != unix.S_IFREG || current.Dev != opened.Dev || current.Ino != opened.Ino {
		return errors.New("OpenCode worker result is not the file at the expected path")
	}
	return nil
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

func agentPrompt(prompt string) string {
	return prompt + "\n\nWorkflow constraints:\n" + baseWorkflowInstructions
}

func repositoryChanged(ctx context.Context, run func([]string) (CommandResult, error)) (bool, error) {
	result, err := run([]string{"git", "status", "--porcelain", "--untracked-files=all"})
	if err != nil {
		return false, commandFailure(ctx, "inspect repository changes", err)
	}
	return result.Stdout != "", nil
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

func runContainedStreamingCommandWithEnvironment(ctx context.Context, dir string, command []string, output io.Writer, secrets []string, outputLimit int, environment map[string]string) (CommandResult, error) {
	stdout := &readableStream{output: output, name: "stdout", secrets: secrets}
	stderr := &readableStream{output: output, name: "stderr", secrets: secrets}
	result, runErr := runContainedCommandInDirWithOutputEnv(ctx, dir, command, stdout, stderr, outputLimit, environment)
	if errors.Is(runErr, errCommandOutputDrain) {
		return result, runErr
	}
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
