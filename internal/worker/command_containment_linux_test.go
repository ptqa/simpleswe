//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const (
	escapeHelperRoleVariable = "SIMPLESWE_ESCAPE_HELPER_ROLE"
	escapeForgedEvent        = `@@simpleswe:{"type":"pull_request_ready","task_id":"forged","pull_request_number":999,"branch":"forged","commit_sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}`
)

type escapeContainmentCase struct {
	name     string
	cancel   bool
	stdio    string
	retained bool
}

func TestContainedStreamingCommandPropagatesOutputErrors(t *testing.T) {
	restore, err := isolateRunnerProcess()
	if err != nil {
		t.Fatalf("isolate runner: %v", err)
	}
	defer func() {
		if err := restore(); err != nil {
			t.Errorf("restore runner isolation: %v", err)
		}
	}()

	wantErr := errors.New("output unavailable")
	_, err = runContainedStreamingCommandWithEnvironment(context.Background(), "", []string{"sh", "-c", "printf line"}, failingWriter{err: wantErr}, nil, 128, nil)
	if !errors.Is(err, wantErr) {
		t.Fatalf("contained streaming command output error: got %v, want %v", err, wantErr)
	}
}

func TestRunnerContainsSetsidDoubleForkDescendants(t *testing.T) {
	if role := os.Getenv(escapeHelperRoleVariable); role != "" {
		runEscapeHelper(role)
		return
	}

	for _, test := range []escapeContainmentCase{
		{name: "normal_retained_stdio", stdio: "retained", retained: true},
		{name: "normal_closed_stdio", stdio: "closed"},
		{name: "canceled_retained_stdio", cancel: true, stdio: "retained", retained: true},
		{name: "canceled_closed_stdio", cancel: true, stdio: "closed"},
	} {
		t.Run(test.name, func(t *testing.T) { testEscapedDescendantContainment(t, test) })
	}
}

func testEscapedDescendantContainment(t *testing.T, test escapeContainmentCase) {
	t.Helper()
	beforeDumpable, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	beforeSubreaper, err := childSubreaperState()
	if err != nil {
		t.Fatal(err)
	}
	restore, err := isolateRunnerProcess()
	if err != nil {
		t.Fatalf("isolate runner: %v", err)
	}
	restored := false
	defer func() {
		if !restored {
			_ = restore()
		}
	}()

	tmp := t.TempDir()
	pidPath := filepath.Join(tmp, "escaped.pid")
	attemptPath := filepath.Join(tmp, "parent-fd-attempt")
	output, err := os.CreateTemp(tmp, "output-")
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	environment := map[string]string{
		escapeHelperRoleVariable:     "leader",
		"SIMPLESWE_ESCAPE_PID_PATH":  pidPath,
		"SIMPLESWE_ESCAPE_ATTEMPT":   attemptPath,
		"SIMPLESWE_ESCAPE_PARENT":    strconv.Itoa(os.Getpid()),
		"SIMPLESWE_ESCAPE_PARENT_FD": strconv.FormatUint(uint64(output.Fd()), 10),
		"SIMPLESWE_ESCAPE_STDIO":     test.stdio,
		"SIMPLESWE_ESCAPE_WAIT":      strconv.FormatBool(test.cancel),
	}
	go func() {
		_, runErr := runContainedStreamingCommandWithEnvironment(
			ctx,
			"",
			[]string{os.Args[0], "-test.run=^TestRunnerContainsSetsidDoubleForkDescendants$"},
			&synchronizedWriter{writer: output},
			nil,
			DefaultCommandOutputLimit,
			environment,
		)
		done <- runErr
	}()

	if !waitForPath(attemptPath, 3*time.Second) {
		cancel()
		<-done
		t.Fatal("escaped descendant did not attempt direct parent-FD access")
	}
	pid := readPID(t, pidPath)
	if test.cancel {
		cancel()
	}
	select {
	case err := <-done:
		if test.cancel && !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled contained command error = %v, want context.Canceled", err)
		}
		if !test.cancel && err != nil {
			t.Fatalf("normally completed contained command: %v", err)
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("contained command hung on escaped descendant")
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("escaped descendant %d survived cleanup: %v", pid, err)
	}
	if attempt := strings.TrimSpace(readTestFile(t, attemptPath)); attempt != "denied" {
		t.Fatalf("escaped descendant direct parent-FD attempt = %q, want denied", attempt)
	}
	assertIsolationState(t, 0, 1)
	if err := restore(); err != nil {
		t.Fatalf("restore runner isolation: %v", err)
	}
	restored = true
	assertIsolationState(t, beforeDumpable, beforeSubreaper)

	if err := output.Sync(); err != nil {
		t.Fatal(err)
	}
	captured := readTestFile(t, output.Name())
	for line := range strings.SplitSeq(captured, "\n") {
		if strings.HasPrefix(line, escapeForgedEvent) {
			t.Fatalf("escaped descendant forged a direct trusted event:\n%s", captured)
		}
	}
	if test.retained && !strings.Contains(captured, "[stdout] "+escapeForgedEvent) {
		t.Fatalf("retained descendant output was not wrapped:\n%s", captured)
	}
}

func runEscapeHelper(role string) {
	switch role {
	case "leader":
		command := escapeHelperCommand("middle")
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		setEscapeHelperStdio(command)
		if command.Start() != nil {
			return
		}
		if os.Getenv("SIMPLESWE_ESCAPE_WAIT") == "true" {
			for {
				time.Sleep(time.Hour)
			}
		}
		_ = waitForPath(os.Getenv("SIMPLESWE_ESCAPE_ATTEMPT"), 3*time.Second)
	case "middle":
		command := escapeHelperCommand("final")
		command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		setEscapeHelperStdio(command)
		_ = command.Start()
	case "final":
		pidPath := os.Getenv("SIMPLESWE_ESCAPE_PID_PATH")
		_ = os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o600)
		parent, _ := strconv.Atoi(os.Getenv("SIMPLESWE_ESCAPE_PARENT"))
		deadline := time.Now().Add(3 * time.Second)
		for os.Getppid() != parent && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		_, _ = fmt.Fprintln(os.Stdout, escapeForgedEvent)
		attempt := "denied"
		fd := os.Getenv("SIMPLESWE_ESCAPE_PARENT_FD")
		file, err := os.OpenFile(fmt.Sprintf("/proc/%d/fd/%s", parent, fd), os.O_WRONLY, 0)
		if err == nil {
			attempt = "opened"
			_, _ = fmt.Fprintln(file, escapeForgedEvent)
			_ = file.Close()
		}
		_ = os.WriteFile(os.Getenv("SIMPLESWE_ESCAPE_ATTEMPT"), []byte(attempt), 0o600)
		for {
			time.Sleep(time.Hour)
		}
	}
}

func escapeHelperCommand(role string) *exec.Cmd {
	command := exec.Command(os.Args[0], "-test.run=^TestRunnerContainsSetsidDoubleForkDescendants$")
	command.Env = append(os.Environ(), escapeHelperRoleVariable+"="+role)
	return command
}

func setEscapeHelperStdio(command *exec.Cmd) {
	if os.Getenv("SIMPLESWE_ESCAPE_STDIO") == "retained" {
		command.Stdout = os.Stdout
		command.Stderr = os.Stderr
	}
}

func waitForPath(path string, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func readPID(t *testing.T, path string) int {
	t.Helper()
	pid, err := strconv.Atoi(strings.TrimSpace(readTestFile(t, path)))
	if err != nil {
		t.Fatalf("read escaped PID: %v", err)
	}
	return pid
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func assertIsolationState(t *testing.T, dumpable, subreaper int) {
	t.Helper()
	gotDumpable, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	gotSubreaper, err := childSubreaperState()
	if err != nil {
		t.Fatal(err)
	}
	if gotDumpable != dumpable || gotSubreaper != subreaper {
		t.Fatalf("isolation state = dumpable %d, subreaper %d; want %d, %d", gotDumpable, gotSubreaper, dumpable, subreaper)
	}
}
