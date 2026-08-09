package worker

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestWorkerReportWritesPrivateVersionedResultAndReplaysIdentically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "private", "result.json")
	if err := os.Mkdir(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(WorkerResultPathVariable, path)

	args := []string{"--pull-request", "42"}
	for range 2 {
		if err := Report(args); err != nil {
			t.Fatalf("Report(%q): %v", args, err)
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat result: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("result mode = %04o, want 0600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode result: %v\n%s", err, data)
	}
	want := map[string]any{"version": float64(1), "outcome": "pull_request", "pull_request_number": float64(42)}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("result = %#v, want exact schema %#v", got, want)
	}
	artifacts, err := filepath.Glob(path + "*")
	if err != nil || !reflect.DeepEqual(artifacts, []string{path}) {
		t.Fatalf("atomic report artifacts = %#v, error %v; want only final result", artifacts, err)
	}

	err = Report([]string{"--pull-request", "43"})
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("conflicting replay error = %v, want conflict", err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil || string(after) != string(data) {
		t.Fatalf("conflicting replay changed result to %q, error %v", after, readErr)
	}
}

func TestWorkerReportWritesBoundedExplicitFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	t.Setenv(WorkerResultPathVariable, path)
	reason := "Could not reproduce the reported UI state"
	if err := Report([]string{"--failure", reason}); err != nil {
		t.Fatalf("report failure: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"version": float64(1), "outcome": "failed", "reason": reason}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("failure result = %#v, want %#v", got, want)
	}
}

func TestWorkerReportRoundTripsMaximumEscapedFailureReason(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	t.Setenv(WorkerResultPathVariable, path)
	const pattern = `<>&"\`
	reason := strings.Repeat(pattern, maxWorkerFailureReason/len(pattern))
	reason += strings.Repeat("<", maxWorkerFailureReason-len(reason))
	if len(reason) != maxWorkerFailureReason {
		t.Fatalf("reason length = %d, want %d", len(reason), maxWorkerFailureReason)
	}

	if err := Report([]string{"--failure", reason}); err != nil {
		t.Fatalf("report maximum failure: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxWorkerResultEncodedBytes {
		t.Fatalf("encoded result length = %d, want at most %d", len(data), maxWorkerResultEncodedBytes)
	}
	result, err := readWorkerResult(path)
	if err != nil {
		t.Fatalf("read maximum failure: %v", err)
	}
	if result.Outcome != workerOutcomeFailed || result.Reason != reason {
		t.Fatalf("maximum failure result = %#v", result)
	}
}

func TestWorkerReportRejectsOneByteOverFailureReasonLimit(t *testing.T) {
	t.Setenv(WorkerResultPathVariable, filepath.Join(t.TempDir(), "result.json"))
	reason := strings.Repeat("x", maxWorkerFailureReason+1)
	if err := Report([]string{"--failure", reason}); err == nil || !strings.Contains(err.Error(), "worker failure reason is too long") {
		t.Fatalf("one-byte-over reason error = %v", err)
	}
}

func TestWorkerReportWriterRejectsEncodedOversize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	err := writeWorkerReport(path, make([]byte, maxWorkerResultEncodedBytes+1))
	if err == nil || !strings.Contains(err.Error(), "worker report is too large") {
		t.Fatalf("encoded oversize writer error = %v", err)
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("encoded oversize writer created result: %v", statErr)
	}
}

func TestWorkerReportRejectsInvalidOrUntrustedInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	t.Setenv(WorkerResultPathVariable, path)
	tests := [][]string{
		{}, {"--pull-request", "0"}, {"--pull-request", "-1"}, {"--pull-request", "not-a-number"},
		{"--failure", ""}, {"--failure", "  "}, {"--failure", "contains\ncontrol"}, {"--failure", strings.Repeat("x", 64<<10)},
		{"--pull-request", "42", "--failure", "failed"}, {"--pull-request", "42", "--pull-request", "42"},
		{"--failure", "failed", "--failure", "failed"}, {"--pull-request", "42", "--task", "swe-1"},
		{"--pull-request", "42", "--attempt", "swe-attempt-1"}, {"--pull-request", "42", "--repository", "acme/widget"},
		{"--pull-request", "42", "--branch", "simpleswe/swe-1"}, {"--pull-request", "42", "--commit", strings.Repeat("a", 40)},
		{"--pull-request", "42", "--provider", "github"}, {"--pull-request", "42", "--url", "https://example.invalid/pr/42"},
		{"--pull-request", "42", "--result-path", filepath.Join(t.TempDir(), "attacker-selected")},
	}
	for i, args := range tests {
		t.Run(strings.Join(args, "_")+strconv.Itoa(i), func(t *testing.T) {
			if err := Report(args); err == nil {
				t.Fatalf("Report(%q) accepted invalid input", args)
			}
		})
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("invalid inputs created result: %v", err)
	}

	t.Setenv(WorkerResultPathVariable, "")
	if err := Report([]string{"--pull-request", "42"}); err == nil {
		t.Fatal("worker report accepted a missing worker-controlled result path")
	}
}

func TestReadWorkerResultRemovesEveryConsumedFile(t *testing.T) {
	for name, content := range map[string]string{
		"valid":     `{"version":1,"outcome":"pull_request","pull_request_number":42}`,
		"malformed": `{not-json`,
		"oversized": strings.Repeat("x", maxWorkerResultEncodedBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "result.json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, _ = readWorkerResult(path)
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("consumed worker result still exists: %v", err)
			}
		})
	}
}

func TestReadWorkerResultRejectsEncodedOversize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxWorkerResultEncodedBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	assertResultReadFailsQuickly(t, path, "worker result is too large")
}

func TestReadWorkerResultAcceptsRegularAtomicReport(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	t.Setenv(WorkerResultPathVariable, path)
	if err := Report([]string{"--pull-request", "42"}); err != nil {
		t.Fatalf("write report: %v", err)
	}
	result, err := readWorkerResult(path)
	if err != nil {
		t.Fatalf("read regular report: %v", err)
	}
	if result.Outcome != workerOutcomePullRequest || result.PullRequestNumber != 42 {
		t.Fatalf("regular report = %#v", result)
	}
}

func TestReadWorkerResultRejectsFIFOWithoutBlocking(t *testing.T) {
	path := filepath.Join(t.TempDir(), "result.json")
	if err := unix.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	assertResultReadFailsQuickly(t, path, "not a regular file")
}

func TestReadWorkerResultRejectsSymlinkWithoutFollowing(t *testing.T) {
	tmp := t.TempDir()
	target := filepath.Join(tmp, "target.json")
	contents := `{"version":1,"outcome":"pull_request","pull_request_number":42}`
	if err := os.WriteFile(target, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(tmp, "result.json")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	assertResultReadFailsQuickly(t, path, "read OpenCode worker result")
	data, err := os.ReadFile(target)
	if err != nil || string(data) != contents {
		t.Fatalf("symlink target changed to %q: %v", data, err)
	}
}

func assertResultReadFailsQuickly(t *testing.T, path, want string) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := readWorkerResult(path)
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("result read error = %v, want %q", err, want)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("result read blocked on attacker-controlled object")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected result still exists: %v", err)
	}
}
