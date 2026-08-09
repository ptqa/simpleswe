//go:build linux

package worker

import (
	"testing"

	"golang.org/x/sys/unix"
)

func TestRunnerIsolationRestoresDumpableState(t *testing.T) {
	before, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatalf("read initial dumpable state: %v", err)
	}
	beforeSubreaper, err := childSubreaperState()
	if err != nil {
		t.Fatalf("read initial child subreaper state: %v", err)
	}
	restore, err := isolateRunnerProcess()
	if err != nil {
		t.Fatalf("isolate runner process: %v", err)
	}
	during, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		_ = restore()
		t.Fatalf("read isolated dumpable state: %v", err)
	}
	if during != 0 {
		_ = restore()
		t.Fatalf("isolated dumpable state = %d, want 0", during)
	}
	duringSubreaper, err := childSubreaperState()
	if err != nil || duringSubreaper != 1 {
		_ = restore()
		t.Fatalf("isolated child subreaper state = %d, %v; want 1", duringSubreaper, err)
	}
	if err := restore(); err != nil {
		t.Fatalf("restore dumpable state: %v", err)
	}
	after, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		t.Fatalf("read restored dumpable state: %v", err)
	}
	if after != before {
		t.Fatalf("restored dumpable state = %d, want %d", after, before)
	}
	afterSubreaper, err := childSubreaperState()
	if err != nil {
		t.Fatalf("read restored child subreaper state: %v", err)
	}
	if afterSubreaper != beforeSubreaper {
		t.Fatalf("restored child subreaper state = %d, want %d", afterSubreaper, beforeSubreaper)
	}
}
