//go:build linux

package worker

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

var runnerIsolationMu sync.Mutex

func isolateRunnerProcess() (func() error, error) {
	runnerIsolationMu.Lock()
	children, err := runnerChildPIDs()
	if err != nil {
		runnerIsolationMu.Unlock()
		return nil, fmt.Errorf("inspect runner children: %w", err)
	}
	if len(children) != 0 {
		runnerIsolationMu.Unlock()
		return nil, fmt.Errorf("runner isolation requires no existing child processes; found %v", children)
	}
	previousDumpable, err := unix.PrctlRetInt(unix.PR_GET_DUMPABLE, 0, 0, 0, 0)
	if err != nil {
		runnerIsolationMu.Unlock()
		return nil, fmt.Errorf("inspect runner process isolation: %w", err)
	}
	previousSubreaper, err := childSubreaperState()
	if err != nil {
		runnerIsolationMu.Unlock()
		return nil, fmt.Errorf("inspect runner child subreaper state: %w", err)
	}
	if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0); err != nil {
		runnerIsolationMu.Unlock()
		return nil, fmt.Errorf("enable runner child subreaper: %w", err)
	}
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		restoreErr := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(previousSubreaper), 0, 0, 0)
		runnerIsolationMu.Unlock()
		return nil, errors.Join(fmt.Errorf("isolate runner process: %w", err), restoreErr)
	}
	return func() error {
		defer runnerIsolationMu.Unlock()
		if err := terminateRunnerDescendants(); err != nil {
			return fmt.Errorf("prove runner descendant cleanup before restoring isolation: %w", err)
		}
		if err := unix.Prctl(unix.PR_SET_CHILD_SUBREAPER, uintptr(previousSubreaper), 0, 0, 0); err != nil {
			return fmt.Errorf("restore runner child subreaper state: %w", err)
		}
		if err := unix.Prctl(unix.PR_SET_DUMPABLE, uintptr(previousDumpable), 0, 0, 0); err != nil {
			return fmt.Errorf("restore runner process isolation: %w", err)
		}
		return nil
	}, nil
}

func childSubreaperState() (int, error) {
	var state int32
	err := unix.Prctl(unix.PR_GET_CHILD_SUBREAPER, reflect.ValueOf(&state).Pointer(), 0, 0, 0)
	runtime.KeepAlive(&state)
	return int(state), err
}

func runnerChildPIDs() (_ []int, resultErr error) {
	const taskPath = "/proc/self/task"
	entries, err := os.ReadDir(taskPath)
	if err != nil {
		return nil, fmt.Errorf("read runner task directory: %w", err)
	}
	root, err := os.OpenRoot(taskPath)
	if err != nil {
		return nil, fmt.Errorf("open runner task directory: %w", err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("close runner task directory: %w", err))
		}
	}()

	children := make(map[int]struct{})
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		data, err := root.ReadFile(entry.Name() + "/children")
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read runner children for task %s: %w", entry.Name(), err)
		}
		for field := range strings.FieldsSeq(string(data)) {
			pid, err := strconv.Atoi(field)
			if err != nil || pid <= 0 {
				return nil, fmt.Errorf("invalid child PID %q", field)
			}
			children[pid] = struct{}{}
		}
	}
	pids := make([]int, 0, len(children))
	for pid := range children {
		pids = append(pids, pid)
	}
	sort.Ints(pids)
	return pids, nil
}

func killRunnerChildrenExcept(excludedPID int) error {
	children, err := runnerChildPIDs()
	if err != nil {
		return err
	}
	var killErr error
	for _, pid := range children {
		if pid == excludedPID {
			continue
		}
		if err := unix.Kill(pid, unix.SIGKILL); err != nil && !errors.Is(err, unix.ESRCH) {
			killErr = errors.Join(killErr, fmt.Errorf("kill runner child %d: %w", pid, err))
		}
	}
	return killErr
}

func terminateRunnerDescendants() error {
	const cleanupLimit = 2 * time.Second
	deadline := time.Now().Add(cleanupLimit)
	for {
		if err := reapRunnerChildren(); err != nil {
			return err
		}
		children, err := runnerChildPIDs()
		if err != nil {
			return err
		}
		if len(children) == 0 {
			return nil
		}
		if err := killRunnerChildrenExcept(0); err != nil {
			return err
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("runner descendants did not terminate: %v", children)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func reapRunnerChildren() error {
	for {
		var status unix.WaitStatus
		pid, err := unix.Wait4(-1, &status, unix.WNOHANG, nil)
		switch {
		case errors.Is(err, unix.EINTR):
			continue
		case errors.Is(err, unix.ECHILD):
			return nil
		case err != nil:
			return fmt.Errorf("reap runner descendants: %w", err)
		case pid == 0:
			return nil
		}
	}
}
