//go:build linux

package worker

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"golang.org/x/sys/unix"
)

func waitCommandWithGroupCleanup(ctx context.Context, cmd *exec.Cmd, containDescendants bool) error {
	leaderExited := make(chan error, 1)
	go func() {
		var info unix.Siginfo
		for {
			err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
			if !errors.Is(err, unix.EINTR) {
				leaderExited <- err
				return
			}
		}
	}()

	canceled := false
	var observeErr error
	select {
	case observeErr = <-leaderExited:
	case <-ctx.Done():
		canceled = true
		killProcessGroup(cmd.Process.Pid)
		observeErr = <-leaderExited
	}
	// The leader is still waitable, so its PID/process-group ID cannot be
	// reused before this cleanup signal and cmd.Wait preserve its exit result.
	killProcessGroup(cmd.Process.Pid)
	var descendantErr error
	if containDescendants {
		descendantErr = killRunnerChildrenExcept(cmd.Process.Pid)
	}
	waitErr := cmd.Wait()
	if containDescendants {
		descendantErr = errors.Join(descendantErr, terminateRunnerDescendants())
	}
	if canceled {
		return errors.Join(fmt.Errorf("command canceled: %w", ctx.Err()), descendantErr)
	}
	if observeErr != nil {
		return errors.Join(fmt.Errorf("observe command leader exit: %w", observeErr), waitErr, descendantErr)
	}
	if waitErr != nil {
		return errors.Join(fmt.Errorf("wait for command: %w", waitErr), descendantErr)
	}
	return descendantErr
}
