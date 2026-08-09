//go:build !linux

package worker

import (
	"context"
	"os/exec"
)

func waitCommandWithGroupCleanup(ctx context.Context, cmd *exec.Cmd, _ bool) error {
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		killProcessGroup(cmd.Process.Pid)
		return err
	case <-ctx.Done():
		killProcessGroup(cmd.Process.Pid)
		<-done
		return ctx.Err()
	}
}
