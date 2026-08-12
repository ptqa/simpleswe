package gui

import (
	"context"
	"testing"
)

func TestCancellationRequestsQuit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	quit := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		waitForCancellation(ctx, make(chan struct{}), func() { quit <- struct{}{} })
		close(done)
	}()
	cancel()
	receiveGUI(t, quit)
	receiveGUI(t, done)
}

func TestSurfaceAvailableRetriesCanceledQuit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	quit := make(chan struct{}, 1)
	quitOnCancellation(ctx, make(chan struct{}), func() { quit <- struct{}{} })()
	receiveGUI(t, quit)
}
