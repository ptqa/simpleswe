package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/simpleswe/simpleswe/internal/app"
	"github.com/simpleswe/simpleswe/internal/run"
	"k8s.io/klog/v2"
)

func main() {
	os.Exit(execute())
}

func execute() int {
	if len(os.Args) > 1 && os.Args[1] == "tui" {
		defer discardKubernetesLogs()()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.Run(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, run.Dependencies()); err != nil && !errors.Is(err, context.Canceled) {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func discardKubernetesLogs() func() {
	state := klog.CaptureState()
	klog.SetSlogLogger(slog.New(slog.DiscardHandler))
	return state.Restore
}
