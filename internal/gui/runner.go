package gui

import (
	"context"
	"errors"
	"fmt"

	_ "github.com/gogpu/gg/gpu" // Enable gogpu/ui's GPU renderer; software fallback remains available.
	"github.com/gogpu/gogpu"
	uiapp "github.com/gogpu/ui/app"
	"github.com/gogpu/ui/desktop"
	"github.com/gogpu/ui/theme"
	"github.com/simpleswe/simpleswe/internal/client"
)

// Run opens the desktop task operator connected to address.
func Run(ctx context.Context, address, kubeContext, namespace string) error {
	if ctx == nil {
		return errors.New("gui context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("start GUI: %w", err)
	}
	api := client.New(address, nil)
	controller := newController(ctx, api, Options{
		KubeContext: kubeContext, Namespace: namespace,
	})
	host := gogpu.NewApp(gogpu.DefaultConfig().
		WithTitle("simpleswe").
		WithSize(1200, 800).
		WithMinSize(760, 520))
	windowClosed := make(chan struct{})
	host.OnSurfaceAvailable(quitOnCancellation(ctx, windowClosed, host.Quit))
	themes := []*theme.Theme{theme.DefaultDark(), theme.DefaultLight(), theme.DefaultHighContrast()}
	application := uiapp.New(
		uiapp.WithWindowProvider(host),
		uiapp.WithPlatformProvider(host),
		uiapp.WithEventSource(host.EventSource()),
		uiapp.WithTheme(themes[0]),
		uiapp.WithRenderMode(uiapp.RenderModeFrameworkManaged),
	)
	application.SetRoot(controller.root(ctx))
	controller.mu.Lock()
	controller.requestRedraw = host.RequestRedraw
	controller.themeChanged = func(index int) {
		if index >= 0 && index < len(themes) {
			application.SetTheme(themes[index])
			host.RequestRedraw()
		}
	}
	controller.mu.Unlock()
	controller.start(ctx)
	defer controller.stop()

	defer close(windowClosed)
	go waitForCancellation(ctx, windowClosed, host.Quit)
	err := desktop.Run(host, application)
	if ctx.Err() != nil {
		return fmt.Errorf("run GUI: %w", ctx.Err())
	}
	if err != nil {
		return fmt.Errorf("run GUI: %w", err)
	}
	return nil
}

func waitForCancellation(ctx context.Context, windowClosed <-chan struct{}, quit func()) {
	select {
	case <-ctx.Done():
		quit()
	case <-windowClosed:
	}
}

func quitOnCancellation(ctx context.Context, windowClosed <-chan struct{}, quit func()) func() {
	return func() {
		select {
		case <-ctx.Done():
			quit()
		case <-windowClosed:
		default:
		}
	}
}
