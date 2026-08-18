package tui

import (
	"context"
	"testing"
)

func TestThemePersistsAcrossRestart(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	options := (Options{LogCapacity: 1}).withDefaults()

	app := newApplication(context.Background(), nil, nil, options)
	app.selectTheme(len(themes) - 1)

	restarted := newApplication(context.Background(), nil, nil, options)
	if got := restarted.colors().name; got != "Tokyo Night" {
		t.Fatalf("theme after restart = %q, want Tokyo Night", got)
	}
}
