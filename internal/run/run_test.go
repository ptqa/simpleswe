package run

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestMountedSecretsCannotEscapeConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "inside"), []byte("inside-secret\n"), 0o600); err != nil {
		t.Fatalf("write mounted secret: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outside, "outside"), []byte("outside-secret\n"), 0o600); err != nil {
		t.Fatalf("write outside secret: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "outside"), filepath.Join(root, "escape")); err != nil {
		t.Fatalf("create escaping symlink: %v", err)
	}

	got, err := mountedSecrets(root)
	if err != nil {
		t.Fatalf("mountedSecrets() error = %v", err)
	}
	if want := []string{"inside-secret"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mountedSecrets() = %q, want %q", got, want)
	}
}

func TestAutomaticPortForwardOptionsUseStartupBudget(t *testing.T) {
	got := automaticPortForwardOptions("production", "team-a")
	if got.StartupTimeout != 15*time.Second {
		t.Fatalf("automatic startup timeout = %s, want 15s", got.StartupTimeout)
	}
}
