package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestExecuteReturnsCommandExitStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/v1/tasks" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, `{"data":{"tasks":[]}}`)
	}))
	t.Cleanup(server.Close)

	stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout"))
	if err != nil {
		t.Fatal(err)
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(t.TempDir(), "stderr"))
	if err != nil {
		t.Fatal(err)
	}
	defer stderr.Close()
	originalArgs, originalStdout, originalStderr := os.Args, os.Stdout, os.Stderr
	t.Cleanup(func() {
		os.Args, os.Stdout, os.Stderr = originalArgs, originalStdout, originalStderr
	})
	os.Stdout, os.Stderr = stdout, stderr

	os.Args = []string{"simpleswe", "task", "list", "--address", server.URL}
	if got := execute(); got != 0 {
		t.Fatalf("execute(valid task list) = %d, want 0", got)
	}
	os.Args = []string{"simpleswe", "unknown"}
	if got := execute(); got != 1 {
		t.Fatalf("execute(unknown command) = %d, want 1", got)
	}
}
