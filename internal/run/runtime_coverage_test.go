package run

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/store"
	"github.com/simpleswe/simpleswe/internal/task"
)

const dependencyTaskJSON = `{"task_id":"task-1","state":"queued","created_at":"2026-08-06T00:00:00Z","updated_at":"2026-08-06T00:00:00Z"}`
const dependencyReadyTaskJSON = `{"task_id":"task-1","state":"ready","pull_request":{"state":"open","url":"https://bitbucket.example/acme/widget/pull-requests/1"}}`

func TestDependenciesCallControllerAPI(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks":
			fmt.Fprintf(w, `{"data":{"tasks":[%s]}}`, dependencyTaskJSON)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1":
			fmt.Fprintf(w, `{"data":%s}`, dependencyReadyTaskJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/task-1/cancel":
			fmt.Fprintf(w, `{"data":%s}`, dependencyTaskJSON)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tasks/task-1/retry":
			fmt.Fprintf(w, `{"data":%s}`, dependencyTaskJSON)
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tasks/task-1/logs":
			if r.URL.Query().Get("follow") != "true" || r.URL.Query().Get("tail_lines") != "200" {
				t.Errorf("log query = %q", r.URL.RawQuery)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: worker output\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)

	deps := Dependencies()
	ctx := context.Background()
	listed, err := deps.ListTasks(ctx, server.URL)
	if err != nil || len(listed.Tasks) != 1 || listed.Tasks[0].ID != "task-1" {
		t.Fatalf("ListTasks() = %#v, %v", listed, err)
	}
	for name, call := range map[string]func() (string, error){
		"show": func() (string, error) {
			result, err := deps.ShowTask(ctx, server.URL, "task-1")
			if err != nil {
				return "", fmt.Errorf("show task: %w", err)
			}
			return result.ID, nil
		},
		"cancel": func() (string, error) {
			result, err := deps.CancelTask(ctx, server.URL, "task-1")
			if err != nil {
				return "", fmt.Errorf("cancel task: %w", err)
			}
			return result.ID, nil
		},
		"retry": func() (string, error) {
			result, err := deps.RetryTask(ctx, server.URL, "task-1")
			if err != nil {
				return "", fmt.Errorf("retry task: %w", err)
			}
			return result.ID, nil
		},
	} {
		if id, err := call(); err != nil || id != "task-1" {
			t.Errorf("%s() = %q, %v", name, id, err)
		}
	}
	waited, err := deps.WaitTask(ctx, server.URL, "task-1")
	if err != nil || waited.ID != "task-1" || waited.State != "ready" {
		t.Fatalf("WaitTask() = %#v, %v, want ready task", waited, err)
	}
	var output bytes.Buffer
	if err := deps.StreamLogs(ctx, server.URL, "task-1", &output); err != nil {
		t.Fatalf("StreamLogs() error = %v", err)
	}
	if output.String() != "worker output\n" {
		t.Fatalf("StreamLogs() output = %q", output.String())
	}
}

func TestWorkerAndLocalRuntimeSetup(t *testing.T) {
	secretRoot := t.TempDir()
	if err := os.WriteFile(filepath.Join(secretRoot, "token"), []byte("secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SIMPLESWE_SECRET_PATHS", secretRoot+string(os.PathListSeparator)+secretRoot)
	err := RunWorker(context.Background(), filepath.Join(t.TempDir(), "missing.json"), t.TempDir(), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "manifest") {
		t.Fatalf("RunWorker() error = %v, want manifest error", err)
	}

	workspace, cleanup, err := newWorkspace()
	if err != nil {
		t.Fatalf("newWorkspace() error = %v", err)
	}
	parent := filepath.Dir(workspace)
	if filepath.Base(workspace) != "workspace" {
		t.Fatalf("newWorkspace() = %q", workspace)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup workspace: %v", err)
	}
	if _, err := os.Stat(parent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace parent still exists: %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := portForward(cancelled, "missing", "simpleswe"); !errors.Is(err, context.Canceled) {
		t.Fatalf("portForward(cancelled) error = %v", err)
	}
	if err := runTUI(cancelled, "http://controller", "", "simpleswe", strings.NewReader(""), io.Discard, io.Discard); err == nil {
		t.Fatal("runTUI(cancelled) error = nil")
	}
}

func TestRunControllerReportsStartupFailures(t *testing.T) {
	if err := RunController(context.Background(), filepath.Join(t.TempDir(), "missing.yaml"), filepath.Join(t.TempDir(), "tasks.db"), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "open config") {
		t.Fatalf("RunController(missing config) error = %v", err)
	}
	invalid := filepath.Join(t.TempDir(), "invalid.yaml")
	if err := os.WriteFile(invalid, []byte("repositories: ["), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RunController(context.Background(), invalid, filepath.Join(t.TempDir(), "tasks.db"), io.Discard, io.Discard); err == nil {
		t.Fatal("RunController(invalid config) error = nil")
	}

	valid := filepath.Join(t.TempDir(), "config.yaml")
	configuration := `bitbucket:
  webhook_secret:
    env: BITBUCKET_WEBHOOK_SECRET
repositories:
  - clone_url: https://bitbucket.example/acme/widget.git
    default_branch: main
    worker:
      image: registry.example/widget:latest
    bitbucket:
      workspace: acme
      repository: widget
      credentials_secret_name: widget-bitbucket
`
	if err := os.WriteFile(valid, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("BITBUCKET_WEBHOOK_SECRET", "test-secret")
	err := RunController(context.Background(), valid, filepath.Join(t.TempDir(), "tasks.db"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "in-cluster Kubernetes config") {
		t.Fatalf("RunController(outside cluster) error = %v", err)
	}
}

func TestReadSecretSelectionAndValidation(t *testing.T) {
	t.Setenv("DIRECT_SECRET", "  from environment  ")
	if got, err := readSecret(config.SecretSource{Env: "DIRECT_SECRET"}, "", "", "", "token"); err != nil || got != "from environment" {
		t.Fatalf("readSecret(explicit env) = %q, %v", got, err)
	}
	secretFile := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(secretFile, []byte("  from file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readSecret(config.SecretSource{File: secretFile}, "", "", "", "token"); err != nil || got != "from file" {
		t.Fatalf("readSecret(explicit file) = %q, %v", got, err)
	}

	t.Setenv("SECRET_FILE_NAME", secretFile)
	if got, err := readSecret(config.SecretSource{}, "SECRET_VALUE", "SECRET_FILE_NAME", "", "token"); err != nil || got != "from file" {
		t.Fatalf("readSecret(file env) = %q, %v", got, err)
	}
	t.Setenv("SECRET_FILE_NAME", "")
	if got, err := readSecret(config.SecretSource{}, "SECRET_VALUE", "SECRET_FILE_NAME", secretFile, "token"); err != nil || got != "from file" {
		t.Fatalf("readSecret(default file) = %q, %v", got, err)
	}
	t.Setenv("SECRET_VALUE", "fallback")
	if got, err := readSecret(config.SecretSource{}, "SECRET_VALUE", "SECRET_FILE_NAME", filepath.Join(t.TempDir(), "absent"), "token"); err != nil || got != "fallback" {
		t.Fatalf("readSecret(value env) = %q, %v", got, err)
	}

	if _, err := readSecret(config.SecretSource{File: filepath.Join(t.TempDir(), "absent")}, "", "", "", "token"); err == nil || !strings.Contains(err.Error(), "read token file") {
		t.Fatalf("readSecret(missing file) error = %v", err)
	}
	tooLarge := filepath.Join(t.TempDir(), "large")
	if err := os.WriteFile(tooLarge, bytes.Repeat([]byte("x"), 1<<20+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSecret(config.SecretSource{File: tooLarge}, "", "", "", "token"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("readSecret(large file) error = %v", err)
	}
	t.Setenv("EMPTY_SECRET", " \n")
	if _, err := readSecret(config.SecretSource{Env: "EMPTY_SECRET"}, "", "", "", "token"); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("readSecret(empty env) error = %v", err)
	}
}

func TestRunControllerComponentsStopsAndCancelsPeers(t *testing.T) {
	address := freeAddress(t)
	server := &http.Server{Addr: address, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})}
	webhookServer := &http.Server{Addr: freeAddress(t), Handler: http.NotFoundHandler()}
	err := runControllerComponents(context.Background(), server, webhookServer, func(ctx context.Context) error {
		if err := waitForHTTP(ctx, "http://"+address); err != nil {
			return err
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "controller component 1 stopped unexpectedly") {
		t.Fatalf("runControllerComponents(unexpected stop) error = %v", err)
	}

	address = freeAddress(t)
	server = &http.Server{Addr: address, Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})}
	webhookAddress := freeAddress(t)
	webhookServer = &http.Server{Addr: webhookAddress, Handler: http.NotFoundHandler()}
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- runControllerComponents(ctx, server, webhookServer, func(componentCtx context.Context) error {
			if err := waitForHTTP(componentCtx, "http://"+address); err != nil {
				return err
			}
			if err := waitForHTTP(componentCtx, "http://"+webhookAddress); err != nil {
				return err
			}
			close(ready)
			<-componentCtx.Done()
			return componentCtx.Err()
		})
	}()
	select {
	case <-ready:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("controller HTTP server did not start")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runControllerComponents(cancelled) error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runControllerComponents did not stop")
	}
}

func TestRunControllerComponentsStopsWhenWebhookServerFails(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = occupied.Close() })

	componentCancelled := make(chan struct{})
	err = runControllerComponents(
		context.Background(),
		&http.Server{Addr: freeAddress(t), Handler: http.NotFoundHandler()},
		&http.Server{Addr: occupied.Addr().String(), Handler: http.NotFoundHandler()},
		func(ctx context.Context) error {
			<-ctx.Done()
			close(componentCancelled)
			return ctx.Err()
		},
	)
	if err == nil {
		t.Fatal("runControllerComponents(webhook listen failure) error = nil")
	}
	select {
	case <-componentCancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("webhook failure did not cancel controller components")
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForHTTP(ctx context.Context, address string) error {
	client := &http.Client{Timeout: 50 * time.Millisecond}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, address, nil)
		if err != nil {
			return fmt.Errorf("create readiness request: %w", err)
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for HTTP server: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

type failingResponseWriter struct{ header http.Header }

func (w *failingResponseWriter) Header() http.Header     { return w.header }
func (*failingResponseWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func (*failingResponseWriter) WriteHeader(int)           {}

func TestMetricsHandlerReportsTaskStateAndFailures(t *testing.T) {
	db := openRunTestStore(t)
	ctx := context.Background()
	active, err := db.CreateTask(ctx, store.CreateTaskParams{Repository: "repo", Prompt: "active", IdempotencyKey: "generic-1"})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := db.CreateTask(ctx, store.CreateTaskParams{Repository: "repo", Prompt: "failed"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transition(ctx, failed.ID, task.RECEIVED, task.FAILED, store.TransitionParams{Reason: "failed", Trigger: "system"}); err != nil {
		t.Fatal(err)
	}
	cancelled, err := db.CreateTask(ctx, store.CreateTaskParams{Repository: "repo", Prompt: "cancelled"})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Transition(ctx, cancelled.ID, task.RECEIVED, task.CANCELLED, store.TransitionParams{Reason: "cancelled", Trigger: "system"}); err != nil {
		t.Fatal(err)
	}

	handler := metricsHandler{store: db}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	for _, want := range []string{
		`simpleswe_tasks_total{state="cancelled"} 1`,
		`simpleswe_tasks_total{state="failed"} 1`,
		`simpleswe_tasks_total{state="received"} 1`,
		"simpleswe_tasks_created_total 3",
		"simpleswe_tasks_failed_total 1",
		"simpleswe_tasks_cancelled_total 1",
		"simpleswe_active_tasks 1",
		"simpleswe_jobs_created_total 3",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body missing %q:\n%s", want, body)
		}
	}
	if contentType := recorder.Header().Get("Content-Type"); contentType != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("Content-Type = %q", contentType)
	}

	methodRecorder := httptest.NewRecorder()
	handler.ServeHTTP(methodRecorder, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if methodRecorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST metrics status = %d", methodRecorder.Code)
	}
	handler.ServeHTTP(&failingResponseWriter{header: make(http.Header)}, request)

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	unavailable := httptest.NewRecorder()
	handler.ServeHTTP(unavailable, request)
	if unavailable.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed-store metrics status = %d", unavailable.Code)
	}
	_ = active
}

func TestMetricsHandlerOmitsSlackMetricNames(t *testing.T) {
	handler := metricsHandler{store: openRunTestStore(t)}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", recorder.Code)
	}
	for line := range strings.SplitSeq(recorder.Body.String(), "\n") {
		if strings.HasPrefix(line, "#") || strings.TrimSpace(line) == "" {
			continue
		}
		name := strings.Fields(line)[0]
		if strings.Contains(strings.ToLower(name), "slack") {
			t.Fatalf("metrics contains Slack metric name %q", name)
		}
	}
}

func TestBitbucketConfigurationAndMountedSecretErrors(t *testing.T) {
	root := t.TempDir()
	credentialDir := filepath.Join(root, "credentials")
	if err := os.Mkdir(credentialDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(credentialDir, "username"), []byte("user"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(credentialDir, "app-password"), []byte("password"), 0o600); err != nil {
		t.Fatal(err)
	}
	repository := config.RepositoryConfig{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "Acme", Repository: "Widget", CredentialsSecret: "credentials"}}
	cfg := config.Config{Bitbucket: config.BitbucketConfig{BaseURL: "https://api.bitbucket.org"}, Repositories: config.RepositoryConfigs{repository, repository}}
	router, err := newForgeRouter(cfg, root, t.TempDir())
	if err != nil || len(router) != 1 {
		t.Fatalf("newForgeRouter(duplicate) = %#v, %v", router, err)
	}
	target := forge.Target{
		Provider: forge.ProviderBitbucket, BaseURL: cfg.Bitbucket.BaseURL,
		Owner: "ACME", Repository: "WIDGET", CredentialsSecret: "credentials",
	}
	if _, err := router.client(target); err != nil {
		t.Fatalf("case-insensitive route error = %v", err)
	}
	target.Owner, target.Repository = "missing", "repo"
	if _, err := router.client(target); err == nil || !forge.IsPermanent(err) {
		t.Fatal("router.client(missing) error = nil")
	}
	if _, err := router.CreatePullRequest(context.Background(), target, forge.CreatePullRequestRequest{}); err == nil || !forge.IsPermanent(err) {
		t.Fatal("CreatePullRequest(missing route) error = nil")
	}
	if _, _, err := router.FindPullRequest(context.Background(), target, "branch", "main", "task"); err == nil || !forge.IsPermanent(err) {
		t.Fatal("FindPullRequest(missing route) error = nil")
	}

	invalid := config.Config{Repositories: config.RepositoryConfigs{{}}}
	if _, err := newForgeRouter(invalid, root, t.TempDir()); err == nil {
		t.Fatal("newForgeRouter(missing fields) error = nil")
	}
	conflicting := repository
	conflicting.Bitbucket.CredentialsSecret = "other"
	cfg.Repositories[1] = conflicting
	if _, err := newForgeRouter(cfg, root, t.TempDir()); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("newForgeRouter(conflicting credentials) error = %v", err)
	}
	cfg = config.Config{Bitbucket: config.BitbucketConfig{BaseURL: "://invalid"}, Repositories: config.RepositoryConfigs{repository}}
	if _, err := newForgeRouter(cfg, root, t.TempDir()); err == nil {
		t.Fatal("newForgeRouter(invalid URL) error = nil")
	}

	empty := filepath.Join(root, "empty")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBitbucketCredential(root, "", "empty"); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("readBitbucketCredential(empty) error = %v", err)
	}
	large := filepath.Join(root, "large")
	if err := os.WriteFile(large, bytes.Repeat([]byte("x"), 1<<20+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBitbucketCredential(root, "", "large"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("readBitbucketCredential(large) error = %v", err)
	}

	if values, err := mountedSecrets(filepath.Join(t.TempDir(), "absent")); err != nil || values != nil {
		t.Fatalf("mountedSecrets(absent) = %#v, %v", values, err)
	}
	mount := t.TempDir()
	if err := os.Mkdir(filepath.Join(mount, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mount, "nested", "value"), []byte(" mounted \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mount, "blank"), []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if values, err := mountedSecrets(mount); err != nil || len(values) != 1 || values[0] != "mounted" {
		t.Fatalf("mountedSecrets() = %#v, %v", values, err)
	}
	if err := os.WriteFile(filepath.Join(mount, "large"), bytes.Repeat([]byte("x"), 1<<20+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := mountedSecrets(mount); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("mountedSecrets(large) error = %v", err)
	}
}

func openRunTestStore(t *testing.T) *store.Store {
	t.Helper()
	db, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "run.sqlite"))
	if err != nil {
		t.Fatalf("store.Open() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
