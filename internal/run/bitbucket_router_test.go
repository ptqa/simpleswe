package run

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
)

func TestNewBitbucketRouterReadsOnlyRepositoryCredentialMounts(t *testing.T) {
	credentials := map[string][2]string{
		"widget-bitbucket": {"widget-user", "widget-password"},
		"gadget-bitbucket": {"gadget-user", "gadget-password"},
	}
	root := t.TempDir()
	for name, values := range credentials {
		dir := filepath.Join(root, name)
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "username"), []byte(values[0]), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "app-password"), []byte(values[1]), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, _ := r.BasicAuth()
		if r.URL.Path == "/2.0/repositories/acme/widget/pullrequests/42" && (username != "widget-user" || password != "widget-password") {
			t.Errorf("widget credentials = %q/%q", username, password)
		}
		if r.URL.Path == "/2.0/repositories/labs/gadget/pullrequests/42" && (username != "gadget-user" || password != "gadget-password") {
			t.Errorf("gadget credentials = %q/%q", username, password)
		}
		owner, repository := "acme", "widget"
		if r.URL.Path == "/2.0/repositories/labs/gadget/pullrequests/42" {
			owner, repository = "labs", "gadget"
		}
		_, _ = io.WriteString(w, `{"id":42,"state":"OPEN","title":"PR","links":{"html":{"href":"https://bitbucket.example/`+owner+`/`+repository+`/pull-requests/42"}},"source":{"branch":{"name":"feature/task"},"commit":{"hash":"0123456789abcdef0123456789abcdef01234567"},"repository":{"full_name":"`+owner+`/`+repository+`"}},"destination":{"branch":{"name":"main"}}}`)
	}))
	defer server.Close()

	cfg := config.Config{Bitbucket: config.BitbucketConfig{BaseURL: server.URL}, Repositories: config.RepositoryConfigs{
		{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "widget", CredentialsSecret: "widget-bitbucket"}},
		{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "labs", Repository: "gadget", CredentialsSecret: "gadget-bitbucket"}},
	}}
	router, err := newForgeRouter(cfg, root, t.TempDir())
	if err != nil {
		t.Fatalf("newForgeRouter: %v", err)
	}
	for _, repository := range cfg.Repositories {
		target := forge.Target{Provider: forge.ProviderBitbucket, BaseURL: cfg.Bitbucket.BaseURL, Owner: repository.Bitbucket.Workspace, Repository: repository.Bitbucket.Repository, CredentialsSecret: repository.Bitbucket.CredentialsSecret}
		if _, err := router.GetPullRequest(context.Background(), target, 42); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNewBitbucketRouterDoesNotFallbackToGlobalCredentials(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "username"), []byte("global-user"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "app-password"), []byte("global-password"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Config{Bitbucket: config.BitbucketConfig{BaseURL: "https://api.bitbucket.org"}, Repositories: config.RepositoryConfigs{{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "widget", CredentialsSecret: "missing-secret"}}}}
	if _, err := newForgeRouter(cfg, root, t.TempDir()); err == nil {
		t.Fatal("newForgeRouter used global credential files")
	}
	cfg.Repositories[0].Bitbucket.CredentialsSecret = "../unsafe"
	if _, err := newForgeRouter(cfg, root, t.TempDir()); err == nil {
		t.Fatal("newForgeRouter accepted unsafe Secret name")
	}
}
