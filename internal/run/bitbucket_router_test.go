package run

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
)

func TestBitbucketRouterRoutesFindAndCreateToRepositoryClient(t *testing.T) {
	type testRepository struct {
		workspace, repository, username, password string
		id                                        int
	}
	tests := []testRepository{
		{workspace: "acme", repository: "widget", username: "widget-user", password: "widget-password", id: 11},
		{workspace: "labs", repository: "gadget", username: "gadget-user", password: "gadget-password", id: 22},
	}
	router := make(bitbucketRouter, len(tests))
	var requests atomic.Int32
	for _, test := range tests {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			if username, password, ok := r.BasicAuth(); !ok || username != test.username || password != test.password {
				t.Errorf("%s/%s BasicAuth = %q/%q/%t", test.workspace, test.repository, username, password, ok)
			}
			wantPath := "/2.0/repositories/" + test.workspace + "/" + test.repository + "/pullrequests"
			if r.URL.Path != wantPath {
				t.Errorf("path = %q, want %q", r.URL.Path, wantPath)
			}
			w.Header().Set("Content-Type", "application/json")
			if r.Method == http.MethodGet {
				fmt.Fprintf(w, `{"values":[{"id":%d,"description":"Created by simpleswe task task-1","source":{"branch":{"name":"feature/task-1"}},"links":{"html":{"href":"https://bitbucket.example/pr/%d"}}}]}`, test.id, test.id)
				return
			}
			fmt.Fprintf(w, `{"id":%d,"links":{"html":{"href":"https://bitbucket.example/pr/%d"}}}`, test.id, test.id)
		}))
		t.Cleanup(server.Close)
		client, err := bitbucket.NewClient(server.URL, test.username, test.password)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		router[bitbucketRoute(test.workspace, test.repository)] = client
	}

	for _, test := range tests {
		found, ok, err := router.FindPullRequest(context.Background(), test.workspace, test.repository, "feature/task-1", "task-1")
		if err != nil || !ok || found.ID != test.id {
			t.Fatalf("FindPullRequest(%s/%s) = %#v, %t, %v", test.workspace, test.repository, found, ok, err)
		}
		created, err := router.CreatePullRequest(context.Background(), test.workspace, test.repository, bitbucket.CreatePullRequestRequest{})
		if err != nil || created.ID != test.id {
			t.Fatalf("CreatePullRequest(%s/%s) = %#v, %v", test.workspace, test.repository, created, err)
		}
	}
	if requests.Load() != 4 {
		t.Fatalf("requests = %d, want 4", requests.Load())
	}
}

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
		workspace := r.URL.Path[len("/2.0/repositories/"):]
		username, password, _ := r.BasicAuth()
		if workspace == "acme/widget/pullrequests" && (username != "widget-user" || password != "widget-password") {
			t.Errorf("widget credentials = %q/%q", username, password)
		}
		if workspace == "labs/gadget/pullrequests" && (username != "gadget-user" || password != "gadget-password") {
			t.Errorf("gadget credentials = %q/%q", username, password)
		}
		io.WriteString(w, `{"id":1,"links":{"html":{"href":"https://bitbucket.example/pr/1"}}}`)
	}))
	defer server.Close()

	cfg := config.Config{Bitbucket: config.BitbucketConfig{BaseURL: server.URL}, Repositories: config.RepositoryConfigs{
		{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "widget", CredentialsSecret: "widget-bitbucket"}},
		{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "labs", Repository: "gadget", CredentialsSecret: "gadget-bitbucket"}},
	}}
	router, err := newBitbucketRouter(cfg, root)
	if err != nil {
		t.Fatalf("newBitbucketRouter: %v", err)
	}
	for _, repository := range cfg.Repositories {
		if _, err := router.CreatePullRequest(context.Background(), repository.Bitbucket.Workspace, repository.Bitbucket.Repository, bitbucket.CreatePullRequestRequest{}); err != nil {
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
	cfg := config.Config{Bitbucket: config.BitbucketConfig{BaseURL: "https://api.bitbucket.org"}, Repositories: config.RepositoryConfigs{
		{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "widget", CredentialsSecret: "missing-secret"}},
	}}
	if _, err := newBitbucketRouter(cfg, root); err == nil {
		t.Fatal("newBitbucketRouter used global credential files")
	}
	cfg.Repositories[0].Bitbucket.CredentialsSecret = "../unsafe"
	if _, err := newBitbucketRouter(cfg, root); err == nil {
		t.Fatal("newBitbucketRouter accepted unsafe Secret name")
	}
}
