package run

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
)

type pullRequestInspectionClient struct {
	owner, repository string
	getNumber         int
}

func (c *pullRequestInspectionClient) GetPullRequest(_ context.Context, owner, repository string, number int) (forge.PullRequestState, error) {
	c.owner, c.repository, c.getNumber = owner, repository, number
	return forge.PullRequestState{Number: number, State: "open"}, nil
}

func TestForgeRouterRoutesPullRequestInspection(t *testing.T) {
	target := forge.Target{Provider: forge.ProviderGitHub, BaseURL: "https://api.github.com", Owner: "Acme", Repository: "Widget", CredentialsSecret: "widget-github"}
	client := new(pullRequestInspectionClient)
	router := forgeRouter{forgeRoute(target): client}
	got, err := router.GetPullRequest(context.Background(), target, 42)
	if err != nil || got.Number != 42 || client.owner != target.Owner || client.repository != target.Repository || client.getNumber != 42 {
		t.Fatalf("GetPullRequest = %#v, %v; routed client = %#v", got, err, client)
	}
}

func TestForgeRouterDispatchesConfiguredGitHubAndBitbucketInspection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch strings.ToLower(r.URL.Path) {
		case "/2.0/repositories/acme/bitbucket-repo/pullrequests/11":
			username, password, ok := r.BasicAuth()
			if !ok || username != "bitbucket-user" || password != "bitbucket-password" {
				t.Errorf("Bitbucket BasicAuth = %q/%q/%t", username, password, ok)
			}
			_, _ = io.WriteString(w, `{"id":11,"state":"OPEN","title":"Bitbucket PR","links":{"html":{"href":"https://bitbucket.example/acme/bitbucket-repo/pull-requests/11"}},"source":{"branch":{"name":"feature/task"},"commit":{"hash":"0123456789abcdef0123456789abcdef01234567"},"repository":{"full_name":"acme/bitbucket-repo"}},"destination":{"branch":{"name":"main"}}}`)
		case "/repos/acme/github-repo/pulls/22":
			if got := r.Header.Get("Authorization"); got != "Bearer github-token" {
				t.Errorf("GitHub Authorization = %q", got)
			}
			_, _ = io.WriteString(w, `{"number":22,"state":"open","title":"GitHub PR","html_url":"https://github.example/acme/github-repo/pull/22","head":{"ref":"feature/task","sha":"0123456789abcdef0123456789abcdef01234567","repo":{"name":"github-repo","owner":{"login":"acme"}}},"base":{"ref":"main"}}`)
		default:
			t.Errorf("unexpected forge path %q", r.URL.Path)
		}
	}))
	defer server.Close()

	bitbucketRoot, githubRoot := t.TempDir(), t.TempDir()
	if err := os.Mkdir(filepath.Join(bitbucketRoot, "bitbucket-secret"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(bitbucketRoot, "bitbucket-secret", "username"), "bitbucket-user")
	writeFile(t, filepath.Join(bitbucketRoot, "bitbucket-secret", "app-password"), "bitbucket-password")
	writeGithubToken(t, githubRoot, "github-secret", "github-token")
	cfg := config.Config{
		Bitbucket: config.BitbucketConfig{BaseURL: server.URL}, GitHub: config.GitHubConfig{BaseURL: server.URL},
		Repositories: config.RepositoryConfigs{
			{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "bitbucket-repo", CredentialsSecret: "bitbucket-secret"}},
			{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "github-repo", CredentialsSecret: "github-secret"}},
		},
	}
	router, err := newForgeRouter(cfg, bitbucketRoot, githubRoot)
	if err != nil {
		t.Fatalf("newForgeRouter: %v", err)
	}
	for _, test := range []struct {
		target forge.Target
		number int
	}{
		{forge.Target{Provider: forge.ProviderBitbucket, BaseURL: server.URL, Owner: "ACME", Repository: "BITBUCKET-REPO", CredentialsSecret: "bitbucket-secret"}, 11},
		{forge.Target{Provider: forge.ProviderGitHub, BaseURL: server.URL, Owner: "ACME", Repository: "GITHUB-REPO", CredentialsSecret: "github-secret"}, 22},
	} {
		got, err := router.GetPullRequest(context.Background(), test.target, test.number)
		if err != nil || got.Number != test.number {
			t.Fatalf("GetPullRequest(%s) = %#v, %v", test.target.Provider, got, err)
		}
	}
}

func TestForgeRouterRejectsChangedImmutableRouteWithoutDispatch(t *testing.T) {
	var oldRequests, currentRequests atomic.Int32
	oldServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { oldRequests.Add(1) }))
	defer oldServer.Close()
	currentServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { currentRequests.Add(1) }))
	defer currentServer.Close()
	root := t.TempDir()
	writeGithubToken(t, root, "current-github", "current-token")
	router, err := newForgeRouter(config.Config{GitHub: config.GitHubConfig{BaseURL: currentServer.URL}, Repositories: config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "widget", CredentialsSecret: "current-github"}}}}, t.TempDir(), root)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []forge.Target{
		{Provider: forge.ProviderGitHub, BaseURL: oldServer.URL, Owner: "acme", Repository: "widget", CredentialsSecret: "current-github"},
		{Provider: forge.ProviderGitHub, BaseURL: currentServer.URL, Owner: "acme", Repository: "widget", CredentialsSecret: "old-github"},
	} {
		if _, err := router.GetPullRequest(context.Background(), target, 42); err == nil || !forge.IsPermanent(err) || !strings.Contains(err.Error(), "immutable forge route") {
			t.Fatalf("GetPullRequest error = %v; want permanent immutable route error", err)
		}
	}
	if oldRequests.Load() != 0 || currentRequests.Load() != 0 {
		t.Fatalf("changed route dispatched requests old=%d current=%d", oldRequests.Load(), currentRequests.Load())
	}
}

func TestNewForgeRouterRejectsUnsafeMissingEmptyOversizedAndConflictingGitHubTokens(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, root string)
		repos config.RepositoryConfigs
	}{
		{name: "unsafe secret", repos: githubRepositories("../unsafe")},
		{name: "missing token", repos: githubRepositories("missing-github")},
		{name: "empty token", setup: func(t *testing.T, root string) { writeGithubToken(t, root, "empty-github", " \n") }, repos: githubRepositories("empty-github")},
		{name: "oversized token", setup: func(t *testing.T, root string) {
			writeGithubToken(t, root, "large-github", string(bytes.Repeat([]byte{'x'}, 1<<20+1)))
		}, repos: githubRepositories("large-github")},
		{name: "conflicting credentials", setup: func(t *testing.T, root string) {
			writeGithubToken(t, root, "first-github", "first-token")
			writeGithubToken(t, root, "second-github", "second-token")
		}, repos: config.RepositoryConfigs{
			{GitHub: config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: "first-github"}},
			{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "widget", CredentialsSecret: "second-github"}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			if test.setup != nil {
				test.setup(t, root)
			}
			if _, err := newForgeRouter(config.Config{GitHub: config.GitHubConfig{BaseURL: "https://api.github.com"}, Repositories: test.repos}, t.TempDir(), root); err == nil {
				t.Fatalf("newForgeRouter accepted %s", test.name)
			}
		})
	}
}

func githubRepositories(secret string) config.RepositoryConfigs {
	return config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: secret}}}
}

func writeGithubToken(t *testing.T, root, secret, token string) {
	t.Helper()
	if err := os.Mkdir(filepath.Join(root, secret), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(root, secret, "token"), token)
}

func writeFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
		t.Fatal(err)
	}
}
