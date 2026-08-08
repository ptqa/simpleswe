package run

import (
	"bytes"
	"context"
	"fmt"
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

func (*pullRequestInspectionClient) CreatePullRequest(context.Context, string, string, forge.CreatePullRequestRequest) (forge.PullRequest, error) {
	return forge.PullRequest{}, nil
}
func (*pullRequestInspectionClient) FindPullRequest(context.Context, string, string, string, string, string) (forge.PullRequest, bool, error) {
	return forge.PullRequest{}, false, nil
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
func TestForgeRouterRoutesGitHubFindAndCreateByCaseInsensitiveOwnerAndRepository(t *testing.T) {
	type testRepository struct {
		owner, repository, token string
		id                       int
	}
	tests := []testRepository{
		{owner: "acme", repository: "widget", token: "widget-token", id: 11},
		{owner: "labs", repository: "gadget", token: "gadget-token", id: 22},
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var test testRepository
		switch strings.ToLower(r.URL.Path) {
		case "/repos/acme/widget/pulls":
			test = tests[0]
		case "/repos/labs/gadget/pulls":
			test = tests[1]
		default:
			t.Errorf("unexpected GitHub path %q", r.URL.Path)
			return
		}
		if got, want := r.Header.Get("Authorization"), "Bearer "+test.token; got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			fmt.Fprintf(w, `[{"number":%d,"state":"open","body":"Created by simpleswe task task-1","head":{"ref":"feature/task-1","repo":{"name":%q,"owner":{"login":%q}}},"base":{"ref":"main"},"html_url":"https://github.example/pr/%d"}]`, test.id, test.repository, test.owner, test.id)
			return
		}
		io.WriteString(w, fmt.Sprintf(`{"number":%d,"html_url":"https://github.example/pr/%d"}`, test.id, test.id))
	}))
	t.Cleanup(server.Close)

	root := t.TempDir()
	for _, test := range tests {
		writeGithubToken(t, root, test.owner+"-github", test.token)
	}
	cfg := config.Config{
		GitHub: config.GitHubConfig{BaseURL: server.URL},
		Repositories: config.RepositoryConfigs{
			{GitHub: config.RepositoryGitHubConfig{Owner: "Acme", Repository: "Widget", CredentialsSecret: "acme-github"}},
			{GitHub: config.RepositoryGitHubConfig{Owner: "Labs", Repository: "Gadget", CredentialsSecret: "labs-github"}},
		},
	}
	router, err := newForgeRouter(cfg, t.TempDir(), root)
	if err != nil {
		t.Fatalf("newForgeRouter: %v", err)
	}

	for _, test := range tests {
		owner, repository := strings.ToUpper(test.owner), strings.ToUpper(test.repository)
		target := forge.Target{
			Provider: forge.ProviderGitHub, BaseURL: server.URL, Owner: owner, Repository: repository,
			CredentialsSecret: test.owner + "-github",
		}
		found, ok, err := router.FindPullRequest(context.Background(), target, "feature/task-1", "main", "task-1")
		if err != nil || !ok || found.ID != test.id || found.HTMLURL != "https://github.example/pr/"+fmt.Sprint(test.id) {
			t.Fatalf("FindPullRequest(%s/%s) = %#v, %t, %v", owner, repository, found, ok, err)
		}
		created, err := router.CreatePullRequest(context.Background(), target, forge.CreatePullRequestRequest{
			Title:             "task title",
			Description:       "task description",
			SourceBranch:      "feature/task-1",
			DestinationBranch: "main",
		})
		if err != nil || created.ID != test.id || created.HTMLURL != "https://github.example/pr/"+fmt.Sprint(test.id) {
			t.Fatalf("CreatePullRequest(%s/%s) = %#v, %v", owner, repository, created, err)
		}
	}
	if requests.Load() != int32(len(tests)*2) {
		t.Fatalf("requests = %d, want %d", requests.Load(), len(tests)*2)
	}
	unknown := forge.Target{
		Provider: forge.ProviderGitHub, BaseURL: server.URL, Owner: "unknown", Repository: "repository",
		CredentialsSecret: "unknown-github",
	}
	if _, err := router.CreatePullRequest(context.Background(), unknown, forge.CreatePullRequestRequest{}); err == nil || !forge.IsPermanent(err) {
		t.Fatalf("CreatePullRequest unknown route error = %v; want permanent", err)
	}
	unknown.Provider = "gitlab"
	if _, _, err := router.FindPullRequest(context.Background(), unknown, "branch", "main", "task"); err == nil || !forge.IsPermanent(err) {
		t.Fatalf("FindPullRequest unknown provider error = %v; want permanent", err)
	}
}

func TestForgeRouterDispatchesMixedBitbucketAndGitHubTargets(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/2.0/repositories/acme/bitbucket-repo/pullrequests":
			username, password, ok := r.BasicAuth()
			if !ok || username != "bitbucket-user" || password != "bitbucket-password" {
				t.Errorf("Bitbucket BasicAuth = %q/%q/%t", username, password, ok)
			}
			_, _ = io.WriteString(w, `{"id":11,"links":{"html":{"href":"https://bitbucket.example/pr/11"}}}`)
		case "/repos/acme/github-repo/pulls":
			if got := r.Header.Get("Authorization"); got != "Bearer github-token" {
				t.Errorf("GitHub Authorization = %q", got)
			}
			_, _ = io.WriteString(w, `{"number":22,"html_url":"https://github.example/pr/22"}`)
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
		Bitbucket: config.BitbucketConfig{BaseURL: server.URL},
		GitHub:    config.GitHubConfig{BaseURL: server.URL},
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
		id     int
	}{
		{target: forge.Target{Provider: forge.ProviderBitbucket, BaseURL: server.URL, Owner: "acme", Repository: "bitbucket-repo", CredentialsSecret: "bitbucket-secret"}, id: 11},
		{target: forge.Target{Provider: forge.ProviderGitHub, BaseURL: server.URL, Owner: "acme", Repository: "github-repo", CredentialsSecret: "github-secret"}, id: 22},
	} {
		pullRequest, err := router.CreatePullRequest(context.Background(), test.target, forge.CreatePullRequestRequest{})
		if err != nil || pullRequest.ID != test.id {
			t.Fatalf("CreatePullRequest(%s) = %#v, %v; want id %d", test.target.Provider, pullRequest, err, test.id)
		}
	}
}

func TestForgeRouterRejectsChangedImmutableBaseURLAndCredentialRouteWithoutDispatch(t *testing.T) {
	var oldRequests, currentRequests atomic.Int32
	oldServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { oldRequests.Add(1) }))
	t.Cleanup(oldServer.Close)
	currentServer := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { currentRequests.Add(1) }))
	t.Cleanup(currentServer.Close)

	githubRoot := t.TempDir()
	writeGithubToken(t, githubRoot, "current-github", "current-token")
	router, err := newForgeRouter(config.Config{
		GitHub: config.GitHubConfig{BaseURL: currentServer.URL},
		Repositories: config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{
			Owner: "acme", Repository: "widget", CredentialsSecret: "current-github",
		}}},
	}, t.TempDir(), githubRoot)
	if err != nil {
		t.Fatalf("newForgeRouter: %v", err)
	}

	for name, target := range map[string]forge.Target{
		"changed base URL": {
			Provider: forge.ProviderGitHub, BaseURL: oldServer.URL, Owner: "acme", Repository: "widget", CredentialsSecret: "current-github",
		},
		"changed credential selector": {
			Provider: forge.ProviderGitHub, BaseURL: currentServer.URL, Owner: "acme", Repository: "widget", CredentialsSecret: "old-github",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := router.FindPullRequest(context.Background(), target, "feature/task-1", "main", "task-1")
			if err == nil || !forge.IsPermanent(err) || !strings.Contains(err.Error(), "immutable forge route") {
				t.Fatalf("FindPullRequest error = %v; want permanent immutable route error", err)
			}
		})
	}
	if got := oldRequests.Load(); got != 0 {
		t.Fatalf("old endpoint requests = %d; want 0", got)
	}
	if got := currentRequests.Load(); got != 0 {
		t.Fatalf("current endpoint requests = %d; want 0", got)
	}
}

func TestNewForgeRouterRejectsUnsafeMissingEmptyOversizedAndConflictingGitHubTokens(t *testing.T) {
	tests := []struct {
		name   string
		secret string
		setup  func(t *testing.T, root string)
		repos  config.RepositoryConfigs
	}{
		{name: "unsafe secret", secret: "../unsafe", repos: githubRepositories("../unsafe")},
		{name: "missing token", secret: "missing-github", setup: func(t *testing.T, root string) {
			writeFile(t, filepath.Join(root, "token"), "global-token")
		}, repos: githubRepositories("missing-github")},
		{name: "empty token", secret: "empty-github", setup: func(t *testing.T, root string) {
			writeGithubToken(t, root, "empty-github", " \n")
		}, repos: githubRepositories("empty-github")},
		{name: "oversized token", secret: "large-github", setup: func(t *testing.T, root string) {
			writeGithubToken(t, root, "large-github", string(bytes.Repeat([]byte{'x'}, 1<<20+1)))
		}, repos: githubRepositories("large-github")},
		{name: "conflicting credentials", secret: "second-github", setup: func(t *testing.T, root string) {
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
			cfg := config.Config{GitHub: config.GitHubConfig{BaseURL: "https://api.github.com"}, Repositories: test.repos}
			if _, err := newForgeRouter(cfg, t.TempDir(), root); err == nil {
				t.Fatalf("newForgeRouter accepted %s %q", test.name, test.secret)
			}
		})
	}
}

func githubRepositories(secret string) config.RepositoryConfigs {
	return config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{
		Owner: "Acme", Repository: "Widget", CredentialsSecret: secret,
	}}}
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
