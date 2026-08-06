package bitbucket

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	testWorkspace   = "workspace"
	testRepository  = "repository"
	testUsername    = "bot@example.com"
	testAppPassword = "app-password"
)

func TestCreatePullRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/2.0/repositories/workspace/repository/pullrequests" {
			t.Errorf("path = %q, want /2.0/repositories/workspace/repository/pullrequests", r.URL.Path)
		}
		if r.URL.RawQuery != "" {
			t.Errorf("query = %q, want empty", r.URL.RawQuery)
		}
		if r.URL.User != nil {
			t.Errorf("URL user info contains credentials: %v", r.URL.User)
		}
		if strings.Contains(r.RequestURI, testUsername) || strings.Contains(r.RequestURI, testAppPassword) {
			t.Errorf("request URI contains credentials: %q", r.RequestURI)
		}

		username, password, ok := r.BasicAuth()
		if !ok || username != testUsername || password != testAppPassword {
			t.Errorf("BasicAuth() = (%q, %q, %t), want (%q, %q, true)", username, password, ok, testUsername, testAppPassword)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}

		var body struct {
			Title             string    `json:"title"`
			Description       string    `json:"description"`
			Source            branchRef `json:"source"`
			Destination       branchRef `json:"destination"`
			CloseSourceBranch *bool     `json:"close_source_branch"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
			return
		}
		if body.Title != "Fix flaky test" {
			t.Errorf("title = %q, want Fix flaky test", body.Title)
		}
		if body.Description != "Details from the worker" {
			t.Errorf("description = %q, want Details from the worker", body.Description)
		}
		if body.Source.Branch.Name != "feature/fix-flaky-test" {
			t.Errorf("source branch = %q, want feature/fix-flaky-test", body.Source.Branch.Name)
		}
		if body.Destination.Branch.Name != "main" {
			t.Errorf("destination branch = %q, want main", body.Destination.Branch.Name)
		}
		if body.CloseSourceBranch == nil || *body.CloseSourceBranch {
			t.Errorf("close_source_branch = %v, want explicit false", body.CloseSourceBranch)
		}

		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":42,"links":{"html":{"href":"https://bitbucket.example/pull-requests/42"}}}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, testUsername, testAppPassword)
	got, err := client.CreatePullRequest(context.Background(), testWorkspace, testRepository, CreatePullRequestRequest{
		Title:             "Fix flaky test",
		Description:       "Details from the worker",
		SourceBranch:      "feature/fix-flaky-test",
		DestinationBranch: "main",
	})
	if err != nil {
		t.Fatalf("CreatePullRequest: %v", err)
	}
	if got.ID != 42 {
		t.Errorf("ID = %d, want 42", got.ID)
	}
	if got.HTMLURL != "https://bitbucket.example/pull-requests/42" {
		t.Errorf("HTMLURL = %q, want https://bitbucket.example/pull-requests/42", got.HTMLURL)
	}
}

func TestFindPullRequestMatchesSourceBranchAndTaskMarker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Query().Get("state") != "OPEN" {
			t.Errorf("state query = %q, want OPEN", r.URL.Query().Get("state"))
		}
		io.WriteString(w, `{"values":[`+
			`{"id":41,"description":"Created by simpleswe task other","source":{"branch":{"name":"feature/task"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/41"}}},`+
			`{"id":42,"description":"Created by simpleswe task swe-123","source":{"branch":{"name":"feature/task"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/42"}}}`+
			`]}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, testUsername, testAppPassword)
	got, found, err := client.FindPullRequest(context.Background(), testWorkspace, testRepository, "feature/task", "swe-123")
	if err != nil {
		t.Fatalf("FindPullRequest: %v", err)
	}
	if !found || got.ID != 42 || !strings.HasSuffix(got.HTMLURL, "/42") {
		t.Fatalf("FindPullRequest = %#v, %t; want pull request 42", got, found)
	}
}

func TestCreatePullRequestPropagatesErrorBodyWithoutCredentialLeak(t *testing.T) {
	const responseBody = "bitbucket rejected this pull request"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		io.WriteString(w, responseBody)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, testUsername, testAppPassword)
	_, err := client.CreatePullRequest(context.Background(), testWorkspace, testRepository, CreatePullRequestRequest{
		Title:             "Fix flaky test",
		Description:       "Details from the worker",
		SourceBranch:      "feature/fix-flaky-test",
		DestinationBranch: "main",
	})
	if err == nil {
		t.Fatal("CreatePullRequest returned nil error for 422 response")
	}
	if !strings.Contains(err.Error(), responseBody) {
		t.Errorf("error = %q, want response body %q", err, responseBody)
	}
	if !strings.Contains(err.Error(), "422") {
		t.Errorf("error = %q, want HTTP status 422", err)
	}
	if strings.Contains(err.Error(), testUsername) || strings.Contains(err.Error(), testAppPassword) {
		t.Errorf("error leaks credentials: %q", err)
	}
	if !IsPermanent(err) {
		t.Errorf("422 error was not classified permanent: %v", err)
	}
}

func TestProviderErrorClassificationRetriesOnlyTransientStatuses(t *testing.T) {
	for _, test := range []struct {
		status    int
		permanent bool
	}{{http.StatusBadRequest, true}, {http.StatusUnauthorized, true}, {http.StatusUnprocessableEntity, true}, {http.StatusRequestTimeout, false}, {http.StatusConflict, false}, {http.StatusTooManyRequests, false}, {http.StatusInternalServerError, false}} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(test.status) }))
			defer server.Close()
			_, err := newTestClient(t, server.URL, "", "").CreatePullRequest(context.Background(), testWorkspace, testRepository, CreatePullRequestRequest{})
			if err == nil || IsPermanent(err) != test.permanent {
				t.Fatalf("status %d error/permanent = %v/%t; want error/%t", test.status, err, IsPermanent(err), test.permanent)
			}
		})
	}
}

func TestCreatePullRequestContextCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, testUsername, testAppPassword)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.CreatePullRequest(ctx, testWorkspace, testRepository, CreatePullRequestRequest{})
		errCh <- err
	}()

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("context canceled before request reached server")
	}
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
		if err != nil && (strings.Contains(err.Error(), testUsername) || strings.Contains(err.Error(), testAppPassword)) {
			t.Errorf("error leaks credentials: %q", err)
		}
	case <-time.After(time.Second):
		t.Fatal("CreatePullRequest did not return after cancellation")
	}
}

func TestNewClientRequiresHTTPSExceptLoopback(t *testing.T) {
	for _, baseURL := range []string{"http://bitbucket.example", "ftp://bitbucket.example", "https://user:password@bitbucket.example", "https://bitbucket.example?password=secret"} {
		if _, err := NewClient(baseURL, "", ""); err == nil {
			t.Errorf("NewClient(%q) accepted unsafe base URL", baseURL)
		}
	}
	for _, baseURL := range []string{"https://bitbucket.example", "http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"} {
		if _, err := NewClient(baseURL, "", ""); err != nil {
			t.Errorf("NewClient(%q): %v", baseURL, err)
		}
	}
}

func newTestClient(t *testing.T, baseURL, username, appPassword string) *Client {
	t.Helper()
	client, err := NewClient(baseURL, username, appPassword)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

type branchRef struct {
	Branch branch `json:"branch"`
}

type branch struct {
	Name string `json:"name"`
}
