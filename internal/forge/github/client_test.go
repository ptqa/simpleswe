package github

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

	"github.com/simpleswe/simpleswe/internal/forge"
)

const (
	testOwner      = "acme"
	testRepository = "service"
	testToken      = "github-token"
	testMaxBody    = 1 << 20
)

func TestCreatePullRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.URL.EscapedPath(); got != "/api/repos/team%20name/repo%2Fname/pulls" {
			t.Errorf("escaped path = %q, want /api/repos/team%%20name/repo%%2Fname/pulls", got)
		}
		if r.URL.RawQuery != "" {
			t.Errorf("query = %q, want empty", r.URL.RawQuery)
		}
		assertGitHubHeaders(t, r, testToken)
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", got)
		}

		var body struct {
			Title string `json:"title"`
			Body  string `json:"body"`
			Head  string `json:"head"`
			Base  string `json:"base"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
			return
		}
		if body.Title != "Fix flaky test" {
			t.Errorf("title = %q, want Fix flaky test", body.Title)
		}
		if body.Body != "Details from the worker" {
			t.Errorf("body = %q, want Details from the worker", body.Body)
		}
		if body.Head != "feature/fix-flaky-test" {
			t.Errorf("head = %q, want feature/fix-flaky-test", body.Head)
		}
		if body.Base != "main" {
			t.Errorf("base = %q, want main", body.Base)
		}

		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"number":42,"id":9001,"html_url":"https://github.example/pull/42"}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/api/", testToken)
	got, err := client.CreatePullRequest(context.Background(), "team name", "repo/name", forge.CreatePullRequestRequest{
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
	if got.HTMLURL != "https://github.example/pull/42" {
		t.Errorf("HTMLURL = %q, want https://github.example/pull/42", got.HTMLURL)
	}
}

func TestFindPullRequestMatchesTargetRepositorySourceBaseAndTaskMarker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.EscapedPath() != "/repos/acme/service/pulls" {
			t.Errorf("escaped path = %q, want /repos/acme/service/pulls", r.URL.EscapedPath())
		}
		assertGitHubHeaders(t, r, testToken)
		query := r.URL.Query()
		if query.Get("state") != "open" {
			t.Errorf("state query = %q, want open", query.Get("state"))
		}
		if query.Get("head") != "acme:feature/task" {
			t.Errorf("head query = %q, want acme:feature/task", query.Get("head"))
		}
		if query.Get("base") != "main" {
			t.Errorf("base query = %q, want main", query.Get("base"))
		}

		io.WriteString(w, `[`+
			`{"number":40,"state":"open","body":"Created by simpleswe task swe-123","head":{"ref":"other","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/40"},`+
			`{"number":41,"state":"open","body":"Created by simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"release"},"html_url":"https://github.example/pull/41"},`+
			`{"number":42,"state":"open","body":"Created by simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"fork","owner":{"login":"other"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/42"},`+
			`{"number":43,"state":"open","body":"Created by another worker","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/43"},`+
			`{"number":44,"state":"open","body":"Created by simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/44"}`+
			`]`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, testToken)
	got, found, err := client.FindPullRequest(context.Background(), testOwner, testRepository, "feature/task", "main", "swe-123")
	if err != nil {
		t.Fatalf("FindPullRequest: %v", err)
	}
	if !found || got.ID != 44 || got.HTMLURL != "https://github.example/pull/44" {
		t.Fatalf("FindPullRequest = %#v, %t; want pull request 44", got, found)
	}
}

func TestFindPullRequestIgnoresClosedAndMergedMatches(t *testing.T) {
	for _, candidate := range []string{
		`{"number":44,"state":"closed","body":"Created by simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/44"}`,
		`{"number":44,"state":"open","merged":true,"body":"Created by simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/44"}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			io.WriteString(w, "["+candidate+"]")
		}))
		got, found, err := newTestClient(t, server.URL, "").FindPullRequest(context.Background(), testOwner, testRepository, "feature/task", "main", "swe-123")
		server.Close()
		if err != nil || found || got != (forge.PullRequest{}) {
			t.Fatalf("FindPullRequest non-open candidate = %#v, %t, %v; want no match", got, found, err)
		}
	}
}

func TestFindPullRequestReturnsNoMatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `[{"number":41,"body":"simpleswe task other","head":{"ref":"other"},"html_url":"https://github.example/pull/41"}]`)
	}))
	defer server.Close()

	got, found, err := newTestClient(t, server.URL, "").FindPullRequest(context.Background(), testOwner, testRepository, "feature/task", "main", "swe-123")
	if err != nil || found || got != (forge.PullRequest{}) {
		t.Fatalf("FindPullRequest = %#v, %t, %v; want no match", got, found, err)
	}
}

func TestGetPullRequestParsesStateAndRefsFromTargetScopedEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/repos/acme/service/pulls/42" || r.URL.RawQuery != "" {
			t.Fatalf("request = %s %s; want GET /repos/acme/service/pulls/42", r.Method, r.URL.RequestURI())
		}
		io.WriteString(w, `{"number":42,"state":"open","merged":false,"head":{"ref":"feature/task","sha":"0123456789abcdef0123456789abcdef01234567","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"}}`)
	}))
	defer server.Close()

	got, err := newTestClient(t, server.URL, "").GetPullRequest(context.Background(), testOwner, testRepository, 42)
	want := forge.PullRequestState{Number: 42, State: "open", SourceOwner: "acme", SourceRepository: "service", SourceBranch: "feature/task", DestinationBranch: "main", HeadSHA: "0123456789abcdef0123456789abcdef01234567"}
	if err != nil || got != want {
		t.Fatalf("GetPullRequest = %#v, %v; want %#v", got, err, want)
	}
}

func TestGetPullRequestRejectsMissingHeadSHA(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"number":42,"state":"open","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"}}`)
	}))
	defer server.Close()

	if _, err := newTestClient(t, server.URL, "").GetPullRequest(context.Background(), testOwner, testRepository, 42); err == nil || !forge.IsPermanent(err) {
		t.Fatalf("GetPullRequest missing head SHA error = %v; want permanent", err)
	}
}

func TestCreatePullRequestPropagatesErrorBodyWithoutTokenLeak(t *testing.T) {
	const responseBody = "github rejected github-token"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		io.WriteString(w, responseBody)
	}))
	defer server.Close()

	_, err := newTestClient(t, server.URL, testToken).CreatePullRequest(context.Background(), testOwner, testRepository, forge.CreatePullRequestRequest{})
	if err == nil {
		t.Fatal("CreatePullRequest returned nil error for 422 response")
	}
	if !strings.Contains(err.Error(), "github rejected") {
		t.Errorf("error = %q, want response body", err)
	}
	if strings.Contains(err.Error(), testToken) {
		t.Errorf("error leaks token: %q", err)
	}
	if !IsPermanent(err) {
		t.Errorf("422 error was not classified permanent: %v", err)
	}
}

func TestProviderErrorClassificationRetriesOnlyTransientStatuses(t *testing.T) {
	for _, test := range []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true},
		{http.StatusUnauthorized, true},
		{http.StatusForbidden, true},
		{http.StatusNotFound, true},
		{http.StatusUnprocessableEntity, true},
		{http.StatusRequestTimeout, false},
		{http.StatusConflict, false},
		{http.StatusLocked, false},
		{http.StatusTooEarly, false},
		{http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false},
		{http.StatusBadGateway, false},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
			}))
			defer server.Close()

			_, err := newTestClient(t, server.URL, "").CreatePullRequest(context.Background(), testOwner, testRepository, forge.CreatePullRequestRequest{})
			if err == nil || IsPermanent(err) != test.permanent {
				t.Fatalf("status %d error/permanent = %v/%t; want error/%t", test.status, err, IsPermanent(err), test.permanent)
			}
		})
	}
}

func TestGitHubRateLimitAndSpamResponsesRemainRetryable(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		headers   map[string]string
		body      string
		permanent bool
	}{
		{name: "retry after", status: http.StatusForbidden, headers: map[string]string{"Retry-After": "60"}},
		{name: "429 retry after", status: http.StatusTooManyRequests, headers: map[string]string{"Retry-After": "60"}},
		{name: "primary limit exhausted", status: http.StatusForbidden, headers: map[string]string{"X-RateLimit-Remaining": "0"}},
		{name: "primary limit message", status: http.StatusForbidden, body: `{"message":"API rate limit exceeded for installation"}`},
		{name: "secondary limit message", status: http.StatusForbidden, body: `{"message":"You have exceeded a secondary rate limit."}`},
		{name: "spam protection", status: http.StatusUnprocessableEntity, body: `{"message":"The endpoint has been spammed."}`},
		{name: "ordinary permission", status: http.StatusForbidden, body: `{"message":"Resource not accessible by integration"}`, permanent: true},
		{name: "ordinary validation", status: http.StatusUnprocessableEntity, body: `{"message":"Validation Failed"}`, permanent: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				for name, value := range test.headers {
					w.Header().Set(name, value)
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()

			_, err := newTestClient(t, server.URL, "").CreatePullRequest(context.Background(), testOwner, testRepository, forge.CreatePullRequestRequest{})
			if err == nil || IsPermanent(err) != test.permanent {
				t.Fatalf("error/permanent = %v/%t; want error/%t", err, IsPermanent(err), test.permanent)
			}
		})
	}
}

func TestNetworkErrorIsRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	baseURL := server.URL
	server.Close()

	_, err := newTestClient(t, baseURL, testToken).CreatePullRequest(context.Background(), testOwner, testRepository, forge.CreatePullRequestRequest{})
	if err == nil {
		t.Fatal("CreatePullRequest returned nil error for a failed network request")
	}
	if IsPermanent(err) {
		t.Fatalf("network error was classified permanent: %v", err)
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

	client := newTestClient(t, server.URL, testToken)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := client.CreatePullRequest(ctx, testOwner, testRepository, forge.CreatePullRequestRequest{})
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
	case <-time.After(time.Second):
		t.Fatal("CreatePullRequest did not return after cancellation")
	}
}

func TestCreatePullRequestRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", testMaxBody+1))
	}))
	defer server.Close()

	_, err := newTestClient(t, server.URL, "").CreatePullRequest(context.Background(), testOwner, testRepository, forge.CreatePullRequestRequest{})
	if err == nil || !strings.Contains(err.Error(), "response body exceeds") {
		t.Fatalf("CreatePullRequest error = %v; want bounded response body error", err)
	}
}

func TestCreatePullRequestValidatesResponse(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing number", body: `{"html_url":"https://github.example/pull/1"}`, want: "missing pull request id"},
		{name: "zero number", body: `{"number":0,"html_url":"https://github.example/pull/1"}`, want: "missing pull request id"},
		{name: "missing URL", body: `{"number":1}`, want: "missing pull request URL"},
		{name: "invalid URL", body: `{"number":1,"html_url":"/pull/1"}`, want: "invalid pull request URL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				io.WriteString(w, test.body)
			}))
			defer server.Close()

			_, err := newTestClient(t, server.URL, "").CreatePullRequest(context.Background(), testOwner, testRepository, forge.CreatePullRequestRequest{})
			if err == nil || !strings.Contains(err.Error(), test.want) || !IsPermanent(err) {
				t.Fatalf("CreatePullRequest error = %v; want permanent %q", err, test.want)
			}
		})
	}
}

func TestFindPullRequestValidatesMatchedResponse(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing number", body: `[{"state":"open","body":"simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"https://github.example/pull/1"}]`, want: "missing pull request id"},
		{name: "missing URL", body: `[{"number":1,"state":"open","body":"simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"}}]`, want: "missing pull request URL"},
		{name: "invalid URL", body: `[{"number":1,"state":"open","body":"simpleswe task swe-123","head":{"ref":"feature/task","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"},"html_url":"/pull/1"}]`, want: "invalid pull request URL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				io.WriteString(w, test.body)
			}))
			defer server.Close()

			_, _, err := newTestClient(t, server.URL, "").FindPullRequest(context.Background(), testOwner, testRepository, "feature/task", "main", "swe-123")
			if err == nil || !strings.Contains(err.Error(), test.want) || !IsPermanent(err) {
				t.Fatalf("FindPullRequest error = %v; want permanent %q", err, test.want)
			}
		})
	}
}

func TestHTTPErrorWithoutMessage(t *testing.T) {
	err := (&HTTPError{Status: "500 Internal Server Error"}).Error()
	if err != "GitHub returned 500 Internal Server Error" {
		t.Fatalf("HTTPError.Error() = %q", err)
	}
}

func TestNewClientRequiresHTTPSExceptLoopback(t *testing.T) {
	for _, baseURL := range []string{
		"http://github.example",
		"ftp://github.example",
		"https://user:password@github.example",
		"https://github.example?token=secret",
	} {
		if _, err := NewClient(baseURL, ""); err == nil {
			t.Errorf("NewClient(%q) accepted unsafe base URL", baseURL)
		}
	}
	for _, baseURL := range []string{
		"https://api.github.com",
		"http://localhost:8080",
		"http://127.0.0.1:8080",
		"http://[::1]:8080",
	} {
		if _, err := NewClient(baseURL, ""); err != nil {
			t.Errorf("NewClient(%q): %v", baseURL, err)
		}
	}
}

func newTestClient(t *testing.T, baseURL, token string) *Client {
	t.Helper()
	client, err := NewClient(baseURL, token)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

func assertGitHubHeaders(t *testing.T, r *http.Request, token string) {
	t.Helper()
	if got := r.Header.Get("Authorization"); got != "Bearer "+token {
		t.Errorf("Authorization = %q, want Bearer token", got)
	}
	if got := r.Header.Get("Accept"); got != "application/vnd.github+json" {
		t.Errorf("Accept = %q, want application/vnd.github+json", got)
	}
	if got := r.Header.Get("X-Github-Api-Version"); got != "2022-11-28" {
		t.Errorf("X-GitHub-Api-Version = %q, want 2022-11-28", got)
	}
}
