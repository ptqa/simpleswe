package github

import (
	"context"
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
)

func TestGetPullRequestParsesTargetScopedProviderState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/repos/acme/service/pulls/42" || r.URL.RawQuery != "" {
			t.Fatalf("request = %s %s; want GET /repos/acme/service/pulls/42", r.Method, r.URL.RequestURI())
		}
		assertGitHubHeaders(t, r, testToken)
		_, _ = io.WriteString(w, `{"number":42,"state":"open","merged":false,"title":"Fix flaky validation","html_url":"https://github.example/acme/service/pull/42","head":{"ref":"feature/task","sha":"0123456789abcdef0123456789abcdef01234567","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"}}`)
	}))
	defer server.Close()

	got, err := newTestClient(t, server.URL, testToken).GetPullRequest(context.Background(), testOwner, testRepository, 42)
	want := forge.PullRequestState{Number: 42, State: "open", HTMLURL: "https://github.example/acme/service/pull/42", Title: "Fix flaky validation", SourceOwner: "acme", SourceRepository: "service", SourceBranch: "feature/task", DestinationBranch: "main", HeadSHA: "0123456789abcdef0123456789abcdef01234567"}
	if err != nil || got != want {
		t.Fatalf("GetPullRequest = %#v, %v; want %#v", got, err, want)
	}
}

func TestGetPullRequestRejectsInvalidProviderMetadata(t *testing.T) {
	valid := `{"number":42,"state":"open","title":"Fix it","html_url":"%s","head":{"ref":"feature/task","sha":"0123456789abcdef0123456789abcdef01234567","repo":{"name":"service","owner":{"login":"acme"}}},"base":{"ref":"main"}}`
	for name, body := range map[string]string{
		"missing head SHA": strings.Replace(valid, `"sha":"0123456789abcdef0123456789abcdef01234567",`, "", 1),
		"missing title":    strings.Replace(valid, `"title":"Fix it",`, "", 1),
		"insecure URL":     strings.Replace(valid, "%s", "http://github.example/acme/service/pull/42", 1),
		"credentials":      strings.Replace(valid, "%s", "https://user@github.example/acme/service/pull/42", 1),
		"query":            strings.Replace(valid, "%s", "https://github.example/acme/service/pull/42?token=x", 1),
		"fragment":         strings.Replace(valid, "%s", "https://github.example/acme/service/pull/42#details", 1),
	} {
		t.Run(name, func(t *testing.T) {
			body = strings.Replace(body, "%s", "https://github.example/acme/service/pull/42", 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
			defer server.Close()
			if _, err := newTestClient(t, server.URL, "").GetPullRequest(context.Background(), testOwner, testRepository, 42); err == nil || !forge.IsPermanent(err) {
				t.Fatalf("GetPullRequest error = %v; want permanent", err)
			}
		})
	}
}

func TestGetPullRequestClassifiesProviderErrors(t *testing.T) {
	for _, test := range []struct {
		status    int
		permanent bool
	}{
		{http.StatusBadRequest, true}, {http.StatusUnauthorized, true}, {http.StatusForbidden, true}, {http.StatusNotFound, true}, {http.StatusUnprocessableEntity, true},
		{http.StatusRequestTimeout, false}, {http.StatusConflict, false}, {http.StatusLocked, false}, {http.StatusTooEarly, false}, {http.StatusTooManyRequests, false},
		{http.StatusInternalServerError, false}, {http.StatusBadGateway, false},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(test.status) }))
			defer server.Close()
			_, err := newTestClient(t, server.URL, "").GetPullRequest(context.Background(), testOwner, testRepository, 42)
			if err == nil || IsPermanent(err) != test.permanent {
				t.Fatalf("status %d error/permanent = %v/%t; want error/%t", test.status, err, IsPermanent(err), test.permanent)
			}
		})
	}
}

func TestGetPullRequestHandlesProviderBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, body, want string
		status           int
		transport        error
		permanent        bool
		forbidden        []string
	}{
		{name: "transport failure", transport: errors.New("transport unavailable"), want: "transport unavailable"},
		{name: "oversized response", body: strings.Repeat("x", maxResponseBody+1), want: "response body exceeds"},
		{name: "malformed response", body: "not-json", want: "decode GitHub pull request response", permanent: true},
		{name: "incomplete response", body: `{}`, want: "incomplete pull request", permanent: true},
		{name: "credential redaction", status: http.StatusBadRequest, body: "github rejected " + testToken, want: "github rejected", permanent: true, forbidden: []string{testToken}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, "https://github.example", testToken)
			if test.transport != nil {
				client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, test.transport })}
			} else {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					if test.status != 0 {
						w.WriteHeader(test.status)
					}
					_, _ = io.WriteString(w, test.body)
				}))
				defer server.Close()
				client = newTestClient(t, server.URL, testToken)
			}
			_, err := client.GetPullRequest(context.Background(), testOwner, testRepository, 42)
			if err == nil || !strings.Contains(err.Error(), test.want) || IsPermanent(err) != test.permanent {
				t.Fatalf("GetPullRequest error/permanent = %v/%t; want %q/%t", err, IsPermanent(err), test.want, test.permanent)
			}
			for _, secret := range test.forbidden {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("GetPullRequest error leaks credential %q: %v", secret, err)
				}
			}
		})
	}
}

func TestGetPullRequestContextCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := newTestClient(t, server.URL, testToken).GetPullRequest(ctx, testOwner, testRepository, 42)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetPullRequest cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("GetPullRequest did not return after context cancellation")
	}
}

func TestGetPullRequestGitHubRateLimitsRemainRetryable(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    int
		headers   map[string]string
		body      string
		delay     time.Duration
		permanent bool
	}{
		{name: "retry after", status: http.StatusForbidden, headers: map[string]string{"Retry-After": "60"}, delay: time.Minute},
		{name: "429", status: http.StatusTooManyRequests},
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
			_, err := newTestClient(t, server.URL, "").GetPullRequest(context.Background(), testOwner, testRepository, 42)
			if err == nil || IsPermanent(err) != test.permanent || forge.RetryDelay(err) != test.delay {
				t.Fatalf("GetPullRequest error/permanent/delay = %v/%t/%v; want error/%t/%v", err, IsPermanent(err), forge.RetryDelay(err), test.permanent, test.delay)
			}
		})
	}
}

func TestHTTPErrorWithoutMessage(t *testing.T) {
	if got := (&HTTPError{Status: "500 Internal Server Error"}).Error(); got != "GitHub returned 500 Internal Server Error" {
		t.Fatalf("HTTPError.Error() = %q", got)
	}
}

func TestNewClientRequiresHTTPSExceptLoopback(t *testing.T) {
	for _, baseURL := range []string{"http://github.example", "ftp://github.example", "https://user:password@github.example", "https://github.example?token=secret", "https://github.example#fragment"} {
		if _, err := NewClient(baseURL, ""); err == nil {
			t.Errorf("NewClient(%q) accepted unsafe base URL", baseURL)
		}
	}
	for _, baseURL := range []string{"https://api.github.com", "http://localhost:8080", "http://127.0.0.1:8080", "http://[::1]:8080"} {
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
		t.Errorf("Accept = %q", got)
	}
	if got := r.Header.Get("X-Github-Api-Version"); got != "2022-11-28" {
		t.Errorf("X-GitHub-Api-Version = %q", got)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
