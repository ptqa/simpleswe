package bitbucket

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
	testWorkspace   = "workspace"
	testRepository  = "repository"
	testUsername    = "bot@example.com"
	testAppPassword = "app-password"
)

func TestGetPullRequestParsesTargetScopedProviderState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/2.0/repositories/workspace/repository/pullrequests/42" || r.URL.RawQuery != "" {
			t.Fatalf("request = %s %s; want target pull request 42", r.Method, r.URL.RequestURI())
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != testUsername || password != testAppPassword {
			t.Fatalf("BasicAuth = %q/%q/%t", username, password, ok)
		}
		_, _ = io.WriteString(w, `{"id":42,"state":"OPEN","title":"Fix flaky validation","links":{"html":{"href":"https://bitbucket.example/workspace/repository/pull-requests/42"}},"source":{"branch":{"name":"feature/task"},"commit":{"hash":"0123456789abcdef0123456789abcdef01234567"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}}}`)
	}))
	defer server.Close()

	got, err := newTestClient(t, server.URL, testUsername, testAppPassword).GetPullRequest(context.Background(), testWorkspace, testRepository, 42)
	want := forge.PullRequestState{Number: 42, State: "open", HTMLURL: "https://bitbucket.example/workspace/repository/pull-requests/42", Title: "Fix flaky validation", SourceOwner: "workspace", SourceRepository: "repository", SourceBranch: "feature/task", DestinationBranch: "main", HeadSHA: "0123456789abcdef0123456789abcdef01234567"}
	if err != nil || got != want {
		t.Fatalf("GetPullRequest = %#v, %v; want %#v", got, err, want)
	}
}

func TestGetPullRequestRejectsInvalidProviderMetadata(t *testing.T) {
	valid := `{"id":42,"state":"OPEN","title":"Fix it","links":{"html":{"href":"%s"}},"source":{"branch":{"name":"feature/task"},"commit":{"hash":"0123456789abcdef0123456789abcdef01234567"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}}}`
	for name, body := range map[string]string{
		"missing head SHA": strings.Replace(valid, `"commit":{"hash":"0123456789abcdef0123456789abcdef01234567"},`, "", 1),
		"missing title":    strings.Replace(valid, `"title":"Fix it",`, "", 1),
		"insecure URL":     strings.Replace(valid, "%s", "http://bitbucket.example/workspace/repository/pull-requests/42", 1),
		"credentials":      strings.Replace(valid, "%s", "https://user@bitbucket.example/workspace/repository/pull-requests/42", 1),
		"query":            strings.Replace(valid, "%s", "https://bitbucket.example/workspace/repository/pull-requests/42?token=x", 1),
		"fragment":         strings.Replace(valid, "%s", "https://bitbucket.example/workspace/repository/pull-requests/42#details", 1),
	} {
		t.Run(name, func(t *testing.T) {
			body = strings.Replace(body, "%s", "https://bitbucket.example/workspace/repository/pull-requests/42", 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
			defer server.Close()
			if _, err := newTestClient(t, server.URL, "", "").GetPullRequest(context.Background(), testWorkspace, testRepository, 42); err == nil || !forge.IsPermanent(err) {
				t.Fatalf("GetPullRequest error = %v; want permanent", err)
			}
		})
	}
}

func TestGetPullRequestClassifiesProviderErrors(t *testing.T) {
	for _, test := range []struct {
		status    int
		permanent bool
	}{{http.StatusBadRequest, true}, {http.StatusUnauthorized, true}, {http.StatusUnprocessableEntity, true}, {http.StatusRequestTimeout, false}, {http.StatusConflict, false}, {http.StatusTooManyRequests, false}, {http.StatusInternalServerError, false}} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(test.status) }))
			defer server.Close()
			_, err := newTestClient(t, server.URL, "", "").GetPullRequest(context.Background(), testWorkspace, testRepository, 42)
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
		{name: "malformed response", body: "not-json", want: "decode Bitbucket pull request response", permanent: true},
		{name: "incomplete response", body: `{}`, want: "incomplete pull request", permanent: true},
		{name: "credential redaction", status: http.StatusBadRequest, body: testUsername + " rejected " + testAppPassword, want: "rejected", permanent: true, forbidden: []string{testUsername, testAppPassword}},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, "https://bitbucket.example", testUsername, testAppPassword)
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
				client = newTestClient(t, server.URL, testUsername, testAppPassword)
			}
			_, err := client.GetPullRequest(context.Background(), testWorkspace, testRepository, 42)
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
		_, err := newTestClient(t, server.URL, testUsername, testAppPassword).GetPullRequest(ctx, testWorkspace, testRepository, 42)
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("GetPullRequest cancellation error = %v, want context.Canceled", err)
		}
		if strings.Contains(err.Error(), testUsername) || strings.Contains(err.Error(), testAppPassword) {
			t.Fatalf("GetPullRequest cancellation error leaks credentials: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("GetPullRequest did not return after context cancellation")
	}
}

func TestGetPullRequestBitbucketRateLimitsHonorRetryAfter(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		header string
		delay  time.Duration
	}{
		{name: "retry after", status: http.StatusForbidden, header: "60", delay: time.Minute},
		{name: "rate limit", status: http.StatusTooManyRequests},
		{name: "rate limit retry after", status: http.StatusTooManyRequests, header: "120", delay: 2 * time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", test.header)
				w.WriteHeader(test.status)
			}))
			defer server.Close()
			_, err := newTestClient(t, server.URL, "", "").GetPullRequest(context.Background(), testWorkspace, testRepository, 42)
			if err == nil || IsPermanent(err) || forge.RetryDelay(err) != test.delay {
				t.Fatalf("GetPullRequest error/permanent/delay = %v/%t/%v; want retryable/%v", err, IsPermanent(err), forge.RetryDelay(err), test.delay)
			}
		})
	}
}

func TestEndpointAndRedactionHandleNilAndEscapedValues(t *testing.T) {
	var nilClient *Client
	if got := nilClient.redact("message"); got != "message" {
		t.Fatalf("nil Client.redact() = %q", got)
	}
	client := newTestClient(t, "https://bitbucket.example/api/", "user", "pass")
	endpoint, err := client.endpoint("team name", "repo/name")
	if err != nil || !strings.Contains(endpoint, "/api/2.0/repositories/team%20name/repo%2Fname/pullrequests") {
		t.Fatalf("endpoint() = %q, %v", endpoint, err)
	}
	if got := client.redact("user pass"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redact() = %q", got)
	}
}

func TestNewClientRequiresHTTPSExceptLoopback(t *testing.T) {
	for _, baseURL := range []string{"http://bitbucket.example", "ftp://bitbucket.example", "https://user:password@bitbucket.example", "https://bitbucket.example?password=secret", "https://bitbucket.example#fragment"} {
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
