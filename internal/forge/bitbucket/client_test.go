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

	"github.com/simpleswe/simpleswe/internal/forge"
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

func TestFindPullRequestMatchesTargetRepositorySourceBaseAndTaskMarker(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Query().Get("state") != "OPEN" {
			t.Errorf("state query = %q, want OPEN", r.URL.Query().Get("state"))
		}
		io.WriteString(w, `{"values":[`+
			`{"id":40,"state":"OPEN","description":"Created by simpleswe task swe-123","source":{"branch":{"name":"other"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/40"}}},`+
			`{"id":41,"state":"OPEN","description":"Created by simpleswe task swe-123","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"release"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/41"}}},`+
			`{"id":42,"state":"OPEN","description":"Created by simpleswe task swe-123","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"other/fork"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/42"}}},`+
			`{"id":43,"state":"OPEN","description":"Created by simpleswe task other","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/43"}}},`+
			`{"id":44,"state":"OPEN","description":"Created by simpleswe task swe-123","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/44"}}}`+
			`]}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL, testUsername, testAppPassword)
	got, found, err := client.FindPullRequest(context.Background(), testWorkspace, testRepository, "feature/task", "main", "swe-123")
	if err != nil {
		t.Fatalf("FindPullRequest: %v", err)
	}
	if !found || got.ID != 44 || !strings.HasSuffix(got.HTMLURL, "/44") {
		t.Fatalf("FindPullRequest = %#v, %t; want pull request 44", got, found)
	}
}

func TestFindPullRequestIgnoresClosedAndMergedMatches(t *testing.T) {
	for _, state := range []string{"DECLINED", "MERGED"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			io.WriteString(w, `{"values":[{"id":44,"state":"`+state+`","description":"Created by simpleswe task swe-123","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/44"}}}]}`)
		}))
		got, found, err := newTestClient(t, server.URL, "", "").FindPullRequest(context.Background(), testWorkspace, testRepository, "feature/task", "main", "swe-123")
		server.Close()
		if err != nil || found || got != (forge.PullRequest{}) {
			t.Fatalf("FindPullRequest %s candidate = %#v, %t, %v; want no match", state, got, found, err)
		}
	}
}

func TestGetPullRequestParsesStateAndRefsFromTargetScopedEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != "/2.0/repositories/workspace/repository/pullrequests/42" || r.URL.RawQuery != "" {
			t.Fatalf("request = %s %s; want GET target pull request 42", r.Method, r.URL.RequestURI())
		}
		io.WriteString(w, `{"id":42,"state":"OPEN","source":{"branch":{"name":"feature/task"},"commit":{"hash":"0123456789abcdef0123456789abcdef01234567"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}}}`)
	}))
	defer server.Close()

	got, err := newTestClient(t, server.URL, "", "").GetPullRequest(context.Background(), testWorkspace, testRepository, 42)
	want := forge.PullRequestState{Number: 42, State: "open", SourceOwner: "workspace", SourceRepository: "repository", SourceBranch: "feature/task", DestinationBranch: "main", HeadSHA: "0123456789abcdef0123456789abcdef01234567"}
	if err != nil || got != want {
		t.Fatalf("GetPullRequest = %#v, %v; want %#v", got, err, want)
	}
}

func TestGetPullRequestRejectsMissingHeadSHA(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		io.WriteString(w, `{"id":42,"state":"OPEN","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}}}`)
	}))
	defer server.Close()

	if _, err := newTestClient(t, server.URL, "", "").GetPullRequest(context.Background(), testWorkspace, testRepository, 42); err == nil || !forge.IsPermanent(err) {
		t.Fatalf("GetPullRequest missing head SHA error = %v; want permanent", err)
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

func TestHTTPErrorWithoutMessage(t *testing.T) {
	err := (&HTTPError{Status: "500 Internal Server Error"}).Error()
	if err != "Bitbucket returned 500 Internal Server Error" {
		t.Fatalf("HTTPError.Error() = %q", err)
	}
}

func TestCreatePullRequestRejectsInvalidEndpointAndContext(t *testing.T) {
	client := newTestClient(t, "https://bitbucket.example", testUsername, testAppPassword)
	if _, err := client.CreatePullRequest(context.Background(), "", testRepository, CreatePullRequestRequest{}); err == nil {
		t.Fatal("CreatePullRequest accepted an empty workspace")
	}
	var nilContext context.Context
	if _, err := client.CreatePullRequest(nilContext, testWorkspace, testRepository, CreatePullRequestRequest{}); err == nil || !strings.Contains(err.Error(), "create Bitbucket request") {
		t.Fatalf("CreatePullRequest(nil context) error = %v; want request creation error", err)
	}
}

func TestCreatePullRequestHandlesTransportAndResponseErrors(t *testing.T) {
	tests := []struct {
		name   string
		client func(t *testing.T) *Client
		want   string
	}{
		{
			name: "transport",
			client: func(t *testing.T) *Client {
				client := newTestClient(t, "https://bitbucket.example", testUsername, testAppPassword)
				client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("network down")
				})}
				return client
			},
			want: "Bitbucket request failed",
		},
		{
			name: "read failure",
			client: func(t *testing.T) *Client {
				client := newTestClient(t, "https://bitbucket.example", testUsername, testAppPassword)
				client.httpClient = responseClient(&http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: failingReadCloser{}})
				return client
			},
			want: "read Bitbucket response",
		},
		{
			name: "oversized body",
			client: func(t *testing.T) *Client {
				client := newTestClient(t, "https://bitbucket.example", testUsername, testAppPassword)
				client.httpClient = responseClient(&http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(strings.Repeat("x", maxResponseBody+1)))})
				return client
			},
			want: "response body exceeds",
		},
		{
			name: "malformed JSON",
			client: func(t *testing.T) *Client {
				client := newTestClient(t, "https://bitbucket.example", testUsername, testAppPassword)
				client.httpClient = responseClient(&http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader("not-json"))})
				return client
			},
			want: "decode Bitbucket response",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.client(t).CreatePullRequest(context.Background(), testWorkspace, testRepository, CreatePullRequestRequest{})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("CreatePullRequest error = %v; want %q", err, test.want)
			}
		})
	}
}

func TestCreatePullRequestValidatesResponse(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want string
	}{
		{name: "missing ID", body: `{"links":{"html":{"href":"https://bitbucket.example/pull-requests/1"}}}`, want: "missing pull request id"},
		{name: "missing URL", body: `{"id":1}`, want: "missing pull request URL"},
		{name: "invalid URL", body: `{"id":1,"links":{"html":{"href":"/pull-requests/1"}}}`, want: "invalid pull request URL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, test.body) }))
			defer server.Close()
			_, err := newTestClient(t, server.URL, "", "").CreatePullRequest(context.Background(), testWorkspace, testRepository, CreatePullRequestRequest{})
			if err == nil || !strings.Contains(err.Error(), test.want) || !IsPermanent(err) {
				t.Fatalf("CreatePullRequest error = %v; want permanent %q", err, test.want)
			}
		})
	}
}

func TestFindPullRequestHandlesLookupErrorsAndNoMatch(t *testing.T) {
	tests := []struct {
		name string
		body string
		code int
		want string
	}{
		{name: "status", body: "denied", code: http.StatusForbidden, want: "denied"},
		{name: "malformed JSON", body: "not-json", code: http.StatusOK, want: "decode Bitbucket lookup response"},
		{name: "incomplete match", body: `{"values":[{"id":0,"state":"OPEN","description":"simpleswe task swe-123","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/1"}}}]}`, code: http.StatusOK, want: "incomplete pull request"},
		{name: "invalid URL", body: `{"values":[{"id":1,"state":"OPEN","description":"simpleswe task swe-123","source":{"branch":{"name":"feature/task"},"repository":{"full_name":"workspace/repository"}},"destination":{"branch":{"name":"main"}},"links":{"html":{"href":"/pull-requests/1"}}}]}`, code: http.StatusOK, want: "invalid pull request URL"},
		{name: "no match", body: `{"values":[{"id":1,"description":"other","source":{"branch":{"name":"other"}},"links":{"html":{"href":"https://bitbucket.example/pull-requests/1"}}}]}`, code: http.StatusOK},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("pagelen") != "50" || r.URL.Query().Get("q") != `source.branch.name="feature/task" AND destination.branch.name="main"` {
					t.Errorf("lookup query = %q; want pagelen=50 and exact branch filters", r.URL.RawQuery)
				}
				w.WriteHeader(test.code)
				io.WriteString(w, test.body)
			}))
			defer server.Close()
			_, found, err := newTestClient(t, server.URL, "", "").FindPullRequest(context.Background(), testWorkspace, testRepository, "feature/task", "main", "swe-123")
			if test.want == "" {
				if err != nil || found {
					t.Fatalf("FindPullRequest = found %t, error %v; want no match", found, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.want) || !IsPermanent(err) {
				t.Fatalf("FindPullRequest error = %v; want permanent %q", err, test.want)
			}
		})
	}
}

func TestFindPullRequestHandlesTransportAndReadErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		body io.ReadCloser
		want string
	}{
		{name: "transport", want: "Bitbucket lookup failed"},
		{name: "read", body: failingReadCloser{}, want: "read Bitbucket lookup response"},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestClient(t, "https://bitbucket.example", "", "")
			if test.body != nil {
				client.httpClient = responseClient(&http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: test.body})
			} else {
				client.httpClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("lookup unavailable")
				})}
			}
			_, _, err := client.FindPullRequest(context.Background(), testWorkspace, testRepository, "feature/task", "main", "swe-123")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("FindPullRequest error = %v; want %q", err, test.want)
			}
		})
	}
}

func TestEndpointAndRedactionHandleNilAndEscapedValues(t *testing.T) {
	var nilClient *Client
	if got := nilClient.redact("message"); got != "message" {
		t.Fatalf("nil Client.redact() = %q; want message", got)
	}
	client := newTestClient(t, "https://bitbucket.example/api/", "user", "pass")
	endpoint, err := client.endpoint("team name", "repo/name")
	if err != nil {
		t.Fatalf("endpoint() error = %v", err)
	}
	if !strings.Contains(endpoint, "/api/2.0/repositories/team%20name/repo%2Fname/pullrequests") {
		t.Fatalf("endpoint() = %q; want escaped path", endpoint)
	}
	if got := client.redact("user pass"); got != "[REDACTED] [REDACTED]" {
		t.Fatalf("redact() = %q", got)
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

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("read failed") }

func (failingReadCloser) Close() error { return nil }

func responseClient(response *http.Response) *http.Client {
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return response, nil })}
}
