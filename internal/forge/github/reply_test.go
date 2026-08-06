package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/simpleswe/simpleswe/internal/forge"
)

func TestReplyToPullRequest(t *testing.T) {
	tests := []struct {
		name    string
		request forge.ReplyRequest
		path    string
	}{
		{
			name: "review comment reply",
			request: forge.ReplyRequest{
				PullRequestNumber: 42, CommentID: 202, CommentKind: "review_comment", Body: "Fixed in the latest commit.",
			},
			path: "/repos/acme/service/pulls/42/comments/202/replies",
		},
		{
			name: "general issue reply",
			request: forge.ReplyRequest{
				PullRequestNumber: 42, CommentID: 101, CommentKind: "issue_comment", Body: "The quality gate is fixed.",
			},
			path: "/repos/acme/service/issues/42/comments",
		},
		{
			name: "review summary reply",
			request: forge.ReplyRequest{
				PullRequestNumber: 42, CommentID: 303, CommentKind: "review", Body: "The review summary is addressed.\n\nAll checks pass.",
			},
			path: "/repos/acme/service/issues/42/comments",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method = %s, want POST", r.Method)
				}
				if r.URL.EscapedPath() != test.path {
					t.Errorf("path = %q, want %q", r.URL.EscapedPath(), test.path)
				}
				assertGitHubHeaders(t, r, testToken)
				if got := r.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", got)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request body: %v", err)
					return
				}
				want := map[string]any{"body": test.request.Body}
				if !reflect.DeepEqual(body, want) {
					t.Errorf("body = %#v, want %#v", body, want)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			if err := newTestClient(t, server.URL, testToken).ReplyToPullRequest(context.Background(), testOwner, testRepository, test.request); err != nil {
				t.Fatalf("ReplyToPullRequest: %v", err)
			}
		})
	}
}

func TestReplyLocalValidationFailuresArePermanent(t *testing.T) {
	client := newTestClient(t, "http://127.0.0.1:1", testToken)
	request := forge.ReplyRequest{PullRequestNumber: 42, CommentID: 1, CommentKind: "bitbucket-comment", Body: "fix"}
	if err := client.ReplyToPullRequest(t.Context(), testOwner, testRepository, request); !forge.IsPermanent(err) {
		t.Fatalf("ReplyToPullRequest local error = %v, want permanent", err)
	}
	if _, err := client.PullRequestReplyExists(t.Context(), testOwner, testRepository, request, "marker"); !forge.IsPermanent(err) {
		t.Fatalf("PullRequestReplyExists local error = %v, want permanent", err)
	}
}

func TestPullRequestReplyExistsUsesConfiguredPaginatedCommentChannel(t *testing.T) {
	marker := "<!-- simpleswe:abc123 -->"
	for _, test := range []struct {
		name, kind, path string
	}{
		{name: "review reply", kind: "review_comment", path: "/repos/acme/service/pulls/42/comments"},
		{name: "review summary is an issue comment", kind: "review", path: "/repos/acme/service/issues/42/comments"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var pages []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				page := r.URL.Query().Get("page")
				pages = append(pages, page)
				if r.Method != http.MethodGet || r.URL.Path != test.path || r.URL.Query().Get("per_page") != "100" || r.URL.Query().Has("sort") || r.URL.Query().Has("direction") {
					t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
				}
				assertGitHubHeaders(t, r, testToken)
				if page == "1" {
					w.Header().Set("Link", `<https://attacker.example/steal?page=3>; rel="last", <https://attacker.example/steal?page=2>; rel="next"`)
					_ = json.NewEncoder(w).Encode([]map[string]string{{"body": "unrelated"}})
					return
				}
				_ = json.NewEncoder(w).Encode([]map[string]string{{"body": "done " + marker}})
			}))
			defer server.Close()
			found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(context.Background(), testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42, CommentID: 202, CommentKind: test.kind}, marker)
			if err != nil || !found || !reflect.DeepEqual(pages, []string{"1", "3"}) {
				t.Fatalf("PullRequestReplyExists = %t, %v for pages %v", found, err, pages)
			}
		})
	}
}

func TestPullRequestReplyExistsInspectsPageOneBeforeNewestPage(t *testing.T) {
	marker := "<!-- simpleswe:on-page-one -->"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.Header().Set("Link", `<https://attacker.example/comments?page=9>; rel="last"`)
		_ = json.NewEncoder(w).Encode([]map[string]string{{"body": marker}})
	}))
	defer server.Close()
	found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(t.Context(), testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
	if err != nil || !found || requests != 1 {
		t.Fatalf("PullRequestReplyExists page-one marker = %t, %v after %d requests", found, err, requests)
	}
}

func TestPullRequestReplyExistsReadsValidProviderMaxPageLargerThanOrdinaryResponseLimit(t *testing.T) {
	marker := "<!-- simpleswe:large-page -->"
	comments := make([]map[string]string, 100)
	for i := range comments {
		comments[i] = map[string]string{"body": strings.Repeat("x", 12<<10)}
	}
	comments[len(comments)-1]["body"] += marker
	payload, err := json.Marshal(comments)
	if err != nil || len(payload) <= maxResponseBody {
		t.Fatalf("large valid GitHub comment page bytes = %d, %v", len(payload), err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(t.Context(), testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
	if err != nil || !found {
		t.Fatalf("PullRequestReplyExists large valid page = %t, %v", found, err)
	}
}

func TestPullRequestReplyExistsHandlesNotFoundMalformedAndHTTPFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		status  int
		body    string
		wantErr bool
	}{
		{name: "not found", status: http.StatusOK, body: `[{"body":"other"}]`},
		{name: "malformed", status: http.StatusOK, body: `{`, wantErr: true},
		{name: "HTTP failure", status: http.StatusBadGateway, body: testToken, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(context.Background(), testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, "<!-- simpleswe:missing -->")
			if found || (err != nil) != test.wantErr {
				t.Fatalf("PullRequestReplyExists = %t, %v", found, err)
			}
			if err != nil && strings.Contains(err.Error(), testToken) {
				t.Fatalf("error leaked token: %v", err)
			}
		})
	}
}

func TestPullRequestReplyExistsScansOnlyBoundedNewestPagesAndHonorsCancellation(t *testing.T) {
	marker := "<!-- simpleswe:older-than-window -->"
	var pages []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page, err := strconv.Atoi(r.URL.Query().Get("page"))
		if err != nil {
			t.Errorf("page = %q", r.URL.Query().Get("page"))
		}
		pages = append(pages, page)
		if page == 1 {
			w.Header().Set("Link", `<https://attacker.example/steal?page=20>; rel="last", <https://attacker.example/steal?page=2>; rel="next"`)
		}
		body := "unrelated"
		if page == 10 {
			body = marker
		}
		_ = json.NewEncoder(w).Encode([]map[string]string{{"body": body}})
	}))
	defer server.Close()
	found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(t.Context(), testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
	wantPages := []int{1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11}
	if err != nil || found || !reflect.DeepEqual(pages, wantPages) {
		t.Fatalf("PullRequestReplyExists = %t, %v for pages %v; want bounded %v", found, err, pages, wantPages)
	}

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	pages = nil
	if found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(cancelled, testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker); found || !errors.Is(err, context.Canceled) || len(pages) != 0 {
		t.Fatalf("cancelled PullRequestReplyExists = %t, %v for pages %v", found, err, pages)
	}
}

func TestPullRequestReplyExistsRejectsMalformedOrInconsistentLastPageMetadata(t *testing.T) {
	for _, link := range []string{
		`<https://api.github.example/comments?page=not-a-number>; rel="last"`,
		`<https://api.github.example/comments>; rel="last"`,
		`<https://api.github.example/comments?page=2>; rel="last", <https://api.github.example/comments?page=3>; rel="last"`,
		`<https://api.github.example/comments?page=2>; rel="next"`,
		`<https://api.github.example/comments?page=1>; rel="last", <https://api.github.example/comments?page=2>; rel="next"`,
		`not-a-link`,
	} {
		t.Run(link, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				w.Header().Set("Link", link)
				_, _ = w.Write([]byte(`[]`))
			}))
			defer server.Close()
			found, err := newTestClient(t, server.URL, testToken).PullRequestReplyExists(t.Context(), testOwner, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, "missing")
			if found || err == nil || requests != 1 {
				t.Fatalf("PullRequestReplyExists malformed Link = %t, %v after %d requests", found, err, requests)
			}
		})
	}
}
