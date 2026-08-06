package bitbucket

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
		body    map[string]any
	}{
		{
			name: "comment reply",
			request: forge.ReplyRequest{
				PullRequestNumber: 42, CommentID: 501, CommentKind: "comment", Body: "Fixed in the latest commit.",
			},
			body: map[string]any{
				"content": map[string]any{"raw": "Fixed in the latest commit."},
				"parent":  map[string]any{"id": float64(501)},
			},
		},
		{
			name: "general reply",
			request: forge.ReplyRequest{
				PullRequestNumber: 42, Body: "The quality gate is fixed.",
			},
			body: map[string]any{
				"content": map[string]any{"raw": "The quality gate is fixed."},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost {
					t.Errorf("method = %s, want POST", r.Method)
				}
				if r.URL.Path != "/2.0/repositories/workspace/repository/pullrequests/42/comments" {
					t.Errorf("path = %q, want /2.0/repositories/workspace/repository/pullrequests/42/comments", r.URL.Path)
				}
				username, password, ok := r.BasicAuth()
				if !ok || username != testUsername || password != testAppPassword {
					t.Errorf("BasicAuth() = (%q, %q, %t), want (%q, %q, true)", username, password, ok, testUsername, testAppPassword)
				}
				if got := r.Header.Get("Content-Type"); got != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", got)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode request body: %v", err)
					return
				}
				if !reflect.DeepEqual(body, test.body) {
					t.Errorf("body = %#v, want %#v", body, test.body)
				}
				w.WriteHeader(http.StatusNoContent)
			}))
			defer server.Close()

			if err := newTestClient(t, server.URL, testUsername, testAppPassword).ReplyToPullRequest(context.Background(), testWorkspace, testRepository, test.request); err != nil {
				t.Fatalf("ReplyToPullRequest: %v", err)
			}
		})
	}
}

func TestReplyLocalValidationFailuresArePermanent(t *testing.T) {
	client := newTestClient(t, "http://127.0.0.1:1", testUsername, testAppPassword)
	request := forge.ReplyRequest{PullRequestNumber: 42, CommentID: 1, CommentKind: "issue_comment", Body: "fix"}
	if err := client.ReplyToPullRequest(t.Context(), testWorkspace, testRepository, request); !forge.IsPermanent(err) {
		t.Fatalf("ReplyToPullRequest local error = %v, want permanent", err)
	}
	if _, err := client.PullRequestReplyExists(t.Context(), testWorkspace, testRepository, forge.ReplyRequest{}, "marker"); !forge.IsPermanent(err) {
		t.Fatalf("PullRequestReplyExists local error = %v, want permanent", err)
	}
}

func TestPullRequestReplyExistsSearchesChildrenWithBoundedPageNumbers(t *testing.T) {
	marker := "<!-- simpleswe:abc123 -->"
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/2.0/repositories/workspace/repository/pullrequests/42/comments" || r.URL.Query().Get("pagelen") != "100" {
			t.Errorf("request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		username, password, ok := r.BasicAuth()
		if !ok || username != testUsername || password != testAppPassword {
			t.Errorf("BasicAuth() = (%q, %q, %t)", username, password, ok)
		}
		if requests == 1 {
			_, _ = w.Write([]byte(`{"page":1,"pagelen":100,"size":200,"values":[{"content":{"raw":"other"}}],"next":"https://attacker.example/steal?page=999"}`))
			return
		}
		_, _ = w.Write([]byte(`{"page":2,"pagelen":100,"size":200,"values":[{"content":{"raw":"parent"},"children":[{"content":{"raw":"done ` + marker + `"}}]}]}`))
	}))
	defer server.Close()
	found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(context.Background(), testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
	if err != nil || !found || requests != 2 {
		t.Fatalf("PullRequestReplyExists = %t, %v after %d requests", found, err, requests)
	}
}

func TestPullRequestReplyExistsUsesPageMetadataToSearchNewestPageFirst(t *testing.T) {
	marker := "<!-- simpleswe:newest-page -->"
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		pages = append(pages, page)
		if page == "1" {
			_, _ = w.Write([]byte(`{"page":1,"pagelen":100,"size":300,"values":[],"next":"https://attacker.example/steal"}`))
			return
		}
		_, _ = w.Write([]byte(`{"page":3,"pagelen":100,"size":300,"values":[{"content":{"raw":"` + marker + `"}}]}`))
	}))
	defer server.Close()

	found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(t.Context(), testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
	if err != nil || !found || !reflect.DeepEqual(pages, []string{"1", "3"}) {
		t.Fatalf("PullRequestReplyExists newest pages = %t, %v, pages %v", found, err, pages)
	}
}

func TestPullRequestReplyExistsReadsValidProviderMaxPageLargerThanOrdinaryResponseLimit(t *testing.T) {
	marker := "<!-- simpleswe:large-page -->"
	comments := make([]bitbucketReplyComment, 100)
	for i := range comments {
		comments[i].Content.Raw = strings.Repeat("x", 12<<10)
	}
	comments[len(comments)-1].Content.Raw += marker
	payload, err := json.Marshal(bitbucketReplyList{Values: comments})
	if err != nil || len(payload) <= maxResponseBody {
		t.Fatalf("large valid Bitbucket comment page bytes = %d, %v", len(payload), err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(payload)
	}))
	defer server.Close()

	found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(t.Context(), testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
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
		{name: "not found", status: http.StatusOK, body: `{"values":[{"content":{"raw":"other"}}]}`},
		{name: "malformed", status: http.StatusOK, body: `{`, wantErr: true},
		{name: "HTTP failure", status: http.StatusBadGateway, body: testAppPassword, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(context.Background(), testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, "<!-- simpleswe:missing -->")
			if found || (err != nil) != test.wantErr {
				t.Fatalf("PullRequestReplyExists = %t, %v", found, err)
			}
			if err != nil && strings.Contains(err.Error(), testAppPassword) {
				t.Fatalf("error leaked password: %v", err)
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
		body, next := "unrelated", ""
		if page == 1 || page < 20 {
			next = `,"next":"https://attacker.example/steal?page=999"`
		}
		if page == 10 {
			body = marker
		}
		_, _ = w.Write([]byte(`{"page":` + strconv.Itoa(page) + `,"pagelen":100,"size":2000,"values":[{"content":{"raw":"` + body + `"}}]` + next + `}`))
	}))
	defer server.Close()
	found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(t.Context(), testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker)
	wantPages := []int{1, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11}
	if err != nil || found || !reflect.DeepEqual(pages, wantPages) {
		t.Fatalf("PullRequestReplyExists = %t, %v for pages %v; want bounded %v", found, err, pages, wantPages)
	}

	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	pages = nil
	if found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(cancelled, testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, marker); found || !errors.Is(err, context.Canceled) || len(pages) != 0 {
		t.Fatalf("cancelled PullRequestReplyExists = %t, %v for pages %v", found, err, pages)
	}
}

func TestPullRequestReplyExistsRejectsInvalidOrInconsistentPaginationMetadata(t *testing.T) {
	for name, body := range map[string]string{
		"next without metadata": `{"values":[],"next":"https://attacker.example/forever"}`,
		"invalid page":          `{"page":0,"pagelen":100,"size":200,"values":[],"next":"https://attacker.example/forever"}`,
		"invalid pagelen":       `{"page":1,"pagelen":0,"size":200,"values":[],"next":"https://attacker.example/forever"}`,
		"invalid size":          `{"page":1,"pagelen":100,"size":-1,"values":[],"next":"https://attacker.example/forever"}`,
		"page mismatch":         `{"page":2,"pagelen":100,"size":200,"values":[],"next":"https://attacker.example/forever"}`,
		"next past last page":   `{"page":1,"pagelen":100,"size":1,"values":[],"next":"https://attacker.example/forever"}`,
		"missing expected next": `{"page":1,"pagelen":100,"size":200,"values":[]}`,
	} {
		t.Run(name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests++
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			found, err := newTestClient(t, server.URL, testUsername, testAppPassword).PullRequestReplyExists(t.Context(), testWorkspace, testRepository, forge.ReplyRequest{PullRequestNumber: 42}, "missing")
			if found || err == nil || requests != 1 {
				t.Fatalf("PullRequestReplyExists invalid metadata = %t, %v after %d requests", found, err, requests)
			}
		})
	}
}
