package run

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/store"
)

const (
	githubWebhookSecret      = "github-webhook-test-secret"
	bitbucketWebhookSecret   = "bitbucket-webhook-test-secret"
	webhookReviewQuietPeriod = 30 * time.Minute
)

func TestWebhookHTTPReception(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", githubWebhookSecret)
	t.Setenv("BITBUCKET_WEBHOOK_SECRET", bitbucketWebhookSecret)
	db, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "webhooks.sqlite"))
	if err != nil {
		t.Fatalf("open webhook store: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	handler, err := newWebhookHandler(config.Config{
		GitHub:    config.GitHubConfig{WebhookSecret: config.SecretSource{Env: "GITHUB_WEBHOOK_SECRET"}},
		Bitbucket: config.BitbucketConfig{WebhookSecret: config.SecretSource{Env: "BITBUCKET_WEBHOOK_SECRET"}},
		Repositories: config.RepositoryConfigs{
			{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "service"}},
			{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "service"}},
		},
	}, db)
	if err != nil {
		t.Fatalf("newWebhookHandler: %v", err)
	}

	tests := []struct {
		name         string
		provider     string
		path         string
		secret       string
		delivery     string
		event        string
		body         []byte
		method       string
		signature    string
		omitDelivery bool
		omitEvent    bool
		wantStatus   int
		wantEvent    bool
	}{
		{
			name:     "signed actionable GitHub",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-delivery-1", event: "issue_comment", body: []byte(githubActionablePayload),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusAccepted, wantEvent: true,
		},
		{
			name:     "signed actionable Bitbucket",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-delivery-1", event: "pullrequest:comment_created", body: []byte(bitbucketActionablePayload),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusAccepted, wantEvent: true,
		},
		{
			name:     "signed actionable Bitbucket changes request",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-changes-request-1", event: "pullrequest:changes_request_created", body: []byte(bitbucketChangesRequestPayload),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusAccepted, wantEvent: true,
		},
		{
			name:     "invalid signature is checked before JSON",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-invalid-signature", event: "issue_comment", body: []byte(`{"action":`),
			method: http.MethodPost, signature: "invalid", wantStatus: http.StatusUnauthorized,
		},
		{
			name:     "missing GitHub delivery header",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-missing-delivery", event: "pull_request_review", body: []byte(`{}`),
			method: http.MethodPost, signature: "valid", omitDelivery: true, wantStatus: http.StatusBadRequest,
		},
		{
			name:     "missing GitHub event header",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-missing-event", event: "issue_comment", body: []byte(`{}`),
			method: http.MethodPost, signature: "valid", omitEvent: true, wantStatus: http.StatusBadRequest,
		},
		{
			name:     "missing Bitbucket event header",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-missing-event", event: "pullrequest:comment_created", body: []byte(`{}`),
			method: http.MethodPost, signature: "valid", omitEvent: true, wantStatus: http.StatusBadRequest,
		},
		{
			name:     "missing Bitbucket delivery header",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-missing-delivery", event: "pullrequest:comment_created", body: []byte(`{}`),
			method: http.MethodPost, signature: "valid", omitDelivery: true, wantStatus: http.StatusBadRequest,
		},
		{
			name:     "missing signature",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-missing-signature", event: "ping", body: []byte(`{"zen":"Keep it logically awesome."}`),
			method: http.MethodPost, signature: "missing", wantStatus: http.StatusUnauthorized,
		},
		{
			name:     "missing Bitbucket signature",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-missing-signature", event: "repo:push", body: []byte(`{}`),
			method: http.MethodPost, signature: "missing", wantStatus: http.StatusUnauthorized,
		},
		{
			name:     "malformed JSON",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-malformed-json", event: "pullrequest:comment_created", body: []byte(`{"comment":`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusBadRequest,
		},
		{
			name:     "ignored valid event",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-ping", event: "ping", body: []byte(`{"zen":"Keep it logically awesome."}`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusAccepted,
		},
		{
			name:     "unsupported event is acknowledged",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-unsupported", event: "repo:future_event", body: []byte(`{}`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusAccepted,
		},
		{
			name:     "malformed unsupported event is rejected",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-malformed-unsupported", event: "future_event", body: []byte(`{"future":`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusBadRequest,
		},
		{
			name:     "array unsupported event is rejected",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-array-unsupported", event: "future_event", body: []byte(`[]`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusBadRequest,
		},
		{
			name:     "scalar unsupported event is rejected",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-scalar-unsupported", event: "repo:future_event", body: []byte(`true`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusBadRequest,
		},
		{
			name:     "null unsupported event is rejected",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-null-unsupported", event: "repo:future_event", body: []byte(`null`),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusBadRequest,
		},
		{
			name:     "body limit",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-too-large", event: "push", body: bytes.Repeat([]byte{'x'}, 1<<20+1),
			method: http.MethodPost, signature: "valid", wantStatus: http.StatusRequestEntityTooLarge,
		},
		{
			name:     "GET method",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-get", event: "ping", body: []byte(`{}`),
			method: http.MethodGet, wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:     "PUT method",
			provider: "bitbucket", path: "/v1/webhooks/bitbucket", secret: bitbucketWebhookSecret,
			delivery: "bitbucket-put", event: "repo:push", body: []byte(`{}`),
			method: http.MethodPut, wantStatus: http.StatusMethodNotAllowed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := newWebhookRequest(tt.method, tt.path, tt.provider, tt.secret, tt.delivery, tt.event, tt.body, tt.signature, tt.omitDelivery, tt.omitEvent)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, tt.wantStatus, recorder.Body.String())
			}
			if tt.wantStatus >= http.StatusBadRequest && !strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") {
				t.Fatalf("Content-Type = %q, want application/json", recorder.Header().Get("Content-Type"))
			}
			if tt.signature == "invalid" {
				if _, err := db.GetForgeEvent(t.Context(), tt.provider+":"+tt.delivery); !errors.Is(err, store.ErrNotFound) {
					t.Fatalf("invalid signature persisted event: %v", err)
				}
			}
			if tt.wantEvent {
				stored, err := db.GetForgeEvent(t.Context(), tt.provider+":"+tt.delivery)
				if err != nil {
					t.Fatalf("get accepted event: %v", err)
				}
				if stored.Body == "" {
					t.Fatal("accepted event has empty normalized body")
				}
			}
		})
	}
}

func TestWebhookReturnsInternalErrorWhenStoreFails(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", githubWebhookSecret)
	db, err := store.Open(t.Context(), filepath.Join(t.TempDir(), "webhooks.sqlite"))
	if err != nil {
		t.Fatalf("open webhook store: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close webhook store: %v", err)
	}
	handler, err := newWebhookHandler(config.Config{
		GitHub:       config.GitHubConfig{WebhookSecret: config.SecretSource{Env: "GITHUB_WEBHOOK_SECRET"}},
		Repositories: config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "service"}}},
	}, db)
	if err != nil {
		t.Fatalf("newWebhookHandler: %v", err)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newWebhookRequest(http.MethodPost, "/v1/webhooks/github", "github", githubWebhookSecret, "github-store-error", "issue_comment", []byte(githubActionablePayload), "valid", false, false))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusInternalServerError, recorder.Body.String())
	}
}

func TestWebhookReviewQuietPeriod(t *testing.T) {
	t.Setenv("BITBUCKET_WEBHOOK_SECRET", bitbucketWebhookSecret)
	db := openRunTestStore(t)
	handler, err := newWebhookHandler(config.Config{
		Bitbucket:    config.BitbucketConfig{WebhookSecret: config.SecretSource{Env: "BITBUCKET_WEBHOOK_SECRET"}},
		Repositories: config.RepositoryConfigs{{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "service"}}},
	}, db)
	if err != nil {
		t.Fatalf("newWebhookHandler: %v", err)
	}

	firstBefore := time.Now().UTC()
	firstRecorder := httptest.NewRecorder()
	handler.ServeHTTP(firstRecorder, newWebhookRequest(http.MethodPost, "/v1/webhooks/bitbucket", "bitbucket", bitbucketWebhookSecret, "review-first", "pullrequest:comment_created", []byte(bitbucketActionablePayload), "valid", false, false))
	firstAfter := time.Now().UTC()
	if firstRecorder.Code != http.StatusAccepted {
		t.Fatalf("first review status = %d, want %d: %s", firstRecorder.Code, http.StatusAccepted, firstRecorder.Body.String())
	}
	first, err := db.GetForgeEvent(t.Context(), "bitbucket:review-first")
	if err != nil {
		t.Fatalf("get first review event: %v", err)
	}
	assertWebhookReviewDeadline(t, first, firstBefore, firstAfter)
	due, err := db.ListIncompleteForgeEvents(t.Context())
	if err != nil {
		t.Fatalf("list events during first review quiet period: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("events due during first review quiet period = %#v, want none", due)
	}
}

func TestWebhookKeepsGitHubIssueCommentQuietPeriodsIndependent(t *testing.T) {
	t.Setenv("GITHUB_WEBHOOK_SECRET", githubWebhookSecret)
	db := openRunTestStore(t)
	handler, err := newWebhookHandler(config.Config{
		GitHub:       config.GitHubConfig{WebhookSecret: config.SecretSource{Env: "GITHUB_WEBHOOK_SECRET"}},
		Repositories: config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "service"}}},
	}, db)
	if err != nil {
		t.Fatal(err)
	}

	firstRecorder := httptest.NewRecorder()
	handler.ServeHTTP(firstRecorder, newWebhookRequest(http.MethodPost, "/v1/webhooks/github", "github", githubWebhookSecret, "issue-first", "issue_comment", []byte(githubActionablePayload), "valid", false, false))
	if firstRecorder.Code != http.StatusAccepted {
		t.Fatalf("first issue comment status = %d: %s", firstRecorder.Code, firstRecorder.Body.String())
	}
	first, err := db.GetForgeEvent(t.Context(), "github:issue-first")
	if err != nil || first.CommitSHA != "" || first.NextAttemptAt == nil {
		t.Fatalf("first issue comment = %#v, %v; want delayed SHA-less event", first, err)
	}
	firstDeadline := *first.NextAttemptAt
	time.Sleep(time.Millisecond)

	secondPayload := strings.ReplaceAll(githubActionablePayload, `"id":101`, `"id":102`)
	secondPayload = strings.ReplaceAll(secondPayload, "issuecomment-101", "issuecomment-102")
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, newWebhookRequest(http.MethodPost, "/v1/webhooks/github", "github", githubWebhookSecret, "issue-second", "issue_comment", []byte(secondPayload), "valid", false, false))
	if secondRecorder.Code != http.StatusAccepted {
		t.Fatalf("second issue comment status = %d: %s", secondRecorder.Code, secondRecorder.Body.String())
	}

	first, err = db.GetForgeEvent(t.Context(), first.ID)
	if err != nil || first.NextAttemptAt == nil || !first.NextAttemptAt.Equal(firstDeadline) {
		t.Fatalf("first issue deadline after sibling = %v, %v; want unchanged %v", first.NextAttemptAt, err, firstDeadline)
	}
	second, err := db.GetForgeEvent(t.Context(), "github:issue-second")
	if err != nil || second.CommitSHA != "" || second.NextAttemptAt == nil || !second.NextAttemptAt.After(firstDeadline) {
		t.Fatalf("second issue comment = %#v, %v; want its own later quiet deadline", second, err)
	}
}

func TestWebhookQualityGateRemainsImmediate(t *testing.T) {
	t.Setenv("BITBUCKET_WEBHOOK_SECRET", bitbucketWebhookSecret)
	db := openRunTestStore(t)
	handler, err := newWebhookHandler(config.Config{
		Bitbucket:    config.BitbucketConfig{WebhookSecret: config.SecretSource{Env: "BITBUCKET_WEBHOOK_SECRET"}},
		Repositories: config.RepositoryConfigs{{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "service"}}},
	}, db)
	if err != nil {
		t.Fatalf("newWebhookHandler: %v", err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, newWebhookRequest(http.MethodPost, "/v1/webhooks/bitbucket", "bitbucket", bitbucketWebhookSecret, "quality-first", "pipeline:build:completed", []byte(bitbucketQualityGatePayload), "valid", false, false))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("quality-gate status = %d, want %d: %s", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	stored, err := db.GetForgeEvent(t.Context(), "bitbucket:quality-first")
	if err != nil {
		t.Fatalf("get quality-gate event: %v", err)
	}
	if stored.Kind != "quality_gate_failed" || stored.NextAttemptAt != nil {
		t.Fatalf("quality-gate event = %#v, want immediate quality_gate_failed event", stored)
	}
	due, err := db.ListIncompleteForgeEvents(t.Context())
	if err != nil {
		t.Fatalf("list immediately due quality-gate events: %v", err)
	}
	if len(due) != 1 || due[0].ID != "bitbucket:quality-first" {
		t.Fatalf("immediately due quality-gate events = %#v, want quality event", due)
	}
}

func assertWebhookReviewDeadline(t *testing.T, event store.ForgeEvent, before, after time.Time) {
	t.Helper()
	if event.NextAttemptAt == nil {
		t.Fatalf("review event %q has no next_attempt_at", event.ID)
	}
	if event.NextAttemptAt.Before(before.Add(webhookReviewQuietPeriod-2*time.Minute)) || event.NextAttemptAt.After(after.Add(webhookReviewQuietPeriod+2*time.Minute)) {
		t.Fatalf("review event %q next_attempt_at = %s, want approximately 30 minutes after receipt", event.ID, event.NextAttemptAt)
	}
}

func TestWebhookHandlerReadsSecretsOnlyForConfiguredRepositories(t *testing.T) {
	db := openRunTestStore(t)
	githubSecret := filepath.Join(t.TempDir(), "github")
	bitbucketSecret := filepath.Join(t.TempDir(), "bitbucket")
	if err := os.WriteFile(githubSecret, []byte(githubWebhookSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bitbucketSecret, []byte(bitbucketWebhookSecret), 0o600); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "not-mounted")

	tests := []struct {
		name         string
		cfg          config.Config
		wantProvider string
	}{
		{
			name: "no repositories",
			cfg: config.Config{
				GitHub:    config.GitHubConfig{WebhookSecret: config.SecretSource{File: missing}},
				Bitbucket: config.BitbucketConfig{WebhookSecret: config.SecretSource{File: missing}},
			},
		},
		{
			name: "GitHub only",
			cfg: config.Config{
				GitHub:       config.GitHubConfig{WebhookSecret: config.SecretSource{File: githubSecret}},
				Bitbucket:    config.BitbucketConfig{WebhookSecret: config.SecretSource{File: missing}},
				Repositories: config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "widget"}}},
			},
			wantProvider: "github",
		},
		{
			name: "Bitbucket only",
			cfg: config.Config{
				GitHub:       config.GitHubConfig{WebhookSecret: config.SecretSource{File: missing}},
				Bitbucket:    config.BitbucketConfig{WebhookSecret: config.SecretSource{File: bitbucketSecret}},
				Repositories: config.RepositoryConfigs{{Bitbucket: config.RepositoryBitbucketConfig{Workspace: "acme", Repository: "widget"}}},
			},
			wantProvider: "bitbucket",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, err := newWebhookHandler(tt.cfg, db)
			if err != nil {
				t.Fatalf("newWebhookHandler() error = %v", err)
			}
			secrets := handler.(webhookHandler).secrets
			wantCount := 0
			if tt.wantProvider != "" {
				wantCount = 1
			}
			if len(secrets) != wantCount {
				t.Fatalf("loaded webhook secrets = %#v", secrets)
			}
			for provider := range secrets {
				if string(provider) != tt.wantProvider {
					t.Fatalf("loaded provider = %q, want %q", provider, tt.wantProvider)
				}
			}
		})
	}
}

func TestKnownUnconfiguredWebhookRouteReturnsJSONNotFound(t *testing.T) {
	handler, err := newWebhookHandler(config.Config{}, openRunTestStore(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/webhooks/github", "/v1/webhooks/bitbucket"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, nil))
		if recorder.Code != http.StatusNotFound || !strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") || !strings.Contains(recorder.Body.String(), `"code":"not_found"`) {
			t.Fatalf("%s response = %d %q %s; want JSON not_found", path, recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.String())
		}
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/arbitrary", nil))
	if recorder.Code != http.StatusNotFound || strings.HasPrefix(recorder.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("arbitrary response = %d %q; want normal mux-style 404", recorder.Code, recorder.Header().Get("Content-Type"))
	}
}

func TestWebhookSigningSecretsPreserveSignificantWhitespace(t *testing.T) {
	file := filepath.Join(t.TempDir(), "github-webhook-secret")
	if err := os.WriteFile(file, []byte("  file secret  \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXACT_WEBHOOK_SECRET", "  environment secret  ")
	for _, test := range []struct {
		name   string
		source config.SecretSource
		secret string
	}{
		{name: "file is exact", source: config.SecretSource{File: file}, secret: "  file secret  \n"},
		{name: "environment is exact", source: config.SecretSource{Env: "EXACT_WEBHOOK_SECRET"}, secret: "  environment secret  "},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := openRunTestStore(t)
			handler, err := newWebhookHandler(config.Config{
				GitHub:       config.GitHubConfig{WebhookSecret: test.source},
				Repositories: config.RepositoryConfigs{{GitHub: config.RepositoryGitHubConfig{Owner: "acme", Repository: "service"}}},
			}, db)
			if err != nil {
				t.Fatalf("newWebhookHandler: %v", err)
			}
			body := []byte(`{"zen":"exact bytes"}`)
			for name, secret := range map[string]string{"exact": test.secret, "trimmed": strings.TrimSpace(test.secret)} {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, newWebhookRequest(http.MethodPost, "/v1/webhooks/github", "github", secret, "whitespace-"+name, "ping", body, "valid", false, false))
				want := http.StatusUnauthorized
				if name == "exact" {
					want = http.StatusAccepted
				}
				if recorder.Code != want {
					t.Fatalf("%s signature status = %d, want %d", name, recorder.Code, want)
				}
			}
		})
	}
}

func TestWebhookSigningSecretRejectsOnlyEmptyValues(t *testing.T) {
	directory := t.TempDir()
	empty, whitespace := filepath.Join(directory, "empty"), filepath.Join(directory, "whitespace")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(whitespace, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readWebhookSecret(config.SecretSource{File: empty}, "test secret"); err == nil {
		t.Fatal("zero-byte webhook secret file was accepted")
	}
	if got, err := readWebhookSecret(config.SecretSource{File: whitespace}, "test secret"); err != nil || got != " \n" {
		t.Fatalf("whitespace file secret = %q, %v", got, err)
	}
	t.Setenv("WHITESPACE_WEBHOOK_SECRET", " \n")
	if got, err := readWebhookSecret(config.SecretSource{Env: "WHITESPACE_WEBHOOK_SECRET"}, "test secret"); err != nil || got != " \n" {
		t.Fatalf("whitespace environment secret = %q, %v", got, err)
	}
}

func newWebhookRequest(method, path, provider, secret, delivery, event string, body []byte, signature string, omitDelivery, omitEvent bool) *http.Request {
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	if provider == "github" {
		if !omitDelivery {
			request.Header.Set("X-GitHub-Delivery", delivery)
		}
		if !omitEvent {
			request.Header.Set("X-GitHub-Event", event)
		}
		if signature != "missing" {
			request.Header.Set("X-Hub-Signature-256", webhookTestSignature(secret, body, signature))
		}
		return request
	}
	if !omitDelivery {
		request.Header.Set("X-Request-UUID", delivery)
	}
	if !omitEvent {
		request.Header.Set("X-Event-Key", event)
	}
	if signature != "missing" {
		request.Header.Set("X-Hub-Signature", webhookTestSignature(secret, body, signature))
	}
	return request
}

func webhookTestSignature(secret string, body []byte, mode string) string {
	if mode == "invalid" {
		return "sha256=" + strings.Repeat("0", sha256.Size*2)
	}
	digest := hmac.New(sha256.New, []byte(secret))
	_, _ = digest.Write(body)
	return "sha256=" + hex.EncodeToString(digest.Sum(nil))
}

const githubActionablePayload = `{"action":"created","issue":{"number":42,"title":"Fix flaky test","html_url":"https://github.com/acme/service/pull/42","pull_request":{"url":"https://api.github.com/repos/acme/service/pulls/42"}},"comment":{"id":101,"body":"Please fix this","author_association":"MEMBER","user":{"login":"reviewer"},"html_url":"https://github.com/acme/service/issues/42#issuecomment-101"},"repository":{"name":"service","owner":{"login":"acme"}}}`

const bitbucketActionablePayload = `{"comment":{"id":501,"content":{"raw":"Please fix this"},"user":{"uuid":"reviewer-uuid","nickname":"reviewer","display_name":"reviewer"},"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42/_/diff#comment-501"}}},"pullrequest":{"id":42,"title":"Fix flaky test","reviewers":[{"uuid":"reviewer-uuid"}],"links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42"}},"source":{"branch":{"name":"feature/fix"},"commit":{"hash":"abc123"}}},"repository":{"name":"service","slug":"service","workspace":{"slug":"acme"}}}`

const bitbucketQualityGatePayload = `{"actor":{"display_name":"Bitbucket Pipelines"},"repository":{"name":"service","slug":"service","workspace":{"slug":"acme"}},"pipeline":{"uuid":"{pipeline-uuid}","build_number":404,"state":{"name":"COMPLETED","result":{"name":"FAILED"}},"target":{"ref_name":"feature/fix","commit":{"hash":"abc123"}},"links":{"self":{"href":"https://api.bitbucket.org/2.0/repositories/acme/service/pipelines/{pipeline-uuid}"}}}}`

const bitbucketChangesRequestPayload = `{"actor":{"nickname":"reviewer","display_name":"Reviewer Display Name"},"repository":{"name":"Service Display Name","slug":"service","workspace":{"slug":"acme"}},"pullrequest":{"id":42,"title":"Fix flaky test","links":{"html":{"href":"https://bitbucket.org/acme/service/pull-requests/42"}},"source":{"branch":{"name":"feature/fix"},"commit":{"hash":"abc123"}}},"changes_request":{"date":"2015-04-06T16:34:59.195330+00:00","user":{"nickname":"reviewer"}}}`
