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

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/store"
)

const (
	githubWebhookSecret    = "github-webhook-test-secret"
	bitbucketWebhookSecret = "bitbucket-webhook-test-secret"
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
		name          string
		provider      string
		path          string
		secret        string
		delivery      string
		event         string
		body          []byte
		method        string
		signature     string
		omitDelivery  bool
		omitEvent     bool
		wantStatus    int
		wantEvent     bool
		duplicateBody []byte
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
			name:     "duplicate delivery does not replace the first payload",
			provider: "github", path: "/v1/webhooks/github", secret: githubWebhookSecret,
			delivery: "github-duplicate", event: "issue_comment", body: []byte(githubActionablePayload),
			duplicateBody: []byte(strings.Replace(githubActionablePayload, "Please fix this", "replacement payload", 1)),
			method:        http.MethodPost, signature: "valid", wantStatus: http.StatusAccepted,
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

			if len(tt.duplicateBody) == 0 {
				return
			}
			duplicate := newWebhookRequest(tt.method, tt.path, tt.provider, tt.secret, tt.delivery, tt.event, tt.duplicateBody, tt.signature, tt.omitDelivery, tt.omitEvent)
			duplicateRecorder := httptest.NewRecorder()
			handler.ServeHTTP(duplicateRecorder, duplicate)
			if duplicateRecorder.Code != http.StatusAccepted {
				t.Fatalf("duplicate status = %d, want %d: %s", duplicateRecorder.Code, http.StatusAccepted, duplicateRecorder.Body.String())
			}
			stored, err := db.GetForgeEvent(t.Context(), tt.provider+":"+tt.delivery)
			if err != nil {
				t.Fatalf("get duplicate event: %v", err)
			}
			if stored.Body != "Please fix this" {
				t.Fatalf("duplicate replaced event body = %q, want original payload", stored.Body)
			}
			incomplete, err := db.ListIncompleteForgeEvents(t.Context())
			if err != nil {
				t.Fatalf("list incomplete forge events: %v", err)
			}
			count := 0
			for _, event := range incomplete {
				if event.ID == tt.provider+":"+tt.delivery {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("durable duplicate event count = %d, want 1", count)
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
