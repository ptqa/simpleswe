package run

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/simpleswe/simpleswe/internal/config"
	"github.com/simpleswe/simpleswe/internal/forge"
	"github.com/simpleswe/simpleswe/internal/forge/bitbucket"
	"github.com/simpleswe/simpleswe/internal/forge/github"
	"github.com/simpleswe/simpleswe/internal/store"
)

const (
	maxWebhookBodyBytes    = 1 << 20
	forgeReviewQuietWindow = 30 * time.Minute
)

type webhookHandler struct {
	store   *store.Store
	secrets map[forge.Provider]string
}

func newWebhookHandler(cfg config.Config, db *store.Store) (http.Handler, error) {
	if db == nil {
		return nil, errors.New("webhook Store is nil")
	}
	secrets := make(map[forge.Provider]string, 2)
	usedProviders := make(map[forge.Provider]bool, 2)
	for _, repository := range cfg.Repositories {
		if repository.GitHub.Owner != "" || repository.GitHub.Repository != "" || repository.GitHub.CredentialsSecret != "" {
			usedProviders[forge.ProviderGitHub] = true
		}
		if repository.Bitbucket.Workspace != "" || repository.Bitbucket.Repository != "" || repository.Bitbucket.CredentialsSecret != "" {
			usedProviders[forge.ProviderBitbucket] = true
		}
	}
	for _, provider := range []struct {
		name   forge.Provider
		source config.SecretSource
	}{
		{forge.ProviderGitHub, cfg.GitHub.WebhookSecret},
		{forge.ProviderBitbucket, cfg.Bitbucket.WebhookSecret},
	} {
		if !usedProviders[provider.name] {
			continue
		}
		secret, err := readWebhookSecret(provider.source, string(provider.name)+" webhook secret")
		if err != nil {
			return nil, err
		}
		secrets[provider.name] = secret
	}
	return webhookHandler{store: db, secrets: secrets}, nil
}

func readWebhookSecret(source config.SecretSource, description string) (string, error) {
	if source.File != "" {
		// #nosec G304 -- file is an explicit trusted config setting.
		data, err := os.ReadFile(source.File)
		if err != nil {
			return "", fmt.Errorf("read %s file: %w", description, err)
		}
		if len(data) > 1<<20 {
			return "", fmt.Errorf("%s file is too large", description)
		}
		if len(data) == 0 {
			return "", fmt.Errorf("%s is not configured", description)
		}
		return string(data), nil
	}
	value := os.Getenv(source.Env)
	if value == "" {
		return "", fmt.Errorf("%s is not configured", description)
	}
	return value, nil
}

func (h webhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	provider, deliveryHeader, eventHeader, signatureHeader := webhookRoute(r.URL.Path)
	secret, configured := h.secrets[provider]
	if provider == "" {
		http.NotFound(w, r)
		return
	}
	if !configured {
		writeWebhookError(w, http.StatusNotFound, "not_found", "not found")
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeWebhookError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBodyBytes+1))
	if err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_request", "invalid request")
		return
	}
	if len(body) > maxWebhookBodyBytes {
		writeWebhookError(w, http.StatusRequestEntityTooLarge, "payload_too_large", "payload too large")
		return
	}
	if !validWebhookSignature(r.Header.Values(signatureHeader), secret, body) {
		writeWebhookError(w, http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	delivery, ok := requiredWebhookHeader(r, deliveryHeader)
	if !ok {
		writeWebhookError(w, http.StatusBadRequest, "invalid_request", "invalid request")
		return
	}
	eventName, ok := requiredWebhookHeader(r, eventHeader)
	if !ok {
		writeWebhookError(w, http.StatusBadRequest, "invalid_request", "invalid request")
		return
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_request", "invalid request")
		return
	}
	var event forge.Event
	var actionable bool
	if provider == forge.ProviderGitHub {
		event, actionable, err = github.ParseWebhook(delivery, eventName, body)
	} else {
		event, actionable, err = bitbucket.ParseWebhook(delivery, eventName, body)
	}
	if err != nil {
		writeWebhookError(w, http.StatusBadRequest, "invalid_request", "invalid request")
		return
	}
	if actionable {
		if _, err := h.store.PutForgeEventAfter(r.Context(), store.ForgeEvent{
			ID: event.DeliveryID, Provider: string(event.Provider), Kind: event.Kind,
			Owner: event.Owner, Repository: event.Repository, PullRequestNumber: event.PullRequestNumber,
			CommitSHA: event.CommitSHA, Branch: event.Branch, CommentID: event.CommentID,
			CommentKind: event.CommentKind, Title: event.Title, Body: event.Body,
			Author: event.Author, URL: event.URL,
		}, forgeReviewQuietWindow); err != nil {
			writeWebhookError(w, http.StatusInternalServerError, "internal_error", "internal server error")
			return
		}
	}
	w.WriteHeader(http.StatusAccepted)
}

func webhookRoute(path string) (forge.Provider, string, string, string) {
	switch path {
	case "/v1/webhooks/github":
		return forge.ProviderGitHub, "X-GitHub-Delivery", "X-GitHub-Event", "X-Hub-Signature-256"
	case "/v1/webhooks/bitbucket":
		return forge.ProviderBitbucket, "X-Request-UUID", "X-Event-Key", "X-Hub-Signature"
	default:
		return "", "", "", ""
	}
}

func requiredWebhookHeader(r *http.Request, name string) (string, bool) {
	values := r.Header.Values(name)
	if len(values) != 1 {
		return "", false
	}
	value := values[0]
	return value, value != "" && value == strings.TrimSpace(value) && len(value) <= 1024 && strings.IndexFunc(value, unicode.IsControl) < 0
}

func validWebhookSignature(values []string, secret string, body []byte) bool {
	if len(values) != 1 || len(values[0]) != len("sha256=")+sha256.Size*2 || !strings.HasPrefix(values[0], "sha256=") {
		return false
	}
	provided, err := hex.DecodeString(strings.TrimPrefix(values[0], "sha256="))
	if err != nil || len(provided) != sha256.Size {
		return false
	}
	digest := hmac.New(sha256.New, []byte(secret))
	_, _ = digest.Write(body)
	return hmac.Equal(provided, digest.Sum(nil))
}

func writeWebhookError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": message}})
}
