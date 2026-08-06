// Package forge defines the provider-neutral pull-request contract.
package forge

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"unicode"
)

type Provider string

const (
	ProviderBitbucket Provider = "bitbucket"
	ProviderGitHub    Provider = "github"
)

// Target explicitly identifies a configured forge repository.
type Target struct {
	Provider          Provider `json:"provider"`
	BaseURL           string   `json:"base_url"`
	Owner             string   `json:"owner"`
	Repository        string   `json:"repository"`
	CredentialsSecret string   `json:"credentials_secret_name"`
}

// ValidateTarget rejects incomplete or unsupported pull-request routes.
func ValidateTarget(target Target) error {
	if target.Provider != ProviderBitbucket && target.Provider != ProviderGitHub {
		return fmt.Errorf("forge target provider %q is not supported", target.Provider)
	}
	baseURL, err := url.Parse(target.BaseURL)
	if err != nil || baseURL.Host == "" || baseURL.User != nil || baseURL.RawQuery != "" || baseURL.Fragment != "" || (baseURL.Scheme != "https" && !(baseURL.Scheme == "http" && loopbackHost(baseURL.Hostname()))) {
		return errors.New("forge target base_url must be an HTTPS URL without credentials (HTTP is allowed only for loopback test servers)")
	}
	for name, value := range map[string]string{"owner": target.Owner, "repository": target.Repository} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("forge target %s is required", name)
		}
		if value != strings.TrimSpace(value) {
			return fmt.Errorf("forge target %s must not have surrounding whitespace", name)
		}
		if len(value) > 256 {
			return fmt.Errorf("forge target %s is too long", name)
		}
		if strings.IndexFunc(value, unicode.IsControl) >= 0 {
			return fmt.Errorf("forge target %s contains control characters", name)
		}
	}
	if !validSecretName(target.CredentialsSecret) {
		return errors.New("forge target credentials_secret_name must be a valid Kubernetes Secret name")
	}
	return nil
}

func loopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func validSecretName(name string) bool {
	if len(name) == 0 || len(name) > 253 {
		return false
	}
	for label := range strings.SplitSeq(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, char := range label {
			if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' {
				return false
			}
		}
	}
	return true
}

type CreatePullRequestRequest struct {
	Title             string
	Description       string
	SourceBranch      string
	DestinationBranch string
}

type PullRequest struct {
	ID      int
	HTMLURL string
}

type statusCoder interface {
	HTTPStatusCode() int
}

type retryableError interface {
	Retryable() bool
}

type permanentError interface {
	Permanent() bool
}

type markedPermanentError struct{ error }

func (markedPermanentError) Permanent() bool { return true }
func (e markedPermanentError) Unwrap() error { return e.error }

// MarkPermanent classifies a configuration or routing failure as non-retryable.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}
	return markedPermanentError{err}
}

// IsPermanent reports only clear client errors. Timeout, conflict, locked,
// too-early, and rate-limit responses remain retryable.
func IsPermanent(err error) bool {
	var permanent permanentError
	if errors.As(err, &permanent) && permanent.Permanent() {
		return true
	}
	var retryable retryableError
	if errors.As(err, &retryable) && retryable.Retryable() {
		return false
	}
	var provider statusCoder
	if !errors.As(err, &provider) {
		return false
	}
	status := provider.HTTPStatusCode()
	if status < 400 || status >= 500 {
		return false
	}
	switch status {
	case http.StatusRequestTimeout, http.StatusConflict, http.StatusLocked, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	default:
		return true
	}
}
