// Package forge defines the provider-neutral pull-request contract.
package forge

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
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

// PullRequestState is the provider-reported identity and lifecycle state used
// to verify an owned pull request before starting follow-up work.
type PullRequestState struct {
	Number            int
	State             string
	SourceOwner       string
	SourceRepository  string
	SourceBranch      string
	DestinationBranch string
	HeadSHA           string
}

// Event is the normalized subset of a provider webhook that can affect a
// simpleswe pull request.
type Event struct {
	DeliveryID        string
	Provider          Provider
	Kind              string
	Owner             string
	Repository        string
	PullRequestNumber int
	CommitSHA         string
	Branch            string
	CommentID         int
	CommentKind       string
	Title             string
	Body              string
	Author            string
	URL               string
}

// ReplyMarker identifies one event's provider reply across process crashes.
func ReplyMarker(eventID string) string {
	digest := sha256.Sum256([]byte(eventID))
	return fmt.Sprintf("<!-- simpleswe:%x -->", digest[:8])
}

var replyMarkerPattern = regexp.MustCompile(`<!-- simpleswe:[0-9a-f]{16} -->`)

// ContainsReplyMarker reports only comments emitted by ReplyMarker.
func ContainsReplyMarker(body string) bool {
	return replyMarkerPattern.MatchString(body)
}

const MaxNormalizedTextLength = 128 * 1024

// ValidateNormalizedText checks text before it crosses into the worker
// prompt boundary.
func ValidateNormalizedText(name, value string, required bool) error {
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if len(value) > MaxNormalizedTextLength {
		return fmt.Errorf("%s is too long", name)
	}
	if strings.IndexFunc(value, func(char rune) bool {
		return unicode.IsControl(char) && char != '\n' && char != '\r' && char != '\t'
	}) >= 0 {
		return fmt.Errorf("%s contains control characters", name)
	}
	return nil
}

// ValidateNormalizedIdentity checks identifiers and routing coordinates, where
// control whitespace is not permitted.
func ValidateNormalizedIdentity(name, value string, required bool) error {
	if err := ValidateNormalizedText(name, value, required); err != nil {
		return err
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return fmt.Errorf("%s contains control characters", name)
	}
	return nil
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

type retryDelayer interface {
	RetryDelay() time.Duration
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

// RetryDelay returns a provider-requested retry delay when one was parsed
// reliably from the response.
func RetryDelay(err error) time.Duration {
	var delayed retryDelayer
	if errors.As(err, &delayed) {
		return delayed.RetryDelay()
	}
	return 0
}

// ParseRetryAfter accepts the two standard Retry-After representations.
func ParseRetryAfter(value string, now time.Time) time.Duration {
	const maximum = 24 * time.Hour
	trimmed := strings.TrimSpace(value)
	if trimmed != "" && strings.Trim(trimmed, "0123456789") == "" {
		seconds, err := strconv.ParseUint(trimmed, 10, 64)
		if seconds == 0 {
			return 0
		}
		if err != nil || seconds >= uint64(maximum/time.Second) {
			return maximum
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return min(when.Sub(now), maximum)
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
