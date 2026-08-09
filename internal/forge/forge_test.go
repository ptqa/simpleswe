package forge

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestReplyMarkerIsDeterministicAndParserSafe(t *testing.T) {
	first := ReplyMarker("delivery-123")
	if first != ReplyMarker("delivery-123") || first == ReplyMarker("delivery-456") {
		t.Fatalf("reply markers are not deterministic and event-specific: %q", first)
	}
	if !strings.HasPrefix(first, "<!-- simpleswe:") || !strings.HasSuffix(first, " -->") || len(first) > 64 {
		t.Fatalf("reply marker = %q; want short SimpleSWE HTML comment", first)
	}
}

type delayedTestError struct{ delay time.Duration }

func (e delayedTestError) Error() string             { return "retry later" }
func (e delayedTestError) RetryDelay() time.Duration { return e.delay }

func TestRetryDelayParsesAndSurvivesWrapping(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{
		"30":                       30 * time.Second,
		"86401":                    24 * time.Hour,
		"999999999999999999999999": 24 * time.Hour,
		now.Add(time.Minute).Format(http.TimeFormat):         time.Minute,
		now.Add(30 * 24 * time.Hour).Format(http.TimeFormat): 24 * time.Hour,
		now.Format(http.TimeFormat):                          0,
		now.Add(-time.Second).Format(http.TimeFormat):        0,
		"0":       0,
		"invalid": 0,
	} {
		if got := ParseRetryAfter(value, now); got != want {
			t.Errorf("ParseRetryAfter(%q) = %v, want %v", value, got, want)
		}
	}
	if got := RetryDelay(fmt.Errorf("provider: %w", delayedTestError{2 * time.Minute})); got != 2*time.Minute {
		t.Fatalf("RetryDelay(wrapped) = %v", got)
	}
}

func TestContainsReplyMarkerRecognizesOnlyGeneratedMarkers(t *testing.T) {
	body := ReplyMarker("delivery-123") + " fixed"
	if !ContainsReplyMarker(body) {
		t.Fatalf("ContainsReplyMarker(%q) = false", body)
	}
	for _, body := range []string{
		"simpleswe fixed this",
		"<!-- simpleswe -->",
		"<!-- simpleswe:not-a-digest -->",
		"<!-- simpleswe:0123456789abcdef0 -->",
	} {
		if ContainsReplyMarker(body) {
			t.Fatalf("ContainsReplyMarker(%q) = true", body)
		}
	}
}

func TestValidateTargetRequiresCompleteSafeRouteIdentity(t *testing.T) {
	valid := Target{
		Provider: ProviderGitHub, BaseURL: "https://api.github.com",
		Owner: "Acme", Repository: "Widget", CredentialsSecret: "widget-github",
	}
	if err := ValidateTarget(valid); err != nil {
		t.Fatalf("ValidateTarget(valid) error = %v", err)
	}

	tests := map[string]func(*Target){
		"provider":          func(target *Target) { target.Provider = "gitlab" },
		"base URL":          func(target *Target) { target.BaseURL = "http://forge.example" },
		"owner":             func(target *Target) { target.Owner = "" },
		"repository":        func(target *Target) { target.Repository = " widget" },
		"credential Secret": func(target *Target) { target.CredentialsSecret = "../secret" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			target := valid
			mutate(&target)
			if err := ValidateTarget(target); err == nil {
				t.Fatal("ValidateTarget accepted incomplete or unsafe target")
			}
		})
	}
}

func TestMarkedPermanentErrorClassification(t *testing.T) {
	err := MarkPermanent(errors.New("route removed"))
	if !IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = false", err)
	}
}

func TestValidatePullRequestMetadataRequiresSafeHTTPSURL(t *testing.T) {
	if err := ValidatePullRequestMetadata("https://github.com/acme/widget/pull/42", "Fix it"); err != nil {
		t.Fatalf("valid metadata: %v", err)
	}
	if err := ValidatePullRequestMetadata("http://127.0.0.1:8080/acme/widget/pull/42", "Fix it"); err != nil {
		t.Fatalf("loopback metadata: %v", err)
	}
	for _, value := range []string{
		"http://github.com/acme/widget/pull/42",
		"https://user@github.com/acme/widget/pull/42",
		"https://github.com/acme/widget/pull/42?token=x",
		"https://github.com/acme/widget/pull/42#details",
	} {
		if err := ValidatePullRequestMetadata(value, "Fix it"); err == nil {
			t.Errorf("ValidatePullRequestMetadata(%q) accepted unsafe URL", value)
		}
	}
}

func TestValidateNormalizedTextAllowsRenderableWhitespaceOnly(t *testing.T) {
	if err := ValidateNormalizedText("body", "first\nsecond\r\n\tindented", true); err != nil {
		t.Fatalf("ValidateNormalizedText(multiline) error = %v", err)
	}
	for _, value := range []string{"unsafe\x00text", "unsafe\x01text"} {
		if err := ValidateNormalizedText("body", value, true); err == nil {
			t.Fatalf("ValidateNormalizedText(%q) accepted unsafe control character", value)
		}
	}
	if err := ValidateNormalizedIdentity("delivery ID", "line\nbreak", true); err == nil {
		t.Fatal("ValidateNormalizedIdentity accepted a newline")
	}
}
