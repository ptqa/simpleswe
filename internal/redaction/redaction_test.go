package redaction

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestRedactLiteralSecretsInStdoutAndStderr(t *testing.T) {
	secrets := []string{"pa$$word", "a+b", ""}
	stdout := "stdout: pa$$word; expression=a+b"
	stderr := "stderr: pa$$word"

	gotStdout := Redact(stdout, secrets)
	gotStderr := Redact(stderr, secrets)

	if want := "stdout: [REDACTED]; expression=[REDACTED]"; gotStdout != want {
		t.Errorf("stdout redaction: got %q, want %q", gotStdout, want)
	}
	if want := "stderr: [REDACTED]"; gotStderr != want {
		t.Errorf("stderr redaction: got %q, want %q", gotStderr, want)
	}
	for _, secret := range secrets[:2] {
		if strings.Contains(gotStdout, secret) || strings.Contains(gotStderr, secret) {
			t.Errorf("secret %q remains in redacted output", secret)
		}
	}
}

func TestRedactIgnoresEmptySecrets(t *testing.T) {
	const output = "ordinary output"

	if got := Redact(output, []string{""}); got != output {
		t.Fatalf("empty secret changed output: got %q, want %q", got, output)
	}
}

func TestExpandSecretsIncludesWholeValuesAndNontrivialMultilineParts(t *testing.T) {
	const multiline = "-----BEGIN TOKEN-----\nabc123-secret-line\nxy\n-----END TOKEN-----"
	secrets := ExpandSecrets([]string{multiline, "environment-secret"})
	output := "whole=" + multiline + " line=abc123-secret-line short=xy env=environment-secret"
	redacted := Redact(output, secrets)

	for _, leaked := range []string{multiline, "abc123-secret-line", "environment-secret"} {
		if strings.Contains(redacted, leaked) {
			t.Errorf("expanded secret %q remains in output %q", leaked, redacted)
		}
	}
	if !strings.Contains(redacted, "short=xy") {
		t.Errorf("trivial multiline fragment was redacted: %q", redacted)
	}
}

func TestExpandSecretsRedactsJSONEscapedValues(t *testing.T) {
	const secret = "quoted=\"value\" path=C:\\private & <token>\nsecond=\"line\"\\end"
	secrets := ExpandSecrets([]string{secret, secret, "", "plain-secret"})

	for i, value := range secrets {
		if value == "" {
			t.Fatal("expanded secrets contain an empty value")
		}
		if i > 0 && len(secrets[i-1]) < len(value) {
			t.Fatalf("expanded secrets are not longest first: %#v", secrets)
		}
		for _, duplicate := range secrets[:i] {
			if duplicate == value {
				t.Fatalf("expanded secrets contain duplicate %q", value)
			}
		}
	}

	jsonContent := func(escapeHTML bool) string {
		t.Helper()
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		encoder.SetEscapeHTML(escapeHTML)
		if err := encoder.Encode(secret); err != nil {
			t.Fatal(err)
		}
		value := strings.TrimSuffix(encoded.String(), "\n")
		return value[1 : len(value)-1]
	}
	for _, escapeHTML := range []bool{false, true} {
		escaped := jsonContent(escapeHTML)
		if !slices.Contains(secrets, escaped) {
			t.Fatalf("expanded secrets do not contain JSON content %q", escaped)
		}
		var line bytes.Buffer
		encoder := json.NewEncoder(&line)
		encoder.SetEscapeHTML(escapeHTML)
		if err := encoder.Encode(map[string]any{
			"type": "text",
			"part": map[string]string{"type": "text", "text": secret},
		}); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line.String(), secret) {
			t.Fatalf("test JSON unexpectedly contains literal secret: %q", line.String())
		}

		redacted := Redact(line.String(), secrets)
		if !strings.Contains(redacted, Placeholder) {
			t.Fatalf("JSON-escaped secret was not redacted: %q", redacted)
		}
		for _, leaked := range secrets {
			if strings.Contains(redacted, leaked) {
				t.Fatalf("expanded secret %q remains in JSON output %q", leaked, redacted)
			}
		}
	}
}

func TestExpandSecretsRedactsNestedOpenCodeReplyJSON(t *testing.T) {
	const secret = "quoted=\"value\" path=C:\\private & <token>\nsecond=\"line\"\\end"
	secrets := ExpandSecrets([]string{secret})

	jsonContent := func(value string, escapeHTML bool) string {
		t.Helper()
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		encoder.SetEscapeHTML(escapeHTML)
		if err := encoder.Encode(value); err != nil {
			t.Fatal(err)
		}
		text := strings.TrimSuffix(encoded.String(), "\n")
		return text[1 : len(text)-1]
	}
	for _, innerEscapeHTML := range []bool{false, true} {
		for _, outerEscapeHTML := range []bool{false, true} {
			var reply bytes.Buffer
			innerEncoder := json.NewEncoder(&reply)
			innerEncoder.SetEscapeHTML(innerEscapeHTML)
			if err := innerEncoder.Encode(map[int]string{123: secret}); err != nil {
				t.Fatal(err)
			}

			var event bytes.Buffer
			outerEncoder := json.NewEncoder(&event)
			outerEncoder.SetEscapeHTML(outerEscapeHTML)
			if err := outerEncoder.Encode(map[string]any{
				"type": "text",
				"part": map[string]string{"type": "text", "text": strings.TrimSuffix(reply.String(), "\n")},
			}); err != nil {
				t.Fatal(err)
			}

			onceEscaped := jsonContent(secret, innerEscapeHTML)
			twiceEscaped := jsonContent(onceEscaped, outerEscapeHTML)
			if !slices.Contains(secrets, twiceEscaped) {
				t.Fatalf("expanded secrets do not contain nested JSON content %q", twiceEscaped)
			}
			if !strings.Contains(event.String(), twiceEscaped) {
				t.Fatalf("nested event does not contain expected secret form %q: %q", twiceEscaped, event.String())
			}
			redacted := Redact(event.String(), secrets)
			if !strings.Contains(redacted, Placeholder) {
				t.Fatalf("nested secret was not redacted: %q", redacted)
			}
			for name, leaked := range map[string]string{
				"raw":           secret,
				"once escaped":  onceEscaped,
				"twice escaped": twiceEscaped,
			} {
				if strings.Contains(redacted, leaked) {
					t.Fatalf("%s secret %q remains in nested event %q", name, leaked, redacted)
				}
			}
		}
	}
}
