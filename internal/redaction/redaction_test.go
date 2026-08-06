package redaction

import (
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
