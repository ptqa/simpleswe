package protocol

import (
	"reflect"
	"strings"
	"testing"
)

func TestValidateManifest(t *testing.T) {
	valid := TaskManifest{
		TaskID:            "task-1",
		Repository:        "https://bitbucket.example/acme/repo.git",
		Prompt:            "add focused tests",
		ValidationCommand: []string{"go", "test", "./..."},
		MaxFixAttempts:    2,
	}

	if err := ValidateManifest(valid); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}

	zeroAttempts := valid
	zeroAttempts.MaxFixAttempts = 0
	if err := ValidateManifest(zeroAttempts); err != nil {
		t.Fatalf("zero fix attempts rejected: %v", err)
	}

	tests := []struct {
		name string
		edit func(*TaskManifest)
	}{
		{name: "missing task id", edit: func(m *TaskManifest) { m.TaskID = "" }},
		{name: "missing repository", edit: func(m *TaskManifest) { m.Repository = "" }},
		{name: "missing prompt", edit: func(m *TaskManifest) { m.Prompt = "" }},
		{name: "missing validation command", edit: func(m *TaskManifest) { m.ValidationCommand = nil }},
		{name: "negative fix attempts", edit: func(m *TaskManifest) { m.MaxFixAttempts = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manifest := valid
			tt.edit(&manifest)
			if err := ValidateManifest(manifest); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestParseLineKeepsRawLogsReadable(t *testing.T) {
	for _, raw := range []string{
		"plain worker output",
		"INFO @@simpleswe:{\"type\":\"not-at-start\"}",
		"@@simpleswe :{\"type\":\"not-prefixed\"}",
	} {
		parsed, err := ParseLine(raw)
		if err != nil {
			t.Fatalf("raw line %q rejected: %v", raw, err)
		}
		if parsed.Event != nil {
			t.Fatalf("raw line %q parsed as an event", raw)
		}
		if parsed.Raw != raw {
			t.Errorf("raw line changed: got %q, want %q", parsed.Raw, raw)
		}
	}
}

func TestParseLineRequiresExactPrefixAndValidJSON(t *testing.T) {
	parsed, err := ParseLine("@@simpleswe: {\"type\":\"started\"}")
	if err != nil {
		t.Fatalf("valid event rejected: %v", err)
	}
	if parsed.Event == nil || parsed.Event.Type != "started" {
		t.Fatalf("event not parsed: %#v", parsed.Event)
	}

	if _, err := ParseLine("@@simpleswe:{not-json}"); err == nil {
		t.Fatal("invalid JSON event accepted")
	}
}

func TestEventRoundTrip(t *testing.T) {
	want := Event{
		Type:    "validation.failed",
		TaskID:  "task-1",
		Message: "stdout contained a quoted \"value\"",
	}

	line, err := EncodeEvent(want)
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	if !strings.HasPrefix(line, "@@simpleswe:") {
		t.Fatalf("encoded event missing prefix: %q", line)
	}

	parsed, err := ParseLine(line)
	if err != nil {
		t.Fatalf("parse encoded event: %v", err)
	}
	if parsed.Event == nil {
		t.Fatal("encoded event parsed as raw output")
	}
	if !reflect.DeepEqual(*parsed.Event, want) {
		t.Errorf("round trip mismatch: got %#v, want %#v", *parsed.Event, want)
	}
}

func TestValidateBranchPushedRequiresExpectedBranchAndFullCommitSHA(t *testing.T) {
	valid := Event{
		Type:      EventBranchPushed,
		TaskID:    "task-1",
		Branch:    "simpleswe/task-1",
		CommitSHA: strings.Repeat("a", 40),
	}
	if err := ValidateEvent(valid, "simpleswe/task-1"); err != nil {
		t.Fatalf("valid branch_pushed rejected: %v", err)
	}

	wrongBranch := valid
	wrongBranch.Branch = "simpleswe/another-task"
	if err := ValidateEvent(wrongBranch, "simpleswe/task-1"); err == nil {
		t.Fatal("branch_pushed for an unexpected branch was accepted")
	}
	shortSHA := valid
	shortSHA.CommitSHA = "abc123"
	if err := ValidateEvent(shortSHA, valid.Branch); err == nil {
		t.Fatal("branch_pushed with an abbreviated commit SHA was accepted")
	}
	nonHexSHA := valid
	nonHexSHA.CommitSHA = strings.Repeat("z", 40)
	if err := ValidateEvent(nonHexSHA, valid.Branch); err == nil {
		t.Fatal("branch_pushed with a non-hex commit SHA was accepted")
	}
}
