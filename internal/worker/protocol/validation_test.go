package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateManifestAcceptsFullShape(t *testing.T) {
	manifest := TaskManifest{
		TaskID:             "task-1",
		Repository:         "acme/widget",
		CloneURL:           "ssh://git@bitbucket.org/acme/widget.git",
		BaseBranch:         "main",
		TaskBranch:         "simpleswe/task-1",
		Prompt:             "add focused tests",
		OpenCodeCommand:    []string{"opencode", "run"},
		ValidationCommands: [][]string{{"go", "test", "./..."}},
		MaxFixAttempts:     maxFixAttempts,
	}

	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("full manifest rejected: %v", err)
	}
}

func TestTaskManifestSerializationOmitsSlackField(t *testing.T) {
	manifest := TaskManifest{
		TaskID:            "task-1",
		Repository:        "acme/widget",
		Prompt:            "prompt",
		ValidationCommand: []string{"go", "test"},
	}
	payload, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if _, ok := fields["slack"]; ok {
		t.Fatalf("serialized manifest contains removed Slack field: %s", payload)
	}
}

func TestValidateManifestValidationCommandsErrors(t *testing.T) {
	valid := TaskManifest{
		TaskID:             "task-1",
		Repository:         "acme/widget",
		Prompt:             "prompt",
		ValidationCommands: [][]string{{"go", "test"}},
	}
	for name, edit := range map[string]func(*TaskManifest){
		"empty command":     func(m *TaskManifest) { m.ValidationCommands = [][]string{{}} },
		"too many attempts": func(m *TaskManifest) { m.MaxFixAttempts = maxFixAttempts + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			manifest := valid
			edit(&manifest)
			if err := ValidateManifest(manifest); err == nil {
				t.Fatal("invalid manifest accepted")
			}
		})
	}
}

func TestValidateCloneURL(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "optional", want: true},
		{name: "valid", value: "https://bitbucket.org/acme/widget.git", want: true},
		{name: "ssh git user", value: "ssh://git@bitbucket.org/acme/widget.git", want: true},
		{name: "parse error", value: "%", want: false},
		{name: "too long", value: strings.Repeat("a", 2049), want: false},
		{name: "control character", value: "https://example.com/\x00", want: false},
		{name: "password", value: "https://user:password@example.com/repo.git", want: false},
		{name: "non git user", value: "ssh://user@example.com/repo.git", want: false},
		{name: "ssh password", value: "ssh://git:password@example.com/repo.git", want: false},
		{name: "missing host", value: "file:repo.git", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateCloneURL("clone_url", test.value)
			if (err == nil) != test.want {
				t.Fatalf("validateCloneURL(%q) error = %v, want success %t", test.value, err, test.want)
			}
		})
	}
}

func TestValidateBranch(t *testing.T) {
	for _, test := range []struct {
		name  string
		value string
		want  bool
	}{
		{name: "optional", want: true},
		{name: "valid", value: "feature/task-1", want: true},
		{name: "too long", value: strings.Repeat("a", 256), want: false},
		{name: "control character", value: "feature/\x00", want: false},
		{name: "unsafe", value: "-feature", want: false},
		{name: "double dot", value: "feature..task", want: false},
		{name: "double slash", value: "feature//task", want: false},
		{name: "reflog", value: "feature@{1}", want: false},
		{name: "trailing dot", value: "feature.", want: false},
		{name: "trailing slash", value: "feature/", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateBranch("branch", test.value)
			if (err == nil) != test.want {
				t.Fatalf("validateBranch(%q) error = %v, want success %t", test.value, err, test.want)
			}
		})
	}
}

func TestValidateCommandAndText(t *testing.T) {
	for _, test := range []struct {
		name string
		fn   func() error
		want bool
	}{
		{name: "empty command is optional", fn: func() error { return validateCommand("command", nil) }, want: true},
		{name: "too many arguments", fn: func() error { return validateCommand("command", make([]string, 129)) }, want: false},
		{name: "empty executable", fn: func() error { return validateCommand("command", []string{"  "}) }, want: false},
		{name: "NUL argument", fn: func() error { return validateCommand("command", []string{"go", "test\x00"}) }, want: false},
		{name: "long argument", fn: func() error { return validateCommand("command", []string{"go", strings.Repeat("x", 4097)}) }, want: false},
		{name: "valid command", fn: func() error { return validateCommand("command", []string{"go", "test"}) }, want: true},
		{name: "optional text", fn: func() error { return validateText("text", "", 10, false) }, want: true},
		{name: "required text", fn: func() error { return validateText("text", "", 10, true) }, want: false},
		{name: "long text", fn: func() error { return validateText("text", "123456", 5, false) }, want: false},
		{name: "control text", fn: func() error { return validateText("text", "ok\n", 10, false) }, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.fn()
			if (err == nil) != test.want {
				t.Fatalf("error = %v, want success %t", err, test.want)
			}
		})
	}
}

func TestParseLineRejectsMissingJSONObject(t *testing.T) {
	for _, line := range []string{"@@simpleswe:", "@@simpleswe: []"} {
		if _, err := ParseLine(line); err == nil {
			t.Fatalf("ParseLine(%q) accepted a non-object payload", line)
		}
	}
}

func TestValidateEventIgnoresOtherEventTypes(t *testing.T) {
	if err := ValidateEvent(Event{Type: EventValidationSucceeded}, "branch"); err != nil {
		t.Fatalf("non-branch event rejected: %v", err)
	}
}
