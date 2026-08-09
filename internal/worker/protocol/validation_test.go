package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestValidateManifestAcceptsFullShape(t *testing.T) {
	manifest := TaskManifest{
		TaskID:                     "task-1",
		Repository:                 "acme/widget",
		CloneURL:                   "ssh://git@bitbucket.org/acme/widget.git",
		BaseBranch:                 "main",
		TaskBranch:                 "simpleswe/task-1",
		Prompt:                     "add focused tests",
		OpenCodeCommand:            []string{"opencode", "run"},
		ValidationCommands:         [][]string{{"go", "test", "./..."}},
		MaxFixAttempts:             maxFixAttempts,
		ForgeProvider:              "github",
		ForgeOwner:                 "acme",
		ForgeRepository:            "widget",
		RequestedPullRequestTitle:  "Fix widget",
		ExistingPullRequestNumber:  42,
		ExistingPullRequestHeadSHA: "0123456789abcdef0123456789abcdef01234567",
	}

	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("full manifest rejected: %v", err)
	}
}

func TestManifestForgeContextIsStrictAndContainsNoEvidencePayloads(t *testing.T) {
	valid := TaskManifest{
		TaskID: "task-1", CloneURL: "https://github.com/acme/widget.git", BaseBranch: "main",
		TaskBranch: "simpleswe/task-1", Prompt: "fix it", OpenCodeCommand: []string{"opencode"},
		ValidationCommand: []string{"go", "test", "./..."}, ForgeProvider: "github", ForgeOwner: "acme",
		ForgeRepository: "widget", RequestedPullRequestTitle: "Fix widget", ExistingPullRequestNumber: 42,
		ExistingPullRequestHeadSHA: "0123456789abcdef0123456789abcdef01234567",
	}
	if err := ValidateManifest(valid); err != nil {
		t.Fatalf("valid forge context rejected: %v", err)
	}
	for name, edit := range map[string]func(*TaskManifest){
		"unsupported provider":  func(m *TaskManifest) { m.ForgeProvider = "gitlab" },
		"missing owner":         func(m *TaskManifest) { m.ForgeOwner = "" },
		"missing repository":    func(m *TaskManifest) { m.ForgeRepository = "" },
		"invalid existing PR":   func(m *TaskManifest) { m.ExistingPullRequestNumber = -1 },
		"missing existing head": func(m *TaskManifest) { m.ExistingPullRequestHeadSHA = "" },
		"short existing head":   func(m *TaskManifest) { m.ExistingPullRequestHeadSHA = "0123456" },
		"uppercase existing head": func(m *TaskManifest) {
			m.ExistingPullRequestHeadSHA = strings.ToUpper(m.ExistingPullRequestHeadSHA)
		},
		"head without existing PR": func(m *TaskManifest) { m.ExistingPullRequestNumber = 0 },
		"blank requested title":    func(m *TaskManifest) { m.RequestedPullRequestTitle = " \t" },
		"long requested title":     func(m *TaskManifest) { m.RequestedPullRequestTitle = strings.Repeat("x", 257) },
	} {
		t.Run(name, func(t *testing.T) {
			manifest := valid
			edit(&manifest)
			if err := ValidateManifest(manifest); err == nil {
				t.Fatal("invalid forge context accepted")
			}
		})
	}

	payload, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"screenshot", "screenshots", "summary", "attachment", "attachments", "upload", "uploads", "pull_request_url"} {
		if _, exists := fields[forbidden]; exists {
			t.Errorf("manifest contains forbidden field %q: %s", forbidden, payload)
		}
	}
}

func TestValidateManifestAcceptsSHA256ExistingHead(t *testing.T) {
	manifest := TaskManifest{
		TaskID: "task-1", Repository: "acme/widget", BaseBranch: "main", TaskBranch: "simpleswe/task-1",
		Prompt: "fix it", OpenCodeCommand: []string{"opencode"}, ValidationCommand: []string{"go", "test"},
		ForgeProvider: "github", ForgeOwner: "acme", ForgeRepository: "widget", ExistingPullRequestNumber: 42,
		ExistingPullRequestHeadSHA: strings.Repeat("a", 64),
	}
	if err := ValidateManifest(manifest); err != nil {
		t.Fatalf("SHA-256 existing head rejected: %v", err)
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
		ForgeProvider:      "bitbucket",
		ForgeOwner:         "acme",
		ForgeRepository:    "widget",
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
