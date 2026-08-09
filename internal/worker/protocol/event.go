package protocol

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const EventPrefix = "@@simpleswe:"

const MaxWorkerEventMessageLen = 32 << 10

const (
	EventAgentStarted         = "agent_started"
	EventValidationStarted    = "validation_started"
	EventValidationResult     = "validation_result"
	EventValidationSucceeded  = "validation_succeeded"
	EventValidationFailed     = "validation_failed"
	EventPullRequestPublished = "pull_request_published"
	EventPullRequestReady     = "pull_request_ready"
	EventWorkerFailed         = "worker_failed"
	EventBranchPushed         = "branch_pushed"
	SecretEnvNamesVariable    = "SIMPLESWE_SECRET_ENV_NAMES"
)

// Event is the structured subset of worker output. Any fields not represented
// here remain in the raw log stream rather than being guessed at by the parser.
type Event struct {
	Type              string    `json:"type"`
	TaskID            string    `json:"task_id,omitempty"`
	Message           string    `json:"message,omitempty"`
	Timestamp         time.Time `json:"timestamp,omitzero"`
	Command           []string  `json:"command,omitempty"`
	ExitCode          int       `json:"exit_code"`
	Branch            string    `json:"branch,omitempty"`
	CommitSHA         string    `json:"commit_sha,omitempty"`
	PullRequestNumber int       `json:"pull_request_number,omitempty"`
}

type ParsedLine struct {
	Raw   string
	Event *Event
}

func EncodeEvent(event Event) (string, error) {
	payload, err := json.Marshal(event)
	if err != nil {
		return "", fmt.Errorf("encode worker event: %w", err)
	}
	return EventPrefix + string(payload), nil
}

func ParseLine(line string) (ParsedLine, error) {
	parsed := ParsedLine{Raw: line}
	if !strings.HasPrefix(line, EventPrefix) {
		return parsed, nil
	}

	payload := bytes.TrimSpace([]byte(strings.TrimPrefix(line, EventPrefix)))
	if len(payload) == 0 || payload[0] != '{' {
		return parsed, fmt.Errorf("worker event must contain a JSON object")
	}
	var event Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return parsed, fmt.Errorf("decode worker event: %w", err)
	}
	parsed.Event = &event
	return parsed, nil
}

// ValidateEvent checks event fields that cross the worker protocol boundary.
// expectedBranch is required for pull_request_ready so a worker cannot select a
// different branch than the immutable task manifest.
func ValidateEvent(event Event, expectedBranch string) error {
	if event.Type == EventWorkerFailed {
		if strings.TrimSpace(event.Message) == "" || len(event.Message) > MaxWorkerEventMessageLen {
			return fmt.Errorf("worker_failed message must be nonblank and at most %d bytes", MaxWorkerEventMessageLen)
		}
		return nil
	}
	if event.Type != EventPullRequestPublished && event.Type != EventPullRequestReady && event.Type != EventBranchPushed {
		return nil
	}
	if event.Type == EventBranchPushed && (len(event.Branch) > 1024 || event.PullRequestNumber != 0 || event.Message != "" || len(event.Command) != 0 || event.ExitCode != 0) {
		return fmt.Errorf("branch_pushed contains unsupported or oversized legacy fields")
	}
	if event.Type != EventBranchPushed && (event.PullRequestNumber <= 0 || event.Message != "" || len(event.Command) != 0 || event.ExitCode != 0) {
		return fmt.Errorf("%s must contain only a positive pull_request_number, branch, commit_sha, task_id, and timestamp", event.Type)
	}
	if expectedBranch == "" || event.Branch != expectedBranch {
		return fmt.Errorf("%s branch %q does not match expected branch %q", event.Type, event.Branch, expectedBranch)
	}
	if !FullLowerGitObjectID(event.CommitSHA) {
		if len(event.CommitSHA) != 40 && len(event.CommitSHA) != 64 {
			return fmt.Errorf("%s commit_sha must be a full Git object ID", event.Type)
		}
		return fmt.Errorf("%s commit_sha must be lowercase hexadecimal", event.Type)
	}
	return nil
}

// FullLowerGitObjectID reports whether value is a complete lowercase SHA-1 or
// SHA-256 Git object ID.
func FullLowerGitObjectID(value string) bool {
	if (len(value) != 40 && len(value) != 64) || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
