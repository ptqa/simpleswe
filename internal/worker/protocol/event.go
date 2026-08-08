package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"
)

const EventPrefix = "@@simpleswe:"

// ReviewReplyInstruction is the immutable output requirement for forge review follow-ups.
const ReviewReplyInstruction = "After completing the code changes, make your final assistant answer exactly a JSON object mapping each comment_id to a concise reply"

const (
	maxEventReplies    = 32
	maxReplyDraftBytes = 2 << 10
)

const (
	EventAgentStarted        = "agent_started"
	EventValidationStarted   = "validation_started"
	EventValidationResult    = "validation_result"
	EventValidationSucceeded = "validation_succeeded"
	EventValidationFailed    = "validation_failed"
	EventBranchPushed        = "branch_pushed"
	SecretEnvNamesVariable   = "SIMPLESWE_SECRET_ENV_NAMES"
)

// Event is the structured subset of worker output. Any fields not represented
// here remain in the raw log stream rather than being guessed at by the parser.
type Event struct {
	Type      string         `json:"type"`
	TaskID    string         `json:"task_id,omitempty"`
	Message   string         `json:"message,omitempty"`
	Timestamp time.Time      `json:"timestamp,omitzero"`
	Command   []string       `json:"command,omitempty"`
	ExitCode  int            `json:"exit_code"`
	Branch    string         `json:"branch,omitempty"`
	CommitSHA string         `json:"commit_sha,omitempty"`
	Replies   map[int]string `json:"replies,omitempty"`
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
// expectedBranch is required for branch_pushed so a worker cannot select a
// different branch than the immutable task manifest.
func ValidateEvent(event Event, expectedBranch string) error {
	if event.Type != EventBranchPushed {
		return nil
	}
	if expectedBranch == "" || event.Branch != expectedBranch {
		return fmt.Errorf("branch_pushed branch %q does not match expected branch %q", event.Branch, expectedBranch)
	}
	if len(event.CommitSHA) != 40 && len(event.CommitSHA) != 64 {
		return fmt.Errorf("branch_pushed commit_sha must be a full Git object ID")
	}
	for _, char := range event.CommitSHA {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return fmt.Errorf("branch_pushed commit_sha must be lowercase hexadecimal")
		}
	}
	if len(event.Replies) > maxEventReplies {
		return fmt.Errorf("branch_pushed replies exceed %d entries", maxEventReplies)
	}
	for commentID, draft := range event.Replies {
		if commentID <= 0 {
			return fmt.Errorf("branch_pushed reply comment ID must be positive")
		}
		if strings.TrimSpace(draft) == "" {
			return fmt.Errorf("branch_pushed reply draft must not be blank")
		}
		if len(draft) > maxReplyDraftBytes {
			return fmt.Errorf("branch_pushed reply draft exceeds %d bytes", maxReplyDraftBytes)
		}
		if strings.IndexFunc(draft, unicode.IsControl) >= 0 {
			return fmt.Errorf("branch_pushed reply draft contains control characters")
		}
	}
	return nil
}
