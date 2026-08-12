package gui

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/simpleswe/simpleswe/internal/tui"
)

func boundedLine(line string) string {
	line = strings.ToValidUTF8(line, "�")
	line = stripANSI(line)
	line = strings.Map(func(r rune) rune {
		if r == '\t' {
			return ' '
		}
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, line)
	if len(line) <= maxLogLineBytes {
		return line
	}
	line = strings.ToValidUTF8(line[:maxLogLineBytes-len("…")], "")
	return line + "…"
}

func stripANSI(text string) string {
	var clean strings.Builder
	clean.Grow(len(text))
	for index := 0; index < len(text); {
		next, ok := ansiSequenceEnd(text, index)
		if ok {
			index = next
			continue
		}
		r, size := utf8.DecodeRuneInString(text[index:])
		clean.WriteRune(r)
		index += size
	}
	return clean.String()
}

func ansiSequenceEnd(text string, start int) (int, bool) {
	switch {
	case text[start] == '\x1b' && start+1 < len(text) && text[start+1] == '[':
		return csiEnd(text, start+2)
	case text[start] == '\x1b' && start+1 < len(text) && text[start+1] == ']':
		return stringSequenceEnd(text, start+2)
	case text[start] == '\x1b' && start+1 < len(text) && strings.ContainsRune("PX^_", rune(text[start+1])):
		return stringSequenceEnd(text, start+2)
	case text[start] == '\x1b' && start+1 < len(text) && text[start+1] >= 0x30 && text[start+1] <= 0x7e:
		return start + 2, true
	case strings.HasPrefix(text[start:], "\u009b"):
		return csiEnd(text, start+len("\u009b"))
	case strings.HasPrefix(text[start:], "\u009d"):
		return stringSequenceEnd(text, start+len("\u009d"))
	case strings.HasPrefix(text[start:], "\u0090"), strings.HasPrefix(text[start:], "\u0098"),
		strings.HasPrefix(text[start:], "\u009e"), strings.HasPrefix(text[start:], "\u009f"):
		return stringSequenceEnd(text, start+len("\u0090"))
	default:
		return start, false
	}
}

func csiEnd(text string, index int) (int, bool) {
	for ; index < len(text); index++ {
		switch value := text[index]; {
		case value >= 0x40 && value <= 0x7e:
			return index + 1, true
		case value >= 0x20 && value <= 0x3f:
		default:
			return index, false
		}
	}
	return index, false
}

func stringSequenceEnd(text string, index int) (int, bool) {
	for index < len(text) {
		switch {
		case text[index] == '\a':
			return index + 1, true
		case strings.HasPrefix(text[index:], "\x1b\\"):
			return index + 2, true
		case strings.HasPrefix(text[index:], "\u009c"):
			return index + len("\u009c"), true
		default:
			_, size := utf8.DecodeRuneInString(text[index:])
			index += size
		}
	}
	return index, false
}

func boundedDisplayText(text string) string {
	if len(text) <= maxSurfaceBytes {
		return validUTF8Prefix(text, maxSurfaceBytes)
	}
	const marker = "\n… display truncated …\n"
	available := maxSurfaceBytes - len(marker)
	headBytes, tailBytes := available/2, available-available/2
	head := validUTF8Prefix(text[:headBytes], headBytes)
	tail := validUTF8Prefix(text[len(text)-tailBytes:], tailBytes)
	return head + marker + tail
}

func validUTF8Prefix(text string, limit int) string {
	text = strings.ToValidUTF8(text, "�")
	if len(text) <= limit {
		return text
	}
	return strings.ToValidUTF8(text[:limit], "")
}

func shortError(err error) string {
	if err == nil {
		return ""
	}
	text := strings.ReplaceAll(err.Error(), "\n", " ")
	if len(text) > 180 {
		return text[:177] + "…"
	}
	return text
}

func currentAttempt(detail tui.TaskDetail) tui.Attempt {
	for _, attempt := range detail.Attempts {
		if attempt.ID == detail.Task.CurrentAttemptID {
			return attempt
		}
	}
	if len(detail.Attempts) > 0 {
		return detail.Attempts[len(detail.Attempts)-1]
	}
	return tui.Attempt{}
}

func formatDetails(detail tui.TaskDetail) string {
	if detail.Task.ID == "" {
		return "Select a task to see details."
	}
	attempt := currentAttempt(detail)
	attemptLabel := "not scheduled"
	if attempt.ID != "" {
		attemptLabel = fmt.Sprintf("#%d  %s", attempt.Number, attempt.ID)
	}
	return fmt.Sprintf("Task: %s\nState: %s\nRepository: %s\nAge: %s\nCreated: %s\nUpdated: %s\nPrompt: %s\nAttempt: %s\nAttempt state: %s\nPull request: %s",
		detail.Task.ID, detail.Task.State, detail.Task.Repository, compactAge(detail.Task.CreatedAt),
		formatTime(detail.Task.CreatedAt), formatTime(detail.Task.UpdatedAt), detail.Task.Prompt,
		attemptLabel, firstNonempty(attempt.State, "—"), firstNonempty(detail.Task.PullRequest.URL, "—"))
}

func formatLogs(detail tui.TaskDetail, logs []string, capacity int, follow string) string {
	attempt := currentAttempt(detail)
	pod := firstNonempty(attempt.KubernetesPod.ResourceIdentity.Name, detail.Task.KubernetesPod.ResourceIdentity.Name, "—")
	attemptLabel := firstNonempty(attempt.ID, "—")
	if attempt.ID != "" {
		attemptLabel = fmt.Sprintf("#%d  %s", attempt.Number, attempt.ID)
	}
	output := firstNonempty(strings.Join(logs, "\n"), "Waiting for log output.")
	return fmt.Sprintf("Task: %s\nAttempt: %s\nPod: %s\nBuffered: %d / %d lines\nFollow: %s\n\n%s",
		firstNonempty(detail.Task.ID, "—"), attemptLabel, pod, len(logs), capacity, follow, output)
}

func formatEvents(events []tui.Event) string {
	if len(events) == 0 {
		return "No lifecycle events."
	}
	lines := make([]string, 0, len(events))
	for _, event := range events {
		transition := event.ToState
		if event.FromState != "" {
			transition = event.FromState + " → " + event.ToState
		}
		lines = append(lines, fmt.Sprintf("%s  %s  %s", event.OccurredAt.Local().Format("2006-01-02 15:04:05"), transition, firstNonempty(event.Reason, event.Trigger)))
	}
	return strings.Join(lines, "\n")
}

func formatJob(detail tui.TaskDetail, namespace string) string {
	job := currentAttempt(detail).KubernetesJob
	if job.ResourceIdentity.Name == "" {
		job = detail.Task.KubernetesJob
	}
	return fmt.Sprintf("Name: %s\nNamespace: %s\nState: %s\nReason: %s",
		firstNonempty(job.ResourceIdentity.Name, "—"), firstNonempty(job.ResourceIdentity.Namespace, namespace),
		firstNonempty(job.State, "—"), firstNonempty(job.Reason, job.Message, "—"))
}

func formatPod(detail tui.TaskDetail, namespace string) string {
	pod := currentAttempt(detail).KubernetesPod
	if pod.ResourceIdentity.Name == "" {
		pod = detail.Task.KubernetesPod
	}
	return fmt.Sprintf("Name: %s\nNamespace: %s\nState: %s\nReason: %s",
		firstNonempty(pod.ResourceIdentity.Name, "—"), firstNonempty(pod.ResourceIdentity.Namespace, namespace),
		firstNonempty(pod.State, "—"), firstNonempty(pod.Reason, pod.Message, "—"))
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func compactAge(created time.Time) string {
	if created.IsZero() {
		return "—"
	}
	age := time.Since(created)
	if age < time.Minute {
		return "now"
	}
	if age < time.Hour {
		return fmt.Sprintf("%dm", int(age.Minutes()))
	}
	if age < 24*time.Hour {
		return fmt.Sprintf("%dh", int(age.Hours()))
	}
	return fmt.Sprintf("%dd", int(age.Hours()/24))
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Local().Format("2006-01-02 15:04:05 MST")
}
