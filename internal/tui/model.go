package tui

import (
	"errors"
	"sync"

	"github.com/simpleswe/simpleswe/internal/client"
)

// The model reuses controller data types but has no terminal/rendering
// dependency. Rendering can consume snapshots of this state independently.
type Task = client.Task
type Attempt = client.Attempt
type Event = client.Event
type Job = client.KubernetesJob
type Pod = client.KubernetesPod
type ValidationRun = client.ValidationRun
type GitResult = client.GitResult
type PullRequest = client.PullRequest
type TaskDetail struct {
	Task     Task
	Attempts []Attempt
	Events   []Event
}

type ConnectivityState string

const (
	ConnectivityUnknown   ConnectivityState = "unknown"
	ConnectivityConnected ConnectivityState = "connected"
	ConnectivityLost      ConnectivityState = "lost"
	ConnectivityRestored  ConnectivityState = "restored"
)

type Action string

const (
	ActionNone       Action = "none"
	ActionDetails    Action = "details"
	ActionLogs       Action = "logs"
	ActionEvents     Action = "events"
	ActionJob        Action = "job"
	ActionPod        Action = "pod"
	ActionShell      Action = "shell"
	ActionRetry      Action = "retry"
	ActionCancel     Action = "cancel"
	ActionRefresh    Action = "refresh"
	ActionSearch     Action = "search"
	ActionTheme      Action = "theme"
	ActionNext       Action = "next"
	ActionPrevious   Action = "previous"
	ActionFirst      Action = "first"
	ActionLast       Action = "last"
	ActionHelp       Action = "help"
	ActionBackOrQuit Action = "back-or-quit"
)

type Model struct {
	mu                  sync.RWMutex
	tasks               []Task
	selectedTaskID      string
	logs                logRing
	connectivity        ConnectivityState
	connectivityMessage string
	detail              TaskDetail
}

func NewModel(logCapacity int) *Model {
	if logCapacity < 0 {
		logCapacity = 0
	}
	return &Model{
		logs:         newLogRing(logCapacity),
		connectivity: ConnectivityUnknown,
	}
}

// RefreshTasks replaces the list while retaining the selected task by ID.
func (m *Model) RefreshTasks(tasks []Task) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = append([]Task(nil), tasks...)
	if m.selectedTaskID != "" && hasTask(m.tasks, m.selectedTaskID) {
		return
	}
	if len(m.tasks) == 0 {
		m.selectedTaskID = ""
		return
	}
	m.selectedTaskID = m.tasks[0].ID
}

func (m *Model) Tasks() []Task {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]Task(nil), m.tasks...)
}

func (m *Model) SetSelectedTask(taskID string) {
	m.mu.Lock()
	m.selectedTaskID = taskID
	m.mu.Unlock()
}

func (m *Model) SelectedTaskID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.selectedTaskID
}

func (m *Model) AppendLog(line string) {
	m.mu.Lock()
	m.logs.append(line)
	m.mu.Unlock()
}

func (m *Model) ResetLogs() {
	m.mu.Lock()
	m.logs = newLogRing(cap(m.logs.lines))
	m.mu.Unlock()
}

func (m *Model) Logs() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.logs.values()
}

func (m *Model) SetConnectivity(state ConnectivityState, message ...string) {
	m.mu.Lock()
	m.connectivity = state
	if len(message) > 0 {
		m.connectivityMessage = message[0]
	} else {
		m.connectivityMessage = defaultConnectivityMessage(state)
	}
	m.mu.Unlock()
}

func (m *Model) Connectivity() ConnectivityState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connectivity
}

func (m *Model) ConnectivityMessage() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connectivityMessage
}

func (m *Model) ActionForKey(key rune) Action {
	switch key {
	case '\r', '\n':
		return ActionDetails
	case 'l':
		return ActionLogs
	case 'e':
		return ActionEvents
	case 'j':
		return ActionNext
	case 'k':
		return ActionPrevious
	case 'g':
		return ActionFirst
	case 'G':
		return ActionLast
	case 'd':
		return ActionJob
	case 'p':
		return ActionPod
	case 's':
		return ActionShell
	case 'r':
		return ActionRetry
	case '\x04':
		return ActionCancel
	case 'R':
		return ActionRefresh
	case '/':
		return ActionSearch
	case 't':
		return ActionTheme
	case '?':
		return ActionHelp
	case 'h', 'q':
		return ActionBackOrQuit
	default:
		return ActionNone
	}
}

func (m *Model) SetDetail(detail TaskDetail) {
	m.mu.Lock()
	m.detail = copyDetail(detail)
	m.mu.Unlock()
}

func (m *Model) Detail() TaskDetail {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return copyDetail(m.detail)
}

func hasTask(tasks []Task, id string) bool {
	for _, task := range tasks {
		if task.ID == id {
			return true
		}
	}
	return false
}

func defaultConnectivityMessage(state ConnectivityState) string {
	switch state {
	case ConnectivityConnected:
		return "controller connected"
	case ConnectivityLost:
		return "controller connection lost"
	case ConnectivityRestored:
		return "controller connection restored"
	default:
		return ""
	}
}

func copyDetail(detail TaskDetail) TaskDetail {
	detail.Attempts = append([]Attempt(nil), detail.Attempts...)
	detail.Events = append([]Event(nil), detail.Events...)
	detail.Task.ValidationRuns = append([]ValidationRun(nil), detail.Task.ValidationRuns...)
	return detail
}

type logRing struct {
	lines []string
	next  int
}

func newLogRing(capacity int) logRing {
	return logRing{lines: make([]string, 0, capacity)}
}

func (r *logRing) append(line string) {
	if cap(r.lines) == 0 {
		return
	}
	if len(r.lines) < cap(r.lines) {
		r.lines = append(r.lines, line)
		return
	}
	r.lines[r.next] = line
	r.next = (r.next + 1) % cap(r.lines)
}

func (r logRing) values() []string {
	if len(r.lines) == 0 {
		return nil
	}
	if len(r.lines) < cap(r.lines) || r.next == 0 {
		return append([]string(nil), r.lines...)
	}
	result := make([]string, 0, len(r.lines))
	result = append(result, r.lines[r.next:]...)
	result = append(result, r.lines[:r.next]...)
	return result
}

var errNoSelectedTask = errors.New("no selected task")

// SelectedTask returns the selected task for callers that need a model-level
// lookup without coupling the model to a renderer.
func (m *Model) SelectedTask() (Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, task := range m.tasks {
		if task.ID == m.selectedTaskID {
			return task, nil
		}
	}
	return Task{}, errNoSelectedTask
}
