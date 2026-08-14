package tui

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/textinput"
)

func (a *application) handleVaxisEvent(event vaxis.Event) (bool, error) {
	switch event := event.(type) {
	case vaxis.Resize:
		a.vx.Resize(event)
	case vaxis.SyncFunc:
		event()
	case vaxis.PasteEndEvent:
		if a.createModal && !a.createPending && !a.createAccepted {
			a.updateCreateInput(event)
		}
	case vaxis.QuitEvent:
		return true, nil
	case vaxis.Mouse:
		if event.EventType != vaxis.EventRelease {
			switch event.Button {
			case vaxis.MouseWheelUp:
				a.logOffset += 3
			case vaxis.MouseWheelDown:
				a.logOffset = max(0, a.logOffset-3)
			default:
			}
		}
	case vaxis.Key:
		if event.EventType == vaxis.EventRelease {
			return false, nil
		}
		return a.handleKey(event)
	}
	return false, nil
}

func (a *application) handleKey(key vaxis.Key) (bool, error) {
	if key.EventType == vaxis.EventPaste {
		if a.createModal && !a.createPending && !a.createAccepted {
			a.updateCreateInput(key)
		}
		return false, nil
	}
	if a.createModal {
		if key.MatchString("Ctrl+c") {
			return true, nil
		}
		return a.handleCreateTaskKey(key)
	}
	if a.themePicker {
		switch {
		case key.MatchString("j"), key.Matches(vaxis.KeyDown):
			a.themeCursor = min(len(themes)-1, a.themeCursor+1)
			a.theme = themeName(a.themeCursor)
		case key.MatchString("k"), key.Matches(vaxis.KeyUp):
			a.themeCursor = max(0, a.themeCursor-1)
			a.theme = themeName(a.themeCursor)
		case key.MatchString("g"):
			a.themeCursor = 0
			a.theme = themeName(a.themeCursor)
		case key.MatchString("G"):
			a.themeCursor = len(themes) - 1
			a.theme = themeName(a.themeCursor)
		case key.Matches(vaxis.KeyEnter):
			a.selectTheme(a.themeCursor)
			a.themePicker = false
		case key.MatchString("t"), key.MatchString("h"), key.MatchString("q"), key.Matches(vaxis.KeyEsc):
			a.theme = a.themePrevious
			a.themePicker = false
		}
		return false, nil
	}
	if a.help {
		if key.MatchString("?") || key.MatchString("q") || key.Matches(vaxis.KeyEsc) {
			a.help = false
		}
		return false, nil
	}
	if a.confirmAction != "" {
		switch {
		case key.MatchString("y"), key.Matches(vaxis.KeyEnter):
			action := a.confirmAction
			a.confirmAction = ""
			a.performAction(action)
		case key.MatchString("n"), key.MatchString("q"), key.Matches(vaxis.KeyEsc):
			if a.confirmAction == "retry" {
				a.message = "restart dismissed"
			} else {
				a.message = "cancellation dismissed"
			}
			a.confirmAction = ""
		}
		return false, nil
	}

	switch {
	case key.MatchString("n"):
		a.openCreateTask()
	case key.MatchString("Ctrl+c"):
		return true, nil
	case key.MatchString("k"), key.Matches(vaxis.KeyUp):
		a.moveSelection(-1)
	case key.MatchString("j"), key.Matches(vaxis.KeyDown):
		a.moveSelection(1)
	case key.MatchString("g"):
		a.moveSelection(-len(a.model.Tasks()))
	case key.MatchString("G"):
		a.moveSelection(len(a.model.Tasks()))
	case key.Matches(vaxis.KeyEnter):
		a.mode = viewDetails
		a.narrowDetail = true
	case key.MatchString("l"):
		a.mode = viewLogs
		a.narrowDetail = true
	case key.MatchString("e"):
		a.mode = viewEvents
		a.narrowDetail = true
	case key.MatchString("d"):
		a.mode = viewJob
		a.narrowDetail = true
	case key.MatchString("p"):
		a.mode = viewPod
		a.narrowDetail = true
	case key.MatchString("w"):
		a.wrapLogs = !a.wrapLogs
		if a.wrapLogs {
			a.message = "wrap: on"
		} else {
			a.message = "wrap: off"
		}
	case key.MatchString("s"):
		return false, a.shell()
	case key.MatchString("r"):
		if a.model.SelectedTaskID() == "" {
			a.message = "no task selected"
		} else {
			a.confirmAction = "retry"
		}
	case key.MatchString("Ctrl+d"):
		if a.model.SelectedTaskID() == "" {
			a.message = "no task selected"
		} else {
			a.confirmAction = "cancel"
		}
	case key.MatchString("R"):
		a.message = "refreshing"
		a.refresh()
	case key.MatchString("t"):
		a.themeCursor = int(a.theme)
		a.themePrevious = a.theme
		a.themePicker = true
	case key.MatchString("?"):
		a.help = true
	case key.MatchString("h"), key.MatchString("q"), key.Matches(vaxis.KeyEsc):
		switch {
		case a.narrowDetail:
			a.narrowDetail = false
			a.mode = viewDetails
		case a.mode != viewDetails:
			a.mode = viewDetails
		default:
			return true, nil
		}
	}
	return false, nil
}

func (a *application) openCreateTask() {
	if a.createRepo != nil {
		a.createModal = true
		return
	}
	a.createModal = true
	a.createField = createRepositoryField
	a.createRepo = textinput.New().SetPrompt("Repository: ")
	a.createPrompt = textinput.New().SetPrompt("Prompt: ")
	a.createEventKey = "tui-" + rand.Text()
	a.createEventPayload = [2]string{}
	a.createError = ""
	a.createAccepted = false
}

func (a *application) resetCreateTask() {
	a.createModal = false
	a.createPending = false
	a.createField = createRepositoryField
	a.createRepo = nil
	a.createPrompt = nil
	a.createEventKey = ""
	a.createEventPayload = [2]string{}
	a.createError = ""
	a.createAccepted = false
}

func (a *application) handleCreateTaskKey(key vaxis.Key) (bool, error) {
	if a.createAccepted {
		a.resetCreateTask()
		return false, nil
	}
	if key.Matches(vaxis.KeyEsc) {
		if a.createPending {
			a.createModal = false
		} else {
			a.resetCreateTask()
		}
		return false, nil
	}
	if a.createPending {
		return false, nil
	}
	switch {
	case len(a.projects) > 0 && a.createField == createRepositoryField && (key.MatchString("j") || key.Matches(vaxis.KeyDown)):
		a.projectCursor = min(len(a.projects)-1, a.projectCursor+1)
	case len(a.projects) > 0 && a.createField == createRepositoryField && (key.MatchString("k") || key.Matches(vaxis.KeyUp)):
		a.projectCursor = max(0, a.projectCursor-1)
	case key.Matches(vaxis.KeyTab):
		a.createField = (a.createField + 1) % 2
	case key.Matches(vaxis.KeyEnter):
		if a.createField == createRepositoryField {
			if len(a.projects) > 0 {
				a.createField = createPromptField
				return false, nil
			}
			if strings.TrimSpace(a.createRepo.String()) == "" {
				a.createError = "repository required"
				return false, nil
			}
			a.createField = createPromptField
			return false, nil
		}
		a.submitCreateTask()
	default:
		a.updateCreateInput(key)
	}
	return false, nil
}

func (a *application) updateCreateInput(event vaxis.Event) {
	input := a.createRepo
	if a.createField == createPromptField {
		input = a.createPrompt
	}
	before := input.String()
	input.Update(event)
	if input.String() != before {
		a.createError = ""
	}
}

func (a *application) submitCreateTask() {
	a.createError = ""
	repository := strings.TrimSpace(a.createRepo.String())
	if len(a.projects) > 0 {
		repository = a.projects[a.projectCursor].Name
	}
	prompt := strings.TrimSpace(a.createPrompt.String())
	if repository == "" {
		a.createError = "repository required"
		return
	}
	if prompt == "" {
		a.createError = "prompt required"
		return
	}
	payload := [2]string{repository, prompt}
	if a.createEventPayload != [2]string{} && a.createEventPayload != payload {
		a.createEventKey = "tui-" + rand.Text()
	}
	a.createEventPayload = payload
	a.createPending = true
	a.message = "create requested"
	eventKey := a.createEventKey
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, a.options.RequestTimeout)
		defer cancel()
		task, err := a.client.CreateTask(ctx, client.CreateTaskRequest{
			Repository: repository, Prompt: prompt, IdempotencyKey: eventKey,
		})
		select {
		case a.actionCh <- actionResult{name: "create", task: task, err: err}:
		case <-a.ctx.Done():
		}
	}()
}

func (a *application) performAction(name string) {
	if a.actionPending {
		return
	}
	taskID := a.model.SelectedTaskID()
	if taskID == "" {
		a.message = "no task selected"
		return
	}
	a.actionPending = true
	a.message = name + " requested"
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, a.options.RequestTimeout)
		defer cancel()
		var task Task
		var err error
		switch name {
		case "retry":
			task, err = a.client.RetryTask(ctx, taskID)
		case "cancel":
			task, err = a.client.CancelTask(ctx, taskID)
		default:
			err = errors.New("unknown action")
		}
		select {
		case a.actionCh <- actionResult{name: name, task: task, err: err}:
		case <-a.ctx.Done():
		}
	}()
}

func (a *application) applyAction(result actionResult) {
	if result.name == "create" {
		a.createPending = false
		if result.err != nil {
			a.message = "create failed"
			a.createError = "create failed: " + shortError(result.err)
			return
		}
		if a.createModal {
			a.createAccepted = true
			a.createError = ""
		}
		existing := a.model.Tasks()
		tasks := make([]Task, 0, min(a.options.TaskLimit, len(existing)+1))
		tasks = append(tasks, result.task)
		for _, task := range existing {
			if task.ID != result.task.ID && len(tasks) < a.options.TaskLimit {
				tasks = append(tasks, task)
			}
		}
		a.model.RefreshTasks(tasks)
		a.selectTask(result.task.ID)
		a.message = "create accepted"
		a.refreshing = false
		a.refresh()
		if !a.createModal {
			a.resetCreateTask()
		}
		return
	}
	a.actionPending = false
	if result.err != nil {
		a.message = result.name + " failed: " + shortError(result.err)
		return
	}
	a.message = result.name + " accepted"
	if result.task.ID == a.model.SelectedTaskID() {
		detail := a.model.Detail()
		previousAttempt := detail.Task.CurrentAttemptID
		detail.Task = result.task
		a.model.SetDetail(detail)
		if previousAttempt != result.task.CurrentAttemptID {
			a.startLogs(result.task.ID)
		}
	}
	a.refresh()
}

func (a *application) shell() error {
	pod, namespace := a.selectedPod()
	if pod == "" {
		a.message = "selected attempt has no pod"
		return nil
	}
	cmd := shellCommand(a.ctx, a.options.KubeContext, namespace, pod)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = a.options.Stdin, a.options.Stdout, a.options.Stderr
	if err := a.vx.Suspend(); err != nil {
		return fmt.Errorf("suspend terminal for shell: %w", err)
	}
	runErr := cmd.Run()
	resumeErr := a.vx.Resume()
	if resumeErr != nil {
		return fmt.Errorf("resume terminal after shell: %w", resumeErr)
	}
	if runErr != nil {
		a.message = "shell exited: " + shortError(runErr)
	} else {
		a.message = "shell closed"
	}
	return nil
}

func shellCommand(ctx context.Context, kubeContext, namespace, pod string) *exec.Cmd {
	args := make([]string, 0, 10)
	if kubeContext != "" {
		args = append(args, "--context", kubeContext)
	}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	args = append(args, "exec", "-it", pod, "--", "/bin/bash")
	// #nosec G204 -- the executable is fixed and arguments are passed without a shell.
	return exec.CommandContext(ctx, "kubectl", args...)
}

func (a *application) selectedPod() (string, string) {
	detail := a.model.Detail()
	for _, attempt := range detail.Attempts {
		if attempt.ID != detail.Task.CurrentAttemptID {
			continue
		}
		identity := attempt.KubernetesPod.ResourceIdentity
		if identity.Name != "" {
			return identity.Name, firstNonempty(identity.Namespace, detail.Task.KubernetesPod.ResourceIdentity.Namespace, a.options.Namespace)
		}
	}
	identity := detail.Task.KubernetesPod.ResourceIdentity
	return identity.Name, firstNonempty(identity.Namespace, a.options.Namespace)
}

func readerOr(value io.Reader, fallback io.Reader) io.Reader {
	if value == nil {
		return fallback
	}
	return value
}

func writerOr(value io.Writer, fallback io.Writer) io.Writer {
	if value == nil {
		return fallback
	}
	return value
}

func sameFile(value any, expected *os.File) bool {
	file, ok := value.(*os.File)
	return ok && file.Fd() == expected.Fd()
}

func boundedLine(line string) string {
	line = strings.ToValidUTF8(line, "�")
	line = strings.Map(func(r rune) rune {
		if r == '\t' {
			return ' '
		}
		if r == '\x1b' {
			return r
		}
		if r < ' ' || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return -1
		}
		return r
	}, line)
	if len(line) <= maxLogLineBytes {
		return line
	}
	line = line[:maxLogLineBytes]
	line = strings.ToValidUTF8(line, "")
	if idx := strings.LastIndex(line, "\x1b"); idx != -1 {
		if !strings.Contains(line[idx:], "m") {
			line = line[:idx]
			line = strings.ToValidUTF8(line, "")
		}
	}
	return line + "…"
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

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
