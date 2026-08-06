package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"go.rockorager.dev/vaxis"
)

func (a *application) handleVaxisEvent(event vaxis.Event) (bool, error) {
	switch event := event.(type) {
	case vaxis.Resize:
		a.vx.Resize(event)
	case vaxis.SyncFunc:
		event()
	case vaxis.QuitEvent:
		return true, nil
	case vaxis.Key:
		if event.EventType == vaxis.EventRelease {
			return false, nil
		}
		return a.handleKey(event)
	}
	return false, nil
}

func (a *application) handleKey(key vaxis.Key) (bool, error) {
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
	if a.confirmCancel {
		switch {
		case key.MatchString("y"), key.Matches(vaxis.KeyEnter):
			a.confirmCancel = false
			a.performAction("cancel")
		case key.MatchString("n"), key.MatchString("q"), key.Matches(vaxis.KeyEsc):
			a.confirmCancel = false
			a.message = "cancellation dismissed"
		}
		return false, nil
	}

	switch {
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
	case key.MatchString("s"):
		return false, a.shell()
	case key.MatchString("r"):
		a.performAction("retry")
	case key.MatchString("Ctrl+d"):
		if a.model.SelectedTaskID() == "" {
			a.message = "no task selected"
		} else {
			a.confirmCancel = true
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
		if r < ' ' || r == 0x7f {
			return -1
		}
		return r
	}, line)
	if len(line) <= maxLogLineBytes {
		return line
	}
	line = line[:maxLogLineBytes]
	return strings.ToValidUTF8(line, "") + "…"
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
