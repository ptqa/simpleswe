package tui

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
)

func TestHandleVaxisEvents(t *testing.T) {
	vx, _ := newTestVaxis(t, 100, 25)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx

	called := false
	if quit, err := app.handleVaxisEvent(vaxis.SyncFunc(func() { called = true })); err != nil || quit || !called {
		t.Fatalf("SyncFunc = quit %v, error %v, called %v", quit, err, called)
	}
	if quit, err := app.handleVaxisEvent(vaxis.Resize{Cols: 90, Rows: 20}); err != nil || quit {
		t.Fatalf("Resize = quit %v, error %v", quit, err)
	}
	if quit, err := app.handleVaxisEvent(vaxis.QuitEvent{}); err != nil || !quit {
		t.Fatalf("QuitEvent = quit %v, error %v", quit, err)
	}
	if quit, err := app.handleVaxisEvent(vaxis.Key{Keycode: 'q', EventType: vaxis.EventRelease}); err != nil || quit {
		t.Fatalf("released q = quit %v, error %v", quit, err)
	}
	if quit, err := app.handleVaxisEvent(struct{}{}); err != nil || quit {
		t.Fatalf("unknown event = quit %v, error %v", quit, err)
	}
	app.model.RefreshTasks([]Task{{ID: "task-1"}, {ID: "task-2"}})
	selected := app.model.SelectedTaskID()
	if quit, err := app.handleVaxisEvent(vaxis.Mouse{Button: vaxis.MouseWheelUp}); err != nil || quit {
		t.Fatalf("wheel up = quit %v, error %v", quit, err)
	}
	if app.logOffset != 3 || app.model.SelectedTaskID() != selected {
		t.Fatalf("wheel up = offset %d, selected %q", app.logOffset, app.model.SelectedTaskID())
	}
	if quit, err := app.handleVaxisEvent(vaxis.Mouse{Button: vaxis.MouseWheelDown}); err != nil || quit || app.logOffset != 0 {
		t.Fatalf("wheel down = quit %v, error %v, offset %d", quit, err, app.logOffset)
	}
}

func TestHandleKeyTransitions(t *testing.T) {
	vx, _ := newTestVaxis(t, 100, 25)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	app.model.RefreshTasks([]Task{{ID: "task-1"}, {ID: "task-2"}})
	app.model.SetDetail(TaskDetail{})

	app.help = true
	pressKey(t, app, key('?'))
	if app.help {
		t.Fatal("? did not close help")
	}
	app.confirmAction = "cancel"
	pressKey(t, app, key('n'))
	if app.confirmAction != "" || app.message != "cancellation dismissed" {
		t.Fatalf("dismiss cancel = confirm %q, message %q", app.confirmAction, app.message)
	}

	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyDown})
	if got := app.model.SelectedTaskID(); got != "task-2" {
		t.Fatalf("down selected %q, want task-2", got)
	}
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyUp})
	if got := app.model.SelectedTaskID(); got != "task-1" {
		t.Fatalf("up selected %q, want task-1", got)
	}
	pressKey(t, app, key('G'))
	if got := app.model.SelectedTaskID(); got != "task-2" {
		t.Fatalf("G selected %q, want task-2", got)
	}
	pressKey(t, app, key('g'))
	pressKey(t, app, key('j'))
	pressKey(t, app, key('k'))
	if got := app.model.SelectedTaskID(); got != "task-1" {
		t.Fatalf("vim navigation selected %q, want task-1", got)
	}

	for _, test := range []struct {
		key  rune
		mode viewMode
	}{{'l', viewLogs}, {'e', viewEvents}, {'d', viewJob}, {'p', viewPod}, {'\r', viewDetails}} {
		pressKey(t, app, key(test.key))
		if app.mode != test.mode || !app.narrowDetail {
			t.Fatalf("key %q = mode %v, detail %v", test.key, app.mode, app.narrowDetail)
		}
	}

	app.model.SetDetail(TaskDetail{})
	pressKey(t, app, key('s'))
	if app.message != "selected attempt has no pod" {
		t.Fatalf("shell without pod message = %q", app.message)
	}
	app.model.SetSelectedTask("")
	pressKey(t, app, key('r'))
	if app.message != "no task selected" {
		t.Fatalf("retry without task message = %q", app.message)
	}
	pressKey(t, app, vaxis.Key{Keycode: 'd', Modifiers: vaxis.ModCtrl})
	if app.confirmAction != "" || app.message != "no task selected" {
		t.Fatalf("cancel without task = confirm %q, message %q", app.confirmAction, app.message)
	}
	app.model.SetSelectedTask("task-1")
	pressKey(t, app, vaxis.Key{Keycode: 'd', Modifiers: vaxis.ModCtrl})
	if app.confirmAction != "cancel" {
		t.Fatal("ctrl-d did not request confirmation")
	}
	pressKey(t, app, key('n'))
	pressKey(t, app, key('?'))
	if !app.help {
		t.Fatal("? did not open help")
	}
	pressKey(t, app, key('q'))
	if app.help {
		t.Fatal("q did not close help")
	}
	pressKey(t, app, key('t'))
	if !app.themePicker || app.themeCursor != int(app.theme) {
		t.Fatalf("theme picker = open %v, cursor %d", app.themePicker, app.themeCursor)
	}
	pressKey(t, app, key('G'))
	if app.colors().name != "Tokyo Night" {
		t.Fatalf("theme preview = %q, want Tokyo Night", app.colors().name)
	}
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	if app.themePicker || app.colors().name != "Tokyo Night" {
		t.Fatalf("applied theme = open %v, theme %q", app.themePicker, app.colors().name)
	}
	pressKey(t, app, key('t'))
	pressKey(t, app, key('g'))
	pressKey(t, app, key('j'))
	pressKey(t, app, key('k'))
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.themePicker || app.colors().name != "Tokyo Night" {
		t.Fatal("closing theme picker changed applied theme")
	}

	app.narrowDetail, app.mode = true, viewLogs
	pressKey(t, app, key('q'))
	if app.narrowDetail || app.mode != viewDetails {
		t.Fatalf("narrow back = detail %v, mode %v", app.narrowDetail, app.mode)
	}
	app.mode = viewEvents
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.mode != viewDetails {
		t.Fatalf("escape mode = %v, want details", app.mode)
	}
	if quit, err := app.handleKey(key('q')); err != nil || !quit {
		t.Fatalf("q = quit %v, error %v", quit, err)
	}
	if quit, err := app.handleKey(vaxis.Key{Keycode: 'c', Modifiers: vaxis.ModCtrl}); err != nil || !quit {
		t.Fatalf("Ctrl+c = quit %v, error %v", quit, err)
	}
}

func TestRestartRequiresConfirmation(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.model.RefreshTasks([]Task{fixture.task})

	pressKey(t, app, key('r'))
	if app.confirmAction != "retry" || app.actionPending {
		t.Fatalf("restart key = confirm %q, pending %v", app.confirmAction, app.actionPending)
	}
	pressKey(t, app, key('n'))
	if app.confirmAction != "" || app.message != "restart dismissed" {
		t.Fatalf("dismissed restart = confirm %q, message %q", app.confirmAction, app.message)
	}
	pressKey(t, app, key('r'))
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	if app.confirmAction != "" || !app.actionPending {
		t.Fatalf("confirmed restart = confirm %q, pending %v", app.confirmAction, app.actionPending)
	}
	if result := receive(t, app.actionCh); result.name != "retry" || result.err != nil {
		t.Fatalf("restart result = %#v", result)
	}
}

func TestActionsAndActionResults(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.model.RefreshTasks([]Task{fixture.task})
	app.model.SetDetail(TaskDetail{Task: fixture.task, Attempts: []Attempt{fixture.attempt}})

	app.performAction("retry")
	if !app.actionPending || app.message != "retry requested" {
		t.Fatalf("retry request = pending %v, message %q", app.actionPending, app.message)
	}
	app.performAction("cancel")
	result := receive(t, app.actionCh)
	if result.err != nil || result.task.CurrentAttemptID != "attempt-2" {
		t.Fatalf("retry result = %#v", result)
	}
	app.applyAction(result)
	if app.actionPending || app.message != "retry accepted" || app.model.Detail().Task.CurrentAttemptID != "attempt-2" {
		t.Fatalf("applied retry = pending %v, message %q, detail %#v", app.actionPending, app.message, app.model.Detail())
	}

	app.refreshing = false
	app.performAction("cancel")
	result = receive(t, app.actionCh)
	if result.err != nil || result.task.State != "cancelled" {
		t.Fatalf("cancel result = %#v", result)
	}
	app.applyAction(result)
	if app.message != "cancel accepted" {
		t.Fatalf("cancel message = %q", app.message)
	}

	app.refreshing = false
	app.performAction("bogus")
	result = receive(t, app.actionCh)
	if result.err == nil {
		t.Fatal("unknown action unexpectedly succeeded")
	}
	app.applyAction(result)
	if app.message != "bogus failed: unknown action" {
		t.Fatalf("unknown action message = %q", app.message)
	}

	app.applyAction(actionResult{name: "retry", err: errors.New("line one\nline two")})
	if app.message != "retry failed: line one line two" {
		t.Fatalf("failed action message = %q", app.message)
	}
	app.model.SetSelectedTask("")
	app.performAction("retry")
	if app.message != "no task selected" {
		t.Fatalf("no-selection action message = %q", app.message)
	}

	fixture.failRetry.Store(true)
	app.model.SetSelectedTask("task-1")
	app.actionPending = false
	app.performAction("retry")
	result = receive(t, app.actionCh)
	if result.err == nil {
		t.Fatal("controller retry failure was not returned")
	}
}

func TestApplicationRefreshDetailLogsAndSelection(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())

	app.refresh()
	app.refresh()
	if !app.refreshing {
		t.Fatal("refresh did not mark request pending")
	}
	app.applyTasks(receive(t, app.tasksCh))
	if got := app.model.SelectedTaskID(); got != "task-1" {
		t.Fatalf("selected task = %q", got)
	}
	app.applyDetail(receive(t, app.detailCh))
	firstLog := receive(t, app.logsCh)
	app.applyLog(firstLog)
	app.drainLogs()
	if got := app.model.Logs(); !reflect.DeepEqual(got, []string{"first line", "second line"}) {
		t.Fatalf("logs = %#v", got)
	}
	if detail := app.model.Detail(); detail.Task.ID != "task-1" || len(detail.Attempts) != 1 || len(detail.Events) != 1 {
		t.Fatalf("detail = %#v", detail)
	}

	app.applyTasks(taskResult{generation: app.refreshGen, tasks: []Task{fixture.task}})
	app.applyDetail(receive(t, app.detailCh))
	app.applyDetail(detailResult{taskID: "stale", generation: app.detailGen, detail: TaskDetail{Task: Task{ID: "stale"}}})
	app.message = "unchanged"
	app.applyDetail(detailResult{taskID: "task-1", generation: app.detailGen, err: context.Canceled})
	if app.message != "unchanged" {
		t.Fatalf("canceled detail changed message to %q", app.message)
	}
	app.applyDetail(detailResult{taskID: "task-1", generation: app.detailGen, err: errors.New("detail failed")})
	if app.message != "details: detail failed" {
		t.Fatalf("detail error message = %q", app.message)
	}

	app.logGen = 7
	app.model.ResetLogs()
	app.applyLog(logResult{taskID: "other", generation: 7, line: "stale task"})
	app.applyLog(logResult{taskID: "task-1", generation: 6, line: "stale generation"})
	app.applyLog(logResult{taskID: "task-1", generation: 7, line: "kept"})
	app.logStop = func() {}
	app.applyLog(logResult{taskID: "task-1", generation: 7, done: true, err: context.Canceled})
	if app.logStop != nil || !reflect.DeepEqual(app.model.Logs(), []string{"kept"}) {
		t.Fatalf("applied logs = stop %v, lines %#v", app.logStop != nil, app.model.Logs())
	}
	app.applyLog(logResult{taskID: "task-1", generation: 7, done: true, err: errors.New("stream broke")})
	if app.message != "log stream interrupted: stream broke" {
		t.Fatalf("log error message = %q", app.message)
	}
	app.logsCh <- logResult{taskID: "task-1", generation: 7, line: "drained"}
	app.drainLogs()
	if got := app.model.Logs(); !reflect.DeepEqual(got, []string{"kept", "drained"}) {
		t.Fatalf("drained logs = %#v", got)
	}

	app.model.RefreshTasks([]Task{{ID: "task-1"}, {ID: "task-2"}})
	app.model.SetSelectedTask("task-1")
	app.moveSelection(-10)
	app.moveSelection(10)
	if got := app.model.SelectedTaskID(); got != "task-2" {
		t.Fatalf("bounded selection = %q", got)
	}
	app.moveSelection(1)
	app.model.RefreshTasks(nil)
	app.moveSelection(1)

	app.logStop = func() {}
	app.applyTasks(taskResult{generation: app.refreshGen, tasks: nil})
	if app.model.SelectedTaskID() != "" || app.logStop != nil || app.model.Detail().Task.ID != "" {
		t.Fatalf("empty task result left state: selected %q, stop %v, detail %#v", app.model.SelectedTaskID(), app.logStop != nil, app.model.Detail())
	}
	app.applyTasks(taskResult{generation: app.refreshGen, err: errors.New("controller down")})
	if app.model.Connectivity() != ConnectivityLost || app.message != "refresh failed" {
		t.Fatalf("failed refresh = connectivity %q, message %q", app.model.Connectivity(), app.message)
	}
	app.applyTasks(taskResult{generation: app.refreshGen, tasks: nil})
	if app.model.Connectivity() != ConnectivityRestored || app.message != "controller connection restored" {
		t.Fatalf("restored refresh = connectivity %q, message %q", app.model.Connectivity(), app.message)
	}
	app.fetchDetail("")
	app.startLogs("")
}

func TestApplyDetailRejectsOlderGenerationForSameTask(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.model.RefreshTasks([]Task{fixture.task})
	app.model.SetDetail(TaskDetail{Task: Task{ID: fixture.task.ID, CurrentAttemptID: "attempt-1"}})
	app.detailGen = 2

	newest := TaskDetail{Task: Task{ID: fixture.task.ID, CurrentAttemptID: "attempt-2"}}
	app.applyDetail(detailResult{taskID: fixture.task.ID, generation: 2, detail: newest})
	logGeneration := app.logGen
	app.applyDetail(detailResult{
		taskID: fixture.task.ID, generation: 1,
		detail: TaskDetail{Task: Task{ID: fixture.task.ID, CurrentAttemptID: "attempt-1"}},
	})

	if got := app.model.Detail().Task.CurrentAttemptID; got != "attempt-2" {
		t.Fatalf("stale detail replaced current attempt with %q", got)
	}
	if app.logGen != logGeneration {
		t.Fatalf("stale detail restarted logs: generation %d, want %d", app.logGen, logGeneration)
	}
}

func TestCompletedLogStreamIsNotPeriodicallyRestarted(t *testing.T) {
	vx, console := newTestVaxis(t, 80, 18)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	app.mode = viewLogs
	app.model.RefreshTasks([]Task{fixture.task})
	app.model.SetDetail(TaskDetail{Task: fixture.task, Attempts: []Attempt{fixture.attempt}})
	app.model.SetConnectivity(ConnectivityConnected)
	app.message = "ready"
	app.startLogs(fixture.task.ID)

	for {
		result := receive(t, app.logsCh)
		app.applyLog(result)
		if result.done {
			break
		}
	}
	want := []string{"first line", "second line"}
	if got := app.model.Logs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("completed logs = %#v, want %#v", got, want)
	}

	app.draw()
	console.resetOutput()
	logGeneration := app.logGen
	app.options.RefreshInterval = time.Millisecond
	quit := time.AfterFunc(20*time.Millisecond, func() { app.vx.PostEvent(vaxis.QuitEvent{}) })
	defer quit.Stop()
	if err := app.run(); err != nil {
		t.Fatalf("run refresh cycle: %v", err)
	}
	if got := app.model.Logs(); !reflect.DeepEqual(got, want) || app.logGen != logGeneration {
		t.Fatalf("unchanged refresh = logs %#v, generation %d", got, app.logGen)
	}

	console.resetOutput()
	app.applyLog(logResult{taskID: fixture.task.ID, generation: app.logGen, line: "new line"})
	app.draw()
	if got := app.model.Logs(); !reflect.DeepEqual(got, append(want, "new line")) || console.output() == "" {
		t.Fatalf("changed logs = %#v, terminal output bytes %d", got, len(console.output()))
	}
}

func TestRunnerEntrypointsAndDefaults(t *testing.T) {
	if err := (*Runner)(nil).Run(context.Background()); err == nil || err.Error() != "tui client is nil" {
		t.Fatalf("nil runner error = %v", err)
	}
	runner := NewRunner(client.New("http://example.test", nil), Options{})
	var nilContext context.Context
	if err := runner.Run(nilContext); err == nil || err.Error() != "tui context is nil" {
		t.Fatalf("nil context error = %v", err)
	}
	runner.Options = Options{TTY: "/dev/tty", Console: newTestConsole(80, 24, "")}
	if err := runner.Run(context.Background()); err == nil || err.Error() != "tui TTY and Console are mutually exclusive" {
		t.Fatalf("conflicting terminal options error = %v", err)
	}

	if err := Run(context.Background(), "http://example.test", bytes.NewReader(nil), io.Discard, io.Discard); !errors.Is(err, ErrInjectedStreamsUnsupported) {
		t.Fatalf("Run() injected streams error = %v", err)
	}
	if !sameFile(os.Stdin, os.Stdin) || sameFile(bytes.NewReader(nil), os.Stdin) {
		t.Fatal("sameFile did not distinguish process file and injected reader")
	}

	options := (Options{RefreshInterval: -1, RequestTimeout: -1, LogCapacity: -1, TaskLimit: 5}).withDefaults()
	if options.RefreshInterval != defaultRefreshInterval || options.RequestTimeout != defaultRequestTimeout || options.LogCapacity != defaultLogCapacity || options.TaskLimit != 5 || options.Namespace != "default" {
		t.Fatalf("defaults = %#v", options)
	}
}

func TestRunnerRunsWithDeterministicConsole(t *testing.T) {
	fixture := newControllerFixture(t)
	console := newTestConsole(100, 25, "q")
	runner := NewRunner(fixture.client(), Options{
		Console: console, RefreshInterval: time.Hour, RequestTimeout: time.Second, LogCapacity: 4,
	})
	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Runner.Run(): %v", err)
	}
	if !strings.Contains(console.output(), "simpleswe") {
		t.Fatalf("runner did not render header: %q", console.output())
	}
}

func TestActionUtilitiesAndPodResolution(t *testing.T) {
	fixture := newControllerFixture(t)
	app := &application{model: NewModel(1), options: Options{Namespace: "fallback"}}
	app.model.SetDetail(TaskDetail{Task: fixture.task, Attempts: []Attempt{fixture.attempt}})
	if pod, namespace := app.selectedPod(); pod != "pod-1" || namespace != "workers" {
		t.Fatalf("attempt pod = %q/%q", namespace, pod)
	}
	attemptWithoutPod := fixture.attempt
	attemptWithoutPod.KubernetesPod = Pod{}
	app.model.SetDetail(TaskDetail{Task: fixture.task, Attempts: []Attempt{attemptWithoutPod}})
	if pod, namespace := app.selectedPod(); pod != "pod-task" || namespace != "task-ns" {
		t.Fatalf("task pod = %q/%q", namespace, pod)
	}

	command := shellCommand(context.Background(), "dev", "workers", "pod-1")
	wantArgs := []string{"kubectl", "--context", "dev", "--namespace", "workers", "exec", "-it", "pod-1", "--", "/bin/bash"}
	if !reflect.DeepEqual(command.Args, wantArgs) {
		t.Fatalf("shell args = %#v, want %#v", command.Args, wantArgs)
	}
	command = shellCommand(context.Background(), "", "", "pod-1")
	if !reflect.DeepEqual(command.Args, []string{"kubectl", "exec", "-it", "pod-1", "--", "/bin/bash"}) {
		t.Fatalf("minimal shell args = %#v", command.Args)
	}

	invalid := string([]byte{'a', 0xff, '\t', '\n', 'b'})
	if got := boundedLine(invalid); got != "a� b" {
		t.Fatalf("boundedLine invalid/control = %q", got)
	}
	long := strings.Repeat("é", maxLogLineBytes)
	if got := boundedLine(long); len(got) > maxLogLineBytes+len("…") || !strings.HasSuffix(got, "…") {
		t.Fatalf("bounded long line has length %d and suffix %q", len(got), got[len(got)-3:])
	}
	if shortError(nil) != "" || shortError(errors.New("a\nb")) != "a b" {
		t.Fatal("shortError did not normalize nil/newline")
	}
	if got := shortError(errors.New(strings.Repeat("x", 200))); len(got) != 180 || !strings.HasSuffix(got, "…") {
		t.Fatalf("shortError long = length %d, suffix %q", len(got), got[len(got)-3:])
	}
	if got := firstNonempty("", "value", "other"); got != "value" || firstNonempty() != "" {
		t.Fatalf("firstNonempty = %q", got)
	}
	fallbackReader := strings.NewReader("fallback")
	if readerOr(nil, fallbackReader) != fallbackReader || readerOr(fallbackReader, nil) != fallbackReader {
		t.Fatal("readerOr returned wrong reader")
	}
	if writerOr(nil, io.Discard) != io.Discard || writerOr(io.Discard, nil) != io.Discard {
		t.Fatal("writerOr returned wrong writer")
	}
}

func key(value rune) vaxis.Key {
	return vaxis.Key{Keycode: value, Text: string(value)}
}

func pressKey(t *testing.T, app *application, value vaxis.Key) {
	t.Helper()
	if quit, err := app.handleKey(value); err != nil || quit {
		t.Fatalf("handleKey(%s) = quit %v, error %v", value.String(), quit, err)
	}
}
