package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/term"
)

func TestDrawRendersViewsLayoutsAndOverlays(t *testing.T) {
	vx, console := newTestVaxis(t, 120, 30)
	fixture := newControllerFixture(t)
	model := NewModel(4)
	other := fixture.task
	other.ID, other.State, other.Repository = "task-2-long-id", "failed", "acme/other"
	model.RefreshTasks([]Task{fixture.task, other})
	model.SetDetail(TaskDetail{Task: fixture.task, Attempts: []Attempt{fixture.attempt}, Events: []Event{fixture.event}})
	model.SetConnectivity(ConnectivityConnected)
	model.AppendLog("build started")
	model.AppendLog("tests passed")
	app := &application{
		vx: vx, model: model, mode: viewDetails, message: "ready", logStop: func() {},
		options: Options{Address: "http://controller", KubeContext: "dev", Namespace: "workers", LogCapacity: 4},
	}
	terminal := term.New()
	terminal.Resize(120, 30)

	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "simpleswe", "TASKS", "TASK / ATTEMPT", "LOGS", "task-1")

	for _, test := range []struct {
		mode viewMode
		text string
	}{{viewLogs, "LOG STREAM"}, {viewEvents, "EVENTS"}, {viewJob, "JOB"}, {viewPod, "POD"}} {
		console.resetOutput()
		app.mode = test.mode
		app.draw()
		assertOutputContains(t, renderedScreen(console, terminal), test.text)
	}

	console.resetOutput()
	app.help = true
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "SIMPLESWE HELP", "Cancellation always asks")
	app.help = false
	console.resetOutput()
	app.confirmCancel = true
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "CONFIRM CANCELLATION", "Cancel task task-1")
	app.confirmCancel = false
	console.resetOutput()
	app.themePicker = true
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "THEMES", "Simpleswe Dark", "Tokyo Night")
	app.themePicker = false
	app.selectTheme(1)
	console.resetOutput()
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "theme:Simpleswe Light")

	vx.Resize(vaxis.Resize{Cols: 60, Rows: 20})
	terminal.Resize(60, 20)
	console.resetOutput()
	app.narrowDetail = false
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "TASKS")
	console.resetOutput()
	app.narrowDetail, app.mode = true, viewLogs
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "LOG STREAM")

	vx.Resize(vaxis.Resize{Cols: 80, Rows: 4})
	terminal.Resize(80, 4)
	console.resetOutput()
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "Terminal too small")
	vx.Resize(vaxis.Resize{Cols: 80, Rows: 1})
	app.draw()
	vx.Resize(vaxis.Resize{})
	app.draw()
}

func TestDrawEmptyAndFallbackViews(t *testing.T) {
	vx, console := newTestVaxis(t, 80, 18)
	model := NewModel(2)
	app := &application{vx: vx, model: model, options: Options{Namespace: "fallback", LogCapacity: 2}}
	root := vx.Window()
	terminal := term.New()
	terminal.Resize(80, 18)

	app.drawTasks(root)
	app.drawDetails(root)
	app.drawEvents(root)
	app.drawLogs(root)
	app.drawJob(root)
	app.drawPod(root)
	app.drawLogSummary(root)
	vx.Render()
	assertOutputContains(t, renderedScreen(console, terminal), "LOG STREAM", "Buffered", "Follow")

	console.resetOutput()
	model.RefreshTasks([]Task{{ID: "only-id", State: "queued"}})
	model.SetDetail(TaskDetail{Task: Task{
		ID: "only-id", State: "queued", Prompt: "", KubernetesJob: Job{
			State: "failed", Reason: "", Message: "job message", ResourceIdentity: client.ResourceIdentity{Name: "job-task"},
		}, KubernetesPod: Pod{State: "pending", Message: "pod message", ResourceIdentity: client.ResourceIdentity{Name: "pod-task"}},
	}})
	app.drawJob(root)
	vx.Render()
	assertOutputContains(t, renderedScreen(console, terminal), "job-task", "job message")
	app.drawDetails(root)
	app.drawPod(root)
	vx.Render()
	assertOutputContains(t, renderedScreen(console, terminal), "pod-task", "pod message")

	app.drawOverlay(root, 7, 3, "ignored", []string{"ignored"})
	if got := app.drawField(root, root.Height, "ignored", "ignored", vaxis.Style{}); got != root.Height {
		t.Fatalf("drawField outside window = %d", got)
	}
	app.drawTasks(vaxis.Window{Vx: vx})
	app.drawLogs(vaxis.Window{Vx: vx})
}

func TestRenderHelpers(t *testing.T) {
	now := time.Now()
	app := &application{}
	palette := app.colors()
	for _, test := range []struct {
		state string
		want  vaxis.Style
	}{{"running", palette.ok}, {"FAILED", palette.bad}, {"pending", palette.warn}, {"mystery", palette.info}} {
		if got := app.stateStyle(test.state); got != test.want {
			t.Errorf("stateStyle(%q) = %#v, want %#v", test.state, got, test.want)
		}
	}
	if len(themes) < 8 {
		t.Fatalf("themes = %d, want at least 8", len(themes))
	}
	names := make(map[string]bool, len(themes))
	for index, theme := range themes {
		if theme.name == "" || theme.base.Foreground == vaxis.ColorDefault || theme.base.Background == vaxis.ColorDefault {
			t.Errorf("theme %d is incomplete: %#v", index, theme)
		}
		if names[theme.name] {
			t.Errorf("duplicate theme name %q", theme.name)
		}
		names[theme.name] = true
	}
	app.selectTheme(len(themes) - 1)
	if app.colors().name != "Tokyo Night" || app.message != "theme: Tokyo Night" {
		t.Fatalf("selected theme = %q, message %q", app.colors().name, app.message)
	}
	app.selectTheme(-1)
	if app.colors().name != "Tokyo Night" {
		t.Fatal("invalid theme selection changed theme")
	}
	base := vaxis.Style{Foreground: vaxis.ColorWhite, Background: vaxis.ColorNavy, Attribute: vaxis.AttrBold}
	accent := vaxis.Style{Foreground: vaxis.ColorRed, Attribute: vaxis.AttrItalic}
	if got := mergeStyle(base, accent); got.Foreground != vaxis.ColorRed || got.Background != vaxis.ColorNavy || got.Attribute != vaxis.AttrBold|vaxis.AttrItalic {
		t.Fatalf("mergeStyle = %#v", got)
	}
	if got := mergeStyle(base, vaxis.Style{}); got.Foreground != vaxis.ColorWhite {
		t.Fatalf("mergeStyle default accent = %#v", got)
	}

	detail := TaskDetail{Task: Task{CurrentAttemptID: "second"}, Attempts: []Attempt{{ID: "first"}, {ID: "second"}}}
	if got := currentAttempt(detail).ID; got != "second" {
		t.Fatalf("currentAttempt selected = %q", got)
	}
	detail.Task.CurrentAttemptID = "missing"
	if got := currentAttempt(detail).ID; got != "second" {
		t.Fatalf("currentAttempt fallback = %q", got)
	}
	if got := currentAttempt(TaskDetail{}).ID; got != "" {
		t.Fatalf("empty currentAttempt = %q", got)
	}
	if selectedIndex([]Task{{ID: "a"}, {ID: "b"}}, "b") != 1 || selectedIndex(nil, "missing") != 0 {
		t.Fatal("selectedIndex returned wrong index")
	}

	for input, want := range map[string]string{
		"https://example.test/acme/widget.git": "acme/widget",
		"team/project/repository":              "project/repository",
		"":                                     "",
	} {
		if got := compactRepository(input); got != want {
			t.Errorf("compactRepository(%q) = %q, want %q", input, got, want)
		}
	}
	if shortID("12345678") != "12345678" || shortID("123456789") != "12345678" {
		t.Fatal("shortID did not enforce eight characters")
	}
	for _, test := range []struct {
		value time.Time
		want  string
	}{{time.Time{}, "—"}, {now.Add(time.Minute), "now"}, {now.Add(-2*time.Minute - 5*time.Second), "2m"}, {now.Add(-2*time.Hour - time.Minute), "2h"}, {now.Add(-48*time.Hour - time.Minute), "2d"}} {
		if got := compactAge(test.value); got != test.want {
			t.Errorf("compactAge(%v) = %q, want %q", test.value, got, test.want)
		}
	}
	if formatTime(time.Time{}) != "—" || !strings.Contains(formatTime(time.Date(2026, 8, 6, 1, 2, 3, 0, time.UTC)), "2026-08-06") {
		t.Fatal("formatTime returned unexpected text")
	}
	values := []string{"a", "b", "c"}
	if got := rangeSlice(values, 1); strings.Join(got, "") != "bc" {
		t.Fatalf("rangeSlice valid = %#v", got)
	}
	if rangeSlice(values, -1)[0] != "a" || rangeSlice(values, len(values))[0] != "a" {
		t.Fatal("rangeSlice invalid start should return all values")
	}
}

func assertOutputContains(t *testing.T, output string, values ...string) {
	t.Helper()
	for _, value := range values {
		if !strings.Contains(output, value) {
			t.Errorf("rendered output does not contain %q: %q", value, output)
		}
	}
}

func renderedScreen(console *testConsole, terminal *term.Model) string {
	terminal.WriteString(console.output())
	console.resetOutput()
	return terminal.String()
}
