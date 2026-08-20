package tui

import (
	"strings"
	"testing"

	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/term"
)

func TestTaskSearchFiltersVisibleTasksAndSelection(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	tasks := []Task{
		{ID: "task-widget", Repository: "acme/widget", Prompt: "fix login", State: "running"},
		{ID: "task-api", Repository: "acme/service", Prompt: "update API docs", State: "failed"},
		{ID: "task-release", Repository: "acme/web", PRTitle: "Release candidate", State: "queued"},
	}
	app.model.RefreshTasks(tasks)

	pressKey(t, app, key('/'))
	if !app.searching || app.searchInput == nil {
		t.Fatal("/ did not open task search")
	}
	pressText(t, app, "API")
	if got := app.visibleTasks(); len(got) != 1 || got[0].ID != "task-api" {
		t.Fatalf("API search = %#v, want task-api", got)
	}
	if got := app.model.SelectedTaskID(); got != "task-api" {
		t.Fatalf("search selection = %q, want task-api", got)
	}
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	if app.searching || app.searchQuery() != "API" {
		t.Fatalf("committed search = active %v, query %q", app.searching, app.searchQuery())
	}
	app.moveSelection(1)
	if got := app.model.SelectedTaskID(); got != "task-api" {
		t.Fatalf("filtered navigation selected %q, want task-api", got)
	}

	pressKey(t, app, key('/'))
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.searching || app.searchQuery() != "" || len(app.visibleTasks()) != len(tasks) {
		t.Fatalf("cleared search = active %v, query %q, tasks %#v", app.searching, app.searchQuery(), app.visibleTasks())
	}
}

func TestTaskSearchMatchesFieldsCaseInsensitively(t *testing.T) {
	tasks := []Task{{
		ID: "Task-123", Repository: "Acme/Widget", Prompt: "Fix OAuth", PRTitle: "Login Repair", State: "RUNNING",
		GitResult: GitResult{Branch: "feature/oauth"}, PullRequest: PullRequest{Number: 246, HeadBranch: "users/tony/login"},
	}}
	for _, query := range []string{"task-123", "widget", "oauth", "repair", "running", "  ACME  ", "246", "#246", "feature/oauth", "users/tony/login"} {
		if got := filterTasks(tasks, query); len(got) != 1 {
			t.Errorf("filterTasks(%q) = %#v, want task", query, got)
		}
	}
	if got := filterTasks(tasks, "missing"); len(got) != 0 {
		t.Fatalf("missing search = %#v, want no tasks", got)
	}
}

func TestTaskSearchNoMatchesClearsAndRestoresSelection(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.model.RefreshTasks([]Task{fixture.task})
	app.model.SetDetail(TaskDetail{Task: fixture.task})
	app.model.AppendLog("line")
	logStopped := false
	app.logStop = func() { logStopped = true }

	pressKey(t, app, key('/'))
	pressText(t, app, "missing")
	if app.model.SelectedTaskID() != "" || app.model.Detail().Task.ID != "" || app.model.Logs() != nil || !logStopped {
		t.Fatalf("no-match state = selected %q, detail %#v, logs %#v, stopped %v", app.model.SelectedTaskID(), app.model.Detail(), app.model.Logs(), logStopped)
	}

	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if got := app.model.SelectedTaskID(); got != fixture.task.ID {
		t.Fatalf("clearing search selected %q, want %q", got, fixture.task.ID)
	}
}

func TestDrawTaskSearchInputAndFilteredCount(t *testing.T) {
	vx, console := newTestVaxis(t, 80, 18)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	other := fixture.task
	other.ID, other.Repository = "task-other", "acme/other"
	app.model.RefreshTasks([]Task{fixture.task, other})
	terminal := term.New()
	terminal.Resize(80, 18)

	pressKey(t, app, key('/'))
	pressText(t, app, "widget")
	app.draw()
	output := renderedScreen(console, terminal)
	assertOutputContains(t, output, "/ widget", "1/2", "acme/widget", "/ search")
	if strings.Contains(output, "acme/other") {
		t.Fatalf("filtered task remained visible: %q", output)
	}

	app.help = true
	app.searching = false
	console.resetOutput()
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "/       search tasks")
}
