package tui

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/term"
)

func TestCreateTaskModalInputAndControls(t *testing.T) {
	vx, console := newTestVaxis(t, 100, 25)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	terminal := term.New()
	terminal.Resize(100, 25)

	app.draw()
	if output := renderedScreen(console, terminal); !strings.Contains(output, " n create") {
		t.Fatalf("footer does not advertise create task: %q", output)
	}
	app.help = true
	app.draw()
	if output := renderedScreen(console, terminal); !strings.Contains(output, "n       create task") {
		t.Fatalf("help does not advertise create task: %q", output)
	}
	app.help = false

	app.actionPending = true
	pressKey(t, app, key('n'))
	app.draw()
	output := renderedScreen(console, terminal)
	assertOutputContains(t, output, "CREATE TASK", "Repository", "Prompt", "tab next field", "enter submit", "esc cancel")
	if strings.Contains(output, "creating task") {
		t.Fatal("unrelated action made create modal look pending")
	}
	app.actionPending = false

	pressText(t, app, "   ")
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	if app.createError != "repository required" {
		t.Fatalf("blank repository error = %q", app.createError)
	}
	assertNoCreateRequest(t, fixture)
	for range 3 {
		pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyBackspace})
	}

	longRepository := "https://example.test/acme/" + strings.Repeat("界", 40) + "/widget.git"
	pressText(t, app, longRepository)
	if got := app.createRepo.String(); got != longRepository {
		t.Fatalf("long repository input = %q, want %q", got, longRepository)
	}
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyTab})
	pressText(t, app, "fix 👩‍💻")
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyBackspace})
	if got := app.createPrompt.String(); got != "fix " {
		t.Fatalf("grapheme backspace left %q", got)
	}
	pressText(t, app, "é")
	app.draw()
	output = renderedScreen(console, terminal)
	assertOutputContains(t, output, "widget.git", "fix é")

	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.createModal || app.createError != "" || app.createRepo != nil || app.createPrompt != nil {
		t.Fatal("escape did not reset create-task modal")
	}
	assertNoCreateRequest(t, fixture)

	pressKey(t, app, key('n'))
	if quit, err := app.handleKey(vaxis.Key{Keycode: 'c', Modifiers: vaxis.ModCtrl}); err != nil || !quit {
		t.Fatalf("Ctrl+c in create modal = quit %v, error %v", quit, err)
	}
}

func TestCreateTaskBracketedPasteDoesNotTriggerShortcuts(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.openCreateTask()
	app.createError = "old error"

	pasted := []vaxis.Key{
		{Keycode: vaxis.KeyTab, Text: "\t", EventType: vaxis.EventPaste},
		{Keycode: vaxis.KeyEnter, Text: "\n", EventType: vaxis.EventPaste},
		{Keycode: 'c', Text: "\x03", Modifiers: vaxis.ModCtrl, EventType: vaxis.EventPaste},
		{Keycode: vaxis.KeyEsc, Text: "\x1b", EventType: vaxis.EventPaste},
	}
	for _, event := range pasted {
		if quit, err := app.handleVaxisEvent(event); err != nil || quit {
			t.Fatalf("pasted %s = quit %v, error %v", event.String(), quit, err)
		}
		if !app.createModal || app.createField != createRepositoryField || app.createPending {
			t.Fatalf("pasted shortcut changed modal state: open %v, field %d, pending %v", app.createModal, app.createField, app.createPending)
		}
	}
	if got := app.createRepo.String(); got != "" {
		t.Fatalf("paste committed before PasteEndEvent: %q", got)
	}
	if quit, err := app.handleVaxisEvent(vaxis.PasteEndEvent{}); err != nil || quit {
		t.Fatalf("PasteEndEvent = quit %v, error %v", quit, err)
	}
	const wantPaste = "        \n\x03\x1b"
	if got := app.createRepo.String(); got != wantPaste {
		t.Fatalf("pasted input = %q, want %q", got, wantPaste)
	}
	if !app.createModal || app.createField != createRepositoryField || app.createPending || app.createPrompt.String() != "" || app.createError != "" {
		t.Fatalf("completed paste changed modal state: open %v, field %d, pending %v, prompt %q, error %q", app.createModal, app.createField, app.createPending, app.createPrompt.String(), app.createError)
	}
	assertNoCreateRequest(t, fixture)

	app.createPending = true
	if quit, err := app.handleVaxisEvent(vaxis.Key{Keycode: vaxis.KeyEnter, Text: "ignored", EventType: vaxis.EventPaste}); err != nil || quit {
		t.Fatalf("pending paste = quit %v, error %v", quit, err)
	}
	app.createPending = false
	if quit, err := app.handleVaxisEvent(vaxis.PasteEndEvent{}); err != nil || quit {
		t.Fatalf("PasteEndEvent after pending paste = quit %v, error %v", quit, err)
	}
	if got := app.createRepo.String(); got != wantPaste {
		t.Fatalf("pending paste changed input to %q, want %q", got, wantPaste)
	}
}

func TestPastedGlobalShortcutsAreIgnoredOutsideCreateModal(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.message = "unchanged"

	for _, value := range []rune{'r', 'q', 's', 'n'} {
		key := vaxis.Key{Keycode: value, Text: string(value), EventType: vaxis.EventPaste}
		if quit, err := app.handleKey(key); err != nil || quit {
			t.Fatalf("pasted %q = quit %v, error %v", value, quit, err)
		}
	}
	if app.message != "unchanged" || app.createModal || app.actionPending {
		t.Fatalf("pasted shortcut changed state: message %q, modal %v, action pending %v", app.message, app.createModal, app.actionPending)
	}
}

func TestCreateTaskSubmissionIsSingleFlightAndImmediatelySelectsAndWatchesTask(t *testing.T) {
	vx, console := newTestVaxis(t, 100, 25)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	app.options.RefreshInterval = 24 * time.Hour
	app.model.RefreshTasks([]Task{fixture.task})
	terminal := term.New()
	terminal.Resize(100, 25)

	enterCreateTask(t, app, "https://git.example/acme/new.git", "fix the new task")
	request := receive(t, fixture.createRequestCh)
	if request.Repository != "https://git.example/acme/new.git" || request.Prompt != "fix the new task" {
		t.Fatalf("create request = %#v", request)
	}
	if !strings.HasPrefix(request.IdempotencyKey, "tui-") {
		t.Fatalf("create idempotency key = %q, want nonempty tui- prefix", request.IdempotencyKey)
	}
	if !app.createPending || app.actionPending {
		t.Fatalf("create pending = %v, action pending = %v", app.createPending, app.actionPending)
	}
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "Esc hides; request continues")

	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	assertNoCreateRequest(t, fixture)
	result := receive(t, app.actionCh)
	if result.err != nil {
		t.Fatalf("create result = %#v", result)
	}
	app.actionPending = true
	app.applyAction(result)

	if app.createPending || !app.createModal || !app.createAccepted || !app.actionPending || app.message != "create accepted" {
		t.Fatalf("applied create = create pending %v, modal %v, accepted %v, action pending %v, message %q", app.createPending, app.createModal, app.createAccepted, app.actionPending, app.message)
	}
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "Task accepted.", "Press any key to close.")
	app.actionPending = false
	if got := app.model.SelectedTaskID(); got != "task-created" {
		t.Fatalf("selected task = %q, want task-created", got)
	}
	if detail := app.model.Detail(); detail.Task.ID != "task-created" {
		t.Fatalf("created detail = %#v", detail)
	}
	if tasks := app.model.Tasks(); len(tasks) != 2 || tasks[0].ID != "task-created" {
		t.Fatalf("visible tasks after create = %#v", tasks)
	}
	if got := receive(t, fixture.detailRequestCh); got != "task-created" {
		t.Fatalf("immediate detail request = %q, want task-created", got)
	}
	if got := receive(t, fixture.logRequestCh); got != "task-created" {
		t.Fatalf("immediate log request = %q, want task-created", got)
	}

	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	if app.createModal || app.createAccepted || app.createRepo != nil || app.createPrompt != nil || app.createError != "" {
		t.Fatal("accepted create modal was not reset on dismissal")
	}
	app.openCreateTask()
	if app.createRepo.String() != "" || app.createPrompt.String() != "" || app.createError != "" {
		t.Fatal("create modal was not reset after success")
	}
}

func TestCreateTaskConfiguredProjectSubmitsName(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.projects = []client.Project{{
		Name:       "acme/widget",
		Repository: "ssh://git@example.test/acme/widget.git",
	}}

	app.openCreateTask()
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	pressText(t, app, "fix configured project")
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})

	request := receive(t, fixture.createRequestCh)
	if request.Repository != "acme/widget" || request.Prompt != "fix configured project" {
		t.Fatalf("configured project create request = %#v", request)
	}
}

func TestCreateTaskSuccessAfterPendingModalIsClosedDoesNotReopenIt(t *testing.T) {
	fixture := newControllerFixture(t)
	releaseCreate := make(chan struct{}, 1)
	fixture.createBlock = releaseCreate
	app := newTestApplication(t, fixture.client())
	app.model.RefreshTasks([]Task{fixture.task})

	enterCreateTask(t, app, "https://git.example/acme/new.git", "finish in background")
	request := receive(t, fixture.createRequestCh)
	_ = receive(t, fixture.createBlocked)
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.createModal || app.createAccepted || !app.createPending {
		t.Fatalf("closed pending modal = modal %v, accepted %v, pending %v", app.createModal, app.createAccepted, app.createPending)
	}
	if app.createRepo == nil || app.createPrompt == nil {
		t.Fatal("closing pending modal discarded create inputs")
	}
	if app.createRepo.String() != request.Repository || app.createPrompt.String() != request.Prompt ||
		app.createEventKey != request.IdempotencyKey || app.createEventPayload != [2]string{request.Repository, request.Prompt} {
		t.Fatalf("closed pending create state = repository %q, prompt %q, key %q, payload %#v; want request %#v",
			app.createRepo.String(), app.createPrompt.String(), app.createEventKey, app.createEventPayload, request)
	}

	releaseCreate <- struct{}{}
	result := receive(t, app.actionCh)
	if result.err != nil {
		t.Fatalf("create result: %v", result.err)
	}
	app.applyAction(result)

	if app.createPending {
		t.Error("completed background create remained pending")
	}
	if app.model.SelectedTaskID() != "task-created" || !hasTask(app.model.Tasks(), "task-created") {
		t.Errorf("created task did not update selection/list: selected %q, tasks %#v", app.model.SelectedTaskID(), app.model.Tasks())
	}
	if got := receive(t, fixture.detailRequestCh); got != "task-created" {
		t.Errorf("detail refresh = %q, want task-created", got)
	}
	if got := receive(t, fixture.logRequestCh); got != "task-created" {
		t.Errorf("log refresh = %q, want task-created", got)
	}
	for {
		detail := receive(t, app.detailCh)
		app.applyDetail(detail)
		if detail.generation == app.detailGen {
			break
		}
	}
	if app.model.Detail().Task.ID != "task-created" {
		t.Errorf("detail after create = %#v, want task-created", app.model.Detail())
	}
	for len(app.model.Logs()) == 0 {
		app.applyLog(receive(t, app.logsCh))
	}
	if logs := app.model.Logs(); len(logs) != 1 || logs[0] != "created line" {
		t.Errorf("logs after create = %#v, want created line", logs)
	}
	tasks := receive(t, app.tasksCh)
	app.applyTasks(tasks)
	if !hasTask(app.model.Tasks(), "task-created") {
		t.Errorf("refreshed task list = %#v, want task-created", app.model.Tasks())
	}
	if app.createModal || app.createAccepted {
		t.Errorf("background success reopened closed modal: modal %v, accepted %v", app.createModal, app.createAccepted)
	}
	if app.createPending || app.createRepo != nil || app.createPrompt != nil || app.createEventKey != "" || app.createEventPayload != [2]string{} || app.createError != "" {
		t.Errorf("background success retained create state: pending %v, repository %v, prompt %v, key %q, payload %#v, error %q",
			app.createPending, app.createRepo, app.createPrompt, app.createEventKey, app.createEventPayload, app.createError)
	}
	pressKey(t, app, key('n'))
	if app.createRepo == nil || app.createPrompt == nil {
		t.Fatal("new form after background success has no inputs")
	}
	if !app.createModal || app.createRepo.String() != "" || app.createPrompt.String() != "" ||
		!strings.HasPrefix(app.createEventKey, "tui-") || app.createEventKey == request.IdempotencyKey {
		t.Errorf("new form after background success = modal %v, repository %q, prompt %q, key %q; want fresh form and key",
			app.createModal, app.createRepo.String(), app.createPrompt.String(), app.createEventKey)
	}
}

func TestCreateTaskAmbiguousFailureWhileModalHiddenRestoresExactRetry(t *testing.T) {
	fixture := newControllerFixture(t)
	fixture.failCreateAfterPersist.Store(true)
	releaseCreate := make(chan struct{}, 1)
	fixture.createBlock = releaseCreate
	app := newTestApplication(t, fixture.client())

	enterCreateTask(t, app, "https://git.example/acme/new.git", "retry hidden request")
	firstRequest := receive(t, fixture.createRequestCh)
	_ = receive(t, fixture.createBlocked)
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.createModal || !app.createPending {
		t.Fatalf("hidden request = modal %v, pending %v; want closed and pending", app.createModal, app.createPending)
	}
	if app.createRepo == nil || app.createPrompt == nil {
		t.Fatal("hiding pending request discarded create inputs")
	}
	if app.createRepo.String() != firstRequest.Repository || app.createPrompt.String() != firstRequest.Prompt ||
		app.createEventKey != firstRequest.IdempotencyKey || app.createEventPayload != [2]string{firstRequest.Repository, firstRequest.Prompt} {
		t.Fatalf("hidden create state = repository %q, prompt %q, key %q, payload %#v; want request %#v",
			app.createRepo.String(), app.createPrompt.String(), app.createEventKey, app.createEventPayload, firstRequest)
	}

	releaseCreate <- struct{}{}
	result := receive(t, app.actionCh)
	if result.err == nil || !strings.Contains(result.err.Error(), "response lost") {
		t.Fatalf("hidden create result = %#v; want ambiguous failure", result)
	}
	app.applyAction(result)
	if app.createModal || app.createPending || !strings.Contains(app.createError, "response lost") {
		t.Fatalf("hidden failure = modal %v, pending %v, error %q", app.createModal, app.createPending, app.createError)
	}
	if app.createRepo == nil || app.createPrompt == nil || app.createRepo.String() != firstRequest.Repository ||
		app.createPrompt.String() != firstRequest.Prompt || app.createEventKey != firstRequest.IdempotencyKey ||
		app.createEventPayload != [2]string{firstRequest.Repository, firstRequest.Prompt} {
		t.Fatalf("hidden failure discarded request state: repository %v, prompt %v, key %q, payload %#v",
			app.createRepo, app.createPrompt, app.createEventKey, app.createEventPayload)
	}

	pressKey(t, app, key('n'))
	if app.createRepo == nil || app.createPrompt == nil {
		t.Fatal("restored hidden failure has no inputs")
	}
	if !app.createModal || !strings.Contains(app.createError, "response lost") || app.createEventKey != firstRequest.IdempotencyKey ||
		app.createRepo.String() != firstRequest.Repository || app.createPrompt.String() != firstRequest.Prompt {
		t.Fatalf("restored form = modal %v, repository %q, prompt %q, key %q, error %q; want hidden failed request",
			app.createModal, app.createRepo.String(), app.createPrompt.String(), app.createEventKey, app.createError)
	}
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	secondRequest := receive(t, fixture.createRequestCh)
	if secondRequest != firstRequest {
		t.Fatalf("hidden request retry = %#v, want exact replay %#v", secondRequest, firstRequest)
	}
	result = receive(t, app.actionCh)
	if result.err != nil || result.task.ID != "task-created" {
		t.Fatalf("hidden request retry result = %#v", result)
	}
}

func TestCreateTaskAmbiguousFailureRetryReusesIdempotencyKey(t *testing.T) {
	fixture := newControllerFixture(t)
	fixture.failCreateAfterPersist.Store(true)
	app := newTestApplication(t, fixture.client())

	enterCreateTask(t, app, "https://git.example/acme/new.git", "retry unchanged")
	firstRequest := receive(t, fixture.createRequestCh)
	if !strings.HasPrefix(firstRequest.IdempotencyKey, "tui-") {
		t.Fatalf("first create idempotency key = %q, want nonempty tui- prefix", firstRequest.IdempotencyKey)
	}
	app.applyAction(receive(t, app.actionCh))
	if app.createPending || app.createEventKey != firstRequest.IdempotencyKey || !strings.Contains(app.createError, "response lost") {
		t.Fatalf("ambiguous failure = pending %v, event key %q, error %q", app.createPending, app.createEventKey, app.createError)
	}

	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	secondRequest := receive(t, fixture.createRequestCh)
	if secondRequest != firstRequest {
		t.Fatalf("retry request = %#v, want exact replay %#v", secondRequest, firstRequest)
	}
	result := receive(t, app.actionCh)
	if result.err != nil || result.task.ID != "task-created" {
		t.Fatalf("deduplicated retry result = %#v", result)
	}
}

func TestCreateTaskAmbiguousFailureChangedPayloadUsesNewIdempotencyKey(t *testing.T) {
	const repository = "https://git.example/acme/new.git"
	const prompt = "retry changed"
	for _, test := range []struct {
		name                       string
		edit                       func(*testing.T, *application)
		wantRepository, wantPrompt string
	}{
		{
			name: "repository",
			edit: func(t *testing.T, app *application) {
				pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyTab})
				pressText(t, app, "/edited")
				pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyTab})
			},
			wantRepository: repository + "/edited",
			wantPrompt:     prompt,
		},
		{
			name: "prompt",
			edit: func(t *testing.T, app *application) {
				pressText(t, app, " again")
			},
			wantRepository: repository,
			wantPrompt:     prompt + " again",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newControllerFixture(t)
			fixture.failCreateAfterPersist.Store(true)
			app := newTestApplication(t, fixture.client())

			enterCreateTask(t, app, repository, prompt)
			firstRequest := receive(t, fixture.createRequestCh)
			app.applyAction(receive(t, app.actionCh))
			if !strings.Contains(app.createError, "response lost") {
				t.Fatalf("ambiguous failure error = %q", app.createError)
			}

			test.edit(t, app)
			pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
			secondRequest := receive(t, fixture.createRequestCh)
			if secondRequest.Repository != test.wantRepository || secondRequest.Prompt != test.wantPrompt {
				t.Fatalf("edited retry request = %#v", secondRequest)
			}
			if !strings.HasPrefix(secondRequest.IdempotencyKey, "tui-") || secondRequest.IdempotencyKey == firstRequest.IdempotencyKey {
				t.Fatalf("edited retry idempotency keys = first %q, second %q", firstRequest.IdempotencyKey, secondRequest.IdempotencyKey)
			}
			result := receive(t, app.actionCh)
			if result.err != nil || result.task.Repository != test.wantRepository || result.task.Prompt != test.wantPrompt {
				t.Fatalf("edited retry result = %#v", result)
			}
		})
	}
}

func TestCreateTaskResetGeneratesNewEventKey(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())

	app.openCreateTask()
	first := app.createEventKey
	app.resetCreateTask()
	if app.createEventKey != "" {
		t.Fatalf("reset create event key = %q, want empty", app.createEventKey)
	}
	app.openCreateTask()
	if !strings.HasPrefix(first, "tui-") || !strings.HasPrefix(app.createEventKey, "tui-") || app.createEventKey == first {
		t.Fatalf("create event keys = first %q, second %q", first, app.createEventKey)
	}
}

func TestCreateTaskAcceptedModalConsumesQueuedShortcut(t *testing.T) {
	for _, queued := range []vaxis.Key{{Keycode: vaxis.KeyEsc}, key('q'), key('r')} {
		t.Run(queued.String(), func(t *testing.T) {
			fixture := newControllerFixture(t)
			app := newTestApplication(t, fixture.client())
			created := fixture.task
			created.ID = "task-created"
			fixture.createdTasks = []Task{created}
			app.model.RefreshTasks([]Task{fixture.task})
			app.openCreateTask()
			app.createPending = true

			app.applyAction(actionResult{name: "create", task: created})
			if !app.createModal || !app.createAccepted {
				t.Fatalf("create completion = modal %v, accepted %v", app.createModal, app.createAccepted)
			}
			quit, err := app.handleKey(queued)
			if err != nil || quit {
				t.Fatalf("queued %s = quit %v, error %v", queued.String(), quit, err)
			}
			if app.createModal || app.createAccepted || app.createPending || app.actionPending {
				t.Fatalf("queued %s escaped modal: modal %v, accepted %v, create pending %v, action pending %v", queued.String(), app.createModal, app.createAccepted, app.createPending, app.actionPending)
			}
		})
	}
}

func TestCreateTaskOptimisticListIsNewestFirstDeduplicatedAndLimited(t *testing.T) {
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.options.TaskLimit = 2
	created := fixture.task
	created.ID = "task-created"
	created.Prompt = "returned task"
	fixture.createdTasks = []Task{created}
	app.model.RefreshTasks([]Task{
		{ID: "task-old"},
		{ID: "task-created", Prompt: "stale duplicate"},
		{ID: "task-over-limit"},
	})

	app.applyAction(actionResult{name: "create", task: created})

	tasks := app.model.Tasks()
	if len(tasks) != 2 || tasks[0].ID != "task-created" || tasks[0].Prompt != "returned task" || tasks[1].ID != "task-old" {
		t.Fatalf("optimistic tasks = %#v", tasks)
	}
	if app.model.SelectedTaskID() != "task-created" || app.model.Detail().Task.ID != "task-created" {
		t.Fatalf("created task not selected: selected %q, detail %#v", app.model.SelectedTaskID(), app.model.Detail())
	}
}

func TestControllerFixturePreservesCreatedTaskIdentityAndRoutes(t *testing.T) {
	fixture := newControllerFixture(t)
	api := fixture.client()
	first, err := api.CreateTask(context.Background(), client.CreateTaskRequest{
		Repository: "https://git.example/acme/first.git", Prompt: "first", IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	second, err := api.CreateTask(context.Background(), client.CreateTaskRequest{
		Repository: "https://git.example/acme/second.git", Prompt: "second", IdempotencyKey: "key-2",
	})
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	replay, err := api.CreateTask(context.Background(), client.CreateTaskRequest{
		Repository: "https://git.example/acme/changed.git", Prompt: "changed", IdempotencyKey: "key-1",
	})
	if err != nil {
		t.Fatalf("replay create: %v", err)
	}
	if first.ID != "task-created" || second.ID != "task-created-2" || replay.ID != first.ID || replay.Prompt != first.Prompt {
		t.Fatalf("created tasks = first %#v, second %#v, replay %#v", first, second, replay)
	}

	list, err := api.ListTasks(context.Background(), client.ListOptions{})
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(list.Tasks) != 3 || list.Tasks[0].ID != second.ID || list.Tasks[1].ID != first.ID || list.Tasks[2].ID != fixture.task.ID {
		t.Fatalf("listed tasks = %#v, want newest created tasks before original", list.Tasks)
	}

	for _, task := range []client.Task{first, second} {
		if got, err := api.ShowTask(context.Background(), task.ID); err != nil || got.ID != task.ID {
			t.Fatalf("show %s = %#v, %v", task.ID, got, err)
		}
		if _, err := api.ListAttempts(context.Background(), task.ID, client.ListOptions{}); err != nil {
			t.Fatalf("attempts %s: %v", task.ID, err)
		}
		if _, err := api.ListEvents(context.Background(), task.ID, client.ListOptions{}); err != nil {
			t.Fatalf("events %s: %v", task.ID, err)
		}
		var lines []string
		if err := api.StreamLogs(context.Background(), task.ID, client.LogOptions{Follow: true}, func(line string) error {
			lines = append(lines, line)
			return nil
		}); err != nil || strings.Join(lines, "\n") != "created line" {
			t.Fatalf("logs %s = %#v, %v", task.ID, lines, err)
		}
	}
}

func TestCreateTaskDiscardsBlockedPreCreateListAndRefreshesAfterCreate(t *testing.T) {
	fixture := newControllerFixture(t)
	release := make(chan struct{})
	fixture.listBlock = release
	app := newTestApplication(t, fixture.client())
	app.model.RefreshTasks([]Task{fixture.task})

	app.refresh()
	_ = receive(t, fixture.listBlocked)
	staleGeneration := app.refreshGen

	enterCreateTask(t, app, "https://git.example/acme/new.git", "fix the new task")
	_ = receive(t, fixture.createRequestCh)
	app.applyAction(receive(t, app.actionCh))
	if app.model.SelectedTaskID() != "task-created" || app.model.Detail().Task.ID != "task-created" || len(app.model.Tasks()) != 2 {
		t.Fatalf("create was not immediately visible and selected: tasks %#v, detail %#v", app.model.Tasks(), app.model.Detail())
	}

	fresh := receive(t, app.tasksCh)
	if fresh.generation == staleGeneration || len(fresh.tasks) != 2 || fresh.tasks[0].ID != "task-created" || fresh.tasks[1].ID != fixture.task.ID {
		t.Fatalf("post-create list = generation %d, tasks %#v", fresh.generation, fresh.tasks)
	}
	app.applyTasks(fresh)
	close(release)
	stale := receive(t, app.tasksCh)
	if stale.generation != staleGeneration || hasTask(stale.tasks, "task-created") {
		t.Fatalf("blocked pre-create list = generation %d, tasks %#v", stale.generation, stale.tasks)
	}
	app.applyTasks(stale)

	if got := fixture.listRequests.Load(); got != 2 {
		t.Fatalf("list request count = %d, want pre-create and one post-create request", got)
	}
	if app.model.SelectedTaskID() != "task-created" || app.model.Detail().Task.ID != "task-created" || !hasTask(app.model.Tasks(), "task-created") {
		t.Fatalf("stale list changed created selection: tasks %#v, detail %#v", app.model.Tasks(), app.model.Detail())
	}
}

func TestCreateTaskFailureRetainsModalInputAndDedicatedError(t *testing.T) {
	vx, console := newTestVaxis(t, 100, 25)
	fixture := newControllerFixture(t)
	fixture.failCreate.Store(true)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	app.model.RefreshTasks([]Task{fixture.task})
	terminal := term.New()
	terminal.Resize(100, 25)

	enterCreateTask(t, app, "https://git.example/acme/broken.git", "keep this input")
	_ = receive(t, fixture.createRequestCh)
	app.applyAction(receive(t, app.actionCh))
	wantError := app.createError
	if !app.createModal || app.createPending || !strings.Contains(wantError, "offline") {
		t.Fatalf("failed create = modal %v, pending %v, error %q", app.createModal, app.createPending, wantError)
	}

	app.applyTasks(taskResult{generation: app.refreshGen, err: errors.New("refresh failed")})
	app.applyDetail(detailResult{taskID: "task-1", generation: app.detailGen, err: errors.New("detail failed")})
	app.applyLog(logResult{taskID: "task-1", generation: app.logGen, done: true, err: errors.New("logs failed")})
	if app.createError != wantError {
		t.Fatalf("unrelated state change replaced create error with %q", app.createError)
	}
	if app.createRepo.String() != "https://git.example/acme/broken.git" || app.createPrompt.String() != "keep this input" {
		t.Fatal("failed create did not preserve inputs")
	}
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "CREATE TASK", "https://git.example/acme/broken.git", "keep this input", "offline")

	pressText(t, app, "!")
	if app.createError != "" {
		t.Fatalf("editing did not clear create error: %q", app.createError)
	}
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	if app.createError != "" || !app.createPending {
		t.Fatalf("resubmit = error %q, pending %v", app.createError, app.createPending)
	}
	_ = receive(t, fixture.createRequestCh)
	app.applyAction(receive(t, app.actionCh))
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEsc})
	if app.createModal || app.createError != "" {
		t.Fatal("cancel did not clear create error")
	}
}

func TestCreateTaskModalRemainsVisibleOnSmallTerminals(t *testing.T) {
	vx, console := newTestVaxis(t, 28, 8)
	fixture := newControllerFixture(t)
	app := newTestApplication(t, fixture.client())
	app.vx = vx
	app.openCreateTask()
	terminal := term.New()
	terminal.Resize(28, 8)

	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "CREATE TASK", "Esc cancels")
	app.createPending = true
	console.resetOutput()
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "Esc hides; request continues")
	vx.Resize(vaxis.Resize{Cols: 80, Rows: 4})
	terminal.Resize(80, 4)
	console.resetOutput()
	app.draw()
	assertOutputContains(t, renderedScreen(console, terminal), "Terminal too small", "Esc hides; request continues")
}

func TestCreateTaskAcceptedGuidanceAtOverlayBoundaries(t *testing.T) {
	for _, size := range []struct {
		name       string
		cols, rows int
	}{{"height-five", 80, 5}, {"height-six", 80, 6}, {"narrow", 11, 8}, {"width-twelve", 12, 8}} {
		t.Run(size.name, func(t *testing.T) {
			vx, console := newTestVaxis(t, size.cols, size.rows)
			fixture := newControllerFixture(t)
			app := newTestApplication(t, fixture.client())
			app.vx = vx
			app.openCreateTask()
			app.createAccepted = true
			terminal := term.New()
			terminal.Resize(size.cols, size.rows)

			app.draw()
			output := renderedScreen(console, terminal)
			assertOutputContains(t, output, "CREATE TASK", "accepted", "any key")
			pressKey(t, app, key('n'))
			if app.createModal || app.createAccepted {
				t.Fatal("accepted compact modal did not dismiss on input")
			}
		})
	}
}

func enterCreateTask(t *testing.T, app *application, repository, prompt string) {
	t.Helper()
	pressKey(t, app, key('n'))
	pressText(t, app, repository)
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
	pressText(t, app, prompt)
	pressKey(t, app, vaxis.Key{Keycode: vaxis.KeyEnter})
}

func pressText(t *testing.T, app *application, text string) {
	t.Helper()
	if text != "" {
		pressKey(t, app, vaxis.Key{Keycode: []rune(text)[0], Text: text})
	}
}

func assertNoCreateRequest(t *testing.T, fixture *controllerFixture) {
	t.Helper()
	select {
	case request := <-fixture.createRequestCh:
		t.Fatalf("unexpected create request: %#v", request)
	default:
	}
}
