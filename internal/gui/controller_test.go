package gui

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/simpleswe/simpleswe/internal/client"
	"github.com/simpleswe/simpleswe/internal/tui"
)

func TestOptionsBoundTaskLimitToControllerContract(t *testing.T) {
	for _, test := range []struct {
		name  string
		limit int
		want  int
	}{
		{name: "default", want: 100},
		{name: "within limit", limit: 25, want: 25},
		{name: "above limit", limit: 101, want: 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := (Options{TaskLimit: test.limit}).withDefaults().TaskLimit; got != test.want {
				t.Fatalf("TaskLimit = %d, want %d", got, test.want)
			}
		})
	}
}

func TestControllerRefreshPreservesSelectionAndRejectsStaleResults(t *testing.T) {
	c := newTestController(t, stubClient{})
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}, {ID: "task-2"}})
	c.model.SetSelectedTask("task-2")
	c.refreshGen = 2

	c.applyTasks(context.Background(), taskResult{generation: 1, tasks: []tui.Task{{ID: "stale"}}})
	if got := c.model.SelectedTaskID(); got != "task-2" {
		t.Fatalf("stale refresh selected %q, want task-2", got)
	}

	c.applyTasks(context.Background(), taskResult{generation: 2, tasks: []tui.Task{{ID: "task-3"}, {ID: "task-2", State: "running"}}})
	if got := c.model.SelectedTaskID(); got != "task-2" {
		t.Fatalf("fresh refresh selected %q, want task-2", got)
	}
	if got := c.model.Tasks(); len(got) != 2 || got[1].State != "running" {
		t.Fatalf("fresh tasks = %#v", got)
	}
	if got := c.model.Connectivity(); got != tui.ConnectivityConnected {
		t.Fatalf("successful refresh connectivity = %q, want connected", got)
	}
	c.refreshGen = 3
	c.applyTasks(context.Background(), taskResult{generation: 3, err: errors.New("controller down")})
	if got := c.model.Connectivity(); got != tui.ConnectivityLost {
		t.Fatalf("failed refresh connectivity = %q, want lost", got)
	}
	c.refreshGen = 4
	c.applyTasks(context.Background(), taskResult{generation: 4, tasks: c.model.Tasks()})
	if got := c.model.Connectivity(); got != tui.ConnectivityRestored {
		t.Fatalf("recovered refresh connectivity = %q, want restored", got)
	}

	c.mu.Lock()
	c.detailGen = 2
	c.mu.Unlock()
	c.applyDetail(context.Background(), detailResult{taskID: "task-2", generation: 2, detail: tui.TaskDetail{Task: tui.Task{ID: "task-2", CurrentAttemptID: "attempt-2"}}})
	c.applyDetail(context.Background(), detailResult{taskID: "task-2", generation: 1, detail: tui.TaskDetail{Task: tui.Task{ID: "task-2", CurrentAttemptID: "attempt-1"}}})
	if got := c.model.Detail().Task.CurrentAttemptID; got != "attempt-2" {
		t.Fatalf("stale detail replaced current attempt with %q", got)
	}
}

func TestControllerUsesBoundedRequestsAndCancellation(t *testing.T) {
	listOptions := make(chan client.ListOptions, 1)
	requestDone := make(chan error, 1)
	api := stubClient{listTasks: func(ctx context.Context, options client.ListOptions) (client.TaskList, error) {
		listOptions <- options
		<-ctx.Done()
		requestDone <- ctx.Err()
		return client.TaskList{}, ctx.Err()
	}}
	ctx, cancel := context.WithCancel(context.Background())
	c := newController(ctx, api, Options{TaskLimit: 500})
	t.Cleanup(c.stop)

	c.refresh(context.Background())
	if got := receiveGUI(t, listOptions); got.Limit != 100 {
		t.Fatalf("ListTasks limit = %d, want 100", got.Limit)
	}
	cancel()
	if err := receiveGUI(t, requestDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("request error = %v, want context.Canceled", err)
	}
}

func TestControllerStartStopWithLiveParentCancelsAndJoins(t *testing.T) {
	requestStarted := make(chan struct{})
	requestDone := make(chan struct{})
	api := stubClient{listTasks: func(ctx context.Context, _ client.ListOptions) (client.TaskList, error) {
		close(requestStarted)
		<-ctx.Done()
		close(requestDone)
		return client.TaskList{}, ctx.Err()
	}}
	c := newController(context.Background(), api, Options{RefreshInterval: time.Hour})
	c.start(context.Background())
	receiveGUI(t, requestStarted)

	stopped := make(chan struct{})
	go func() {
		c.stop()
		c.stop()
		close(stopped)
	}()
	receiveGUI(t, requestDone)
	receiveGUI(t, stopped)
}

func TestControllerStopCancelsAndJoinsBlockedWorkers(t *testing.T) {
	started := make(chan struct{}, 4)
	block := func(ctx context.Context) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	api := stubClient{
		listTasks: func(ctx context.Context, _ client.ListOptions) (client.TaskList, error) {
			return client.TaskList{}, block(ctx)
		},
		showTask: func(ctx context.Context, id string) (client.Task, error) {
			return client.Task{ID: id}, block(ctx)
		},
		streamLogs: func(ctx context.Context, _ string, _ client.LogOptions, _ func(string) error) error {
			return block(ctx)
		},
		createTask: func(ctx context.Context, _ client.CreateTaskRequest) (client.Task, error) {
			return client.Task{}, block(ctx)
		},
	}
	c := newController(context.Background(), api, Options{RefreshInterval: time.Hour})
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})
	c.start(context.Background())
	c.selectTask(context.Background(), "task-1")
	c.submitCreate(context.Background(), "repo", "prompt")
	for range 4 {
		receiveGUI(t, started)
	}

	stopped := make(chan struct{})
	go func() { c.stop(); close(stopped) }()
	receiveGUI(t, stopped)
}

func TestControllerStopCannotRaceWithWorkerRegistration(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := newController(context.Background(), stubClient{}, Options{})
	workerReturned := make(chan struct{})
	c.workerLaunchHook = func() {
		close(entered)
		<-release
	}
	launched := make(chan struct{})
	go func() {
		c.runWorker(context.Background(), func(context.Context) { close(workerReturned) })
		close(launched)
	}()
	receiveGUI(t, entered)
	stopped := make(chan struct{})
	go func() { c.stop(); close(stopped) }()
	close(release)
	receiveGUI(t, launched)
	receiveGUI(t, workerReturned)
	receiveGUI(t, stopped)
}

func TestControllerConcurrentStopCallsJoin(t *testing.T) {
	started := make(chan struct{})
	workerDone := make(chan struct{})
	c := newController(context.Background(), stubClient{}, Options{})
	c.runWorker(context.Background(), func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond)
		close(workerDone)
	})
	receiveGUI(t, started)
	stopped := make(chan struct{}, 2)
	go func() { c.stop(); stopped <- struct{}{} }()
	go func() { c.stop(); stopped <- struct{}{} }()
	receiveGUI(t, workerDone)
	receiveGUI(t, stopped)
	receiveGUI(t, stopped)
}

func TestManualRefreshSupersedesInFlightPeriodicRefresh(t *testing.T) {
	requests := make(chan context.Context, 2)
	api := stubClient{listTasks: func(ctx context.Context, _ client.ListOptions) (client.TaskList, error) {
		requests <- ctx
		<-ctx.Done()
		return client.TaskList{}, ctx.Err()
	}}
	c := newTestController(t, api)
	c.refreshPeriodic(context.Background())
	first := receiveGUI(t, requests)
	c.refreshPeriodic(context.Background())
	assertNoValue(t, requests)
	c.refresh(context.Background())
	second := receiveGUI(t, requests)
	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("manual refresh did not cancel the in-flight refresh")
	}
	if second.Err() != nil {
		t.Fatalf("replacement refresh context error = %v", second.Err())
	}
}

func TestTimedOutOperationsClearPendingStateAndCanRetry(t *testing.T) {
	t.Run("refresh", func(t *testing.T) {
		requests := make(chan struct{}, 2)
		api := stubClient{listTasks: func(ctx context.Context, _ client.ListOptions) (client.TaskList, error) {
			requests <- struct{}{}
			<-ctx.Done()
			return client.TaskList{}, ctx.Err()
		}}
		c := newRunningTestController(t, api, Options{RequestTimeout: time.Millisecond})

		c.refreshPeriodic(context.Background())
		receiveGUI(t, requests)
		waitGUICondition(t, func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return !c.refreshing && strings.Contains(c.message, context.DeadlineExceeded.Error())
		})
		c.refreshPeriodic(context.Background())
		receiveGUI(t, requests)
	})

	t.Run("create", func(t *testing.T) {
		requests := make(chan client.CreateTaskRequest, 2)
		api := stubClient{createTask: func(ctx context.Context, request client.CreateTaskRequest) (client.Task, error) {
			requests <- request
			<-ctx.Done()
			return client.Task{}, ctx.Err()
		}}
		c := newRunningTestController(t, api, Options{RequestTimeout: time.Millisecond})

		c.submitCreate(context.Background(), "repo", "prompt")
		first := receiveGUI(t, requests)
		waitGUICondition(t, func() bool {
			c.mu.Lock()
			defer c.mu.Unlock()
			return !c.createPending && strings.Contains(c.createError, context.DeadlineExceeded.Error())
		})
		c.submitCreate(context.Background(), "repo", "prompt")
		if second := receiveGUI(t, requests); second != first {
			t.Fatalf("timed-out create retry = %#v, want retained request %#v", second, first)
		}
	})

	for _, action := range []string{"retry", "cancel"} {
		t.Run(action, func(t *testing.T) {
			requests := make(chan string, 2)
			request := func(ctx context.Context, taskID string) (client.Task, error) {
				requests <- taskID
				<-ctx.Done()
				return client.Task{}, ctx.Err()
			}
			api := stubClient{}
			if action == "retry" {
				api.retryTask = request
			} else {
				api.cancelTask = request
			}
			c := newRunningTestController(t, api, Options{RequestTimeout: time.Millisecond})

			c.performAction(context.Background(), action, "task-1")
			receiveGUI(t, requests)
			waitGUICondition(t, func() bool {
				c.mu.Lock()
				defer c.mu.Unlock()
				return !c.actionPending && strings.Contains(c.message, action+" failed:") &&
					strings.Contains(c.message, context.DeadlineExceeded.Error())
			})
			c.performAction(context.Background(), action, "task-1")
			if taskID := receiveGUI(t, requests); taskID != "task-1" {
				t.Fatalf("timed-out %s retry task = %q, want task-1", action, taskID)
			}
		})
	}
}

func TestSuccessfulOperationContextsCloseAfterDelivery(t *testing.T) {
	refreshCtx := make(chan context.Context, 1)
	detailCtx := make(chan context.Context, 1)
	logCtx := make(chan context.Context, 1)
	api := stubClient{
		listTasks: func(ctx context.Context, _ client.ListOptions) (client.TaskList, error) {
			refreshCtx <- ctx
			return client.TaskList{Tasks: []tui.Task{{ID: "task-1"}}}, nil
		},
		showTask: func(ctx context.Context, id string) (client.Task, error) {
			detailCtx <- ctx
			return client.Task{ID: id}, nil
		},
		streamLogs: func(ctx context.Context, _ string, _ client.LogOptions, _ func(string) error) error {
			logCtx <- ctx
			return nil
		},
	}
	c := newTestController(t, api)
	c.refresh(context.Background())
	refreshRequest := receiveGUI(t, refreshCtx)
	receiveGUI(t, c.tasksCh)
	assertContextDone(t, refreshRequest, "successful refresh")

	c.stateMu.Lock()
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})
	c.fetchDetailLocked(context.Background(), "task-1")
	c.startLogsLocked(context.Background(), "task-1")
	c.stateMu.Unlock()
	detailRequest := receiveGUI(t, detailCtx)
	logRequest := receiveGUI(t, logCtx)
	receiveGUI(t, c.detailCh)
	receiveGUI(t, c.logsCh)
	assertContextDone(t, detailRequest, "successful detail")
	assertContextDone(t, logRequest, "successful logs")
}

func TestControllerLogsAreBoundedSanitizedAndIgnoreStaleStreams(t *testing.T) {
	c := newTestControllerWithOptions(t, stubClient{}, Options{LogCapacity: 2})
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})
	c.logGen = 3

	c.applyLog(logResult{taskID: "task-1", generation: 2, line: "stale"})
	c.applyLog(logResult{taskID: "task-1", generation: 3, line: string([]byte{'a', 0xff, '\t', '\n', 'b'})})
	if got, want := c.model.Logs(), []string{"a� b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sanitized logs = %#v, want %#v", got, want)
	}
	c.applyLog(logResult{taskID: "task-1", generation: 3, line: "second"})
	c.applyLog(logResult{taskID: "task-1", generation: 3, line: "third"})

	if got, want := c.model.Logs(), []string{"second", "third"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("logs = %#v, want %#v", got, want)
	}
	if got := boundedLine(string([]byte{'a', 0xff, '\t', '\n', 'b'})); got != "a� b" {
		t.Fatalf("boundedLine invalid/control = %q", got)
	}
	if got := boundedLine(strings.Repeat("é", maxLogLineBytes)); len(got) > maxLogLineBytes+len("…") || !strings.HasSuffix(got, "…") {
		t.Fatalf("boundedLine long result has length %d and suffix %q", len(got), got[len(got)-3:])
	}
}

func TestBoundedLineStripsANSISequences(t *testing.T) {
	for _, test := range []struct {
		name, input, want string
	}{
		{name: "SGR", input: "before \x1b[31;1mred\x1b[0m after", want: "before red after"},
		{name: "OSC hyperlink BEL", input: "\x1b]8;;https://example.test\aexample\x1b]8;;\a", want: "example"},
		{name: "OSC hyperlink ST", input: "\x1b]8;;https://example.test\x1b\\example\x1b]8;;\x1b\\", want: "example"},
		{name: "C1 CSI and OSC", input: "\u009b32mgreen\u009b0m \u009d0;title\u009ctext", want: "green text"},
		{name: "ESC control strings BEL", input: "a\x1bPdevice\ab\x1bXsos\ac\x1b^private\ad\x1b_application\ae", want: "abcde"},
		{name: "ESC control strings ST", input: "a\x1bPdevice\x1b\\b\x1bXsos\x1b\\c\x1b^private\x1b\\d\x1b_application\x1b\\e", want: "abcde"},
		{name: "C1 control strings BEL", input: "a\u0090device\ab\u0098sos\ac\u009eprivate\ad\u009fapplication\ae", want: "abcde"},
		{name: "C1 control strings ST", input: "a\u0090device\u009cb\u0098sos\u009cc\u009eprivate\u009cd\u009fapplication\u009ce", want: "abcde"},
		{name: "ordinary ESC sequence", input: "before\x1bMafter", want: "beforeafter"},
		{name: "malformed CSI", input: "before\x1b[31", want: "before[31"},
		{name: "malformed OSC", input: "before\x1b]8;;url", want: "before]8;;url"},
		{name: "malformed DCS", input: "before\x1bPpayload", want: "beforePpayload"},
		{name: "lone ESC", input: "before\x1b", want: "before"},
		{name: "invalid UTF-8", input: string([]byte{'a', 0xff, 'b'}), want: "a�b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := boundedLine(test.input); got != test.want {
				t.Fatalf("boundedLine(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
	input := "\x1b[31m" + strings.Repeat("é", maxLogLineBytes) + "\x1b[0m"
	if got := boundedLine(input); len(got) > maxLogLineBytes || !strings.HasSuffix(got, "…") || !utf8.ValidString(got) {
		t.Fatalf("truncated ANSI line length = %d, valid = %v, suffix = %q", len(got), utf8.ValidString(got), got[len(got)-3:])
	}
}

func TestLogBurstCommitsOnceAndBoundsRetainedSignal(t *testing.T) {
	c := newTestControllerWithOptions(t, stubClient{}, Options{LogCapacity: 500})
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})
	c.logGen = 1
	redraws := 0
	c.requestRedraw = func() { redraws++ }
	for _, line := range []string{"second", "third"} {
		c.logsCh <- logResult{taskID: "task-1", generation: 1, line: line}
	}
	c.applyLogBurst(logResult{taskID: "task-1", generation: 1, line: "first"})
	if redraws != 1 {
		t.Fatalf("burst redraws = %d, want 1", redraws)
	}
	if got, want := c.model.Logs(), []string{"first", "second", "third"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("burst logs = %#v, want %#v", got, want)
	}
	for range c.options.LogCapacity {
		c.model.AppendLog(strings.Repeat("x", maxLogLineBytes))
	}
	c.syncView()
	if got := len(c.logsSignal.Get()); got > maxSurfaceBytes {
		t.Fatalf("retained logs signal bytes = %d, want <= %d", got, maxSurfaceBytes)
	}
}

func TestLogBurstLetsQueuedActionRunWhileProducerContinues(t *testing.T) {
	c := newTestControllerWithOptions(t, stubClient{}, Options{RefreshInterval: time.Hour, LogCapacity: 10_000})
	c.logsCh = make(chan logResult, maxLogResultsPerBurst*2)
	c.stateMu.Lock()
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})
	c.model.SetSelectedTask("task-1")
	c.stateMu.Unlock()
	c.mu.Lock()
	c.logGen = 1
	c.actionPending = true
	c.mu.Unlock()

	result := logResult{taskID: "task-1", generation: 1, line: "line"}
	for range cap(c.logsCh) {
		c.logsCh <- result
	}
	type redrawState struct {
		logs          int
		actionPending bool
	}
	redraws := make(chan redrawState, 1)
	releaseRedraw := make(chan struct{})
	c.mu.Lock()
	c.requestRedraw = func() {
		c.stateMu.Lock()
		logs := len(c.model.Logs())
		c.stateMu.Unlock()
		c.mu.Lock()
		pending := c.actionPending
		c.mu.Unlock()
		select {
		case redraws <- redrawState{logs: logs, actionPending: pending}:
		case <-c.ctx.Done():
			return
		}
		select {
		case <-releaseRedraw:
		case <-c.ctx.Done():
		}
	}
	c.mu.Unlock()

	c.workersWG.Add(1)
	go c.loop(context.Background())
	first := receiveGUI(t, redraws)
	if first.logs != maxLogResultsPerBurst {
		t.Fatalf("first log burst applied %d results, want %d", first.logs, maxLogResultsPerBurst)
	}

	producerCtx, stopProducer := context.WithCancel(context.Background())
	producerFull := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for {
			select {
			case c.logsCh <- result:
				if len(c.logsCh) == cap(c.logsCh) {
					select {
					case producerFull <- struct{}{}:
					case <-producerCtx.Done():
						return
					}
				}
			case <-producerCtx.Done():
				return
			}
		}
	}()
	t.Cleanup(stopProducer)
	receiveGUI(t, producerFull)
	c.actionCh <- actionResult{name: "retry", err: errors.New("action result")}
	releaseRedraw <- struct{}{}

	for range 100 {
		state := receiveGUI(t, redraws)
		if !state.actionPending {
			releaseRedraw <- struct{}{}
			stopProducer()
			receiveGUI(t, producerDone)
			return
		}
		receiveGUI(t, producerFull)
		releaseRedraw <- struct{}{}
	}
	t.Fatal("queued action remained pending while the log producer kept the channel full")
}

func TestLogStreamBackpressureDeliversBeyondChannelCapacityInOrder(t *testing.T) {
	const lineCount = 300
	streamDone := make(chan struct{})
	api := stubClient{streamLogs: func(_ context.Context, _ string, _ client.LogOptions, onLine func(string) error) error {
		defer close(streamDone)
		for i := range lineCount {
			if err := onLine(fmt.Sprintf("line-%03d", i)); err != nil {
				return err
			}
		}
		return nil
	}}
	c := newRunningTestController(t, api, Options{RefreshInterval: time.Hour, LogCapacity: lineCount + 1})
	c.stateMu.Lock()
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})
	c.model.SetSelectedTask("task-1")
	c.startLogsLocked(context.Background(), "task-1")
	c.stateMu.Unlock()

	receiveGUI(t, streamDone)
	waitGUICondition(t, func() bool {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		c.mu.Lock()
		complete := c.logComplete
		c.mu.Unlock()
		return complete && len(c.model.Logs()) == lineCount
	})
	c.stateMu.Lock()
	got := c.model.Logs()
	c.stateMu.Unlock()
	for i, line := range got {
		if want := fmt.Sprintf("line-%03d", i); line != want {
			t.Fatalf("log line %d = %q, want %q", i, line, want)
		}
	}
}

func TestBlockedLogProducerUnblocksOnStop(t *testing.T) {
	started := make(chan struct{})
	callbackErr := make(chan error, 1)
	api := stubClient{streamLogs: func(_ context.Context, _ string, _ client.LogOptions, onLine func(string) error) error {
		close(started)
		err := onLine("blocked")
		callbackErr <- err
		return err
	}}
	c := newTestController(t, api)
	for range cap(c.logsCh) {
		c.logsCh <- logResult{}
	}
	c.stateMu.Lock()
	c.startLogsLocked(context.Background(), "task-1")
	c.stateMu.Unlock()
	receiveGUI(t, started)
	assertNoValue(t, callbackErr)

	stopped := make(chan struct{})
	go func() {
		c.stop()
		close(stopped)
	}()
	if err := receiveGUI(t, callbackErr); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked log callback error = %v, want context.Canceled", err)
	}
	receiveGUI(t, stopped)
}

func TestSelectionChangeAtomicallyRejectsStaleDetailAndLogs(t *testing.T) {
	c := newTestController(t, stubClient{})
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}, {ID: "task-2"}})
	c.selectTask(context.Background(), "task-1")
	c.mu.Lock()
	detailGen, logGen := c.detailGen, c.logGen
	c.mu.Unlock()

	c.selectTask(context.Background(), "task-2")
	c.applyDetail(context.Background(), detailResult{taskID: "task-1", generation: detailGen, detail: tui.TaskDetail{Task: tui.Task{ID: "task-1"}}})
	c.applyLog(logResult{taskID: "task-1", generation: logGen, line: "stale"})
	if got := c.model.Detail().Task.ID; got != "task-2" {
		t.Fatalf("stale detail committed for %q, want task-2", got)
	}
	if got := c.model.Logs(); len(got) != 0 {
		t.Fatalf("stale logs committed: %#v", got)
	}
}

func TestSelectionInterleavingCannotLeaveStaleDetailOrLogs(t *testing.T) {
	c := newTestController(t, stubClient{})
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}, {ID: "task-2"}})
	c.selectTask(context.Background(), "task-1")
	c.mu.Lock()
	detailGen, logGen := c.detailGen, c.logGen
	c.mu.Unlock()

	c.stateMu.Lock()
	selectionDone := make(chan struct{})
	resultDone := make(chan struct{})
	go func() {
		c.selectTask(context.Background(), "task-2")
		close(selectionDone)
	}()
	go func() {
		c.applyDetail(context.Background(), detailResult{taskID: "task-1", generation: detailGen, detail: tui.TaskDetail{Task: tui.Task{ID: "task-1"}}})
		c.applyLog(logResult{taskID: "task-1", generation: logGen, line: "stale"})
		close(resultDone)
	}()
	c.stateMu.Unlock()
	receiveGUI(t, selectionDone)
	receiveGUI(t, resultDone)
	if got := c.model.Detail().Task.ID; got != "task-2" {
		t.Fatalf("interleaving left detail for %q, want task-2", got)
	}
	if got := c.model.Logs(); len(got) != 0 {
		t.Fatalf("interleaving left stale logs: %#v", got)
	}
}

func TestCreateValidatesTrimsSingleFlightsAndKeepsRetryIdentity(t *testing.T) {
	requests := make(chan client.CreateTaskRequest, 4)
	results := make(chan error, 4)
	api := stubClient{createTask: func(_ context.Context, request client.CreateTaskRequest) (client.Task, error) {
		requests <- request
		err := <-results
		return client.Task{ID: "created", Repository: request.Repository, Prompt: request.Prompt}, err
	}}
	c := newTestController(t, api)

	c.submitCreate(context.Background(), "  ", "prompt")
	if c.createError != "repository required" {
		t.Fatalf("blank repository error = %q", c.createError)
	}
	c.submitCreate(context.Background(), "repo", " \n ")
	if c.createError != "prompt required" {
		t.Fatalf("blank prompt error = %q", c.createError)
	}
	assertNoValue(t, requests)

	c.submitCreate(context.Background(), "  https://example.test/acme/widget.git  ", "  fix it  ")
	first := receiveGUI(t, requests)
	if first.Repository != "https://example.test/acme/widget.git" || first.Prompt != "fix it" || !strings.HasPrefix(first.IdempotencyKey, "gui-") {
		t.Fatalf("first create request = %#v", first)
	}
	c.submitCreate(context.Background(), first.Repository, first.Prompt)
	assertNoValue(t, requests)
	results <- errors.New("response lost")
	c.applyAction(context.Background(), receiveGUI(t, c.actionCh))

	c.submitCreate(context.Background(), first.Repository, first.Prompt)
	second := receiveGUI(t, requests)
	if second != first {
		t.Fatalf("unchanged retry = %#v, want %#v", second, first)
	}
	results <- errors.New("response lost again")
	c.applyAction(context.Background(), receiveGUI(t, c.actionCh))

	c.submitCreate(context.Background(), first.Repository, first.Prompt+" now")
	third := receiveGUI(t, requests)
	if third.IdempotencyKey == first.IdempotencyKey || !strings.HasPrefix(third.IdempotencyKey, "gui-") {
		t.Fatalf("edited retry idempotency keys = first %q, edited %q", first.IdempotencyKey, third.IdempotencyKey)
	}
	results <- nil
	c.applyAction(context.Background(), receiveGUI(t, c.actionCh))
}

func TestRetryAndCancelRequireConfirmationAndAreSingleFlight(t *testing.T) {
	retries := make(chan string, 2)
	cancels := make(chan string, 2)
	release := make(chan struct{}, 2)
	api := stubClient{
		retryTask: func(_ context.Context, id string) (client.Task, error) {
			retries <- id
			<-release
			return client.Task{ID: id}, nil
		},
		cancelTask: func(_ context.Context, id string) (client.Task, error) {
			cancels <- id
			<-release
			return client.Task{ID: id}, nil
		},
	}
	c := newTestController(t, api)
	c.model.RefreshTasks([]tui.Task{{ID: "task-1"}})

	c.requestConfirmation("retry")
	if c.confirmAction != "retry" {
		t.Fatalf("retry confirmation = %q", c.confirmAction)
	}
	assertNoValue(t, retries)
	c.confirm(context.Background())
	if got := receiveGUI(t, retries); got != "task-1" {
		t.Fatalf("retry task = %q", got)
	}
	c.confirm(context.Background())
	c.requestConfirmation("cancel")
	assertNoValue(t, retries)
	assertNoValue(t, cancels)
	release <- struct{}{}
	c.applyAction(context.Background(), receiveGUI(t, c.actionCh))

	c.requestConfirmation("cancel")
	if c.confirmAction != "cancel" {
		t.Fatalf("cancel confirmation = %q", c.confirmAction)
	}
	assertNoValue(t, cancels)
	c.confirm(context.Background())
	if got := receiveGUI(t, cancels); got != "task-1" {
		t.Fatalf("cancel task = %q", got)
	}
	release <- struct{}{}
	c.applyAction(context.Background(), receiveGUI(t, c.actionCh))
}

func TestShellCommandUsesFixedExecutableAndArguments(t *testing.T) {
	command := shellCommand(context.Background(), "dev", "workers", "pod-1")
	want := []string{"kubectl", "--context", "dev", "--namespace", "workers", "exec", "-it", "pod-1", "--", "/bin/bash"}
	if !reflect.DeepEqual(command.Args, want) {
		t.Fatalf("shell args = %#v, want %#v", command.Args, want)
	}
}

func TestParityFormattingIncludesTaskAttemptAndLogMetadata(t *testing.T) {
	created := time.Now().Add(-2 * time.Hour)
	updated := created.Add(time.Hour)
	detail := tui.TaskDetail{
		Task:     tui.Task{ID: "task-1", State: "running", Repository: "repo", CreatedAt: created, UpdatedAt: updated, CurrentAttemptID: "attempt-2"},
		Attempts: []tui.Attempt{{ID: "attempt-2", Number: 2, State: "running", KubernetesPod: client.KubernetesPod{ResourceIdentity: client.ResourceIdentity{Name: "pod-2"}}}},
	}
	details := formatDetails(detail)
	for _, want := range []string{"Age: 2h", "Created:", "Updated:", "Attempt: #2  attempt-2"} {
		if !strings.Contains(details, want) {
			t.Errorf("details missing %q:\n%s", want, details)
		}
	}
	logs := formatLogs(detail, []string{"line"}, 500, "connected")
	for _, want := range []string{"Attempt: #2  attempt-2", "Pod: pod-2", "Buffered: 1 / 500 lines", "Follow: connected"} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %q:\n%s", want, logs)
		}
	}
	if summary := formatTaskSummary(detail.Task); !strings.Contains(summary, "2h") {
		t.Fatalf("task summary missing age: %q", summary)
	}
}

type stubClient struct {
	listTasks    func(context.Context, client.ListOptions) (client.TaskList, error)
	showTask     func(context.Context, string) (client.Task, error)
	listAttempts func(context.Context, string, client.ListOptions) (client.AttemptList, error)
	listEvents   func(context.Context, string, client.ListOptions) (client.EventList, error)
	streamLogs   func(context.Context, string, client.LogOptions, func(string) error) error
	createTask   func(context.Context, client.CreateTaskRequest) (client.Task, error)
	retryTask    func(context.Context, string) (client.Task, error)
	cancelTask   func(context.Context, string) (client.Task, error)
}

func (s stubClient) ListTasks(ctx context.Context, options client.ListOptions) (client.TaskList, error) {
	if s.listTasks != nil {
		return s.listTasks(ctx, options)
	}
	return client.TaskList{}, nil
}

func (s stubClient) ShowTask(ctx context.Context, id string) (client.Task, error) {
	if s.showTask != nil {
		return s.showTask(ctx, id)
	}
	return client.Task{ID: id}, nil
}

func (s stubClient) ListAttempts(ctx context.Context, id string, options client.ListOptions) (client.AttemptList, error) {
	if s.listAttempts != nil {
		return s.listAttempts(ctx, id, options)
	}
	return client.AttemptList{}, nil
}

func (s stubClient) ListEvents(ctx context.Context, id string, options client.ListOptions) (client.EventList, error) {
	if s.listEvents != nil {
		return s.listEvents(ctx, id, options)
	}
	return client.EventList{}, nil
}

func (s stubClient) StreamLogs(ctx context.Context, id string, options client.LogOptions, onLine func(string) error) error {
	if s.streamLogs != nil {
		return s.streamLogs(ctx, id, options, onLine)
	}
	return nil
}

func (s stubClient) CreateTask(ctx context.Context, request client.CreateTaskRequest) (client.Task, error) {
	if s.createTask != nil {
		return s.createTask(ctx, request)
	}
	return client.Task{}, nil
}

func (s stubClient) RetryTask(ctx context.Context, id string) (client.Task, error) {
	if s.retryTask != nil {
		return s.retryTask(ctx, id)
	}
	return client.Task{}, nil
}

func (s stubClient) CancelTask(ctx context.Context, id string) (client.Task, error) {
	if s.cancelTask != nil {
		return s.cancelTask(ctx, id)
	}
	return client.Task{}, nil
}

func newTestController(t *testing.T, api stubClient) *controller {
	t.Helper()
	return newTestControllerWithOptions(t, api, Options{})
}

func newTestControllerWithOptions(t *testing.T, api stubClient, options Options) *controller {
	t.Helper()
	c := newController(context.Background(), api, options)
	t.Cleanup(c.stop)
	return c
}

func newRunningTestController(t *testing.T, api stubClient, options Options) *controller {
	t.Helper()
	if options.RefreshInterval == 0 {
		options.RefreshInterval = time.Hour
	}
	c := newTestControllerWithOptions(t, api, options)
	loopCtx, cancel := context.WithCancel(context.Background())
	c.workersWG.Add(1)
	go c.loop(loopCtx)
	t.Cleanup(cancel)
	return c
}

func receiveGUI[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for asynchronous GUI result")
		var zero T
		return zero
	}
}

func assertNoValue[T any](t *testing.T, values <-chan T) {
	t.Helper()
	select {
	case value := <-values:
		t.Fatalf("unexpected value: %#v", value)
	default:
	}
}

func assertContextDone(t *testing.T, ctx context.Context, operation string) {
	t.Helper()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatalf("%s context remained registered after result delivery", operation)
	}
}

func waitGUICondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for GUI state")
		}
		time.Sleep(time.Millisecond)
	}
}
