package gui

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/gogpu/ui/state"
	"github.com/simpleswe/simpleswe/internal/client"
	"github.com/simpleswe/simpleswe/internal/tui"
)

const (
	defaultRefreshInterval = 3 * time.Second
	defaultRequestTimeout  = 10 * time.Second
	defaultLogCapacity     = 500
	maxLogLineBytes        = 4096
	maxSurfaceBytes        = 256 << 10
	maxListLimit           = 100
	maxLogResultsPerBurst  = 64
)

type Options struct {
	KubeContext     string
	Namespace       string
	RefreshInterval time.Duration
	RequestTimeout  time.Duration
	LogCapacity     int
	TaskLimit       int
}

func (o Options) withDefaults() Options {
	if o.RefreshInterval <= 0 {
		o.RefreshInterval = defaultRefreshInterval
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = defaultRequestTimeout
	}
	if o.LogCapacity <= 0 {
		o.LogCapacity = defaultLogCapacity
	}
	if o.TaskLimit <= 0 || o.TaskLimit > maxListLimit {
		o.TaskLimit = maxListLimit
	}
	if o.Namespace == "" {
		o.Namespace = "simpleswe"
	}
	return o
}

type apiClient interface {
	ListTasks(context.Context, client.ListOptions) (client.TaskList, error)
	ShowTask(context.Context, string) (client.Task, error)
	ListAttempts(context.Context, string, client.ListOptions) (client.AttemptList, error)
	ListEvents(context.Context, string, client.ListOptions) (client.EventList, error)
	StreamLogs(context.Context, string, client.LogOptions, func(string) error) error
	CreateTask(context.Context, client.CreateTaskRequest) (client.Task, error)
	RetryTask(context.Context, string) (client.Task, error)
	CancelTask(context.Context, string) (client.Task, error)
}

type taskResult struct {
	generation uint64
	tasks      []tui.Task
	err        error
}

type detailResult struct {
	taskID     string
	generation uint64
	detail     tui.TaskDetail
	err        error
}

type logResult struct {
	taskID     string
	generation uint64
	line       string
	err        error
	done       bool
}

type actionResult struct {
	name string
	task tui.Task
	err  error
}

type controller struct {
	ctx     context.Context
	cancel  context.CancelFunc
	client  apiClient
	options Options
	model   *tui.Model

	mu                 sync.Mutex
	stateMu            sync.Mutex
	viewMu             sync.Mutex
	started            bool
	stopping           bool
	refreshing         bool
	refreshGen         uint64
	refreshStop        context.CancelFunc
	detailGen          uint64
	logGen             uint64
	detailStop         context.CancelFunc
	logStop            context.CancelFunc
	logComplete        bool
	logResultPending   bool
	actionPending      bool
	createPending      bool
	createError        string
	createPayload      [2]string
	createKey          string
	confirmAction      string
	confirmTaskID      string
	message            string
	helpOpen           bool
	requestRedraw      func()
	themeChanged       func(int)
	workersWG          sync.WaitGroup
	stopDone           chan struct{}
	tasksCh            chan taskResult
	detailCh           chan detailResult
	logsCh             chan logResult
	actionCh           chan actionResult
	taskCountSignal    state.Signal[int]
	taskRevisionSignal state.Signal[uint64]
	selectedSignal     state.Signal[int]
	detailsSignal      state.Signal[string]
	logsSignal         state.Signal[string]
	eventsSignal       state.Signal[string]
	jobSignal          state.Signal[string]
	podSignal          state.Signal[string]
	connectivitySignal state.Signal[string]
	messageSignal      state.Signal[string]
	createErrorSignal  state.Signal[string]
	repositorySignal   state.Signal[string]
	promptSignal       state.Signal[string]
	confirmationSignal state.Signal[string]
	helpSignal         state.Signal[string]
	wrapLogsSignal     state.Signal[bool]
	setLogWrapViews    func(bool)
	workerLaunchHook   func()
	taskRevision       uint64
}

func newController(parent context.Context, api apiClient, options Options) *controller {
	ctx, cancel := context.WithCancel(parent)
	c := &controller{
		ctx:                ctx,
		cancel:             cancel,
		client:             api,
		options:            options.withDefaults(),
		model:              tui.NewModel(options.withDefaults().LogCapacity),
		message:            "connecting to controller",
		stopDone:           make(chan struct{}),
		tasksCh:            make(chan taskResult, 1),
		detailCh:           make(chan detailResult, 8),
		logsCh:             make(chan logResult, 256),
		actionCh:           make(chan actionResult, 4),
		taskCountSignal:    state.NewSignal(0),
		taskRevisionSignal: state.NewSignalWithOptions(0, state.Options[uint64]{Equal: func(a, b uint64) bool { return a == b }}),
		selectedSignal:     state.NewSignal(-1),
		detailsSignal:      state.NewSignal("Select a task to see details."),
		logsSignal:         state.NewSignal("Waiting for a task log stream."),
		eventsSignal:       state.NewSignal("No lifecycle events."),
		jobSignal:          state.NewSignal("No Kubernetes Job selected."),
		podSignal:          state.NewSignal("No Kubernetes Pod selected."),
		connectivitySignal: state.NewSignal("○ unknown"),
		messageSignal:      state.NewSignal("connecting to controller"),
		createErrorSignal:  state.NewSignal(""),
		repositorySignal:   state.NewSignal(""),
		promptSignal:       state.NewSignal(""),
		confirmationSignal: state.NewSignal(""),
		wrapLogsSignal:     state.NewSignal(false),
		helpSignal: state.NewSignal(
			"Select a task, then use Details, Logs, Events, Job, or Pod. " +
				"Retry and Cancel require confirmation. Refresh cancels and replaces the active refresh.",
		),
	}
	return c
}

func (c *controller) start(ctx context.Context) {
	c.mu.Lock()
	if c.started || c.stopping {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.workersWG.Add(1)
	c.mu.Unlock()
	go c.loop(ctx)
	c.refreshPeriodic(ctx)
}

func (c *controller) loop(ctx context.Context) {
	defer c.workersWG.Done()
	ticker := time.NewTicker(c.options.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.ctx.Done():
			return
		case result := <-c.tasksCh:
			c.applyTasks(ctx, result)
		case result := <-c.detailCh:
			c.applyDetail(ctx, result)
		case result := <-c.logsCh:
			c.applyLogBurst(result)
		case result := <-c.actionCh:
			c.applyAction(ctx, result)
		case <-ticker.C:
			c.refreshPeriodic(ctx)
			c.restartLogs(ctx)
		}
	}
}

func (c *controller) stop() {
	c.mu.Lock()
	if c.stopping {
		done := c.stopDone
		c.mu.Unlock()
		<-done
		return
	}
	c.stopping = true
	c.cancel()
	if c.refreshStop != nil {
		c.refreshStop()
	}
	if c.detailStop != nil {
		c.detailStop()
	}
	if c.logStop != nil {
		c.logStop()
	}
	c.mu.Unlock()
	c.workersWG.Wait()
	close(c.stopDone)
}

func (c *controller) runWorker(parent context.Context, worker func(context.Context)) bool {
	c.mu.Lock()
	if c.stopping || c.ctx.Err() != nil || parent.Err() != nil {
		c.mu.Unlock()
		return false
	}
	c.workersWG.Add(1)
	hook := c.workerLaunchHook
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	workerCtx, cancel := context.WithCancel(parent)
	lifecycleDone := c.ctx.Done()
	go func() {
		select {
		case <-lifecycleDone:
			cancel()
		case <-workerCtx.Done():
		}
	}()
	go func() {
		defer c.workersWG.Done()
		defer cancel()
		worker(workerCtx)
	}()
	return true
}

func (c *controller) refresh(ctx context.Context) { c.startRefresh(ctx, true) }

func (c *controller) refreshPeriodic(ctx context.Context) { c.startRefresh(ctx, false) }

func (c *controller) startRefresh(parent context.Context, supersede bool) {
	c.mu.Lock()
	if c.client == nil || c.stopping || c.ctx.Err() != nil || c.refreshing && !supersede {
		c.mu.Unlock()
		return
	}
	if c.refreshStop != nil {
		c.refreshStop()
	}
	c.refreshing = true
	c.refreshGen++
	generation := c.refreshGen
	requestCtx, stop := context.WithCancel(parent)
	c.refreshStop = stop
	c.message = "refreshing"
	c.mu.Unlock()
	c.syncView()

	if !c.runWorker(requestCtx, func(ctx context.Context) {
		defer c.finishRefresh(generation, stop)
		requestCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
		defer cancel()
		list, err := c.client.ListTasks(requestCtx, client.ListOptions{Limit: c.options.TaskLimit})
		select {
		case c.tasksCh <- taskResult{generation: generation, tasks: list.Tasks, err: err}:
		case <-c.ctx.Done():
		}
	}) {
		c.finishRefresh(generation, stop)
	}
}

func (c *controller) finishRefresh(generation uint64, stop context.CancelFunc) {
	stop()
	c.mu.Lock()
	if generation == c.refreshGen {
		c.refreshStop = nil
	}
	c.mu.Unlock()
}

func (c *controller) applyTasks(ctx context.Context, result taskResult) {
	c.stateMu.Lock()
	c.mu.Lock()
	if result.generation != c.refreshGen {
		c.mu.Unlock()
		c.stateMu.Unlock()
		return
	}
	c.refreshing = false
	c.refreshStop = nil
	if result.err != nil {
		c.message = "refresh failed: " + shortError(result.err)
		c.mu.Unlock()
		c.model.SetConnectivity(tui.ConnectivityLost, "controller connection lost: "+shortError(result.err))
		c.stateMu.Unlock()
		c.syncView()
		return
	}
	c.message = "ready"
	c.mu.Unlock()

	previousConnectivity := c.model.Connectivity()
	if previousConnectivity == tui.ConnectivityLost {
		c.model.SetConnectivity(tui.ConnectivityRestored)
	} else {
		c.model.SetConnectivity(tui.ConnectivityConnected)
	}
	oldID := c.model.SelectedTaskID()
	c.refreshTasksLocked(result.tasks)
	selectedID := c.model.SelectedTaskID()
	switch {
	case selectedID == "":
		c.clearSelectionLocked()
	case selectedID != oldID || c.model.Detail().Task.ID == "":
		c.selectTaskLocked(ctx, selectedID)
	default:
		c.fetchDetailLocked(ctx, selectedID)
	}
	c.stateMu.Unlock()
	c.syncView()
}

func (c *controller) clearSelectionLocked() {
	c.model.SetDetail(tui.TaskDetail{})
	c.model.ResetLogs()
	c.mu.Lock()
	if c.detailStop != nil {
		c.detailStop()
		c.detailStop = nil
	}
	if c.logStop != nil {
		c.logStop()
		c.logStop = nil
	}
	c.logComplete = false
	c.logResultPending = false
	c.mu.Unlock()
}

func (c *controller) selectTask(ctx context.Context, taskID string) {
	c.stateMu.Lock()
	changed := c.selectTaskLocked(ctx, taskID)
	c.stateMu.Unlock()
	if changed {
		c.syncView()
	}
}

func (c *controller) selectTaskLocked(ctx context.Context, taskID string) bool {
	if taskID == "" || taskID == c.model.SelectedTaskID() && c.model.Detail().Task.ID == taskID {
		return false
	}
	var selected tui.Task
	for _, task := range c.model.Tasks() {
		if task.ID == taskID {
			selected = task
			break
		}
	}
	if selected.ID == "" {
		return false
	}
	c.model.SetSelectedTask(taskID)
	c.model.SetDetail(tui.TaskDetail{Task: selected})
	c.fetchDetailLocked(ctx, taskID)
	c.startLogsLocked(ctx, taskID)
	return true
}

func (c *controller) fetchDetailLocked(parent context.Context, taskID string) {
	if taskID == "" || c.client == nil {
		return
	}
	c.mu.Lock()
	if c.detailStop != nil {
		c.detailStop()
	}
	c.detailGen++
	generation := c.detailGen
	ctx, stop := context.WithCancel(parent)
	c.detailStop = stop
	c.mu.Unlock()

	if !c.runWorker(ctx, func(ctx context.Context) {
		defer c.finishDetail(generation, stop)
		requestCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
		defer cancel()
		task, err := c.client.ShowTask(requestCtx, taskID)
		var attempts client.AttemptList
		if err == nil {
			attempts, err = c.client.ListAttempts(requestCtx, taskID, client.ListOptions{Limit: maxListLimit})
		}
		var events client.EventList
		if err == nil {
			events, err = c.client.ListEvents(requestCtx, taskID, client.ListOptions{Limit: maxListLimit})
		}
		result := detailResult{
			taskID: taskID, generation: generation, err: err,
			detail: tui.TaskDetail{Task: task, Attempts: attempts.Attempts, Events: events.Events},
		}
		select {
		case c.detailCh <- result:
		case <-ctx.Done():
		}
	}) {
		c.finishDetail(generation, stop)
	}
}

func (c *controller) finishDetail(generation uint64, stop context.CancelFunc) {
	stop()
	c.mu.Lock()
	if generation == c.detailGen {
		c.detailStop = nil
	}
	c.mu.Unlock()
}

func (c *controller) applyDetail(ctx context.Context, result detailResult) {
	c.stateMu.Lock()
	c.mu.Lock()
	current := result.generation == c.detailGen
	if current {
		c.detailStop = nil
	}
	c.mu.Unlock()
	if !current || result.taskID != c.model.SelectedTaskID() {
		c.stateMu.Unlock()
		return
	}
	if result.err != nil {
		if !errors.Is(result.err, context.Canceled) {
			c.mu.Lock()
			c.message = "details: " + shortError(result.err)
			c.mu.Unlock()
		}
		c.stateMu.Unlock()
		c.syncView()
		return
	}
	previousAttempt := c.model.Detail().Task.CurrentAttemptID
	c.model.SetDetail(result.detail)
	if previousAttempt != result.detail.Task.CurrentAttemptID {
		c.startLogsLocked(ctx, result.taskID)
	}
	c.stateMu.Unlock()
	c.syncView()
}

func (c *controller) startLogsLocked(parent context.Context, taskID string) {
	if taskID == "" || c.client == nil {
		return
	}
	c.mu.Lock()
	if c.logStop != nil {
		c.logStop()
	}
	c.logGen++
	generation := c.logGen
	ctx, stop := context.WithCancel(parent)
	c.logStop = stop
	c.logComplete = false
	c.logResultPending = false
	c.mu.Unlock()
	c.model.ResetLogs()
	attemptID := c.model.Detail().Task.CurrentAttemptID

	if !c.runWorker(ctx, func(ctx context.Context) {
		defer c.finishLogs(generation, stop)
		err := c.client.StreamLogs(ctx, taskID, client.LogOptions{
			Follow: true, AttemptID: attemptID, TailLines: c.options.LogCapacity,
		}, func(line string) error {
			select {
			case c.logsCh <- logResult{taskID: taskID, generation: generation, line: boundedLine(line)}:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
		c.mu.Lock()
		if generation == c.logGen {
			c.logResultPending = true
		}
		c.mu.Unlock()
		select {
		case c.logsCh <- logResult{taskID: taskID, generation: generation, err: err, done: true}:
		case <-ctx.Done():
			c.mu.Lock()
			if generation == c.logGen {
				c.logResultPending = false
			}
			c.mu.Unlock()
		}
	}) {
		c.finishLogs(generation, stop)
	}
}

func (c *controller) finishLogs(generation uint64, stop context.CancelFunc) {
	stop()
	c.mu.Lock()
	if generation == c.logGen {
		c.logStop = nil
	}
	c.mu.Unlock()
}

func (c *controller) restartLogs(ctx context.Context) {
	c.stateMu.Lock()
	c.mu.Lock()
	restart := c.logStop == nil && !c.logComplete && !c.logResultPending
	c.mu.Unlock()
	if restart {
		c.startLogsLocked(ctx, c.model.SelectedTaskID())
	}
	c.stateMu.Unlock()
	if restart {
		c.syncView()
	}
}

func (c *controller) applyLog(result logResult) {
	c.stateMu.Lock()
	changed := c.applyLogLocked(result)
	c.stateMu.Unlock()
	if changed {
		c.syncView()
	}
}

func (c *controller) applyLogBurst(first logResult) {
	c.stateMu.Lock()
	changed := c.applyLogLocked(first)
	for range maxLogResultsPerBurst - 1 {
		select {
		case result := <-c.logsCh:
			changed = c.applyLogLocked(result) || changed
		default:
			c.stateMu.Unlock()
			if changed {
				c.syncView()
			}
			return
		}
	}
	c.stateMu.Unlock()
	if changed {
		c.syncView()
	}
}

func (c *controller) applyLogLocked(result logResult) bool {
	c.mu.Lock()
	current := result.generation == c.logGen
	if current && result.done {
		c.logStop = nil
		c.logComplete = result.err == nil
		c.logResultPending = false
	}
	c.mu.Unlock()
	if !current || result.taskID != c.model.SelectedTaskID() {
		return false
	}
	if result.done {
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			c.mu.Lock()
			c.message = "log stream interrupted: " + shortError(result.err)
			c.mu.Unlock()
		}
		return true
	}
	c.model.AppendLog(boundedLine(result.line))
	return true
}

func (c *controller) editCreateField() {
	c.mu.Lock()
	changed := c.createError != ""
	c.createError = ""
	c.mu.Unlock()
	if changed {
		c.syncView()
	}
}

func (c *controller) submitCreate(parent context.Context, repository, prompt string) {
	repository, prompt = strings.TrimSpace(repository), strings.TrimSpace(prompt)
	c.mu.Lock()
	if c.createPending || c.stopping || c.client == nil {
		c.mu.Unlock()
		return
	}
	if repository == "" {
		c.createError = "repository required"
		c.mu.Unlock()
		c.syncView()
		return
	}
	if prompt == "" {
		c.createError = "prompt required"
		c.mu.Unlock()
		c.syncView()
		return
	}
	payload := [2]string{repository, prompt}
	if c.createKey == "" || c.createPayload != payload {
		c.createKey = "gui-" + rand.Text()
	}
	c.createPayload = payload
	key := c.createKey
	c.createPending = true
	c.createError = ""
	c.message = "create requested"
	c.mu.Unlock()
	c.syncView()

	c.runWorker(parent, func(ctx context.Context) {
		requestCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
		defer cancel()
		task, err := c.client.CreateTask(requestCtx, client.CreateTaskRequest{
			Repository: repository, Prompt: prompt, IdempotencyKey: key,
		})
		select {
		case c.actionCh <- actionResult{name: "create", task: task, err: err}:
		case <-c.ctx.Done():
		}
	})
}

func (c *controller) requestConfirmation(action string) {
	if action != "retry" && action != "cancel" {
		return
	}
	c.stateMu.Lock()
	c.mu.Lock()
	taskID := c.model.SelectedTaskID()
	if c.actionPending || c.confirmAction != "" || taskID == "" {
		c.mu.Unlock()
		c.stateMu.Unlock()
		return
	}
	c.confirmAction = action
	c.confirmTaskID = taskID
	c.mu.Unlock()
	c.stateMu.Unlock()
	c.syncView()
}

func (c *controller) dismissConfirmation() {
	c.mu.Lock()
	c.confirmAction = ""
	c.confirmTaskID = ""
	c.mu.Unlock()
	c.syncView()
}

func (c *controller) confirm(ctx context.Context) {
	c.mu.Lock()
	action := c.confirmAction
	taskID := c.confirmTaskID
	if action == "" || taskID == "" || c.actionPending {
		c.mu.Unlock()
		return
	}
	c.confirmAction = ""
	c.confirmTaskID = ""
	c.mu.Unlock()
	c.performAction(ctx, action, taskID)
}

func (c *controller) performAction(parent context.Context, name, taskID string) {
	c.mu.Lock()
	if c.actionPending || c.stopping || c.client == nil {
		c.mu.Unlock()
		return
	}
	c.actionPending = true
	c.message = name + " requested"
	c.mu.Unlock()
	c.syncView()

	c.runWorker(parent, func(ctx context.Context) {
		requestCtx, cancel := context.WithTimeout(ctx, c.options.RequestTimeout)
		defer cancel()
		var task tui.Task
		var err error
		switch name {
		case "retry":
			task, err = c.client.RetryTask(requestCtx, taskID)
		case "cancel":
			task, err = c.client.CancelTask(requestCtx, taskID)
		default:
			err = errors.New("unknown action")
		}
		select {
		case c.actionCh <- actionResult{name: name, task: task, err: err}:
		case <-c.ctx.Done():
		}
	})
}

func (c *controller) applyAction(ctx context.Context, result actionResult) {
	if result.name == "create" {
		c.applyCreate(ctx, result)
		return
	}
	c.mu.Lock()
	c.actionPending = false
	if result.err != nil {
		c.message = result.name + " failed: " + shortError(result.err)
		c.mu.Unlock()
		c.syncView()
		return
	}
	c.message = result.name + " accepted"
	c.mu.Unlock()
	c.stateMu.Lock()
	if result.task.ID == c.model.SelectedTaskID() {
		detail := c.model.Detail()
		previousAttempt := detail.Task.CurrentAttemptID
		detail.Task = result.task
		c.model.SetDetail(detail)
		if previousAttempt != result.task.CurrentAttemptID {
			c.startLogsLocked(ctx, result.task.ID)
		}
	}
	c.stateMu.Unlock()
	c.syncView()
	c.refresh(ctx)
}

func (c *controller) applyCreate(ctx context.Context, result actionResult) {
	c.mu.Lock()
	c.createPending = false
	if result.err != nil {
		c.createError = "create failed: " + shortError(result.err)
		c.message = "create failed"
		c.mu.Unlock()
		c.syncView()
		return
	}
	c.createError = ""
	c.createKey = ""
	c.createPayload = [2]string{}
	c.message = "create accepted"
	c.refreshing = false
	c.mu.Unlock()
	c.repositorySignal.Set("")
	c.promptSignal.Set("")

	c.stateMu.Lock()
	existing := c.model.Tasks()
	tasks := make([]tui.Task, 0, min(c.options.TaskLimit, len(existing)+1))
	tasks = append(tasks, result.task)
	for _, task := range existing {
		if task.ID != result.task.ID && len(tasks) < c.options.TaskLimit {
			tasks = append(tasks, task)
		}
	}
	c.refreshTasksLocked(tasks)
	c.selectTaskLocked(ctx, result.task.ID)
	c.stateMu.Unlock()
	c.syncView()
	c.refresh(ctx)
}

func (c *controller) shell(parent context.Context) {
	c.stateMu.Lock()
	pod, namespace := c.selectedPod()
	c.stateMu.Unlock()
	if pod == "" {
		c.setMessage("selected attempt has no pod")
		return
	}
	c.mu.Lock()
	if c.actionPending || c.stopping {
		c.mu.Unlock()
		return
	}
	c.actionPending = true
	c.message = "shell requested"
	c.mu.Unlock()
	c.syncView()

	c.runWorker(parent, func(ctx context.Context) {
		cmd := shellCommand(ctx, c.options.KubeContext, namespace, pod)
		cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
		err := cmd.Run()
		select {
		case c.actionCh <- actionResult{name: "shell", err: err}:
		case <-c.ctx.Done():
		}
	})
}

func shellCommand(ctx context.Context, kubeContext, namespace, pod string) *exec.Cmd {
	args := make([]string, 0, 9)
	if kubeContext != "" {
		args = append(args, "--context", kubeContext)
	}
	if namespace != "" {
		args = append(args, "--namespace", namespace)
	}
	args = append(args, "exec", "-it", pod, "--", "/bin/bash")
	// #nosec G204 -- the executable is fixed and arguments are not shell-expanded.
	return exec.CommandContext(ctx, "kubectl", args...)
}

func (c *controller) selectedPod() (string, string) {
	detail := c.model.Detail()
	for _, attempt := range detail.Attempts {
		if attempt.ID == detail.Task.CurrentAttemptID && attempt.KubernetesPod.ResourceIdentity.Name != "" {
			identity := attempt.KubernetesPod.ResourceIdentity
			return identity.Name, firstNonempty(identity.Namespace, detail.Task.KubernetesPod.ResourceIdentity.Namespace, c.options.Namespace)
		}
	}
	identity := detail.Task.KubernetesPod.ResourceIdentity
	return identity.Name, firstNonempty(identity.Namespace, c.options.Namespace)
}

func (c *controller) setMessage(message string) {
	c.mu.Lock()
	c.message = message
	c.mu.Unlock()
	c.syncView()
}

func (c *controller) syncView() {
	c.viewMu.Lock()
	defer c.viewMu.Unlock()
	c.stateMu.Lock()
	detail := c.model.Detail()
	tasks := c.model.Tasks()
	taskRevision := c.taskRevision
	logs := c.model.Logs()
	selectedID := c.model.SelectedTaskID()
	connectivity := c.model.Connectivity()
	connectivityMessage := c.model.ConnectivityMessage()
	c.stateMu.Unlock()
	c.taskCountSignal.Set(len(tasks))
	c.taskRevisionSignal.Set(taskRevision)
	selected := -1
	for i := range tasks {
		if tasks[i].ID == selectedID {
			selected = i
			break
		}
	}
	c.selectedSignal.Set(selected)
	c.detailsSignal.Set(formatDetails(detail))
	c.logsSignal.Set(boundedDisplayText(formatLogs(detail, logs, c.options.LogCapacity, c.logStatus())))
	c.eventsSignal.Set(formatEvents(detail.Events))
	c.jobSignal.Set(formatJob(detail, c.options.Namespace))
	c.podSignal.Set(formatPod(detail, c.options.Namespace))
	stateName := connectivity
	marker := "○"
	if stateName != tui.ConnectivityUnknown {
		marker = "●"
	}
	c.connectivitySignal.Set(marker + " " + string(stateName) + " — " + connectivityMessage)
	c.mu.Lock()
	message, createError := c.message, c.createError
	confirmation, confirmationTask, helpOpen := c.confirmAction, c.confirmTaskID, c.helpOpen
	c.mu.Unlock()
	c.messageSignal.Set(message)
	c.createErrorSignal.Set(createError)
	if confirmation == "" {
		c.confirmationSignal.Set("Choose Retry or Cancel to request an action.")
	} else {
		c.confirmationSignal.Set(fmt.Sprintf("Confirm %s for %s?", confirmation, confirmationTask))
	}
	if helpOpen {
		c.helpSignal.Set("HELP — Select a task; tabs show details, live logs, events, Job, and Pod. Wrap logs controls line wrapping. Refresh replaces an active refresh. Retry and Cancel require Confirm.")
	} else {
		c.helpSignal.Set("Help is closed.")
	}
	c.mu.Lock()
	redraw := c.requestRedraw
	c.mu.Unlock()
	if redraw != nil {
		redraw()
	}
}

// refreshTasksLocked replaces task data and advances the render revision.
// The caller holds stateMu so list snapshots and their revision stay atomic.
func (c *controller) refreshTasksLocked(tasks []tui.Task) {
	c.model.RefreshTasks(tasks)
	c.taskRevision++
}

func (c *controller) logStatus() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.logStop != nil:
		return "connected"
	case c.logComplete:
		return "complete"
	default:
		return "reconnecting"
	}
}

func (c *controller) setWrapLogs(wrap bool) {
	c.wrapLogsSignal.Set(wrap)
	c.mu.Lock()
	setViews := c.setLogWrapViews
	c.mu.Unlock()
	if setViews != nil {
		setViews(wrap)
	}
	c.setMessage(map[bool]string{true: "log wrap enabled", false: "log wrap disabled"}[wrap])
}

func (c *controller) toggleHelp() {
	c.mu.Lock()
	c.helpOpen = !c.helpOpen
	c.mu.Unlock()
	c.syncView()
}
