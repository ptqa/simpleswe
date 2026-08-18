package tui

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/textinput"
)

const (
	defaultRefreshInterval = 3 * time.Second
	defaultRequestTimeout  = 10 * time.Second
	defaultLogCapacity     = 500
	maxLogLineBytes        = 4096
)

var ErrInjectedStreamsUnsupported = errors.New("vaxis requires a real terminal; injected streams are unsupported by Run; use Runner with Options.Console")

// Options configures the terminal and the Kubernetes context displayed and
// used by shell actions. Console is the Vaxis-supported injection point for
// tests and non-default terminals; TTY and Console are mutually exclusive.
type Options struct {
	Address         string
	KubeContext     string
	Namespace       string
	TTY             string
	Console         vaxis.Console
	Stdin           io.Reader
	Stdout          io.Writer
	Stderr          io.Writer
	RefreshInterval time.Duration
	RequestTimeout  time.Duration
	LogCapacity     int
	TaskLimit       int
	configDir       string
}

type Runner struct {
	Client  *client.Client
	Options Options
}

func NewRunner(api *client.Client, options Options) *Runner {
	return &Runner{Client: api, Options: options}
}

// Run starts the full-screen Vaxis UI. Vaxis owns /dev/tty, so this convenience
// entry point accepts only the process standard streams. Use Options.Console
// when a Vaxis Console implementation is available.
func Run(ctx context.Context, address string, stdin io.Reader, stdout, stderr io.Writer) error {
	stdin = readerOr(stdin, os.Stdin)
	stdout = writerOr(stdout, os.Stdout)
	stderr = writerOr(stderr, os.Stderr)
	if !sameFile(stdin, os.Stdin) || !sameFile(stdout, os.Stdout) || !sameFile(stderr, os.Stderr) {
		return ErrInjectedStreamsUnsupported
	}
	return NewRunner(client.New(address, nil), Options{
		Address: address,
		Stdin:   stdin,
		Stdout:  stdout,
		Stderr:  stderr,
	}).Run(ctx)
}

func (r *Runner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("tui context is nil")
	}
	if r == nil || r.Client == nil {
		return errors.New("tui client is nil")
	}
	options := r.Options.withDefaults()
	if options.TTY != "" && options.Console != nil {
		return errors.New("tui TTY and Console are mutually exclusive")
	}
	vx, err := vaxis.New(vaxis.Options{
		EventQueueSize: 256,
		WithTTY:        options.TTY,
		WithConsole:    options.Console,
	})
	if err != nil {
		return fmt.Errorf("start vaxis: %w", err)
	}
	defer vx.Close()
	vx.SetTitle("simpleswe")
	return newApplication(ctx, vx, r.Client, options).run()
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
	if o.TaskLimit <= 0 {
		o.TaskLimit = 100
	} else if o.TaskLimit > 100 {
		o.TaskLimit = 100
	}
	if o.Namespace == "" {
		o.Namespace = "default"
	}
	if o.configDir == "" {
		o.configDir, _ = os.UserConfigDir()
	}
	o.Stdin = readerOr(o.Stdin, os.Stdin)
	o.Stdout = writerOr(o.Stdout, os.Stdout)
	o.Stderr = writerOr(o.Stderr, os.Stderr)
	return o
}

type viewMode uint8

const (
	viewDetails viewMode = iota
	viewLogs
	viewEvents
	viewJob
	viewPod
)

type taskResult struct {
	generation uint64
	tasks      []Task
	err        error
}

type projectResult struct {
	projects []client.Project
	err      error
}

type detailResult struct {
	taskID     string
	generation uint64
	detail     TaskDetail
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
	task Task
	err  error
}

const (
	createRepositoryField = iota
	createPromptField
)

type application struct {
	ctx     context.Context
	cancel  context.CancelFunc
	vx      *vaxis.Vaxis
	client  *client.Client
	options Options
	model   *Model

	mode               viewMode
	theme              themeName
	themePicker        bool
	themeCursor        int
	themePrevious      themeName
	configDir          string
	help               bool
	confirmAction      string
	narrowDetail       bool
	wrapLogs           bool
	logOffset          int
	message            string
	refreshing         bool
	refreshGen         uint64
	actionPending      bool
	createModal        bool
	createPending      bool
	createField        int
	createRepo         *textinput.Model
	createPrompt       *textinput.Model
	projects           []client.Project
	projectCursor      int
	projectsLoading    bool
	createEventKey     string
	createEventPayload [2]string
	createError        string
	createAccepted     bool

	tasksCh     chan taskResult
	projectsCh  chan projectResult
	detailCh    chan detailResult
	logsCh      chan logResult
	actionCh    chan actionResult
	detailStop  context.CancelFunc
	logStop     context.CancelFunc
	logComplete bool
	detailGen   uint64
	logGen      uint64
}

func newApplication(parent context.Context, vx *vaxis.Vaxis, api *client.Client, options Options) *application {
	ctx, cancel := context.WithCancel(parent)
	return &application{
		ctx:        ctx,
		cancel:     cancel,
		vx:         vx,
		client:     api,
		options:    options,
		model:      NewModel(options.LogCapacity),
		theme:      loadTheme(options.configDir),
		configDir:  options.configDir,
		message:    "connecting to controller",
		tasksCh:    make(chan taskResult, 1),
		projectsCh: make(chan projectResult, 1),
		detailCh:   make(chan detailResult, 8),
		logsCh:     make(chan logResult, 256),
		actionCh:   make(chan actionResult, 2),
	}
}

func (a *application) run() error {
	defer a.stop()
	ticker := time.NewTicker(a.options.RefreshInterval)
	defer ticker.Stop()
	a.refresh()
	a.draw()

	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case event, ok := <-a.vx.Events():
			if !ok {
				return nil
			}
			quit, err := a.handleVaxisEvent(event)
			if err != nil || quit {
				return err
			}
		case result := <-a.tasksCh:
			a.applyTasks(result)
		case result := <-a.projectsCh:
			a.applyProjects(result)
		case result := <-a.detailCh:
			a.applyDetail(result)
		case result := <-a.logsCh:
			a.applyLog(result)
			a.drainLogs()
		case result := <-a.actionCh:
			a.applyAction(result)
		case <-ticker.C:
			a.refresh()
			if a.logStop == nil && !a.logComplete {
				a.startLogs(a.model.SelectedTaskID())
			}
		}
		a.draw()
	}
}

func (a *application) stop() {
	a.cancel()
	if a.detailStop != nil {
		a.detailStop()
	}
	if a.logStop != nil {
		a.logStop()
	}
}

func (a *application) refresh() {
	if a.refreshing {
		return
	}
	a.refreshing = true
	a.refreshGen++
	a.projectsLoading = true
	generation := a.refreshGen
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, a.options.RequestTimeout)
		defer cancel()
		list, err := a.client.ListTasks(ctx, client.ListOptions{Limit: a.options.TaskLimit})
		result := taskResult{generation: generation, tasks: list.Tasks, err: err}
		select {
		case a.tasksCh <- result:
		case <-a.ctx.Done():
		}
	}()
	go func() {
		ctx, cancel := context.WithTimeout(a.ctx, a.options.RequestTimeout)
		defer cancel()
		list, err := a.client.ListProjects(ctx)
		select {
		case a.projectsCh <- projectResult{projects: list.Projects, err: err}:
		case <-a.ctx.Done():
		}
	}()
}

func (a *application) applyProjects(result projectResult) {
	a.projectsLoading = false
	if result.err == nil {
		a.projects = append([]client.Project(nil), result.projects...)
		a.projectCursor = min(a.projectCursor, max(0, len(a.projects)-1))
	}
}

func (a *application) applyTasks(result taskResult) {
	if result.generation != a.refreshGen {
		return
	}
	a.refreshing = false
	if result.err != nil {
		a.model.SetConnectivity(ConnectivityLost, "controller connection lost: "+shortError(result.err))
		a.message = "refresh failed"
		return
	}
	previous := a.model.Connectivity()
	if previous == ConnectivityLost {
		a.model.SetConnectivity(ConnectivityRestored)
		a.message = "controller connection restored"
	} else {
		a.model.SetConnectivity(ConnectivityConnected)
		if a.message == "connecting to controller" || a.message == "refresh failed" {
			a.message = "ready"
		}
	}
	oldID := a.model.SelectedTaskID()
	a.model.RefreshTasks(result.tasks)
	selectedID := a.model.SelectedTaskID()
	if selectedID == "" {
		a.model.SetDetail(TaskDetail{})
		a.model.ResetLogs()
		if a.logStop != nil {
			a.logStop()
			a.logStop = nil
		}
		return
	}
	if selectedID != oldID || a.model.Detail().Task.ID == "" {
		a.selectTask(selectedID)
		return
	}
	a.fetchDetail(selectedID)
}

func (a *application) fetchDetail(taskID string) {
	if taskID == "" {
		return
	}
	if a.detailStop != nil {
		a.detailStop()
	}
	a.detailGen++
	generation := a.detailGen
	ctx, stop := context.WithCancel(a.ctx)
	a.detailStop = stop
	go func() {
		requestCtx, cancel := context.WithTimeout(ctx, a.options.RequestTimeout)
		defer cancel()
		task, err := a.client.ShowTask(requestCtx, taskID)
		var attempts client.AttemptList
		if err == nil {
			attempts, err = a.client.ListAttempts(requestCtx, taskID, client.ListOptions{Limit: 100})
		}
		var events client.EventList
		if err == nil {
			events, err = a.client.ListEvents(requestCtx, taskID, client.ListOptions{Limit: 100})
		}
		result := detailResult{
			taskID:     taskID,
			generation: generation,
			detail:     TaskDetail{Task: task, Attempts: attempts.Attempts, Events: events.Events},
			err:        err,
		}
		select {
		case a.detailCh <- result:
		case <-ctx.Done():
		}
	}()
}

func (a *application) applyDetail(result detailResult) {
	if result.taskID != a.model.SelectedTaskID() || result.generation != a.detailGen {
		return
	}
	if result.err != nil {
		if !errors.Is(result.err, context.Canceled) {
			a.message = "details: " + shortError(result.err)
		}
		return
	}
	previousAttempt := a.model.Detail().Task.CurrentAttemptID
	a.model.SetDetail(result.detail)
	if previousAttempt != result.detail.Task.CurrentAttemptID {
		a.startLogs(result.taskID)
	}
}

func (a *application) startLogs(taskID string) {
	if taskID == "" {
		return
	}
	if a.logStop != nil {
		a.logStop()
	}
	a.logGen++
	generation := a.logGen
	attemptID := a.model.Detail().Task.CurrentAttemptID
	ctx, stop := context.WithCancel(a.ctx)
	a.logStop = stop
	a.logComplete = false
	a.logOffset = 0
	a.model.ResetLogs()
	go func() {
		err := a.client.StreamLogs(ctx, taskID, client.LogOptions{
			Follow: true, AttemptID: attemptID, TailLines: a.options.LogCapacity,
		}, func(line string) error {
			result := logResult{taskID: taskID, generation: generation, line: boundedLine(line)}
			select {
			case a.logsCh <- result:
			default:
			}
			return nil
		})
		result := logResult{taskID: taskID, generation: generation, err: err, done: true}
		select {
		case a.logsCh <- result:
		case <-ctx.Done():
		}
	}()
}

func (a *application) drainLogs() {
	for {
		select {
		case result := <-a.logsCh:
			a.applyLog(result)
		default:
			return
		}
	}
}

func (a *application) applyLog(result logResult) {
	if result.taskID != a.model.SelectedTaskID() || result.generation != a.logGen {
		return
	}
	if result.done {
		a.logStop = nil
		a.logComplete = result.err == nil
		if result.err != nil && !errors.Is(result.err, context.Canceled) {
			a.message = "log stream interrupted: " + shortError(result.err)
		}
		return
	}
	a.model.AppendLog(result.line)
}

func (a *application) selectTask(taskID string) {
	a.model.SetSelectedTask(taskID)
	for _, task := range a.model.Tasks() {
		if task.ID == taskID {
			a.model.SetDetail(TaskDetail{Task: task})
			break
		}
	}
	a.fetchDetail(taskID)
	a.startLogs(taskID)
}

func (a *application) moveSelection(delta int) {
	tasks := a.model.Tasks()
	if len(tasks) == 0 {
		return
	}
	selected := 0
	for i := range tasks {
		if tasks[i].ID == a.model.SelectedTaskID() {
			selected = i
			break
		}
	}
	selected = max(0, min(len(tasks)-1, selected+delta))
	if tasks[selected].ID != a.model.SelectedTaskID() {
		a.selectTask(tasks[selected].ID)
	}
}
