package tui

import (
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	"go.rockorager.dev/vaxis"
)

var palette = struct {
	header, title, dim, selected, border vaxis.Style
	ok, warn, bad, info                  vaxis.Style
}{
	header:   vaxis.Style{Foreground: vaxis.ColorWhite, Background: vaxis.ColorNavy, Attribute: vaxis.AttrBold},
	title:    vaxis.Style{Foreground: vaxis.ColorAqua, Attribute: vaxis.AttrBold},
	dim:      vaxis.Style{Foreground: vaxis.ColorSilver, Attribute: vaxis.AttrDim},
	selected: vaxis.Style{Foreground: vaxis.ColorBlack, Background: vaxis.ColorTeal, Attribute: vaxis.AttrBold},
	border:   vaxis.Style{Foreground: vaxis.ColorGray},
	ok:       vaxis.Style{Foreground: vaxis.ColorLime},
	warn:     vaxis.Style{Foreground: vaxis.ColorYellow},
	bad:      vaxis.Style{Foreground: vaxis.ColorRed, Attribute: vaxis.AttrBold},
	info:     vaxis.Style{Foreground: vaxis.ColorAqua},
}

func (a *application) draw() {
	root := a.vx.Window()
	root.Clear()
	width, height := root.Size()
	if width <= 0 || height <= 0 {
		return
	}
	a.vx.HideCursor()
	a.drawHeader(root.New(0, 0, width, 1))
	if height == 1 {
		a.vx.Render()
		return
	}
	a.drawFooter(root.New(0, height-1, width, 1))
	if height <= 4 {
		root.New(0, 1, width, height-2).PrintTruncate(0, vaxis.Segment{Text: "Terminal too small", Style: palette.warn})
		a.vx.Render()
		return
	}

	contentRows := height - 2
	logRows := 0
	if contentRows >= 10 {
		logRows = max(4, min(10, contentRows/3))
		if a.mode == viewLogs {
			logRows = max(logRows, contentRows/2)
		}
	}
	bodyRows := contentRows - logRows
	body := root.New(0, 1, width, bodyRows)
	narrow := width < 88
	if narrow {
		if a.narrowDetail {
			a.drawView(body)
		} else {
			a.drawTasks(body)
		}
	} else {
		listWidth := max(32, min(46, width*2/5))
		a.drawTasks(body.New(0, 0, listWidth, bodyRows))
		separator := body.New(listWidth, 0, 1, bodyRows)
		for row := 0; row < bodyRows; row++ {
			separator.SetCell(0, row, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: palette.border})
		}
		a.drawView(body.New(listWidth+1, 0, width-listWidth-1, bodyRows))
	}
	if logRows > 0 {
		a.drawLogs(root.New(0, 1+bodyRows, width, logRows))
	}
	if a.help {
		a.drawHelp(root)
	}
	if a.confirmCancel {
		a.drawCancelConfirmation(root)
	}
	a.vx.Render()
}

func (a *application) drawHeader(win vaxis.Window) {
	width, _ := win.Size()
	fill(win, palette.header)
	address := firstNonempty(a.options.Address, "controller")
	contextName := firstNonempty(a.options.KubeContext, "current")
	state := a.model.Connectivity()
	statusStyle := palette.dim
	marker := "○"
	switch state {
	case ConnectivityUnknown:
	case ConnectivityConnected:
		statusStyle, marker = palette.ok, "●"
	case ConnectivityRestored:
		statusStyle, marker = palette.info, "●"
	case ConnectivityLost:
		statusStyle, marker = palette.bad, "●"
	}
	left := fmt.Sprintf(" simpleswe  %s  ctx:%s  ns:%s", address, contextName, a.options.Namespace)
	status := " " + marker + " " + string(state) + " "
	if len(left)+len(status) < width {
		left += strings.Repeat(" ", width-len(left)-len(status))
	}
	win.PrintTruncate(0,
		vaxis.Segment{Text: left, Style: palette.header},
		vaxis.Segment{Text: status, Style: mergeStyle(palette.header, statusStyle)},
	)
}

func (a *application) drawFooter(win vaxis.Window) {
	fill(win, palette.header)
	width, _ := win.Size()
	keys := " ↑↓ move  enter details  l logs  e events  j job  p pod  s shell  r retry  x cancel  R refresh  ? help  q back/quit"
	if width < 82 {
		keys = " ↑↓ move  enter details  ? help  q back/quit"
	}
	statusMessage := a.message
	if state := a.model.Connectivity(); state == ConnectivityLost || state == ConnectivityRestored {
		statusMessage = a.model.ConnectivityMessage()
	}
	message := "  " + statusMessage
	if len(keys)+len(message) < width {
		keys += strings.Repeat(" ", width-len(keys)-len(message)) + message
	}
	win.PrintTruncate(0, vaxis.Segment{Text: keys, Style: palette.header})
}

func (a *application) drawTasks(win vaxis.Window) {
	width, height := win.Size()
	if width <= 0 || height <= 0 {
		return
	}
	tasks := a.model.Tasks()
	win.PrintTruncate(0,
		vaxis.Segment{Text: fmt.Sprintf(" TASKS %d", len(tasks)), Style: palette.title},
		vaxis.Segment{Text: "  live", Style: palette.dim},
	)
	if height == 1 {
		return
	}
	win.PrintTruncate(1, vaxis.Segment{Text: " STATE      REPOSITORY / TASK                 AGE", Style: palette.dim})
	if len(tasks) == 0 {
		if height > 3 {
			win.PrintTruncate(3, vaxis.Segment{Text: " No tasks returned · R refresh", Style: palette.dim})
		}
		return
	}

	selected := selectedIndex(tasks, a.model.SelectedTaskID())
	visible := max(0, height-2)
	start := max(0, selected-visible+1)
	for row, index := 2, start; row < height && index < len(tasks); row, index = row+1, index+1 {
		task := tasks[index]
		state := fmt.Sprintf("%-10s", strings.ToUpper(task.State))
		label := compactRepository(task.Repository)
		if label == "" {
			label = task.ID
		} else if width >= 42 {
			label += "  " + shortID(task.ID)
		}
		age := compactAge(task.CreatedAt)
		line := fmt.Sprintf(" %-10s %-28s %5s", state, label, age)
		style := stateStyle(task.State)
		if index == selected {
			style = palette.selected
		}
		win.PrintTruncate(row, vaxis.Segment{Text: line, Style: style})
	}
}

func (a *application) drawView(win vaxis.Window) {
	switch a.mode {
	case viewLogs:
		a.drawLogSummary(win)
	case viewEvents:
		a.drawEvents(win)
	case viewJob:
		a.drawJob(win)
	case viewPod:
		a.drawPod(win)
	default:
		a.drawDetails(win)
	}
}

func (a *application) drawDetails(win vaxis.Window) {
	detail := a.model.Detail()
	if detail.Task.ID == "" {
		drawEmptySelection(win)
		return
	}
	attempt := currentAttempt(detail)
	win.PrintTruncate(0,
		vaxis.Segment{Text: " TASK / ATTEMPT", Style: palette.title},
		vaxis.Segment{Text: "  enter", Style: palette.dim},
	)
	row := 2
	row = drawField(win, row, "Task", detail.Task.ID, vaxis.Style{})
	row = drawField(win, row, "State", strings.ToUpper(detail.Task.State), stateStyle(detail.Task.State))
	row = drawField(win, row, "Repository", detail.Task.Repository, vaxis.Style{})
	row = drawField(win, row, "Created", formatTime(detail.Task.CreatedAt), vaxis.Style{})
	row = drawField(win, row, "Updated", formatTime(detail.Task.UpdatedAt), vaxis.Style{})
	row++
	if attempt.ID == "" {
		row = drawField(win, row, "Attempt", "not scheduled", palette.dim)
	} else {
		row = drawField(win, row, "Attempt", fmt.Sprintf("#%d  %s", attempt.Number, attempt.ID), vaxis.Style{})
		row = drawField(win, row, "Attempt state", strings.ToUpper(attempt.State), stateStyle(attempt.State))
		row = drawField(win, row, "Job", firstNonempty(attempt.KubernetesJob.ResourceIdentity.Name, "—"), vaxis.Style{})
		row = drawField(win, row, "Job state", firstNonempty(strings.ToUpper(attempt.KubernetesJob.State), "—"), stateStyle(attempt.KubernetesJob.State))
		row = drawField(win, row, "Pod", firstNonempty(attempt.KubernetesPod.ResourceIdentity.Name, "—"), vaxis.Style{})
	}
	row++
	drawField(win, row, "Prompt", firstNonempty(detail.Task.Prompt, "—"), vaxis.Style{})
}

func (a *application) drawEvents(win vaxis.Window) {
	detail := a.model.Detail()
	win.PrintTruncate(0,
		vaxis.Segment{Text: fmt.Sprintf(" EVENTS %d", len(detail.Events)), Style: palette.title},
		vaxis.Segment{Text: "  newest last", Style: palette.dim},
	)
	_, height := win.Size()
	if len(detail.Events) == 0 {
		if height > 2 {
			win.PrintTruncate(2, vaxis.Segment{Text: " No lifecycle events for this task", Style: palette.dim})
		}
		return
	}
	visible := max(0, height-2)
	start := max(0, len(detail.Events)-visible)
	row := 2
	for _, event := range detail.Events[start:] {
		transition := event.ToState
		if event.FromState != "" {
			transition = event.FromState + " → " + event.ToState
		}
		line := fmt.Sprintf(" %s  %-22s  %s", event.OccurredAt.Local().Format("15:04:05"), transition, firstNonempty(event.Reason, event.Trigger))
		win.PrintTruncate(row, vaxis.Segment{Text: line, Style: stateStyle(event.ToState)})
		row++
	}
}

func (a *application) drawJob(win vaxis.Window) {
	detail := a.model.Detail()
	attempt := currentAttempt(detail)
	job := attempt.KubernetesJob
	if job.ResourceIdentity.Name == "" {
		job = detail.Task.KubernetesJob
	}
	win.PrintTruncate(0, vaxis.Segment{Text: " JOB", Style: palette.title})
	row := 2
	row = drawField(win, row, "Name", firstNonempty(job.ResourceIdentity.Name, "—"), vaxis.Style{})
	row = drawField(win, row, "Namespace", firstNonempty(job.ResourceIdentity.Namespace, a.options.Namespace), vaxis.Style{})
	row = drawField(win, row, "State", firstNonempty(strings.ToUpper(job.State), "—"), stateStyle(job.State))
	drawField(win, row, "Reason", firstNonempty(job.Reason, job.Message, "—"), vaxis.Style{})
}

func (a *application) drawPod(win vaxis.Window) {
	detail := a.model.Detail()
	attempt := currentAttempt(detail)
	pod := attempt.KubernetesPod
	if pod.ResourceIdentity.Name == "" {
		pod = detail.Task.KubernetesPod
	}
	win.PrintTruncate(0, vaxis.Segment{Text: " POD", Style: palette.title})
	row := 2
	row = drawField(win, row, "Name", firstNonempty(pod.ResourceIdentity.Name, "—"), vaxis.Style{})
	row = drawField(win, row, "Namespace", firstNonempty(pod.ResourceIdentity.Namespace, a.options.Namespace), vaxis.Style{})
	row = drawField(win, row, "State", firstNonempty(strings.ToUpper(pod.State), "—"), stateStyle(pod.State))
	drawField(win, row, "Reason", firstNonempty(pod.Reason, pod.Message, "—"), vaxis.Style{})
}

func (a *application) drawLogSummary(win vaxis.Window) {
	detail := a.model.Detail()
	attempt := currentAttempt(detail)
	win.PrintTruncate(0, vaxis.Segment{Text: " LOG STREAM", Style: palette.title})
	row := 2
	row = drawField(win, row, "Task", firstNonempty(detail.Task.ID, "—"), vaxis.Style{})
	row = drawField(win, row, "Attempt", firstNonempty(attempt.ID, "—"), vaxis.Style{})
	row = drawField(win, row, "Pod", firstNonempty(attempt.KubernetesPod.ResourceIdentity.Name, detail.Task.KubernetesPod.ResourceIdentity.Name, "—"), vaxis.Style{})
	row = drawField(win, row, "Buffered", fmt.Sprintf("%d / %d lines", len(a.model.Logs()), a.options.LogCapacity), vaxis.Style{})
	drawField(win, row, "Follow", map[bool]string{true: "connected", false: "reconnecting"}[a.logStop != nil], map[bool]vaxis.Style{true: palette.ok, false: palette.warn}[a.logStop != nil])
}

func (a *application) drawLogs(win vaxis.Window) {
	width, height := win.Size()
	if width <= 0 || height <= 0 {
		return
	}
	for col := 0; col < width; col++ {
		win.SetCell(col, 0, vaxis.Cell{Character: vaxis.Character{Grapheme: "─", Width: 1}, Style: palette.border})
	}
	win.PrintTruncate(0,
		vaxis.Segment{Text: " LOGS ", Style: palette.title},
		vaxis.Segment{Text: fmt.Sprintf("%d buffered", len(a.model.Logs())), Style: palette.dim},
	)
	logs := a.model.Logs()
	visible := max(0, height-1)
	if len(logs) == 0 {
		if height > 1 {
			win.PrintTruncate(1, vaxis.Segment{Text: " Waiting for log stream…", Style: palette.dim})
		}
		return
	}
	start := max(0, len(logs)-visible)
	for index, line := range rangeSlice(logs, start) {
		row := index + 1
		if row >= height {
			break
		}
		win.PrintTruncate(row, vaxis.Segment{Text: " " + line, Style: vaxis.Style{}})
	}
}

func (a *application) drawHelp(root vaxis.Window) {
	lines := []string{
		"KEYS",
		"↑ / ↓   select task",
		"enter   task and attempt details",
		"l       logs          e  events",
		"j       Job details   p  Pod details",
		"s       shell         r  retry",
		"x       cancel        R  refresh",
		"?       close help    q  back / quit",
		"",
		"Cancellation always asks for confirmation.",
	}
	drawOverlay(root, min(62, root.Width-4), min(len(lines)+4, root.Height-2), " SIMPLESWE HELP ", lines)
}

func (a *application) drawCancelConfirmation(root vaxis.Window) {
	taskID := a.model.SelectedTaskID()
	lines := []string{
		"Cancel task " + taskID + "?",
		"The controller will stop its active attempt.",
		"",
		"y / enter  confirm     n / esc  keep running",
	}
	drawOverlay(root, min(68, root.Width-4), min(8, root.Height-2), " CONFIRM CANCELLATION ", lines)
}

func drawOverlay(root vaxis.Window, width, height int, title string, lines []string) {
	if width < 8 || height < 4 {
		return
	}
	col, row := (root.Width-width)/2, (root.Height-height)/2
	win := root.New(col, row, width, height)
	style := vaxis.Style{Foreground: vaxis.ColorWhite, Background: vaxis.ColorNavy}
	fill(win, style)
	for x := 0; x < width; x++ {
		win.SetCell(x, 0, vaxis.Cell{Character: vaxis.Character{Grapheme: "─", Width: 1}, Style: style})
		win.SetCell(x, height-1, vaxis.Cell{Character: vaxis.Character{Grapheme: "─", Width: 1}, Style: style})
	}
	for y := 0; y < height; y++ {
		win.SetCell(0, y, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: style})
		win.SetCell(width-1, y, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: style})
	}
	win.PrintTruncate(0, vaxis.Segment{Text: title, Style: mergeStyle(style, palette.title)})
	for i, line := range lines {
		if i+2 >= height-1 {
			break
		}
		win.New(2, i+2, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: line, Style: style})
	}
}

func drawEmptySelection(win vaxis.Window) {
	win.PrintTruncate(0, vaxis.Segment{Text: " TASK / ATTEMPT", Style: palette.title})
	if win.Height > 2 {
		win.PrintTruncate(2, vaxis.Segment{Text: " Select a task with ↑ / ↓", Style: palette.dim})
	}
}

func drawField(win vaxis.Window, row int, label, value string, valueStyle vaxis.Style) int {
	if row >= win.Height {
		return row
	}
	win.PrintTruncate(row,
		vaxis.Segment{Text: fmt.Sprintf(" %-13s", label), Style: palette.dim},
		vaxis.Segment{Text: value, Style: valueStyle},
	)
	return row + 1
}

func fill(win vaxis.Window, style vaxis.Style) {
	win.Fill(vaxis.Cell{Character: vaxis.Character{Grapheme: " ", Width: 1}, Style: style})
}

func mergeStyle(base, accent vaxis.Style) vaxis.Style {
	if accent.Foreground != vaxis.ColorDefault {
		base.Foreground = accent.Foreground
	}
	base.Attribute |= accent.Attribute
	return base
}

func stateStyle(state string) vaxis.Style {
	switch strings.ToLower(state) {
	case "succeeded", "success", "completed", "complete", "running", "active", "ready":
		return palette.ok
	case "failed", "error", "cancelled", "canceled", "lost":
		return palette.bad
	case "queued", "pending", "retrying", "cancelling", "canceling":
		return palette.warn
	default:
		return palette.info
	}
}

func currentAttempt(detail TaskDetail) Attempt {
	for _, attempt := range detail.Attempts {
		if attempt.ID == detail.Task.CurrentAttemptID {
			return attempt
		}
	}
	if len(detail.Attempts) > 0 {
		return detail.Attempts[len(detail.Attempts)-1]
	}
	return Attempt{}
}

func selectedIndex(tasks []Task, id string) int {
	for i := range tasks {
		if tasks[i].ID == id {
			return i
		}
	}
	return 0
}

func compactRepository(repository string) string {
	if parsed, err := url.Parse(repository); err == nil && parsed.Path != "" {
		repository = strings.Trim(parsed.Path, "/")
	}
	repository = strings.TrimSuffix(repository, ".git")
	parts := strings.Split(repository, "/")
	if len(parts) > 2 {
		parts = parts[len(parts)-2:]
	}
	cleaned := strings.Join(parts, "/")
	if cleaned == "" {
		return ""
	}
	return path.Clean(cleaned)
}

func shortID(id string) string {
	if len(id) <= 8 {
		return id
	}
	return id[:8]
}

func compactAge(created time.Time) string {
	if created.IsZero() {
		return "—"
	}
	age := time.Since(created)
	if age < 0 || age < time.Minute {
		return "now"
	}
	if age < time.Hour {
		return fmt.Sprintf("%dm", int(age.Minutes()))
	}
	if age < 24*time.Hour {
		return fmt.Sprintf("%dh", int(age.Hours()))
	}
	return fmt.Sprintf("%dd", int(age.Hours()/24))
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Local().Format("2006-01-02 15:04:05 MST")
}

func rangeSlice(values []string, start int) []string {
	if start < 0 || start >= len(values) {
		return values
	}
	return values[start:]
}
