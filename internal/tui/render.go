package tui

import (
	"fmt"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/simpleswe/simpleswe/internal/client"
	"go.rockorager.dev/vaxis"
)

func (a *application) draw() {
	root := a.vx.Window()
	root.Clear()
	palette := a.colors()
	fill(root, palette.base)
	width, height := root.Size()
	if width <= 0 || height <= 0 {
		return
	}
	a.vx.HideCursor()
	headerRows, footerRows := 2, 2
	if height < 7 {
		headerRows, footerRows = 1, 1
	}
	a.drawHeader(root.New(0, 0, width, headerRows))
	if height == 1 {
		if a.createModal {
			message := "CREATE TASK · Esc cancels"
			if a.createAccepted {
				message = "CREATE TASK · accepted · press any key"
			} else if a.createPending {
				message = "CREATE TASK · Esc hides; request continues"
			}
			root.PrintTruncate(0, vaxis.Segment{Text: message, Style: palette.warn})
		}
		a.vx.Render()
		return
	}
	a.drawFooter(root.New(0, height-footerRows, width, footerRows))
	if height <= headerRows+footerRows+2 {
		message := "Terminal too small"
		if a.createModal {
			switch {
			case a.createAccepted:
				message += " · create accepted · press any key"
			case a.createPending:
				message += " · Esc hides; request continues"
			default:
				message += " · Esc cancels create"
			}
		}
		root.New(0, headerRows, width, 1).PrintTruncate(0, vaxis.Segment{Text: message, Style: palette.warn})
		a.vx.Render()
		return
	}

	contentRows := height - headerRows - footerRows
	contentTop := headerRows
	if a.mode == viewLogs {
		a.drawLogs(root.New(0, contentTop, width, contentRows))
		if a.help {
			a.drawHelp(root)
		}
		if a.themePicker {
			a.drawThemeSwitcher(root)
		}
		if a.confirmAction != "" {
			a.drawActionConfirmation(root)
		}
		if a.createModal {
			a.drawCreateTask(root)
		}
		a.vx.Render()
		return
	}
	narrow := width < 100
	if narrow {
		logRows := 0
		if contentRows >= 10 {
			logRows = max(4, min(10, contentRows/3))
		}
		bodyRows := contentRows - logRows
		if a.narrowDetail {
			a.drawView(root.New(0, contentTop, width, bodyRows))
		} else {
			a.drawTasks(root.New(0, contentTop, width, bodyRows))
		}
		if logRows > 0 {
			a.drawLogs(root.New(0, contentTop+bodyRows, width, logRows))
		}
	} else {
		listWidth := max(32, min(46, width*2/5))
		rightWidth := width - listWidth - 1
		logRows := 0
		if contentRows >= 14 {
			logRows = max(5, min(12, contentRows/4))
		}
		bodyRows := contentRows - logRows
		a.drawTasks(root.New(0, contentTop, listWidth, contentRows))
		separator := root.New(listWidth, contentTop, 1, contentRows)
		for row := range contentRows {
			separator.SetCell(0, row, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: palette.border})
		}
		a.drawView(root.New(listWidth+1, contentTop, rightWidth, bodyRows))
		if logRows > 0 {
			a.drawLogs(root.New(listWidth+1, contentTop+bodyRows, rightWidth, logRows))
		}
	}
	if a.help {
		a.drawHelp(root)
	}
	if a.themePicker {
		a.drawThemeSwitcher(root)
	}
	if a.confirmAction != "" {
		a.drawActionConfirmation(root)
	}
	if a.createModal {
		a.drawCreateTask(root)
	}
	a.vx.Render()
}

func (a *application) drawHeader(win vaxis.Window) {
	palette := a.colors()
	width, height := win.Size()
	fill(win, palette.base)
	if width <= 0 || height <= 0 {
		return
	}
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
	status := " " + marker + " " + string(state) + " "
	statusWidth := textWidth(status)
	leftWidth := max(0, width-statusWidth)
	if leftWidth > 0 {
		left := win.New(0, 0, leftWidth, 1)
		segments := []vaxis.Segment{
			{Text: " SimpleSWE ", Style: palette.title},
			{Text: "│  cluster: ", Style: palette.dim},
			{Text: contextName + "  ", Style: palette.info},
			{Text: "│  namespace: ", Style: palette.dim},
			{Text: a.options.Namespace + "  ", Style: palette.info},
		}
		if width >= 130 && a.options.Address != "" {
			segments = append(segments,
				vaxis.Segment{Text: "│  controller: ", Style: palette.dim},
				vaxis.Segment{Text: a.options.Address + "  ", Style: palette.info},
			)
		}
		left.PrintTruncate(0, segments...)
	}
	if statusWidth <= width {
		win.New(width-statusWidth, 0, statusWidth, 1).PrintTruncate(0, vaxis.Segment{Text: status, Style: mergeStyle(palette.base, statusStyle)})
	}
	if height > 1 {
		drawHorizontalRule(win, 1, palette.border)
	}
}

func (a *application) drawFooter(win vaxis.Window) {
	palette := a.colors()
	fill(win, palette.base)
	width, height := win.Size()
	if width <= 0 || height <= 0 {
		return
	}
	row := 0
	if height > 1 {
		drawHorizontalRule(win, 0, palette.border)
		row = 1
	}
	statusMessage := a.message
	if state := a.model.Connectivity(); state == ConnectivityLost || state == ConnectivityRestored {
		statusMessage = a.model.ConnectivityMessage()
	}
	right := " q quit "
	if statusMessage != "" {
		right = " " + statusMessage + "  " + right
	}
	if len(right) > width/2 {
		right = " q quit "
	}
	rightWidth := min(width, len(right))
	leftWidth := max(0, width-rightWidth)
	shortcuts := []shortcut{{"n", "create"}, {"j/k", "move"}, {"↵", "details"}, {"l", "logs"}, {"e", "events"}, {"d", "job"}, {"p", "pod"}, {"s", "shell"}, {"r", "retry"}, {"^D", "cancel"}, {"t", "theme"}, {"?", "help"}}
	if width < 100 {
		shortcuts = []shortcut{{"n", "create"}, {"j/k", "move"}, {"↵", "details"}, {"l", "logs"}, {"t", "theme"}, {"?", "help"}}
	}
	if leftWidth > 0 {
		drawShortcuts(win.New(0, row, leftWidth, 1), shortcuts, palette)
	}
	if rightWidth > 0 {
		win.New(width-rightWidth, row, rightWidth, 1).PrintTruncate(0, vaxis.Segment{Text: right, Style: palette.dim})
	}
}

func (a *application) drawTasks(win vaxis.Window) {
	palette := a.colors()
	width, height := win.Size()
	if width <= 0 || height <= 0 {
		return
	}
	tasks := a.model.Tasks()
	win.PrintTruncate(0, vaxis.Segment{Text: " TASKS", Style: palette.title})
	count := fmt.Sprintf("%d total ", len(tasks))
	if len(count) < width {
		win.New(width-len(count), 0, len(count), 1).PrintTruncate(0, vaxis.Segment{Text: count, Style: palette.dim})
	}
	if height == 1 {
		return
	}
	header := " STATE       REPOSITORY       TASK      AGE"
	if width < 42 {
		header = " STATE       REPOSITORY           AGE"
	}
	win.PrintTruncate(1, vaxis.Segment{Text: header, Style: palette.dim})
	if len(tasks) == 0 {
		if height > 3 {
			win.PrintTruncate(3, vaxis.Segment{Text: " No tasks returned · R refresh", Style: palette.dim})
		}
		return
	}

	footerRow := height
	if height >= 6 {
		footerRow = height - 2
		drawHorizontalRule(win, footerRow, palette.border)
		win.PrintTruncate(footerRow+1,
			vaxis.Segment{Text: " ↑/↓", Style: palette.info},
			vaxis.Segment{Text: " navigate   ", Style: palette.dim},
			vaxis.Segment{Text: "R", Style: palette.info},
			vaxis.Segment{Text: " refresh", Style: palette.dim},
		)
	}
	selected := selectedIndex(tasks, a.model.SelectedTaskID())
	visible := max(0, footerRow-2)
	start := max(0, selected-visible+1)
	for row, index := 2, start; row < footerRow && index < len(tasks); row, index = row+1, index+1 {
		task := tasks[index]
		rowStyle := palette.base
		if index == selected {
			rowStyle = palette.selected
			fill(win.New(0, row, width, 1), rowStyle)
		}
		state := strings.ToUpper(task.State)
		marker := stateMarker(task.State)
		stateText := fmt.Sprintf(" %s %-9s", marker, truncateText(state, 9))
		stateStyle := mergeStyle(rowStyle, a.stateStyle(task.State))
		repository := compactRepository(task.Repository)
		if repository == "" {
			repository = "—"
		}
		if width >= 42 {
			win.PrintTruncate(row,
				vaxis.Segment{Text: stateText, Style: stateStyle},
				vaxis.Segment{Text: fmt.Sprintf(" %-16s %-8s %4s", truncateText(repository, 16), shortID(task.ID), compactAge(task.CreatedAt)), Style: rowStyle},
			)
		} else {
			win.PrintTruncate(row,
				vaxis.Segment{Text: stateText, Style: stateStyle},
				vaxis.Segment{Text: fmt.Sprintf(" %-16s %4s", truncateText(repository, 16), compactAge(task.CreatedAt)), Style: rowStyle},
			)
		}
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
	palette := a.colors()
	width, height := win.Size()
	if width <= 0 || height <= 0 {
		return
	}
	detail := a.model.Detail()
	if detail.Task.ID == "" {
		a.drawEmptySelection(win)
		return
	}
	attempt := currentAttempt(detail)
	state := strings.ToUpper(firstNonempty(detail.Task.State, "unknown"))
	badge := "[ " + state + " ]"
	badgeWidth := textWidth(badge) + 1
	leftWidth := max(0, width-badgeWidth)
	if leftWidth > 0 {
		win.New(0, 0, leftWidth, 1).PrintTruncate(0,
			vaxis.Segment{Text: " TASK  ", Style: palette.title},
			vaxis.Segment{Text: detail.Task.ID, Style: palette.base},
		)
	}
	if badgeWidth <= width {
		win.New(width-badgeWidth, 0, badgeWidth, 1).PrintTruncate(0, vaxis.Segment{Text: badge + " ", Style: a.stateStyle(detail.Task.State)})
	}

	started, completed := attemptTimes(detail.Task, attempt)
	if height > 1 {
		timing := "Started  " + formatClock(started) + "   Duration  " + formatDuration(started, completed)
		if textWidth(timing)+1 < width {
			win.New(width-textWidth(timing)-1, 1, textWidth(timing)+1, 1).PrintTruncate(0, vaxis.Segment{Text: timing + " ", Style: palette.dim})
		}
	}
	if height > 2 {
		win.PrintTruncate(2,
			vaxis.Segment{Text: " PROMPT      ", Style: palette.dim},
			vaxis.Segment{Text: firstNonempty(detail.Task.Prompt, "—"), Style: palette.base},
		)
	}

	metadataTop := 4
	if metadataTop >= height {
		return
	}
	branch := firstNonempty(attempt.GitResult.Branch, detail.Task.GitResult.Branch, "—")
	pod := firstNonempty(attempt.KubernetesPod.ResourceIdentity.Name, detail.Task.KubernetesPod.ResourceIdentity.Name, "—")
	attemptLabel := "not scheduled"
	if attempt.ID != "" {
		attemptLabel = fmt.Sprintf("#%d", attempt.Number)
	}
	pr := "N/A"
	pullRequest := attempt.PullRequest
	if pullRequest.Number == 0 {
		pullRequest = detail.Task.PullRequest
	}
	if pullRequest.Number > 0 {
		pr = fmt.Sprintf("#%d", pullRequest.Number)
	}
	items := []metadataItem{
		{"▣", "Repository", firstNonempty(compactRepository(detail.Task.Repository), "—")},
		{"⑂", "Branch", branch},
		{"#", "Attempt", attemptLabel},
		{"◇", "Pod", pod},
		{"↗", "PR", pr},
	}
	metadataBottom := drawMetadataCard(win, metadataTop, items, palette)
	a.drawPipeline(win, metadataBottom+2, detail)
}

func (a *application) drawEvents(win vaxis.Window) {
	palette := a.colors()
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
		win.PrintTruncate(row, vaxis.Segment{Text: line, Style: a.stateStyle(event.ToState)})
		row++
	}
}

func (a *application) drawJob(win vaxis.Window) {
	palette := a.colors()
	detail := a.model.Detail()
	attempt := currentAttempt(detail)
	job := attempt.KubernetesJob
	if job.ResourceIdentity.Name == "" {
		job = detail.Task.KubernetesJob
	}
	win.PrintTruncate(0, vaxis.Segment{Text: " ▣  JOB", Style: palette.title})
	row := 2
	row = a.drawField(win, row, "Name", firstNonempty(job.ResourceIdentity.Name, "—"), vaxis.Style{})
	row = a.drawField(win, row, "Namespace", firstNonempty(job.ResourceIdentity.Namespace, a.options.Namespace), vaxis.Style{})
	row = a.drawField(win, row, "State", firstNonempty(strings.ToUpper(job.State), "—"), a.stateStyle(job.State))
	a.drawField(win, row, "Reason", firstNonempty(job.Reason, job.Message, "—"), vaxis.Style{})
}

func (a *application) drawPod(win vaxis.Window) {
	palette := a.colors()
	detail := a.model.Detail()
	attempt := currentAttempt(detail)
	pod := attempt.KubernetesPod
	if pod.ResourceIdentity.Name == "" {
		pod = detail.Task.KubernetesPod
	}
	win.PrintTruncate(0, vaxis.Segment{Text: " ◇  POD", Style: palette.title})
	row := 2
	row = a.drawField(win, row, "Name", firstNonempty(pod.ResourceIdentity.Name, "—"), vaxis.Style{})
	row = a.drawField(win, row, "Namespace", firstNonempty(pod.ResourceIdentity.Namespace, a.options.Namespace), vaxis.Style{})
	row = a.drawField(win, row, "State", firstNonempty(strings.ToUpper(pod.State), "—"), a.stateStyle(pod.State))
	a.drawField(win, row, "Reason", firstNonempty(pod.Reason, pod.Message, "—"), vaxis.Style{})
}

func (a *application) drawLogSummary(win vaxis.Window) {
	palette := a.colors()
	detail := a.model.Detail()
	attempt := currentAttempt(detail)
	win.PrintTruncate(0, vaxis.Segment{Text: " ≡  LOG STREAM", Style: palette.title})
	row := 2
	row = a.drawField(win, row, "Task", firstNonempty(detail.Task.ID, "—"), vaxis.Style{})
	row = a.drawField(win, row, "Attempt", firstNonempty(attempt.ID, "—"), vaxis.Style{})
	row = a.drawField(win, row, "Pod", firstNonempty(attempt.KubernetesPod.ResourceIdentity.Name, detail.Task.KubernetesPod.ResourceIdentity.Name, "—"), vaxis.Style{})
	row = a.drawField(win, row, "Buffered", fmt.Sprintf("%d / %d lines", len(a.model.Logs()), a.options.LogCapacity), vaxis.Style{})
	a.drawField(win, row, "Follow", map[bool]string{true: "connected", false: "reconnecting"}[a.logStop != nil], map[bool]vaxis.Style{true: palette.ok, false: palette.warn}[a.logStop != nil])
}

func (a *application) drawLogs(win vaxis.Window) {
	palette := a.colors()
	width, height := win.Size()
	if width <= 0 || height <= 0 {
		return
	}
	for col := 0; col < width; col++ {
		win.SetCell(col, 0, vaxis.Cell{Character: vaxis.Character{Grapheme: "─", Width: 1}, Style: palette.border})
	}
	title := " ≡  LOGS (latest) "
	if a.mode == viewLogs {
		title = " ≡  LOG STREAM "
	}
	info := fmt.Sprintf("%d lines ", len(a.model.Logs()))
	if a.wrapLogs {
		info = fmt.Sprintf("%d lines · wrap on ", len(a.model.Logs()))
	}
	win.PrintTruncate(0, vaxis.Segment{Text: title, Style: palette.title})
	if textWidth(info) < width {
		win.New(width-textWidth(info), 0, textWidth(info), 1).PrintTruncate(0, vaxis.Segment{Text: info, Style: palette.dim})
	}
	logs := a.model.Logs()
	visible := max(0, height-1)
	if len(logs) == 0 {
		a.logOffset = 0
		if height > 1 {
			win.PrintTruncate(1, vaxis.Segment{Text: " Waiting for log stream…", Style: palette.dim})
		}
		return
	}
	rows := make([][]vaxis.Segment, 0, len(logs))
	for _, line := range logs {
		if a.wrapLogs {
			rows = append(rows, wrappedLogRows(line, palette.base, width)...)
		} else {
			base := palette.base
			lower := strings.ToLower(line)
			if !strings.Contains(line, "\x1b[") && (strings.Contains(lower, "error") || strings.Contains(lower, "failed") || strings.Contains(lower, "fatal")) {
				base = palette.bad
			}
			rows = append(rows, ansiSegments(line, base))
		}
	}
	a.logOffset = min(a.logOffset, max(0, len(rows)-visible))
	start := max(0, len(rows)-visible-a.logOffset)
	end := min(len(rows), start+visible)
	for index, segments := range rows[start:end] {
		row := index + 1
		win.PrintTruncate(row, segments...)
	}
}

func (a *application) drawHelp(root vaxis.Window) {
	lines := []string{
		"KEYS",
		"j / k   select task     g / G  first / last",
		"n       create task",
		"enter   task and attempt details",
		"l       logs          e  events",
		"d       Job details   p  Pod details",
		"s       shell         r  restart",
		"ctrl-d  cancel        R  refresh",
		"t       choose theme  h  back",
		"w       wrap logs     ?  help",
		"q       back / quit",
		"",
		"Restart and cancellation always ask for confirmation.",
	}
	a.drawOverlay(root, min(62, root.Width-4), min(len(lines)+4, root.Height-2), " SIMPLESWE HELP ", lines)
}

func (a *application) drawActionConfirmation(root vaxis.Window) {
	taskID := a.model.SelectedTaskID()
	lines := []string{
		"Restart task " + taskID + "?",
		"The controller will create a new attempt.",
		"",
		"y / enter  confirm     n / esc  keep current attempt",
	}
	title := " CONFIRM RESTART "
	if a.confirmAction == "cancel" {
		title = " CONFIRM CANCELLATION "
		lines = []string{
			"Cancel task " + taskID + "?",
			"The controller will stop its active attempt.",
			"",
			"y / enter  confirm     n / esc  keep running",
		}
	}
	a.drawOverlay(root, min(68, root.Width-4), min(8, root.Height-2), title, lines)
}

func (a *application) drawCreateTask(root vaxis.Window) {
	if a.createAccepted {
		width, height := min(72, root.Width-4), min(8, root.Height-2)
		lines := []string{"Task accepted.", "Press any key to close."}
		if width < len(lines[1])+4 || height < len(lines)+3 {
			row := max(1, root.Height/2-1)
			style := a.colors().warn
			compact := root.New(0, row, root.Width, 3)
			fill(compact, style)
			compact.PrintTruncate(0, vaxis.Segment{Text: "CREATE TASK", Style: style})
			compact.PrintTruncate(1, vaxis.Segment{Text: "accepted", Style: style})
			compact.PrintTruncate(2, vaxis.Segment{Text: "any key", Style: style})
			return
		}
		a.drawOverlay(root, width, height, " CREATE TASK ", lines)
		return
	}
	width := min(72, root.Width-4)
	height := min(18, root.Height-2)
	if width < 32 || height < 8 {
		text := "CREATE TASK · Esc cancels"
		if a.createPending {
			text = "Esc hides; request continues"
		}
		root.New(0, max(1, root.Height/2), root.Width, 1).PrintTruncate(0, vaxis.Segment{Text: text, Style: a.colors().warn})
		return
	}
	win := root.New((root.Width-width)/2, (root.Height-height)/2, width, height)
	a.drawOverlayFrame(win, " CREATE TASK ")
	palette := a.colors()
	repositoryStyle, promptStyle := palette.overlay, palette.overlay
	if a.createField == createRepositoryField {
		repositoryStyle = palette.selected
	} else {
		promptStyle = palette.selected
	}
	if len(a.projects) > 0 {
		win.New(2, 2, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: "Project: " + a.projects[a.projectCursor].Name, Style: repositoryStyle})
		for row, index := 3, max(0, a.projectCursor-2); row < min(height-3, 6) && index < len(a.projects); row, index = row+1, index+1 {
			style := palette.overlay
			if index == a.projectCursor {
				style = palette.selected
			}
			win.New(2, row, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: a.projects[index].Name, Style: style})
		}
	} else {
		a.createRepo.Content, a.createRepo.Prompt = repositoryStyle, repositoryStyle
		a.createRepo.HideCursor = a.createField != createRepositoryField || a.createPending
		a.createRepo.Draw(win.New(2, 2, width-4, 1))
	}
	a.createPrompt.Content, a.createPrompt.Prompt = promptStyle, promptStyle
	a.createPrompt.HideCursor = a.createField != createPromptField || a.createPending
	promptRow := 3
	if len(a.projects) > 0 {
		promptRow = 8
	}
	a.createPrompt.Draw(win.New(2, promptRow, width-4, 1))
	if a.createError != "" {
		win.New(2, 5, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: a.createError, Style: palette.bad})
	}
	instructions := "tab next field  enter submit  esc cancel"
	if len(a.projects) > 0 {
		instructions = "j/k choose project  tab prompt  enter submit  esc cancel"
	}
	if a.createPending {
		instructions = "creating task…  Esc hides; request continues"
	}
	win.New(2, height-2, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: instructions, Style: palette.overlay})
}

func (a *application) drawThemeSwitcher(root vaxis.Window) {
	palette := a.colors()
	width := min(46, root.Width-4)
	height := min(len(themes)+5, root.Height-2)
	if width < 24 || height < 6 {
		return
	}
	win := root.New((root.Width-width)/2, (root.Height-height)/2, width, height)
	a.drawOverlayFrame(win, " THEMES ")
	visible := height - 4
	start := max(0, a.themeCursor-visible+1)
	for row, index := 2, start; row < height-2 && index < len(themes); row, index = row+1, index+1 {
		marker := "  "
		if themeName(index) == a.theme {
			marker = "● "
		}
		style := palette.overlay
		if index == a.themeCursor {
			style = palette.selected
		}
		win.New(2, row, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: marker + themes[index].name, Style: style})
	}
	win.New(2, height-2, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: "j/k preview  enter apply  esc cancel", Style: palette.overlay})
}

func (a *application) drawOverlay(root vaxis.Window, width, height int, title string, lines []string) {
	if width < 8 || height < 4 {
		return
	}
	col, row := (root.Width-width)/2, (root.Height-height)/2
	win := root.New(col, row, width, height)
	a.drawOverlayFrame(win, title)
	style := a.colors().overlay
	for i, line := range lines {
		if i+2 >= height-1 {
			break
		}
		win.New(2, i+2, width-4, 1).PrintTruncate(0, vaxis.Segment{Text: line, Style: style})
	}
}

func (a *application) drawOverlayFrame(win vaxis.Window, title string) {
	palette := a.colors()
	style := palette.overlay
	width, height := win.Size()
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
}

func (a *application) drawEmptySelection(win vaxis.Window) {
	palette := a.colors()
	win.PrintTruncate(0, vaxis.Segment{Text: " TASK", Style: palette.title})
	if win.Height > 2 {
		win.PrintTruncate(2, vaxis.Segment{Text: " Select a task with j / k", Style: palette.dim})
	}
}

func (a *application) drawField(win vaxis.Window, row int, label, value string, valueStyle vaxis.Style) int {
	if row >= win.Height {
		return row
	}
	palette := a.colors()
	win.PrintTruncate(row,
		vaxis.Segment{Text: fmt.Sprintf(" %-13s", label), Style: palette.dim},
		vaxis.Segment{Text: value, Style: mergeStyle(palette.base, valueStyle)},
	)
	return row + 1
}

type shortcut struct {
	key, label string
}

type metadataItem struct {
	icon, label, value string
}

func drawShortcuts(win vaxis.Window, shortcuts []shortcut, palette colorPalette) {
	segments := make([]vaxis.Segment, 0, 1+2*len(shortcuts))
	segments = append(segments, vaxis.Segment{Text: " ", Style: palette.base})
	for _, item := range shortcuts {
		segments = append(segments,
			vaxis.Segment{Text: " " + item.key, Style: palette.overlay},
			vaxis.Segment{Text: " " + item.label + "  ", Style: palette.dim},
		)
	}
	win.PrintTruncate(0, segments...)
}

func drawHorizontalRule(win vaxis.Window, row int, style vaxis.Style) {
	width, height := win.Size()
	if row < 0 || row >= height {
		return
	}
	for column := range width {
		win.SetCell(column, row, vaxis.Cell{Character: vaxis.Character{Grapheme: "─", Width: 1}, Style: style})
	}
}

func drawMetadataCard(win vaxis.Window, row int, items []metadataItem, palette colorPalette) int {
	width, height := win.Size()
	if width < 6 || row < 0 || row+2 >= height || len(items) == 0 {
		return row
	}
	card := win.New(1, row, width-2, 3)
	drawBoxRule(card, 0, "┌", "┐", palette.border)
	drawBoxRule(card, 2, "└", "┘", palette.border)
	card.SetCell(0, 1, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: palette.border})
	card.SetCell(card.Width-1, 1, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: palette.border})
	content := card.New(1, 1, card.Width-2, 1)
	weights := []int{27, 23, 12, 25, 13}
	compactLabels := []string{"Repo", "Branch", "Try", "Pod", "PR"}
	cursor := 0
	for index, item := range items {
		if cursor >= content.Width {
			break
		}
		span := content.Width - cursor
		if index < len(items)-1 && index < len(weights) {
			span = max(1, content.Width*weights[index]/100)
		}
		span = min(span, content.Width-cursor)
		cell := content.New(cursor, 0, span, 1)
		label := item.label
		if content.Width < 100 && index < len(compactLabels) {
			label = compactLabels[index]
		}
		valueStart := 0
		if index > 0 && span > 0 {
			cell.SetCell(0, 0, vaxis.Cell{Character: vaxis.Character{Grapheme: "│", Width: 1}, Style: palette.border})
			valueStart = 1
		}
		if span > valueStart {
			cell.New(valueStart, 0, span-valueStart, 1).PrintTruncate(0,
				vaxis.Segment{Text: " " + item.icon + " ", Style: palette.info},
				vaxis.Segment{Text: label + " ", Style: palette.dim},
				vaxis.Segment{Text: item.value, Style: palette.base},
			)
		}
		cursor += span
	}
	return row + 2
}

func (a *application) drawPipeline(win vaxis.Window, start int, detail TaskDetail) {
	palette := a.colors()
	width, height := win.Size()
	if start < 0 || start >= height || width <= 0 {
		return
	}
	win.PrintTruncate(start, vaxis.Segment{Text: " ┃  PIPELINE", Style: palette.title})
	row := start + 2
	if row >= height {
		return
	}

	failureCode, failureMessage := detailFailure(detail)
	available := height - row
	reservedFailureRows := 0
	if failureMessage != "" && available >= 4 {
		reservedFailureRows = 4
	}
	maxEvents := max(0, (available-reservedFailureRows)/2)
	events := detail.Events
	startIndex := 0
	if len(events) > maxEvents {
		startIndex = len(events) - maxEvents
		events = events[startIndex:]
	}
	if len(events) == 0 && failureMessage == "" {
		win.PrintTruncate(row,
			vaxis.Segment{Text: "  ○  ", Style: palette.dim},
			vaxis.Segment{Text: "Waiting for lifecycle events", Style: palette.dim},
		)
		return
	}

	for index, event := range events {
		if row >= height-reservedFailureRows {
			break
		}
		marker := stateMarker(event.ToState)
		style := a.stateStyle(event.ToState)
		title := humanizeLabel(firstNonempty(event.Reason, event.ToState, "event"))
		badge := "[" + strings.ToUpper(firstNonempty(event.ToState, "event")) + "]"
		right := pipelineEventDuration(detail, startIndex+index) + "  " + badge + " "
		if textWidth(right) > width/2 {
			right = badge + " "
		}
		leftWidth := max(0, width-textWidth(right))
		if leftWidth > 0 {
			win.New(0, row, leftWidth, 1).PrintTruncate(0,
				vaxis.Segment{Text: fmt.Sprintf("  %s  %-2d ", marker, startIndex+index+1), Style: style},
				vaxis.Segment{Text: title, Style: palette.base},
			)
		}
		if textWidth(right) <= width {
			win.New(width-textWidth(right), row, textWidth(right), 1).PrintTruncate(0, vaxis.Segment{Text: right, Style: style})
		}
		row++
		if row >= height-reservedFailureRows {
			break
		}
		transition := event.ToState
		if event.FromState != "" {
			transition = event.FromState + " → " + event.ToState
		}
		if event.Trigger != "" {
			transition += " · " + event.Trigger
		}
		connector := "│"
		if index == len(events)-1 && failureMessage == "" {
			connector = " "
		}
		win.PrintTruncate(row,
			vaxis.Segment{Text: "  " + connector + "      ", Style: palette.border},
			vaxis.Segment{Text: transition, Style: palette.dim},
		)
		row++
	}
	if failureMessage != "" && height-row >= 4 {
		drawFailureBox(win, row, failureCode, failureMessage, palette)
	}
}

func drawFailureBox(win vaxis.Window, row int, code, message string, palette colorPalette) {
	width, height := win.Size()
	if width < 4 || row < 0 || row+3 >= height {
		return
	}
	drawBoxRule(win, row, "┌", "┐", palette.bad)
	title := humanizeLabel(firstNonempty(code, "task failed"))
	win.New(1, row+1, width-2, 1).PrintTruncate(0, vaxis.Segment{Text: " " + title, Style: palette.bad})
	win.New(1, row+2, width-2, 1).PrintTruncate(0, vaxis.Segment{Text: " " + message, Style: palette.dim})
	drawBoxRule(win, row+3, "└", "┘", palette.bad)
}

func drawBoxRule(win vaxis.Window, row int, left, right string, style vaxis.Style) {
	width, height := win.Size()
	if width < 2 || row < 0 || row >= height {
		return
	}
	win.SetCell(0, row, vaxis.Cell{Character: vaxis.Character{Grapheme: left, Width: 1}, Style: style})
	for column := 1; column < width-1; column++ {
		win.SetCell(column, row, vaxis.Cell{Character: vaxis.Character{Grapheme: "─", Width: 1}, Style: style})
	}
	win.SetCell(width-1, row, vaxis.Cell{Character: vaxis.Character{Grapheme: right, Width: 1}, Style: style})
}

func detailFailure(detail TaskDetail) (string, string) {
	for _, event := range slices.Backward(detail.Events) {
		if eventError := event.Error; eventError != nil && (eventError.Code != "" || eventError.Message != "") {
			return eventError.Code, firstNonempty(eventError.Message, eventError.Code)
		}
	}
	attempt := currentAttempt(detail)
	for _, result := range []struct {
		code, message string
	}{
		{errorCode(attempt.GitResult.Error), errorMessage(attempt.GitResult.Error)},
		{errorCode(attempt.PullRequest.Error), errorMessage(attempt.PullRequest.Error)},
		{errorCode(detail.Task.GitResult.Error), errorMessage(detail.Task.GitResult.Error)},
		{errorCode(detail.Task.PullRequest.Error), errorMessage(detail.Task.PullRequest.Error)},
	} {
		if result.code != "" || result.message != "" {
			return result.code, firstNonempty(result.message, result.code)
		}
	}
	runs := attempt.ValidationRuns
	if len(runs) == 0 {
		runs = detail.Task.ValidationRuns
	}
	for _, run := range slices.Backward(runs) {
		if runError := run.Error; runError != nil && (runError.Code != "" || runError.Message != "") {
			return runError.Code, firstNonempty(runError.Message, runError.Code)
		}
	}
	if strings.EqualFold(detail.Task.State, "failed") || strings.EqualFold(attempt.State, "failed") {
		code := firstNonempty(attempt.KubernetesJob.Reason, attempt.KubernetesPod.Reason, "task failed")
		message := firstNonempty(attempt.KubernetesJob.Message, attempt.KubernetesPod.Message, "The current attempt did not complete successfully.")
		return code, message
	}
	return "", ""
}

func errorCode(value *client.Error) string {
	if value == nil {
		return ""
	}
	return value.Code
}

func errorMessage(value *client.Error) string {
	if value == nil {
		return ""
	}
	return value.Message
}

func stateMarker(state string) string {
	switch strings.ToLower(state) {
	case "succeeded", "success", "completed", "complete", "ready", "open", "merged":
		return "✔"
	case "failed", "error", "cancelled", "canceled", "lost":
		return "✖"
	case "running", "active", "retrying", "cancelling", "canceling":
		return "●"
	case "queued", "pending", "received", "unknown", "":
		return "○"
	default:
		return "•"
	}
}

func truncateText(value string, width int) string {
	if width <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= width {
		return value
	}
	if width == 1 {
		return "…"
	}
	return string(runes[:width-1]) + "…"
}

func textWidth(value string) int {
	return len([]rune(value))
}

func humanizeLabel(value string) string {
	value = strings.TrimSpace(strings.NewReplacer("_", " ", "-", " ").Replace(value))
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if runes[0] >= 'a' && runes[0] <= 'z' {
		runes[0] -= 'a' - 'A'
	}
	return string(runes)
}

func attemptTimes(task Task, attempt Attempt) (time.Time, *time.Time) {
	started := attempt.CreatedAt
	switch {
	case attempt.StartedAt != nil:
		started = *attempt.StartedAt
	case attempt.KubernetesJob.StartedAt != nil:
		started = *attempt.KubernetesJob.StartedAt
	case started.IsZero():
		started = task.CreatedAt
	}
	completed := attempt.CompletedAt
	if completed == nil {
		completed = attempt.KubernetesJob.CompletedAt
	}
	if completed == nil && isTerminalState(task.State) && !task.UpdatedAt.IsZero() {
		value := task.UpdatedAt
		completed = &value
	}
	return started, completed
}

func formatClock(value time.Time) string {
	if value.IsZero() {
		return "—"
	}
	return value.Local().Format("15:04:05")
}

func formatDuration(started time.Time, completed *time.Time) string {
	if started.IsZero() {
		return "—"
	}
	end := time.Now()
	if completed != nil {
		end = *completed
	}
	duration := end.Sub(started)
	duration = max(duration, 0)
	duration = duration.Round(time.Second)
	if duration >= time.Hour {
		return fmt.Sprintf("%02dh %02dm", int(duration.Hours()), int(duration.Minutes())%60)
	}
	if duration >= time.Minute {
		return fmt.Sprintf("%02dm %02ds", int(duration.Minutes()), int(duration.Seconds())%60)
	}
	return fmt.Sprintf("%02ds", int(duration.Seconds()))
}

func pipelineEventDuration(detail TaskDetail, index int) string {
	if index < 0 || index >= len(detail.Events) || detail.Events[index].OccurredAt.IsZero() {
		return "—"
	}
	started := detail.Events[index].OccurredAt
	completed := time.Now()
	if index+1 < len(detail.Events) && !detail.Events[index+1].OccurredAt.IsZero() {
		completed = detail.Events[index+1].OccurredAt
	} else {
		attempt := currentAttempt(detail)
		switch {
		case attempt.CompletedAt != nil:
			completed = *attempt.CompletedAt
		case isTerminalState(detail.Task.State) && !detail.Task.UpdatedAt.IsZero():
			completed = detail.Task.UpdatedAt
		}
	}
	duration := max(completed.Sub(started), 0).Round(time.Second)
	if duration >= time.Hour {
		return fmt.Sprintf("%02dh %02dm", int(duration.Hours()), int(duration.Minutes())%60)
	}
	return fmt.Sprintf("%02d:%02d", int(duration.Minutes()), int(duration.Seconds())%60)
}

func isTerminalState(state string) bool {
	switch strings.ToLower(state) {
	case "succeeded", "success", "completed", "complete", "failed", "error", "cancelled", "canceled":
		return true
	default:
		return false
	}
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

func (a *application) stateStyle(state string) vaxis.Style {
	palette := a.colors()
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
