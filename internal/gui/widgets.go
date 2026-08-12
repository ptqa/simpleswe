package gui

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/gogpu/ui/core/button"
	"github.com/gogpu/ui/core/checkbox"
	"github.com/gogpu/ui/core/dropdown"
	"github.com/gogpu/ui/core/listview"
	"github.com/gogpu/ui/core/scrollview"
	"github.com/gogpu/ui/core/splitview"
	"github.com/gogpu/ui/core/tabview"
	"github.com/gogpu/ui/core/textfield"
	"github.com/gogpu/ui/geometry"
	"github.com/gogpu/ui/primitives"
	"github.com/gogpu/ui/state"
	"github.com/gogpu/ui/widget"
	"github.com/simpleswe/simpleswe/internal/tui"
)

func (c *controller) root(ctx context.Context) widget.Widget {
	tasks := identified(listview.New(
		listview.ItemCountSignal(c.taskCountSignal),
		listview.SelectedIndexSignal(c.selectedSignal),
		listview.SelectionModeOpt(listview.SelectionSingle),
		listview.FixedItemHeight(44),
		listview.Divider(true),
		listview.A11yLabel("Tasks"),
		listview.BuildItem(func(item listview.ItemContext) widget.Widget {
			title := c.taskText(item.Index, func(task tui.Task) string { return firstNonempty(task.Repository, task.ID) }, 14)
			title.Bold().MaxLines(1).Ellipsis()
			summary := c.taskText(item.Index, formatTaskSummary, 11)
			summary.MaxLines(1).Ellipsis()
			return primitives.VBox(
				title,
				summary,
			).PaddingXY(8, 4)
		}),
		listview.OnSelectionChange(func(index int) { c.selectTaskAt(ctx, index) }),
		listview.OnItemClick(func(index int) { c.selectTaskAt(ctx, index) }),
	), "task-list")

	details := c.surface("task-details", "Task / attempt", c.detailsSignal)
	logs, setLogWrap := c.logSurface()
	c.mu.Lock()
	c.setLogWrapViews = setLogWrap
	c.mu.Unlock()
	events := c.surface("task-events", "Events", c.eventsSignal)
	job := c.surface("task-job", "Kubernetes Job", c.jobSignal)
	pod := c.surface("task-pod", "Kubernetes Pod", c.podSignal)
	tabs := tabview.New([]tabview.Tab{
		{Label: "Details", Content: details},
		{Label: "Logs", Content: logs},
		{Label: "Events", Content: events},
		{Label: "Job", Content: job},
		{Label: "Pod", Content: pod},
	})

	body := splitview.New(
		splitview.First(primitives.VBox(
			primitives.Text("Tasks").FontSize(18).Bold(),
			primitives.Expanded(tasks),
		).Padding(8).Gap(6)),
		splitview.Second(tabs),
		splitview.InitialRatio(0.32),
		splitview.MinFirst(220),
		splitview.MinSecond(360),
		splitview.CollapsibleOpt(true),
	)

	repositoryField := identified(textfield.New(
		textfield.Placeholder("Repository"),
		textfield.A11yLabel("Repository"),
		textfield.ValueSignal(c.repositorySignal),
		textfield.OnChange(func(string) { c.editCreateField() }),
	), "create-repository")
	promptField := identified(textfield.New(
		textfield.Placeholder("Task prompt"),
		textfield.A11yLabel("Task prompt"),
		textfield.ValueSignal(c.promptSignal),
		textfield.OnChange(func(string) { c.editCreateField() }),
		textfield.OnSubmit(func(string) { c.submitCreate(ctx, c.repositorySignal.Get(), c.promptSignal.Get()) }),
	), "create-prompt")
	create := identified(button.New(
		button.Text("Create task"),
		button.OnClick(func() { c.submitCreate(ctx, c.repositorySignal.Get(), c.promptSignal.Get()) }),
	), "create-task")
	createRow := primitives.VBox(
		primitives.HBox(
			primitives.Expanded(repositoryField),
			primitives.Expanded(promptField),
			create,
		).Gap(8).CrossAlign(primitives.CrossAxisCenter),
		primitives.Text("").ContentSignal(c.createErrorSignal).FontSize(12),
	).Gap(2)

	refresh := identified(button.New(button.Text("Refresh"), button.OnClick(func() { c.refresh(ctx) })), "refresh")
	shell := identified(button.New(button.Text("Shell"), button.OnClick(func() { c.shell(ctx) })), "shell")
	retry := identified(button.New(button.Text("Retry"), button.OnClick(func() { c.requestConfirmation("retry") })), "retry")
	cancel := identified(button.New(button.Text("Cancel"), button.OnClick(func() { c.requestConfirmation("cancel") })), "cancel")
	help := identified(button.New(button.Text("Help"), button.OnClick(c.toggleHelp)), "help")
	wrapLogs := identified(checkbox.New(
		checkbox.Label("Wrap logs"),
		checkbox.CheckedSignal(c.wrapLogsSignal),
		checkbox.OnToggle(c.setWrapLogs),
	), "wrap-logs")
	themePicker := identified(dropdown.New(
		dropdown.Items("Dark", "Light", "High contrast"),
		dropdown.Selected(0),
		dropdown.A11yHint("Color theme"),
		dropdown.OnChange(func(index int, _ string) {
			c.mu.Lock()
			change := c.themeChanged
			c.mu.Unlock()
			if change != nil {
				change(index)
			}
		}),
	), "theme")
	connectivity := identified(primitives.Text("").ContentSignal(c.connectivitySignal).Bold(), "connectivity")

	header := primitives.HBox(
		primitives.Text("simpleswe").FontSize(22).Bold(),
		primitives.Expanded(primitives.Box()),
		connectivity,
	).Gap(8).CrossAlign(primitives.CrossAxisCenter)
	actions := primitives.VBox(
		primitives.HBox(refresh, shell, retry, cancel).Gap(8).CrossAlign(primitives.CrossAxisCenter),
		primitives.HBox(wrapLogs, themePicker, primitives.Expanded(primitives.Box()), help).Gap(8).CrossAlign(primitives.CrossAxisCenter),
	).Gap(6)

	confirmation := primitives.HBox(
		primitives.Expanded(primitives.Text("").ContentSignal(c.confirmationSignal)),
		identified(button.New(button.Text("Confirm"), button.OnClick(func() { c.confirm(ctx) })), "confirm"),
		identified(button.New(button.Text("Dismiss"), button.OnClick(c.dismissConfirmation)), "dismiss-confirmation"),
	).Gap(8).CrossAlign(primitives.CrossAxisCenter)

	return primitives.VBox(
		header,
		actions,
		primitives.Text("").ContentSignal(c.messageSignal).FontSize(12),
		primitives.Expanded(body),
		createRow,
		confirmation,
		primitives.Text("").ContentSignal(c.helpSignal).FontSize(12).MaxLines(2),
	).Padding(12).Gap(8)
}

func formatTaskSummary(task tui.Task) string {
	return fmt.Sprintf("%s  %s  %s", strings.ToUpper(task.State), task.ID, compactAge(task.CreatedAt))
}

func (c *controller) logSurface() (widget.Widget, func(bool)) {
	noWrap := c.surface("task-logs", "Live logs", c.logsSignal, scrollview.Both)
	wrap := c.surface("task-logs-wrapped", "Live logs", c.logsSignal, scrollview.Vertical)
	set := func(enabled bool) {
		noWrap.SetVisible(!enabled)
		wrap.SetVisible(enabled)
	}
	set(false)
	return primitives.Box(noWrap, wrap), set
}

func (c *controller) surface(id, title string, content state.ReadonlySignal[string], directions ...scrollview.ScrollDirection) *primitives.BoxWidget {
	direction := scrollview.Both
	if len(directions) > 0 {
		direction = directions[0]
	}
	text := newLayoutText(primitives.TextFn(func() string { return boundedDisplayText(content.Get()) }).FontSize(13), content)
	scroll := scrollview.New(text, scrollview.DirectionOpt(direction), scrollview.ScrollbarOpt(scrollview.ScrollbarAuto))
	return identified(primitives.VBox(
		primitives.Text(title).FontSize(18).Bold(),
		primitives.Expanded(scroll),
	).Padding(12).Gap(8), id)
}

func (c *controller) taskText(index int, format func(tui.Task) string, fontSize float32) *layoutTextWidget {
	text := primitives.TextFn(func() string {
		c.stateMu.Lock()
		defer c.stateMu.Unlock()
		tasks := c.model.Tasks()
		if index < 0 || index >= len(tasks) {
			return ""
		}
		return format(tasks[index])
	}).FontSize(fontSize)
	return newLayoutText(text, c.taskRevisionSignal)
}

// layoutTextWidget adds the layout-aware signal binding missing from
// TextWidget.ContentSignal in gogpu/ui v0.1.53.
type layoutTextWidget struct {
	*primitives.TextWidget
	bind      func(widget.SchedulerRef)
	wrapWidth float32
}

func newLayoutText[T any](text *primitives.TextWidget, trigger state.ReadonlySignal[T]) *layoutTextWidget {
	result := &layoutTextWidget{TextWidget: text}
	result.bind = func(scheduler widget.SchedulerRef) {
		result.AddBinding(state.BindToSchedulerLayout(trigger, result, scheduler))
	}
	return result
}

func (t *layoutTextWidget) Mount(ctx widget.Context) {
	if scheduler := ctx.Scheduler(); scheduler != nil {
		t.bind(scheduler)
	}
}

func (*layoutTextWidget) Unmount() {}

func (t *layoutTextWidget) Layout(_ widget.Context, constraints geometry.Constraints) geometry.Size {
	content := t.Content()
	if content == "" {
		t.SetBounds(geometry.FromPointSize(t.Position(), geometry.Size{}))
		return geometry.Size{}
	}
	t.wrapWidth = constraints.MaxWidth
	size := constraints.Constrain(iterateTextLines(content, t.Style(), constraints.MaxWidth, nil))
	t.SetBounds(geometry.FromPointSize(t.Position(), size))
	return size
}

func measureMultilineText(content string, style primitives.TextStyle, maxWidth float32) geometry.Size {
	return iterateTextLines(content, style, maxWidth, nil)
}

func (t *layoutTextWidget) Draw(ctx widget.Context, canvas widget.Canvas) {
	if !t.IsVisible() || t.Content() == "" || t.Bounds().IsEmpty() {
		return
	}
	style := t.Style()
	colorCanvas := &textColorCanvas{}
	t.TextWidget.Draw(ctx, colorCanvas)
	color := colorCanvas.color
	bounds := t.Bounds()
	iterateTextLines(t.Content(), style, t.wrapWidth, func(line string, index int, lineHeight float32) {
		lineBounds := geometry.NewRect(bounds.Min.X, bounds.Min.Y+float32(index)*lineHeight, bounds.Width(), lineHeight)
		if style.FontFamily != "" || style.Italic {
			if styled, ok := canvas.(widget.StyledTextDrawer); ok {
				styled.DrawStyledText(line, lineBounds, widget.TextStyle{
					FontFamily: style.FontFamily, FontSize: style.FontSize, Bold: style.Bold,
					Italic: style.Italic, Color: color, Align: style.Align,
				})
				return
			}
		}
		canvas.DrawText(line, lineBounds, style.FontSize, color, style.Bold, style.Align)
	})
}

type textColorCanvas struct {
	widget.Canvas
	color widget.Color
}

func (c *textColorCanvas) DrawText(_ string, _ geometry.Rect, _ float32, color widget.Color, _ bool, _ widget.TextAlign) {
	c.color = color
}

func iterateTextLines(content string, style primitives.TextStyle, maxWidth float32, visit func(string, int, float32)) geometry.Size {
	fontSize := style.FontSize
	if fontSize <= 0 {
		fontSize = 14
	}
	lineHeight := style.LineHeight
	if lineHeight <= 0 {
		lineHeight = 1.2
	}
	charWidth, lineHeight := fontSize*0.6, fontSize*lineHeight
	boundedWidth := maxWidth > 0 && maxWidth < geometry.Infinity
	charsPerLine := 0
	if boundedWidth {
		charsPerLine = max(1, int(maxWidth/charWidth))
	}
	var width float32
	lineStart, lineRunes, lines := 0, 0, 0
	wrappedAtBoundary := false
	emit := func(end int) bool {
		if style.MaxLines > 0 && lines >= style.MaxLines {
			return false
		}
		lineWidth := float32(lineRunes) * charWidth
		width = max(width, lineWidth)
		if visit != nil {
			visit(content[lineStart:end], lines, lineHeight)
		}
		lines++
		lineRunes = 0
		return style.MaxLines <= 0 || lines < style.MaxLines
	}
	for index, r := range content {
		size := utf8.RuneLen(r)
		if r == '\n' {
			if !wrappedAtBoundary && !emit(index) {
				return geometry.Sz(min(width, maxWidth), float32(lines)*lineHeight)
			}
			lineStart = index + size
			wrappedAtBoundary = false
			continue
		}
		wrappedAtBoundary = false
		lineRunes++
		if boundedWidth && lineRunes == charsPerLine {
			if !emit(index + size) {
				return geometry.Sz(min(maxWidth, max(width, float32(lineRunes)*charWidth)), float32(lines)*lineHeight)
			}
			lineStart = index + size
			wrappedAtBoundary = true
		}
	}
	if lineStart < len(content) || strings.HasSuffix(content, "\n") || lines == 0 {
		emit(len(content))
	}
	if boundedWidth {
		width = min(width, maxWidth)
	}
	return geometry.Sz(width, float32(lines)*lineHeight)
}

func (c *controller) selectTaskAt(ctx context.Context, index int) {
	c.stateMu.Lock()
	tasks := c.model.Tasks()
	if index >= 0 && index < len(tasks) {
		changed := c.selectTaskLocked(ctx, tasks[index].ID)
		c.stateMu.Unlock()
		if changed {
			c.syncView()
		}
		return
	}
	c.stateMu.Unlock()
}

func identified[T interface{ SetID(string) }](value T, id string) T {
	value.SetID(id)
	return value
}
