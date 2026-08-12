package gui

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gogpu/gg"
	uiapp "github.com/gogpu/ui/app"
	"github.com/gogpu/ui/core/listview"
	"github.com/gogpu/ui/core/scrollview"
	"github.com/gogpu/ui/core/textfield"
	"github.com/gogpu/ui/event"
	"github.com/gogpu/ui/geometry"
	"github.com/gogpu/ui/primitives"
	"github.com/gogpu/ui/render"
	"github.com/gogpu/ui/state"
	"github.com/gogpu/ui/theme"
	"github.com/gogpu/ui/widget"
	"github.com/simpleswe/simpleswe/internal/tui"
)

func TestMainWidgetTreeExposesOperationalSurfacesHeadlessly(t *testing.T) {
	c := newTestController(t, stubClient{})
	root := c.root(context.Background())
	application := uiapp.New()
	application.SetRoot(root)
	application.Window().Frame()
	t.Cleanup(application.Window().Close)

	visible := visibleWidgetIDs(root)
	for _, id := range []string{
		"task-list", "task-details", "task-logs", "task-events", "task-job", "task-pod",
		"create-task", "refresh", "shell", "retry", "cancel", "wrap-logs", "theme", "help", "connectivity",
	} {
		if !visible[id] {
			t.Errorf("headless widget tree has no visible %q surface", id)
		}
	}
	if visible["task-logs-wrapped"] {
		t.Error("wrapped log renderer is visible before wrap is enabled")
	}
	c.setWrapLogs(true)
	visible = visibleWidgetIDs(root)
	if !visible["task-logs-wrapped"] || visible["task-logs"] {
		t.Fatalf("wrap toggle did not switch log renderers: %#v", visible)
	}
}

func TestMountedSurfaceContentGrowthRecomputesScrollExtent(t *testing.T) {
	c := newTestController(t, stubClient{})
	surface := c.surface("dynamic", "Dynamic", c.detailsSignal, scrollview.Vertical)
	application := uiapp.New()
	application.SetRoot(surface)
	application.Window().Frame()
	t.Cleanup(application.Window().Close)

	scroll, _ := findWidget[*scrollview.Widget](surface)
	text, _ := findWidget[*layoutTextWidget](surface)
	before := scroll.ContentSize().Height
	c.detailsSignal.Set(strings.Repeat("line\n", 80))
	application.Window().Frame()

	if got := scroll.ContentSize().Height; got <= before || got != text.Bounds().Height() {
		t.Fatalf("multiline content extent = %v (text bounds %v), want greater than one-line extent %v", got, text.Bounds().Height(), before)
	}
}

func TestLayoutTextDrawsExplicitAndWrappedLinesOnSeparateBaselines(t *testing.T) {
	application := uiapp.New(uiapp.WithTheme(theme.DefaultDark()))
	t.Cleanup(application.Window().Close)
	text := newLayoutText(primitives.Text("ab\ncdef").FontSize(10).Bold().Align(widget.TextAlignCenter), state.NewSignal(0))
	text.Layout(application.Window().Context(), geometry.Constraints{MaxWidth: 12, MaxHeight: geometry.Infinity})
	canvas := &recordingCanvas{}
	text.Draw(application.Window().Context(), canvas)

	if got, want := len(canvas.text), 3; got != want {
		t.Fatalf("draw count = %d, want %d: %#v", got, want, canvas.text)
	}
	for index, want := range []string{"ab", "cd", "ef"} {
		if canvas.text[index].text != want {
			t.Errorf("draw %d text = %q, want %q", index, canvas.text[index].text, want)
		}
		if index > 0 && canvas.text[index].bounds.Min.Y <= canvas.text[index-1].bounds.Min.Y {
			t.Errorf("draw %d Y = %v, want greater than previous Y %v", index, canvas.text[index].bounds.Min.Y, canvas.text[index-1].bounds.Min.Y)
		}
		if !text.Bounds().ContainsRect(canvas.text[index].bounds) {
			t.Errorf("draw %d bounds = %v, want contained in %v", index, canvas.text[index].bounds, text.Bounds())
		}
		if canvas.text[index].color != theme.DefaultDark().OnSurface() || !canvas.text[index].bold || canvas.text[index].align != widget.TextAlignCenter {
			t.Errorf("draw %d style = color %v, bold %v, align %v", index, canvas.text[index].color, canvas.text[index].bold, canvas.text[index].align)
		}
	}
}

func TestMinimumWindowContainsEveryInteractiveControl(t *testing.T) {
	c := newTestController(t, stubClient{})
	root := c.root(context.Background())
	application := uiapp.New()
	application.SetRoot(root)
	application.Window().HandleResize(760, 520)
	application.Window().Frame()
	t.Cleanup(application.Window().Close)

	rootBounds := geometry.NewRect(0, 0, 760, 520)
	interactive := map[string]bool{
		"task-list": false, "create-repository": false, "create-prompt": false, "create-task": false,
		"refresh": false, "shell": false, "retry": false, "cancel": false, "wrap-logs": false,
		"theme": false, "help": false, "confirm": false, "dismiss-confirmation": false,
	}
	visitWidgetBounds(root, geometry.Point{}, func(id string, bounds geometry.Rect) {
		if _, ok := interactive[id]; !ok {
			return
		}
		interactive[id] = true
		if bounds.IsEmpty() || !rootBounds.ContainsRect(bounds) {
			t.Errorf("interactive %q bounds = %v, want nonempty and contained in %v", id, bounds, rootBounds)
		}
	})
	for id, found := range interactive {
		if !found {
			t.Errorf("interactive %q not found", id)
		}
	}
}

func TestSurfaceBoundsHugeNewlineHeavyContent(t *testing.T) {
	c := newTestController(t, stubClient{})
	surface := c.surface("dynamic", "Dynamic", c.detailsSignal, scrollview.Vertical)
	application := uiapp.New()
	application.SetRoot(surface)
	application.Window().Frame()
	t.Cleanup(application.Window().Close)

	c.detailsSignal.Set(strings.Repeat("\n", maxSurfaceBytes) + string([]byte{0xff}) + strings.Repeat("\n", maxSurfaceBytes))
	application.Window().Frame()
	text, _ := findWidget[*layoutTextWidget](surface)
	content, height := text.Content(), text.Bounds().Height()
	if len(content) > maxSurfaceBytes || !strings.Contains(content, "display truncated") || !utf8.ValidString(content) {
		t.Fatalf("bounded surface content length = %d, valid = %v, marker = %v", len(content), utf8.ValidString(content), strings.Contains(content, "display truncated"))
	}
	if height <= 0 || math.IsNaN(float64(height)) || math.IsInf(float64(height), 0) {
		t.Fatalf("huge content extent = %v, want positive finite height", height)
	}

	measured := measureMultilineText("abcd\nef", primitives.Text("x").FontSize(10).Style(), 12)
	if measured != (geometry.Size{Width: 12, Height: 36}) {
		t.Fatalf("multiline wrapped size = %v, want 12x36", measured)
	}
}

func TestCreateFieldsClearOnSuccessAndRetainOnFailure(t *testing.T) {
	c := newTestController(t, stubClient{})
	root := c.root(context.Background())
	repository := widgetByID[*textfield.Widget](t, root, "create-repository")
	prompt := widgetByID[*textfield.Widget](t, root, "create-prompt")
	repository.SetText("repo")
	prompt.SetText("prompt")

	c.applyCreate(context.Background(), actionResult{name: "create", err: errors.New("rejected")})
	if repository.Text() != "repo" || prompt.Text() != "prompt" {
		t.Fatalf("failed create fields = %q, %q, want retained values", repository.Text(), prompt.Text())
	}
	c.applyCreate(context.Background(), actionResult{name: "create", task: tui.Task{ID: "created"}})
	if repository.Text() != "" || prompt.Text() != "" {
		t.Fatalf("successful create fields = %q, %q, want cleared values", repository.Text(), prompt.Text())
	}
}

func TestCreateFieldEditClearsStaleError(t *testing.T) {
	c := newTestController(t, stubClient{})
	root := c.root(context.Background())
	application := uiapp.New()
	application.SetRoot(root)
	application.Window().Frame()
	t.Cleanup(application.Window().Close)
	for _, id := range []string{"create-repository", "create-prompt"} {
		field := widgetByID[*textfield.Widget](t, root, id)
		c.mu.Lock()
		c.createError = "create failed: rejected"
		c.mu.Unlock()
		c.syncView()
		application.Window().Context().RequestFocus(field)
		application.Window().HandleEvent(event.NewKeyEvent(event.KeyPress, event.KeyR, 'r', event.ModNone))
		if c.createError != "" || c.createErrorSignal.Get() != "" {
			t.Fatalf("create error after editing %s = %q / %q, want cleared", id, c.createError, c.createErrorSignal.Get())
		}
	}
}

func TestSameCountTaskRevisionRebuildsVisibleRows(t *testing.T) {
	c := newTestController(t, stubClient{})
	c.stateMu.Lock()
	c.refreshTasksLocked([]tui.Task{{ID: "task-1", Repository: "repo", State: "queued"}})
	c.stateMu.Unlock()
	c.syncView()

	root := listview.New(
		listview.ItemCountSignal(c.taskCountSignal),
		listview.SelectedIndexSignal(c.selectedSignal),
		listview.FixedItemHeight(44),
		listview.BuildItem(func(item listview.ItemContext) widget.Widget {
			return c.taskText(item.Index, formatTaskSummary, 11)
		}),
	)
	application := uiapp.New()
	application.SetRoot(root)
	application.Window().Frame()
	t.Cleanup(application.Window().Close)
	canvas := render.NewCanvas(gg.NewContext(800, 600), 800, 600)
	root.Draw(application.Window().Context(), canvas)
	if got := textContents(root); !strings.Contains(got, "QUEUED") {
		t.Fatalf("initial row content = %q, want queued", got)
	}

	c.stateMu.Lock()
	c.refreshTasksLocked([]tui.Task{{ID: "task-1", Repository: "repo", State: "running"}})
	c.stateMu.Unlock()
	c.syncView()
	application.Window().Frame()
	root.Draw(application.Window().Context(), canvas)

	if got := textContents(root); !strings.Contains(got, "RUNNING") || strings.Contains(got, "QUEUED") {
		t.Fatalf("same-count refreshed row content = %q, want running without queued", got)
	}
}

func findWidget[T widget.Widget](root widget.Widget) (T, bool) {
	if found, ok := root.(T); ok {
		return found, true
	}
	for _, child := range root.Children() {
		if found, ok := findWidget[T](child); ok {
			return found, true
		}
	}
	var zero T
	return zero, false
}

func widgetByID[T widget.Widget](t *testing.T, root widget.Widget, id string) T {
	t.Helper()
	var walk func(widget.Widget) (T, bool)
	walk = func(current widget.Widget) (T, bool) {
		if found, ok := current.(T); ok {
			if identified, ok := any(found).(interface{ ID() string }); ok && identified.ID() == id {
				return found, true
			}
		}
		for _, child := range current.Children() {
			if found, ok := walk(child); ok {
				return found, true
			}
		}
		var zero T
		return zero, false
	}
	if found, ok := walk(root); ok {
		return found
	}
	t.Fatalf("widget %q not found", id)
	var zero T
	return zero
}

func textContents(root widget.Widget) string {
	var result []string
	if text, ok := root.(interface{ Content() string }); ok {
		result = append(result, text.Content())
	}
	for _, child := range root.Children() {
		result = append(result, textContents(child))
	}
	return strings.Join(result, "\n")
}

func visibleWidgetIDs(root widget.Widget) map[string]bool {
	result := make(map[string]bool)
	var walk func(widget.Widget)
	walk = func(current widget.Widget) {
		if identified, ok := current.(interface {
			ID() string
			IsVisible() bool
		}); ok && identified.ID() != "" && identified.IsVisible() {
			result[identified.ID()] = true
		}
		for _, child := range current.Children() {
			walk(child)
		}
	}
	walk(root)
	return result
}

type textDraw struct {
	text   string
	bounds geometry.Rect
	color  widget.Color
	bold   bool
	align  widget.TextAlign
}

type recordingCanvas struct {
	widget.Canvas
	text []textDraw
}

func (c *recordingCanvas) DrawText(text string, bounds geometry.Rect, _ float32, color widget.Color, bold bool, align widget.TextAlign) {
	c.text = append(c.text, textDraw{text: text, bounds: bounds, color: color, bold: bold, align: align})
}

func visitWidgetBounds(current widget.Widget, parent geometry.Point, visit func(string, geometry.Rect)) {
	bounded, ok := current.(interface{ Bounds() geometry.Rect })
	if !ok {
		return
	}
	bounds := bounded.Bounds().Translate(parent)
	if identified, ok := current.(interface{ ID() string }); ok && identified.ID() != "" {
		visit(identified.ID(), bounds)
	}
	for _, child := range current.Children() {
		visitWidgetBounds(child, bounds.Min, visit)
	}
}
