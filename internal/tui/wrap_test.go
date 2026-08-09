package tui

import (
	"context"
	"strings"
	"testing"

	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/term"
)

func TestWrapToggleDefaultOffAndKey(t *testing.T) {
	vx, _ := newTestVaxis(t, 80, 20)
	app := newApplication(context.Background(), vx, nil, Options{LogCapacity: 4})
	t.Cleanup(app.stop)

	if app.wrapLogs {
		t.Fatal("wrapLogs defaults on")
	}
	pressKey(t, app, key('w'))
	if !app.wrapLogs || app.message != "wrap: on" {
		t.Fatalf("wrap on = %v, message %q", app.wrapLogs, app.message)
	}
	pressKey(t, app, key('w'))
	if app.wrapLogs || app.message != "wrap: off" {
		t.Fatalf("wrap off = %v, message %q", app.wrapLogs, app.message)
	}

	other := newApplication(context.Background(), vx, nil, Options{LogCapacity: 4})
	t.Cleanup(other.stop)
	if other.wrapLogs {
		t.Fatal("wrapLogs is not per-application")
	}

	for _, active := range []string{"help", "themePicker", "confirmCancel", "confirmRetry", "createModal"} {
		app.help, app.themePicker, app.confirmAction = false, false, ""
		app.resetCreateTask()
		app.wrapLogs = false
		app.message = "unchanged"
		switch active {
		case "help":
			app.help = true
		case "themePicker":
			app.themePicker = true
		case "confirmCancel":
			app.confirmAction = "cancel"
		case "confirmRetry":
			app.confirmAction = "retry"
		case "createModal":
			app.openCreateTask()
		}
		pressKey(t, app, key('w'))
		if app.wrapLogs || app.message != "unchanged" {
			t.Fatalf("w toggled with %s active: wrap %v, message %q", active, app.wrapLogs, app.message)
		}
	}
}

func TestDrawLogsUsesMouseScrollOffset(t *testing.T) {
	vx, console := newTestVaxis(t, 20, 5)
	model := NewModel(10)
	for _, line := range []string{"line-1", "line-2", "line-3", "line-4", "line-5", "line-6", "line-7"} {
		model.AppendLog(line)
	}
	app := &application{vx: vx, model: model, logOffset: 3, options: Options{LogCapacity: 10}}
	terminal := term.New()
	terminal.Resize(20, 5)

	app.drawLogs(vx.Window())
	vx.Render()
	output := renderedScreen(console, terminal)
	if !strings.Contains(output, "line-1") || strings.Contains(output, "line-7") {
		t.Fatalf("scrolled logs = %q", output)
	}
}

func TestDrawLogsNoWrapTruncatesLongLine(t *testing.T) {
	vx, console := newTestVaxis(t, 20, 10)
	model := NewModel(4)
	model.AppendLog("prefix-" + strings.Repeat("x", 40) + "-tail")
	app := &application{vx: vx, model: model, options: Options{LogCapacity: 4}}
	terminal := term.New()
	terminal.Resize(20, 10)

	app.drawLogs(vx.Window())
	vx.Render()
	output := renderedScreen(console, terminal)
	if !strings.Contains(output, "prefix-") {
		t.Fatalf("rendered output lost long-line prefix: %q", output)
	}
	if strings.Contains(output, "-tail") {
		t.Fatalf("wrap-off rendered long-line tail: %q", output)
	}
}

func TestDrawLogsWrapShowsWrappedContent(t *testing.T) {
	t.Run("wraps and preserves ANSI", func(t *testing.T) {
		vx, console := newTestVaxis(t, 20, 10)
		model := NewModel(4)
		model.AppendLog("\x1b[31mred text that is very long TAIL\x1b[0m")
		app := &application{vx: vx, model: model, wrapLogs: true, options: Options{LogCapacity: 4}}
		terminal := term.New()
		terminal.Resize(20, 10)

		app.drawLogs(vx.Window())
		vx.Render()
		output := renderedScreen(console, terminal)
		if !strings.Contains(output, "TAIL") {
			t.Fatalf("wrapped output lost tail: %q", output)
		}
		if rowContaining(terminal.Rows(), "TAIL") <= 1 {
			t.Fatalf("wrapped tail did not reach a later row: %#v", terminal.Rows())
		}

		for _, cell := range terminal.Snapshot().Cells {
			if cell.Cell.Grapheme == "T" && cell.Row > 1 && cell.Cell.Foreground == vaxis.IndexColor(1) {
				return
			}
		}
		t.Fatalf("wrapped ANSI tail did not retain red foreground: %#v", terminal.Snapshot().Cells)
	})

	t.Run("evicts oldest wrapped rows", func(t *testing.T) {
		vx, console := newTestVaxis(t, 20, 10)
		model := NewModel(10)
		for _, line := range []string{
			"oldest",
			"first-" + strings.Repeat("a", 45) + "-first-tail",
			"second-" + strings.Repeat("b", 45) + "-second-tail",
			"third-" + strings.Repeat("c", 45) + "-third-tail",
			"newest-" + strings.Repeat("d", 45) + "-newest-tail",
		} {
			model.AppendLog(line)
		}
		app := &application{vx: vx, model: model, wrapLogs: true, options: Options{LogCapacity: 10}}
		terminal := term.New()
		terminal.Resize(20, 10)

		app.drawLogs(vx.Window())
		vx.Render()
		output := renderedScreen(console, terminal)
		if strings.Contains(output, "oldest") {
			t.Fatalf("oldest log was not evicted: %q", output)
		}
		rows := terminal.Rows()
		// With soft-wrap, "newest-tail" is split across rows (e.g. "newest" on penultimate,
		// "-tail" on last), so check that the final wrapped row is at the bottom.
		if !strings.Contains(rows[len(rows)-1], "tail") {
			t.Fatalf("newest tail not at bottom: %#v", rows)
		}
		if !strings.Contains(output, "newest-") {
			t.Fatalf("newest prefix not visible: %q", output)
		}
	})
}

func TestDrawLogsWrapRespectsWindowWidthDynamic(t *testing.T) {
	vx, console := newTestVaxis(t, 20, 10)
	model := NewModel(4)
	model.AppendLog(strings.Repeat("0123456789", 5) + "TAIL")
	app := &application{vx: vx, model: model, wrapLogs: true, options: Options{LogCapacity: 4}}
	terminal := term.New()
	terminal.Resize(20, 10)

	app.drawLogs(vx.Window())
	vx.Render()
	renderedScreen(console, terminal)
	narrowRow := rowContaining(terminal.Rows(), "TAIL")

	vx.Resize(vaxis.Resize{Cols: 40, Rows: 10})
	terminal.Resize(40, 10)
	console.resetOutput()
	app.drawLogs(vx.Window())
	vx.Render()
	renderedScreen(console, terminal)
	wideRow := rowContaining(terminal.Rows(), "TAIL")

	if narrowRow <= wideRow || wideRow != 2 {
		t.Fatalf("tail rows did not respond to resize: narrow %d, wide %d, rows %#v", narrowRow, wideRow, terminal.Rows())
	}
}

func rowContaining(rows []string, value string) int {
	for row, text := range rows {
		if strings.Contains(text, value) {
			return row
		}
	}
	return -1
}
