package tui

import (
	"slices"
	"strings"
	"testing"

	"go.rockorager.dev/vaxis"
	"go.rockorager.dev/vaxis/widgets/term"
)

func TestBoundedLinePreservesAnsiEscape(t *testing.T) {
	input := "hello\x1b[31m red\x1b[0m world"
	got := boundedLine(input)
	if (strings.Contains(got, "[31m") && !strings.Contains(got, "\x1b[31m")) || (strings.Contains(got, "[0m") && !strings.Contains(got, "\x1b[0m")) {
		t.Fatalf("boundedLine left SGR fragment, got %q (should preserve ESC or strip cleanly without \"[0m\")", got)
	}
	if !strings.Contains(got, "\x1b[") {
		t.Fatalf("boundedLine should preserve ANSI escape for later parsing, got %q missing ESC", got)
	}
	// also ensure visible text still present
	if !strings.Contains(got, "hello") || !strings.Contains(got, "red") || !strings.Contains(got, "world") {
		t.Fatalf("boundedLine lost visible text, got %q", got)
	}
}

func TestBoundedLineAnsiDoesNotLeakBracketArtifact(t *testing.T) {
	got := boundedLine("\x1b[0mfoo")
	if got == "[0mfoo" {
		t.Fatalf("boundedLine stripped ESC but left \"[0m\" artifact: got %q", got)
	}
	if strings.Contains(got, "[0m") && !strings.Contains(got, "\x1b[0m") {
		t.Fatalf("boundedLine should not contain literal \"[0m\" without ESC, got %q", got)
	}
	// After fix, either preserves ESC or cleanly strips -> should contain foo and no bracket artifact
	if !strings.Contains(got, "foo") {
		t.Fatalf("boundedLine lost content, got %q", got)
	}
}

func TestAnsiParseSegmentsRedAndReset(t *testing.T) {
	// Simulate render path: boundedLine -> parse ANSI -> segments
	// Currently boundedLine strips ESC, so parsing cannot find colors.
	input := "hello\x1b[31m red\x1b[0m world"
	bounded := boundedLine(input)
	cells := vaxis.ParseStyledString(bounded)
	// Check that parsing yields no literal escape fragments
	var b strings.Builder
	for _, c := range cells {
		b.WriteString(c.Grapheme)
	}
	plain := b.String()
	if strings.Contains(plain, "[31m") || strings.Contains(plain, "[0m") || strings.Contains(plain, "\x1b") {
		t.Fatalf("parsed cells still contain escape fragments, plain=%q bounded=%q", plain, bounded)
	}
	// Expect multiple styles: base, red, base
	hasRed := false
	for _, c := range cells {
		if c.Grapheme == "r" || c.Grapheme == "e" || c.Grapheme == "d" {
			if c.Foreground == vaxis.IndexColor(1) {
				hasRed = true
				break
			}
		}
	}
	if !hasRed {
		t.Fatalf("expected red foreground (IndexColor(1)) for \" red \" segment, got cells %#v bounded=%q", cells, bounded)
	}
	// Check that there's at least one reset to default after red
	foundReset := false
	for _, c := range cells {
		if c.Grapheme == "w" && c.Foreground == vaxis.ColorDefault {
			foundReset = true
			break
		}
	}
	if !foundReset {
		t.Fatalf("expected reset to default after \\x1b[0m, cells %#v", cells)
	}
}

func TestAnsiParseSegmentsBoldAndBright(t *testing.T) {
	input := "a\x1b[1m bold\x1b[0m b\x1b[91m bright\x1b[0m c"
	bounded := boundedLine(input)
	cells := vaxis.ParseStyledString(bounded)
	var b strings.Builder
	for _, c := range cells {
		b.WriteString(c.Grapheme)
	}
	plain := b.String()
	if strings.Contains(plain, "[1m") || strings.Contains(plain, "[91m") {
		t.Fatalf("bold/bright parsing left fragments, plain=%q bounded=%q", plain, bounded)
	}
	hasBold := false
	hasBrightRed := false
	for _, c := range cells {
		if c.Grapheme == "b" && c.Attribute&vaxis.AttrBold != 0 {
			// first bold segment contains " bold"
			hasBold = true
		}
		if c.Foreground == vaxis.IndexColor(9) { // 91 -> bright red = IndexColor(9)
			hasBrightRed = true
		}
	}
	if !hasBold {
		t.Fatalf("expected bold attribute for \" bold \", cells %#v bounded=%q", cells, bounded)
	}
	if !hasBrightRed {
		t.Fatalf("expected bright red IndexColor(9) for \" bright \", cells %#v bounded=%q", cells, bounded)
	}
}

func TestAnsiParseSegments256AndTrueColor(t *testing.T) {
	tests := []struct {
		input string
		check func(vaxis.Cell) bool
		name  string
	}{
		{
			input: "x\x1b[38;5;196m red256\x1b[0m y",
			check: func(c vaxis.Cell) bool { return c.Foreground == vaxis.IndexColor(196) },
			name:  "256-color 38;5;196",
		},
		{
			input: "x\x1b[38;2;255;100;50m true\x1b[0m y",
			check: func(c vaxis.Cell) bool { return c.Foreground == vaxis.RGBColor(255, 100, 50) },
			name:  "true-color 38;2",
		},
	}
	for _, tc := range tests {
		bounded := boundedLine(tc.input)
		cells := vaxis.ParseStyledString(bounded)
		var b strings.Builder
		for _, c := range cells {
			b.WriteString(c.Grapheme)
		}
		plain := b.String()
		if strings.Contains(plain, "[38;") {
			t.Fatalf("%s left fragment, plain=%q bounded=%q", tc.name, plain, bounded)
		}
		if !slices.ContainsFunc(cells, tc.check) {
			t.Fatalf("%s expected color not found, cells %#v bounded=%q", tc.name, cells, bounded)
		}
	}
}

func TestLogAnsiDoesNotShowRawEscapes(t *testing.T) {
	vx, console := newTestVaxis(t, 80, 15)
	model := NewModel(10)
	// Use ANSI red + reset, as seen in screenshot LOGS 136 buffered containing "[0m"
	model.AppendLog("hello\x1b[31m red\x1b[0m world")
	model.AppendLog("\x1b[1m bold\x1b[0m plain")
	model.AppendLog("normal \x1b[38;5;196m256color\x1b[0m end")
	app := &application{
		vx: vx, model: model, options: Options{LogCapacity: 10},
	}
	// Need to trigger draw logs path via app.draw()
	terminal := term.New()
	terminal.Resize(80, 15)
	app.draw()
	output := renderedScreen(console, terminal)
	if strings.Contains(output, "[31m") || strings.Contains(output, "[0m") || strings.Contains(output, "[1m") || strings.Contains(output, "[38;5") {
		t.Fatalf("drawLogs rendered raw SGR fragments, output contains bracket artifact: %q", output)
	}
	if strings.Contains(output, "\x1b[31m") {
		t.Fatalf("drawLogs should not output raw escape, found literal ESC in renderedScreen %q", output)
	}
	// Text should still be visible without escapes
	if !strings.Contains(output, "hello") || !strings.Contains(output, "red") || !strings.Contains(output, "world") {
		t.Fatalf("drawLogs lost visible text, output %q", output)
	}
	if !strings.Contains(output, "bold") || !strings.Contains(output, "plain") {
		t.Fatalf("drawLogs lost bold test strings, output %q", output)
	}
}

func TestLogAnsiRendersColors(t *testing.T) {
	vx, console := newTestVaxis(t, 80, 15)
	model := NewModel(10)
	model.AppendLog("a\x1b[31mR\x1b[0m b")
	app := &application{
		vx: vx, model: model, options: Options{LogCapacity: 10},
	}
	terminal := term.New()
	terminal.Resize(80, 15)
	app.draw()
	// Check raw console output for color SGR generated by vaxis (should contain red)
	raw := console.output()
	terminal.WriteString(raw) // need to populate for second check but also inspect raw
	// The raw vaxis output should contain some color escape for the log line.
	// With current buggy code, log line is single style palette.base, so raw will not have red SGR for that cell.
	// After fix, raw should contain the red SGR (FG 31 or 38;5;1 etc.) before "R"
	if !strings.Contains(raw, "R") {
		t.Fatalf("raw output missing log text \"R\", raw %q", raw)
	}
	hasRedSeq := strings.Contains(raw, "\x1b[31m") || strings.Contains(raw, "\x1b[38;5;1m") || strings.Contains(raw, "\x1b[91m") || strings.Contains(raw, "38;5") || strings.Contains(raw, "31m")
	// More robust: check via term snapshot for colored cell
	snap := terminal.Snapshot()
	foundRed := false
	for _, pc := range snap.Cells {
		if pc.Cell.Grapheme == "R" {
			// After fix, R should have red foreground, not default/base
			if pc.Cell.Foreground != vaxis.ColorDefault {
				// also check it's not the base style? base has its own fg, but red should differ
				foundRed = true
				break
			}
		}
	}
	if !foundRed && !hasRedSeq {
		t.Fatalf("expected log \"R\" to be rendered with ANSI red foreground, raw %q snapshot %#v", raw, snap.Cells)
	}
	// Ensure not leaking bracket artifact in rendered plain text
	out := terminal.String()
	if strings.Contains(out, "[31m") || strings.Contains(out, "[0m") {
		t.Fatalf("rendered plain text leaks bracket artifact %q", out)
	}
}

func TestBoundedLineAnsiTruncationAndControlHandling(t *testing.T) {
	// Tab should become space even with ANSI
	input := "a\x1b[31m\tb\x1b[0m c"
	got := boundedLine(input)
	if strings.Contains(got, "\t") {
		t.Fatalf("boundedLine should convert tab to space, got %q", got)
	}
	if !strings.Contains(got, "a") || !strings.Contains(got, "b") || !strings.Contains(got, "c") {
		t.Fatalf("boundedLine lost text with ANSI+tab, got %q", got)
	}
	if strings.Contains(got, "[31m") && !strings.Contains(got, "\x1b[31m") {
		t.Fatalf("boundedLine leaked bracket with tab test, got %q", got)
	}
	// Invalid UTF8 should become replacement char
	invalid := string([]byte{'x', 0xff, '\x1b', '[', '3', '1', 'm', 'y'})
	got2 := boundedLine(invalid)
	if !strings.Contains(got2, "�") {
		t.Fatalf("boundedLine should replace invalid UTF8 with �, got %q", got2)
	}
	if strings.Contains(got2, "[31m") && !strings.Contains(got2, "\x1b[") {
		// This indicates ESC stripped leaving bracket
		t.Fatalf("boundedLine handling of invalid UTF8 left bracket artifact, got %q", got2)
	}
	// Truncation should respect maxLogLineBytes even with ANSI
	longContent := strings.Repeat("a", maxLogLineBytes) + "\x1b[31mred\x1b[0m"
	got3 := boundedLine(longContent)
	if !strings.HasSuffix(got3, "…") {
		t.Fatalf("boundedLine long with ANSI should truncate with …, got len %d suffix %q", len(got3), got3)
	}
	if len(got3) > maxLogLineBytes+len("…")+20 { // allow some slack for preserved ANSI
		t.Fatalf("boundedLine truncated length too large %d", len(got3))
	}
	if strings.Contains(got3, "[31m") && !strings.Contains(got3, "\x1b[") {
		t.Fatalf("truncated ANSI leaked bracket, got %q", got3)
	}
}
