package tui

import (
	"strings"

	"go.rockorager.dev/vaxis"
)

// wrappedLogRows splits a single logical log line into visual rows that fit
// within width columns, preserving ANSI SGR colors across wraps. The caller
// provides the window width (including the leading padding space). Each
// returned row is a slice of segments ready for Window.Print.
func wrappedLogRows(line string, base vaxis.Style, width int) [][]vaxis.Segment {
	if width <= 0 {
		return nil
	}
	cells := sanitizeLogCells(" "+line, base)
	if len(cells) == 0 {
		return [][]vaxis.Segment{{{Text: " ", Style: base}}}
	}
	rows := chunkCellsByWidth(cells, width)
	out := make([][]vaxis.Segment, 0, len(rows))
	for _, rowCells := range rows {
		segs := cellsToSegments(rowCells, base)
		out = append(out, segs)
	}
	return out
}

func sanitizeLogCells(input string, base vaxis.Style) []vaxis.Cell {
	cells := vaxis.ParseStyledString(input)
	for i := range cells {
		if cells[i].Foreground == vaxis.ColorDefault {
			cells[i].Foreground = base.Foreground
		}
		if cells[i].Background == vaxis.ColorDefault {
			cells[i].Background = base.Background
		}
		if cells[i].UnderlineColor == vaxis.ColorDefault && base.UnderlineColor != vaxis.ColorDefault {
			cells[i].UnderlineColor = base.UnderlineColor
		}
		cells[i].Hyperlink = ""
		cells[i].HyperlinkParams = ""
	}
	return cells
}

func chunkCellsByWidth(cells []vaxis.Cell, width int) [][]vaxis.Cell {
	rows := make([][]vaxis.Cell, 0, (len(cells)+width-1)/max(1, width))
	cur := make([]vaxis.Cell, 0, width)
	curWidth := 0
	for _, cell := range cells {
		w := cell.Width
		if w <= 0 {
			w = 1
		}
		if curWidth+w > width && len(cur) > 0 {
			rows = append(rows, cur)
			cur = make([]vaxis.Cell, 0, width)
			curWidth = 0
		}
		cur = append(cur, cell)
		curWidth += w
	}
	if len(cur) > 0 {
		rows = append(rows, cur)
	}
	return rows
}

func cellsToSegments(cells []vaxis.Cell, base vaxis.Style) []vaxis.Segment {
	var segs []vaxis.Segment
	var curStyle vaxis.Style
	var buf strings.Builder
	first := true
	for _, cell := range cells {
		mapped := cell.Style
		if first {
			curStyle = mapped
			buf.WriteString(cell.Grapheme)
			first = false
			continue
		}
		if mapped == curStyle {
			buf.WriteString(cell.Grapheme)
		} else {
			segs = append(segs, vaxis.Segment{Text: buf.String(), Style: curStyle})
			buf.Reset()
			buf.WriteString(cell.Grapheme)
			curStyle = mapped
		}
	}
	if !first {
		segs = append(segs, vaxis.Segment{Text: buf.String(), Style: curStyle})
	}
	if len(segs) == 0 {
		segs = []vaxis.Segment{{Text: "", Style: base}}
	}
	return segs
}

// ansiSegments parses ANSI SGR escapes in line and returns segments with
// styles derived from base. The returned segments already include a leading
// space with base style for log padding, and are grouped by style for
// efficient rendering via Window.PrintTruncate.
func ansiSegments(line string, base vaxis.Style) []vaxis.Segment {
	// Prepend a space so the log line has left padding and the padding
	// is always rendered with base style, even when the line starts with
	// an ANSI escape. Parsing " " + line ensures the first cell is base.
	input := " " + line
	cells := vaxis.ParseStyledString(input)
	if len(cells) == 0 {
		return []vaxis.Segment{{Text: " ", Style: base}}
	}
	var segments []vaxis.Segment
	var curStyle vaxis.Style
	var cur strings.Builder
	first := true
	for _, cell := range cells {
		mapped := cell.Style
		if mapped.Foreground == vaxis.ColorDefault {
			mapped.Foreground = base.Foreground
		}
		if mapped.Background == vaxis.ColorDefault {
			mapped.Background = base.Background
		}
		// Sanitize hyperlink: untrusted logs must not render clickable OSC 8 links.
		mapped.Hyperlink = ""
		mapped.HyperlinkParams = ""
		// UnderlineColor is rarely used for logs; map default to base if base has one.
		// Base underline color is default (0), so keep as is.
		if first {
			curStyle = mapped
			cur.WriteString(cell.Grapheme)
			first = false
			continue
		}
		if mapped == curStyle {
			cur.WriteString(cell.Grapheme)
		} else {
			segments = append(segments, vaxis.Segment{Text: cur.String(), Style: curStyle})
			cur.Reset()
			cur.WriteString(cell.Grapheme)
			curStyle = mapped
		}
	}
	if !first {
		segments = append(segments, vaxis.Segment{Text: cur.String(), Style: curStyle})
	}
	if len(segments) == 0 {
		return []vaxis.Segment{{Text: " " + line, Style: base}}
	}
	return segments
}
