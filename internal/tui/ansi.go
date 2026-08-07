package tui

import (
	"strings"

	"go.rockorager.dev/vaxis"
)

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
