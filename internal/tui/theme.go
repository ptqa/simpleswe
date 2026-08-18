package tui

import (
	"fmt"
	"os"
	"strings"

	"go.rockorager.dev/vaxis"
)

type colorPalette struct {
	name                                 string
	base, header, title, dim, selected   vaxis.Style
	border, ok, warn, bad, info, overlay vaxis.Style
}

type themeName int

type paletteColors struct {
	background, foreground, header, title, dim uint32
	selected, selectedForeground, border       uint32
	ok, warn, bad, info, overlay               uint32
}

var themes = []colorPalette{
	newPalette("Simpleswe Dark", paletteColors{
		background: 0x07111f, foreground: 0xc9d3e2, header: 0x07111f, title: 0x65d8ff, dim: 0x8793aa,
		selected: 0x6bc8e8, selectedForeground: 0x07111f, border: 0x25354a,
		ok: 0x72d17c, warn: 0xf2c66d, bad: 0xff6577, info: 0x67c7ef, overlay: 0x1b2940,
	}),
	newPalette("Simpleswe Light", paletteColors{
		background: 0xf7f9fc, foreground: 0x182235, header: 0x24324d, title: 0x075985, dim: 0x526176,
		selected: 0x0f766e, selectedForeground: 0xffffff, border: 0x94a3b8,
		ok: 0x166534, warn: 0x854d0e, bad: 0xbe123c, info: 0x0369a1, overlay: 0xe8eef7,
	}),
	newPalette("Nord", paletteColors{
		background: 0x2e3440, foreground: 0xd8dee9, header: 0x3b4252, title: 0x88c0d0, dim: 0x81a1c1,
		selected: 0x88c0d0, selectedForeground: 0x2e3440, border: 0x4c566a,
		ok: 0xa3be8c, warn: 0xebcb8b, bad: 0xbf616a, info: 0x5e81ac, overlay: 0x3b4252,
	}),
	newPalette("Dracula", paletteColors{
		background: 0x282a36, foreground: 0xf8f8f2, header: 0x44475a, title: 0x8be9fd, dim: 0x9aa3c7,
		selected: 0x50fa7b, selectedForeground: 0x282a36, border: 0x6272a4,
		ok: 0x50fa7b, warn: 0xf1fa8c, bad: 0xff5555, info: 0xbd93f9, overlay: 0x44475a,
	}),
	newPalette("Gruvbox", paletteColors{
		background: 0x282828, foreground: 0xebdbb2, header: 0x3c3836, title: 0xfabd2f, dim: 0xa89984,
		selected: 0x83a598, selectedForeground: 0x282828, border: 0x665c54,
		ok: 0xb8bb26, warn: 0xfabd2f, bad: 0xfb4934, info: 0x83a598, overlay: 0x3c3836,
	}),
	newPalette("Catppuccin Mocha", paletteColors{
		background: 0x1e1e2e, foreground: 0xcdd6f4, header: 0x313244, title: 0x89dceb, dim: 0xa6adc8,
		selected: 0xcba6f7, selectedForeground: 0x1e1e2e, border: 0x45475a,
		ok: 0xa6e3a1, warn: 0xf9e2af, bad: 0xf38ba8, info: 0x89b4fa, overlay: 0x313244,
	}),
	newPalette("Solarized Dark", paletteColors{
		background: 0x002b36, foreground: 0x93a1a1, header: 0x073642, title: 0x2aa198, dim: 0x839496,
		selected: 0xb58900, selectedForeground: 0x002b36, border: 0x586e75,
		ok: 0x859900, warn: 0xb58900, bad: 0xdc322f, info: 0x268bd2, overlay: 0x073642,
	}),
	newPalette("Solarized Light", paletteColors{
		background: 0xfdf6e3, foreground: 0x586e75, header: 0x073642, title: 0x2aa198, dim: 0x657b83,
		selected: 0x268bd2, selectedForeground: 0xfdf6e3, border: 0x93a1a1,
		ok: 0x657b83, warn: 0x8b5d00, bad: 0xdc322f, info: 0x076678, overlay: 0xeee8d5,
	}),
	newPalette("Tokyo Night", paletteColors{
		background: 0x1a1b26, foreground: 0xc0caf5, header: 0x24283b, title: 0x7dcfff, dim: 0x9aa5ce,
		selected: 0x7aa2f7, selectedForeground: 0x1a1b26, border: 0x414868,
		ok: 0x9ece6a, warn: 0xe0af68, bad: 0xf7768e, info: 0xbb9af7, overlay: 0x24283b,
	}),
}

func colorStyle(foreground, background uint32, attribute vaxis.AttributeMask) vaxis.Style {
	return vaxis.Style{Foreground: vaxis.HexColor(foreground), Background: vaxis.HexColor(background), Attribute: attribute}
}

func newPalette(name string, colors paletteColors) colorPalette {
	return colorPalette{
		name:     name,
		base:     colorStyle(colors.foreground, colors.background, 0),
		header:   colorStyle(0xf8fafc, colors.header, vaxis.AttrBold),
		title:    colorStyle(colors.title, colors.background, vaxis.AttrBold),
		dim:      colorStyle(colors.dim, colors.background, 0),
		selected: colorStyle(colors.selectedForeground, colors.selected, vaxis.AttrBold),
		border:   colorStyle(colors.border, colors.background, 0),
		ok:       colorStyle(colors.ok, colors.background, 0),
		warn:     colorStyle(colors.warn, colors.background, 0),
		bad:      colorStyle(colors.bad, colors.background, vaxis.AttrBold),
		info:     colorStyle(colors.info, colors.background, 0),
		overlay:  colorStyle(colors.foreground, colors.overlay, 0),
	}
}

func (a *application) colors() colorPalette {
	if int(a.theme) >= len(themes) {
		return themes[0]
	}
	return themes[a.theme]
}

func (a *application) selectTheme(index int) {
	if index >= 0 && index < len(themes) {
		a.theme = themeName(index)
		a.message = "theme: " + themes[index].name
		if err := saveTheme(a.configDir, themes[index].name); err != nil {
			a.message = fmt.Sprintf("theme not saved: %v", err)
		}
	}
}

func loadTheme(configDir string) themeName {
	root, err := os.OpenRoot(configDir)
	if err != nil {
		return 0
	}
	defer func() { _ = root.Close() }()
	data, err := root.ReadFile("simpleswe/theme")
	if err != nil {
		return 0
	}
	name := strings.TrimSpace(string(data))
	for index := range themes {
		if themes[index].name == name {
			return themeName(index)
		}
	}
	return 0
}

func saveTheme(configDir, name string) error {
	if configDir == "" {
		return nil
	}
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return fmt.Errorf("create config directory: %w", err)
	}
	root, err := os.OpenRoot(configDir)
	if err != nil {
		return fmt.Errorf("open config directory: %w", err)
	}
	defer func() { _ = root.Close() }()
	if err := root.MkdirAll("simpleswe", 0o700); err != nil {
		return fmt.Errorf("create theme directory: %w", err)
	}
	if err := root.WriteFile("simpleswe/theme", []byte(name+"\n"), 0o600); err != nil {
		return fmt.Errorf("write theme: %w", err)
	}
	return nil
}
