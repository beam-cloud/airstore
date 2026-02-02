package cli

import (
	"github.com/charmbracelet/lipgloss"
)

// Color palette - professional, subtle colors inspired by modern CLIs
var (
	ColorPrimary = lipgloss.Color("#8B5CF6") // Purple - brand color
	ColorSuccess = lipgloss.Color("#22C55E") // Green
	ColorWarning = lipgloss.Color("#F59E0B") // Amber
	ColorError   = lipgloss.Color("#EF4444") // Red
	ColorInfo    = lipgloss.Color("#3B82F6") // Blue
	ColorSubtle  = lipgloss.Color("#6B7280") // Gray
	ColorMuted   = lipgloss.Color("#9CA3AF") // Light gray
)

// Symbols for consistent visual language
const (
	SymbolSuccess = "✓"
	SymbolError   = "✗"
	SymbolWarning = "!"
	SymbolInfo    = "→"
	SymbolBullet  = "•"
)

// Text styles
var (
	// Brand
	BrandStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(ColorPrimary)

	// Status styles
	SuccessStyle = lipgloss.NewStyle().
			Foreground(ColorSuccess)

	ErrorStyle = lipgloss.NewStyle().
			Foreground(ColorError).
			Bold(true)

	WarningStyle = lipgloss.NewStyle().
			Foreground(ColorWarning)

	InfoStyle = lipgloss.NewStyle().
			Foreground(ColorInfo)

	// Text variations
	BoldStyle = lipgloss.NewStyle().
			Bold(true)

	DimStyle = lipgloss.NewStyle().
			Foreground(ColorSubtle)

	MutedStyle = lipgloss.NewStyle().
			Foreground(ColorMuted)

	// Key-value styles
	KeyStyle = lipgloss.NewStyle().
			Foreground(ColorSubtle).
			Width(12)

	ValueStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FFFFFF"))

	// Table styles
	TableHeaderStyle = lipgloss.NewStyle().
				Bold(true).
				Foreground(ColorSubtle).
				BorderStyle(lipgloss.NormalBorder()).
				BorderBottom(true).
				BorderForeground(ColorSubtle)

	TableCellStyle = lipgloss.NewStyle().
			PaddingRight(2)

	// Section styles
	SectionTitleStyle = lipgloss.NewStyle().
				Bold(true).
				MarginTop(1).
				MarginBottom(1)

	// Box styles for status displays
	StatusBoxStyle = lipgloss.NewStyle().
			Padding(1, 2).
			MarginTop(1)

	// Code/path style
	CodeStyle = lipgloss.NewStyle().
			Foreground(ColorPrimary)

	// Hint style for suggestions
	HintStyle = lipgloss.NewStyle().
			Foreground(ColorMuted).
			Italic(true)
)

// IndentedStyle returns a style with the given indent level (2 spaces per level)
func IndentedStyle(level int) lipgloss.Style {
	return lipgloss.NewStyle().PaddingLeft(level * 2)
}
