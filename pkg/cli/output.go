package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
)

// outputJSON controls whether commands should output JSON instead of styled text
var outputJSON bool

// SetJSONOutput sets the JSON output mode
func SetJSONOutput(enabled bool) {
	outputJSON = enabled
}

// IsJSONOutput returns true if JSON output mode is enabled
func IsJSONOutput() bool {
	return outputJSON
}

// PrintJSON outputs data as JSON if JSON mode is enabled, returns true if it did
func PrintJSON(data interface{}) bool {
	if !outputJSON {
		return false
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(data)
	return true
}

// PrintSuccess prints a success message with a green checkmark
func PrintSuccess(msg string) {
	fmt.Printf("  %s %s\n", SuccessStyle.Render(SymbolSuccess), msg)
}

// PrintSuccessf prints a formatted success message
func PrintSuccessf(format string, args ...interface{}) {
	PrintSuccess(fmt.Sprintf(format, args...))
}

// PrintSuccessWithValue prints a success message with a right-aligned value
func PrintSuccessWithValue(msg, value string) {
	symbol := SuccessStyle.Render(SymbolSuccess)
	// Create a styled line with value right-aligned
	fmt.Printf("  %s %-40s %s\n", symbol, msg, DimStyle.Render(value))
}

// PrintError prints an error message with a red X
func PrintError(err error) {
	fmt.Printf("  %s %s\n", ErrorStyle.Render(SymbolError), ErrorStyle.Render(err.Error()))
}

// PrintErrorMsg prints a simple error message string
func PrintErrorMsg(msg string) {
	fmt.Printf("  %s %s\n", ErrorStyle.Render(SymbolError), ErrorStyle.Render(msg))
}

// PrintWarning prints a warning message with a yellow indicator
func PrintWarning(msg string) {
	fmt.Printf("  %s %s\n", WarningStyle.Render(SymbolWarning), WarningStyle.Render(msg))
}

// PrintInfo prints an info message with an arrow
func PrintInfo(msg string) {
	fmt.Printf("  %s %s\n", InfoStyle.Render(SymbolInfo), msg)
}

// PrintInfof prints a formatted info message
func PrintInfof(format string, args ...interface{}) {
	PrintInfo(fmt.Sprintf(format, args...))
}

// PrintHint prints a subtle hint/suggestion
func PrintHint(msg string) {
	fmt.Printf("\n  %s\n", HintStyle.Render(msg))
}

// PrintSuggestions prints a list of suggestions
func PrintSuggestions(title string, suggestions []string) {
	fmt.Println()
	fmt.Printf("  %s\n", DimStyle.Render(title))
	for _, s := range suggestions {
		fmt.Printf("    %s %s\n", DimStyle.Render(SymbolBullet), s)
	}
}

// PrintHeader prints a section header
func PrintHeader(title string) {
	fmt.Printf("\n  %s\n\n", BoldStyle.Render(title))
}

// PrintKeyValue prints a key-value pair with consistent alignment
func PrintKeyValue(key, value string) {
	styledKey := KeyStyle.Render(key)
	fmt.Printf("  %s %s\n", styledKey, value)
}

// PrintKeyValueStyled prints a key-value pair with a custom value style
func PrintKeyValueStyled(key, value string, valueStyle lipgloss.Style) {
	styledKey := KeyStyle.Render(key)
	fmt.Printf("  %s %s\n", styledKey, valueStyle.Render(value))
}

// PrintBullet prints a bulleted item
func PrintBullet(text string) {
	fmt.Printf("    %s %s\n", DimStyle.Render(SymbolBullet), text)
}

// PrintIndented prints text with indentation
func PrintIndented(text string, level int) {
	indent := strings.Repeat("  ", level)
	fmt.Printf("%s%s\n", indent, text)
}

// PrintNewline prints an empty line
func PrintNewline() {
	fmt.Println()
}

// Table represents a styled table
type Table struct {
	Headers []string
	Rows    [][]string
	Widths  []int
}

// NewTable creates a new table with the given headers
func NewTable(headers ...string) *Table {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	return &Table{
		Headers: headers,
		Widths:  widths,
	}
}

// AddRow adds a row to the table
func (t *Table) AddRow(cells ...string) {
	// Pad or truncate to match header count
	row := make([]string, len(t.Headers))
	for i := range row {
		if i < len(cells) {
			row[i] = cells[i]
			if len(cells[i]) > t.Widths[i] {
				t.Widths[i] = len(cells[i])
			}
		}
	}
	t.Rows = append(t.Rows, row)
}

// Print renders the table to stdout
func (t *Table) Print() {
	if len(t.Rows) == 0 {
		return
	}

	// Print headers
	fmt.Print("  ")
	for i, h := range t.Headers {
		style := TableHeaderStyle.Width(t.Widths[i] + 2)
		fmt.Print(style.Render(h))
	}
	fmt.Println()

	// Print separator
	fmt.Print("  ")
	for i := range t.Headers {
		separator := strings.Repeat("─", t.Widths[i])
		fmt.Print(DimStyle.Render(separator), "  ")
	}
	fmt.Println()

	// Print rows
	for _, row := range t.Rows {
		fmt.Print("  ")
		for i, cell := range row {
			style := TableCellStyle.Width(t.Widths[i] + 2)
			fmt.Print(style.Render(cell))
		}
		fmt.Println()
	}
}

// FormatRelativeTime formats a timestamp as relative time (e.g., "2 hours ago")
func FormatRelativeTime(timestamp string) string {
	// Try parsing various formats
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
	}

	var t time.Time
	var err error
	for _, format := range formats {
		t, err = time.Parse(format, timestamp)
		if err == nil {
			break
		}
	}

	if err != nil {
		return timestamp // Return original if can't parse
	}

	duration := time.Since(t)

	switch {
	case duration < time.Minute:
		return "just now"
	case duration < time.Hour:
		mins := int(duration.Minutes())
		if mins == 1 {
			return "1 minute ago"
		}
		return fmt.Sprintf("%d minutes ago", mins)
	case duration < 24*time.Hour:
		hours := int(duration.Hours())
		if hours == 1 {
			return "1 hour ago"
		}
		return fmt.Sprintf("%d hours ago", hours)
	case duration < 7*24*time.Hour:
		days := int(duration.Hours() / 24)
		if days == 1 {
			return "1 day ago"
		}
		return fmt.Sprintf("%d days ago", days)
	case duration < 30*24*time.Hour:
		weeks := int(duration.Hours() / 24 / 7)
		if weeks == 1 {
			return "1 week ago"
		}
		return fmt.Sprintf("%d weeks ago", weeks)
	default:
		return t.Format("Jan 2, 2006")
	}
}

// Truncate truncates a string to maxLen, adding "..." if needed
func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// PrintMountStatus prints the mount status display
func PrintMountStatus(mountPoint, gateway, mode string, paths map[string]string) {
	fmt.Println()
	fmt.Printf("  %s\n", BrandStyle.Render("airstore mounted"))
	fmt.Println()

	PrintKeyValue("Mount", mountPoint)
	PrintKeyValue("Gateway", gateway)
	PrintKeyValue("Mode", mode)
	fmt.Println()

	fmt.Printf("  %s\n", DimStyle.Render("Available paths:"))
	for path, desc := range paths {
		fmt.Printf("    %s  %s\n", CodeStyle.Render(fmt.Sprintf("%-14s", path)), DimStyle.Render(desc))
	}
	fmt.Println()
	fmt.Printf("  %s\n", DimStyle.Render("Press Ctrl+C to unmount"))
	fmt.Println()
}

// PrintConnectionError prints a styled connection error with suggestions
func PrintConnectionError(addr string, err error) {
	fmt.Println()
	PrintErrorMsg("Cannot connect to gateway")
	fmt.Println()
	fmt.Printf("  The gateway at %s is not responding.\n", CodeStyle.Render(addr))

	PrintSuggestions("Suggestions:", []string{
		"Use local mode: " + CodeStyle.Render("airstore mount /path --config config.local.yaml"),
		"Verify the gateway is running at the specified address",
		"Check your " + CodeStyle.Render("AIRSTORE_GATEWAY") + " environment variable",
	})
	fmt.Println()
}
