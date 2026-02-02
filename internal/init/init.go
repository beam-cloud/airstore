// Package init sets up environment defaults before any other packages initialize.
// Import this package with a blank identifier as the first import to ensure
// logging defaults are set before BAML or other libraries load.
package init

import "os"

func init() {
	// Suppress BAML logs by default unless explicitly enabled
	if os.Getenv("BAML_LOG") == "" {
		os.Setenv("BAML_LOG", "off")
	}
}
