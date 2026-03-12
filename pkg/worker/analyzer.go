package worker

// OutputAnalyzer decides which log lines may contain extractable outputs
// and prepares them for the BAML classifier. Each runner provides its own
// implementation since log formats differ across providers.
type OutputAnalyzer interface {
	// ShouldAnalyze returns true if the parsed JSON log line might
	// contain an extractable output (file write, API call, etc.).
	// This must be fast — no LLM calls, just field inspection.
	ShouldAnalyze(payload map[string]any) bool

	// PrepareInput extracts classifier inputs from a qualifying payload.
	// Returns false if the payload can't be prepared after all.
	PrepareInput(payload map[string]any) (toolName, toolInput, toolResult string, ok bool)
}
