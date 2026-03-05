package types

import "testing"

func TestRunExecutionNormalizeType_DefaultsToBackground(t *testing.T) {
	exec := &RunExecution{}
	exec.NormalizeType()
	if exec.Type != RunExecutionTypeBackground {
		t.Fatalf("expected default type %q, got %q", RunExecutionTypeBackground, exec.Type)
	}
}

func TestRunExecutionIsInteractive(t *testing.T) {
	background := &RunExecution{Type: RunExecutionTypeBackground}
	if background.IsInteractive() {
		t.Fatalf("background run execution should not be interactive")
	}

	interactive := &RunExecution{Type: RunExecutionTypeInteractive}
	if !interactive.IsInteractive() {
		t.Fatalf("interactive run execution should be interactive")
	}
}
