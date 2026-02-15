package types

import "testing"

func TestTaskNormalizeType_DefaultsToBackground(t *testing.T) {
	task := &Task{}
	task.NormalizeType()
	if task.Type != TaskTypeBackground {
		t.Fatalf("expected default type %q, got %q", TaskTypeBackground, task.Type)
	}
}

func TestTaskIsInteractive(t *testing.T) {
	background := &Task{Type: TaskTypeBackground}
	if background.IsInteractive() {
		t.Fatalf("background task should not be interactive")
	}

	interactive := &Task{Type: TaskTypeInteractive}
	if !interactive.IsInteractive() {
		t.Fatalf("interactive task should be interactive")
	}
}
