package types

import "testing"

func TestNormalizeRunInputQueueMode(t *testing.T) {
	tests := []struct {
		name string
		in   AgentQueueMode
		want AgentQueueMode
	}{
		{
			name: "empty defaults to followup",
			in:   "",
			want: AgentQueueModeFollowup,
		},
		{
			name: "keeps queue",
			in:   AgentQueueModeQueue,
			want: AgentQueueModeQueue,
		},
		{
			name: "normalizes steer backlog alias",
			in:   AgentQueueModeSteerBacklog,
			want: AgentQueueModeSteer,
		},
		{
			name: "trims and lowercases",
			in:   AgentQueueMode("  INTERRUPT  "),
			want: AgentQueueModeInterrupt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NormalizeRunInputQueueMode(tt.in)
			if got != tt.want {
				t.Fatalf("unexpected mode: got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestValidateRunInputQueueMode(t *testing.T) {
	supported := []AgentQueueMode{
		AgentQueueModeQueue,
		AgentQueueModeFollowup,
		AgentQueueModeSteer,
		AgentQueueModeSteerBacklog,
		AgentQueueModeInterrupt,
		"",
	}
	for _, mode := range supported {
		if err := ValidateRunInputQueueMode(mode); err != nil {
			t.Fatalf("mode %q should be valid: %v", mode, err)
		}
	}

	if err := ValidateRunInputQueueMode(AgentQueueMode("collect")); err == nil {
		t.Fatalf("unsupported mode should fail validation")
	}
}
