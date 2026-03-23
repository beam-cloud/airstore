package types

import "testing"

func TestNormalizeRunExecutionPostRun_SourceWatchNormalizesRequests(t *testing.T) {
	postRun := NormalizeRunExecutionPostRun(&RunExecutionPostRun{
		SourceWatchRequests: []*SourceWatchRequest{
			{Integration: " gmail ", ThreadID: "thread-1"},
			{Integration: " ", ThreadID: "ignored"},
		},
	})
	if postRun == nil {
		t.Fatal("expected normalized post-run plan")
	}
	if postRun.WaitingForInput {
		t.Fatalf("waiting_for_input = true, want false until settlement applies the source watch")
	}
	if len(postRun.SourceWatchRequests) != 1 {
		t.Fatalf("source watch count = %d, want 1", len(postRun.SourceWatchRequests))
	}
	if postRun.SourceWatchRequests[0].Integration != "gmail" {
		t.Fatalf("integration = %q, want gmail", postRun.SourceWatchRequests[0].Integration)
	}
}

func TestRunExecutionResultSetPostRunMaintainsCompatFields(t *testing.T) {
	result := &RunExecutionResult{ID: "task-1"}
	result.SetPostRun(&RunExecutionPostRun{
		WakeSignal: &RunExecutionWakeSignal{
			DelayMinutes: 5,
			Reason:       "check reply",
		},
		SubtaskRequests: []*SubtaskRequest{
			{Prompt: "follow up with customer"},
		},
	})
	if result.PostRun == nil {
		t.Fatal("expected post-run plan")
	}
	if result.WakeSignal == nil || result.WakeSignal.Reason != "check reply" {
		t.Fatalf("wake signal = %#v, want compat wake signal", result.WakeSignal)
	}
	if len(result.SubtaskRequests) != 1 || result.SubtaskRequests[0].Prompt != "follow up with customer" {
		t.Fatalf("subtask requests = %#v, want compat subtasks", result.SubtaskRequests)
	}
}
