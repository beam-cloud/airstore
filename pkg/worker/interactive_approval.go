package worker

import (
	"context"
	"encoding/json"

	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
)

// tryBuildApprovalSummary extracts a structured summary of what the agent
// is asking the user to approve. Returns a JSON string with {summary, details}
// or "" on failure. Approval is always a single yes/no decision on the whole
// action — per-entity granularity is handled by subtasks, not per-item voting.
func (w *Worker) tryBuildApprovalSummary(ctx context.Context, assistantText string, bamlEnv map[string]string) string {
	if assistantText == "" {
		return ""
	}
	summary, err := agentsignal.ExtractApprovalSummary(ctx, assistantText, agentsignal.WithEnv(bamlEnv))
	if err != nil {
		return ""
	}
	b, _ := json.Marshal(map[string]string{
		"summary": summary.Summary,
		"details": summary.Details,
	})
	return string(b)
}
