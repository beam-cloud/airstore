package worker

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

func (w *Worker) classifySubtasks(
	ctx context.Context,
	tracker *taskOutputTracker,
	agentMsg, userMsg string,
	bamlEnv map[string]string,
) []*types.SubtaskRequest {
	summaries := tracker.TrackedOutputSummaries()
	if !shouldAttemptFanOut(summaries) {
		return nil
	}

	type outputEntry struct {
		ID       string `json:"id"`
		Identity string `json:"identity"`
		Entity   string `json:"entity,omitempty"`
	}
	entries := make([]outputEntry, len(summaries))
	for i, s := range summaries {
		entries[i] = outputEntry{ID: s.OutputID, Identity: s.Identity, Entity: s.EntityKey}
	}
	outputsJSON, err := json.Marshal(entries)
	if err != nil {
		return nil
	}

	fo, err := agentsignal.ClassifyFanOut(
		ctx,
		string(outputsJSON),
		agentMsg,
		userMsg,
		time.Now().UTC().Format(time.RFC3339),
		agentsignal.WithEnv(bamlEnv),
	)
	if err != nil {
		log.Warn().Err(err).Msg("subtask classification failed")
		return nil
	}
	if fo.Intent != signaltypes.FanOutIntentFAN_OUT || len(fo.Specs) == 0 {
		return nil
	}

	reqs := make([]*types.SubtaskRequest, 0, len(fo.Specs))
	for _, s := range fo.Specs {
		reqs = append(reqs, &types.SubtaskRequest{
			SourceOutputID:   s.Source_output_id,
			EntityLabel:      s.Entity_label,
			Prompt:           s.Prompt,
			WakeDelayMinutes: int(s.Wake_delay_minutes),
		})
	}
	log.Info().Int("count", len(reqs)).Msg("subtask requests detected")
	return reqs
}

func shouldAttemptFanOut(summaries []trackedOutputSummary) bool {
	return distinctFanOutEntityCount(summaries) >= 2
}

func distinctFanOutEntityCount(summaries []trackedOutputSummary) int {
	entities := make(map[string]struct{}, len(summaries))
	for _, summary := range summaries {
		entity := strings.TrimSpace(summary.EntityKey)
		if entity == "" {
			continue
		}
		entities[entity] = struct{}{}
	}
	return len(entities)
}
