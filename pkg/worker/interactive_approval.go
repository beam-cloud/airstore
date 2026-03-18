package worker

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

type approvalItemSummary struct {
	OutputID string `json:"output_id"`
	Title    string `json:"title"`
	ItemKey  string `json:"item_key"`
}

// approvalBatchID produces a deterministic UUID for a set of approval items
// so repeated extractions don't create duplicate outputs.
func approvalBatchID(ids taskOutputIDs, prompt string, summary signaltypes.ApprovalSummary, items []signaltypes.ApprovalItem) string {
	keys := make([]string, len(items))
	for i, it := range items {
		keys[i] = strings.TrimSpace(it.Item_key)
	}
	sort.Strings(keys)

	seed := strings.Join(append([]string{
		ids.taskID, ids.runID,
		strings.TrimSpace(prompt),
		strings.TrimSpace(summary.Summary),
	}, keys...), "\x00")

	return uuid.NewSHA1(uuid.NameSpaceOID, []byte(seed)).String()
}

func approvalItemOutputID(batchID, itemKey string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("approval-item:"+batchID+":"+itemKey)).String()
}

// tryBuildApprovalSummary runs BAML extraction on the assistant's message to
// produce a structured approval summary with individual pending outputs.
func (w *Worker) tryBuildApprovalSummary(
	ctx context.Context, task types.RunExecution,
	currentPrompt, assistantText string,
	bamlEnv map[string]string,
) string {
	if assistantText == "" {
		return ""
	}
	summary, err := agentsignal.ExtractApprovalSummary(ctx, assistantText, agentsignal.WithEnv(bamlEnv))
	if err != nil {
		return ""
	}
	items, err := agentsignal.ExtractApprovalItems(ctx, assistantText, agentsignal.WithEnv(bamlEnv))
	if err != nil || len(items) == 0 {
		return marshalApprovalSummary(summary, nil)
	}

	ids := outputIDsFromTask(task)
	batchID := approvalBatchID(ids, currentPrompt, summary, items)
	var created []approvalItemSummary

	for _, item := range items {
		dataMap := map[string]any{"details": item.Details}
		for _, df := range item.Data_fields {
			dataMap[df.Key] = df.Value
		}
		metaJSON, _ := json.Marshal(map[string]any{
			"approval_batch_id":     batchID,
			"item_key":              item.Item_key,
			"_idempotent_output_id": approvalItemOutputID(batchID, item.Item_key),
		})
		dataJSON, _ := json.Marshal(dataMap)

		serverID, err := w.gatewayClient.CreateTaskOutput(ctx, &pb.CreateTaskOutputRequest{
			WorkspaceId:  ids.workspaceID,
			TaskId:       ids.taskID,
			RunId:        ids.runID,
			AgentId:      ids.agentID,
			OutputType:   kindToOutputType(item.Kind),
			Title:        item.Title,
			DataJson:     string(dataJSON),
			MetadataJson: string(metaJSON),
			Status:       types.TaskOutputStatusPending,
		})
		if err != nil {
			log.Warn().Err(err).Str("item_key", item.Item_key).Msg("failed to create pending approval output")
			continue
		}
		created = append(created, approvalItemSummary{OutputID: serverID, Title: item.Title, ItemKey: item.Item_key})
	}

	return marshalApprovalSummary(summary, created)
}

func marshalApprovalSummary(s signaltypes.ApprovalSummary, items []approvalItemSummary) string {
	payload := map[string]any{"summary": s.Summary, "details": s.Details}
	if len(items) > 0 {
		payload["items"] = items
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(b)
}
