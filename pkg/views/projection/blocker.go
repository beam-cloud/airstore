package projection

import (
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

func ProjectBlocker(task *types.AgentTask, outputs []*types.TaskOutput) *types.ResolvedBlocker {
	if task == nil || task.CurrentBlocker == nil || task.CurrentBlocker.Status != types.TaskBlockerStatusOpen {
		return nil
	}
	current := task.CurrentBlocker
	inputKind := current.InputKind
	if inputKind == "" {
		switch current.Kind {
		case types.TaskBlockerKindApproval:
			inputKind = types.InputKindApproveReject
		default:
			inputKind = types.InputKindFreeText
		}
	}
	kind := current.Kind
	if kind == "" {
		kind = types.TaskBlockerKindForInputKind(inputKind)
	}

	outputIDs := uniqueTrimmedStrings(current.OutputIDs)
	outputByID := make(map[string]*types.TaskOutput, len(outputs))
	for _, output := range outputs {
		if output == nil || strings.TrimSpace(output.ID) == "" {
			continue
		}
		outputByID[output.ID] = output
	}

	blocker := &types.ResolvedBlocker{
		ID:              current.ID,
		Kind:            string(kind),
		InputKind:       string(inputKind),
		Status:          string(current.Status),
		WaitGroupID:     trimmedStringPtr(current.WaitGroupID),
		OutputIDs:       outputIDs,
		ApprovalSurface: current.ApprovalSurface(),
		PayloadJSON:     cloneBlockerPayload(current.PayloadJSON),
	}
	for _, outputID := range outputIDs {
		output := outputByID[outputID]
		if output == nil {
			continue
		}
		if blocker.OutputID == "" || output.CreatedAt.After(outputByID[blocker.OutputID].CreatedAt) {
			blocker.OutputID = output.ID
			blocker.OutputStatus = output.Status
		}
	}
	blocker.Summary = blockerPayloadString(current.PayloadJSON, "summary")
	blocker.Details = blockerPayloadString(current.PayloadJSON, "details")
	blocker.Items = blockerPayloadItems(current.PayloadJSON, outputByID, outputIDs)
	return blocker
}

func cloneBlockerPayload(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func blockerPayloadString(payload map[string]any, key string) string {
	if len(payload) == 0 {
		return ""
	}
	raw, ok := payload[key]
	if !ok || raw == nil {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func blockerPayloadItems(
	payload map[string]any,
	outputByID map[string]*types.TaskOutput,
	outputIDs []string,
) []types.ResolvedBlockerItem {
	items := make([]types.ResolvedBlockerItem, 0, len(outputIDs))
	if rawItems, ok := payload["items"].([]any); ok {
		for _, raw := range rawItems {
			record, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			outputID := strings.TrimSpace(blockerPayloadString(record, "output_id"))
			if outputID == "" {
				continue
			}
			item := types.ResolvedBlockerItem{
				OutputID: outputID,
				Title:    blockerPayloadString(record, "title"),
				ItemKey:  blockerPayloadString(record, "item_key"),
			}
			if item.ItemKey == "" {
				item.ItemKey = outputID
			}
			if item.Title == "" {
				if output := outputByID[outputID]; output != nil {
					item.Title = output.Title
				}
			}
			items = append(items, item)
		}
	}
	if len(items) > 0 {
		return items
	}
	items = make([]types.ResolvedBlockerItem, 0, len(outputIDs))
	for _, outputID := range outputIDs {
		item := types.ResolvedBlockerItem{
			OutputID: outputID,
			ItemKey:  outputID,
		}
		if output := outputByID[outputID]; output != nil {
			item.Title = output.Title
		}
		items = append(items, item)
	}
	return items
}

func trimmedStringPtr(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

func uniqueTrimmedStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		unique = append(unique, trimmed)
	}
	if len(unique) == 0 {
		return nil
	}
	return unique
}
