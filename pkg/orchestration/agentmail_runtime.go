package orchestration

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	agentRoutingJSONEnvKey = "AIRSTORE_AGENT_ROUTING_JSON"
	runtimeEmailRoutingHeader     = "Active email routing context:"
	runtimeEmailChannelType       = "email"
	runtimeEmailRoutingToKey      = "to"
	runtimeEmailRoutingReplyToKey = "reply_to"
	runtimeEmailRoutingChannelKey = "channel"
)

func applyAgentMailRuntimeContext(
	ctx context.Context,
	backend repository.BackendRepository,
	env map[string]string,
	run *types.AgentRun,
) {
	if env == nil || run == nil {
		return
	}

	routing := routingFromRun(run)
	if len(routing) > 0 {
		env[agentRoutingJSONEnvKey] = stringifyEnvValue(routing)
	}

	inboxID := resolveBoundAgentMailInbox(ctx, backend, run, routing)

	if prompt := appendAgentMailRuntimeGuidance(env["AIRSTORE_AGENT_SYSTEM_PROMPT"], inboxID, routing); prompt != "" {
		env["AIRSTORE_AGENT_SYSTEM_PROMPT"] = prompt
	}
}

func routingFromRun(run *types.AgentRun) map[string]any {
	if run == nil || len(run.DeliveryJSON) == 0 {
		return map[string]any{}
	}
	return mapFromPayload(run.DeliveryJSON, "routing")
}

func resolveBoundAgentMailInbox(
	ctx context.Context,
	backend repository.BackendRepository,
	run *types.AgentRun,
	routing map[string]any,
) string {
	if backend == nil || run == nil || run.WorkspaceID == 0 {
		return ""
	}

	var agentBindings []*types.ChannelBinding
	if run.AgentID != nil && strings.TrimSpace(*run.AgentID) != "" {
		if bindings, err := backend.ListChannelBindings(ctx, run.WorkspaceID, run.AgentID); err == nil {
			agentBindings = bindings
		}
	}

	workspaceBindings, err := backend.ListChannelBindings(ctx, run.WorkspaceID, nil)
	if err != nil {
		workspaceBindings = nil
	}

	if preferred := normalizeChannelAddress(stringFromPayload(routing, runtimeEmailRoutingToKey)); preferred != "" {
		if binding := findEmailBindingByAddress(preferred, agentBindings, workspaceBindings); binding != nil {
			return strings.TrimSpace(binding.Address)
		}
	}

	if binding := firstActiveEmailBinding(agentBindings); binding != nil {
		return strings.TrimSpace(binding.Address)
	}
	if binding := firstActiveEmailBinding(workspaceBindings); binding != nil {
		return strings.TrimSpace(binding.Address)
	}

	return ""
}

func findEmailBindingByAddress(address string, bindingSets ...[]*types.ChannelBinding) *types.ChannelBinding {
	normalized := normalizeChannelAddress(address)
	if normalized == "" {
		return nil
	}
	for _, bindings := range bindingSets {
		for _, binding := range bindings {
			if !isActiveEmailBinding(binding) {
				continue
			}
			if normalizeChannelAddress(binding.Address) == normalized {
				return binding
			}
		}
	}
	return nil
}

func firstActiveEmailBinding(bindings []*types.ChannelBinding) *types.ChannelBinding {
	for _, binding := range bindings {
		if isActiveEmailBinding(binding) {
			return binding
		}
	}
	return nil
}

func isActiveEmailBinding(binding *types.ChannelBinding) bool {
	if binding == nil || !binding.Active {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(binding.ChannelType), runtimeEmailChannelType) {
		return false
	}
	return strings.TrimSpace(binding.Address) != ""
}

func normalizeChannelAddress(address string) string {
	return strings.ToLower(strings.TrimSpace(address))
}

func appendAgentMailRuntimeGuidance(prompt, inboxID string, routing map[string]any) string {
	guidance := agentMailRuntimeGuidance(inboxID, routing)
	if guidance == "" {
		return prompt
	}
	trimmed := strings.TrimSpace(prompt)
	if strings.Contains(trimmed, runtimeEmailRoutingHeader) {
		return trimmed
	}
	if trimmed == "" {
		return guidance
	}
	return trimmed + "\n\n" + guidance
}

func agentMailRuntimeGuidance(inboxID string, routing map[string]any) string {
	to := strings.TrimSpace(stringFromPayload(routing, runtimeEmailRoutingToKey))
	replyTo := strings.TrimSpace(stringFromPayload(routing, runtimeEmailRoutingReplyToKey))
	channel := strings.TrimSpace(stringFromPayload(routing, runtimeEmailRoutingChannelKey))
	if inboxID == "" && to == "" && replyTo == "" && !strings.EqualFold(channel, runtimeEmailChannelType) {
		return ""
	}

	lines := []string{runtimeEmailRoutingHeader}
	if inboxID != "" {
		lines = append(lines, fmt.Sprintf(
			"- Your default sender inbox is %s — use it for AgentMail send/reply unless the user explicitly requests another inbox.",
			inboxID,
		))
	}
	if to != "" {
		lines = append(lines, fmt.Sprintf("- routing.to = %s. This is the inbox that received the message.", to))
	}
	if replyTo != "" {
		lines = append(lines, fmt.Sprintf("- routing.reply_to = %s. This is the correspondent address.", replyTo))
	}
	return strings.Join(lines, "\n")
}
