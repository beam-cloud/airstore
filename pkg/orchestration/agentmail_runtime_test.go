package orchestration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type agentMailRuntimeBackend struct {
	repository.BackendRepository
	agentBindings     []*types.ChannelBinding
	workspaceBindings []*types.ChannelBinding
}

func (b *agentMailRuntimeBackend) ListChannelBindings(_ context.Context, _ uint, agentID *string) ([]*types.ChannelBinding, error) {
	if agentID != nil {
		return b.agentBindings, nil
	}
	return b.workspaceBindings, nil
}

func TestApplyAgentMailRuntimeContextPrefersAgentBinding(t *testing.T) {
	agentID := "agent-1"
	run := &types.AgentRun{
		WorkspaceID: 7,
		AgentID:     &agentID,
		DeliveryJSON: map[string]any{
			"routing": map[string]any{
				"channel": "email",
			},
		},
	}
	backend := &agentMailRuntimeBackend{
		agentBindings: []*types.ChannelBinding{
			{ChannelType: "email", Address: "agent@agentmail.to", Active: true},
		},
		workspaceBindings: []*types.ChannelBinding{
			{ChannelType: "email", Address: "workspace@agentmail.to", Active: true},
		},
	}
	env := map[string]string{
		"AIRSTORE_AGENT_SYSTEM_PROMPT": "You are a helpful agent.",
	}

	applyAgentMailRuntimeContext(context.Background(), backend, env, run)

	if got := env[agentRoutingJSONEnvKey]; got == "" {
		t.Fatal("expected routing json env to be populated")
	}
	if prompt := env["AIRSTORE_AGENT_SYSTEM_PROMPT"]; !containsAllFragments(prompt, runtimeEmailRoutingHeader, "agent@agentmail.to") {
		t.Fatalf("expected runtime prompt hint, got:\n%s", prompt)
	}
}

func TestApplyAgentMailRuntimeContextFallsBackToWorkspaceBinding(t *testing.T) {
	agentID := "agent-1"
	run := &types.AgentRun{WorkspaceID: 7, AgentID: &agentID}
	backend := &agentMailRuntimeBackend{
		workspaceBindings: []*types.ChannelBinding{
			{ChannelType: "email", Address: "workspace@agentmail.to", Active: true},
		},
	}
	env := map[string]string{}

	applyAgentMailRuntimeContext(context.Background(), backend, env, run)

	if prompt := env["AIRSTORE_AGENT_SYSTEM_PROMPT"]; !containsAllFragments(prompt, "workspace@agentmail.to") {
		t.Fatalf("expected workspace inbox in prompt, got:\n%s", prompt)
	}
}

func TestApplyAgentMailRuntimeContextPrefersRoutingTargetWhenBound(t *testing.T) {
	agentID := "agent-1"
	run := &types.AgentRun{
		WorkspaceID: 7,
		AgentID:     &agentID,
		DeliveryJSON: map[string]any{
			"routing": map[string]any{
				"channel":  "email",
				"to":       "workspace@agentmail.to",
				"reply_to": "lead@example.com",
			},
		},
	}
	backend := &agentMailRuntimeBackend{
		agentBindings: []*types.ChannelBinding{
			{ChannelType: "email", Address: "agent@agentmail.to", Active: true},
		},
		workspaceBindings: []*types.ChannelBinding{
			{ChannelType: "email", Address: "workspace@agentmail.to", Active: true},
		},
	}
	env := map[string]string{}

	applyAgentMailRuntimeContext(context.Background(), backend, env, run)

	if !containsAllFragments(env["AIRSTORE_AGENT_SYSTEM_PROMPT"], "workspace@agentmail.to", "routing.to = workspace@agentmail.to", "routing.reply_to = lead@example.com") {
		t.Fatalf("expected routing guidance in prompt, got:\n%s", env["AIRSTORE_AGENT_SYSTEM_PROMPT"])
	}

	var routing map[string]any
	if err := json.Unmarshal([]byte(env[agentRoutingJSONEnvKey]), &routing); err != nil {
		t.Fatalf("unmarshal routing env: %v", err)
	}
	if got, want := routing["to"], "workspace@agentmail.to"; got != want {
		t.Fatalf("routing.to = %#v, want %#v", got, want)
	}
}

func TestApplyAgentMailRuntimeContextFallsBackWhenRoutingTargetIsUnbound(t *testing.T) {
	agentID := "agent-1"
	run := &types.AgentRun{
		WorkspaceID: 7,
		AgentID:     &agentID,
		DeliveryJSON: map[string]any{
			"routing": map[string]any{
				"channel": "email",
				"to":      "unknown@agentmail.to",
			},
		},
	}
	backend := &agentMailRuntimeBackend{
		agentBindings: []*types.ChannelBinding{
			{ChannelType: "email", Address: "agent@agentmail.to", Active: true},
		},
	}
	env := map[string]string{}

	applyAgentMailRuntimeContext(context.Background(), backend, env, run)

	if prompt := env["AIRSTORE_AGENT_SYSTEM_PROMPT"]; !containsAllFragments(prompt, "agent@agentmail.to") {
		t.Fatalf("expected agent inbox in prompt, got:\n%s", prompt)
	}
}

func containsAllFragments(s string, fragments ...string) bool {
	for _, fragment := range fragments {
		if fragment == "" {
			continue
		}
		if !strings.Contains(s, fragment) {
			return false
		}
	}
	return true
}
