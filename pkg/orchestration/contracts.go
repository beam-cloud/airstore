package orchestration

import (
	"fmt"
	"strings"
)

type InputProvenance struct {
	Source        *string `json:"source,omitempty"`
	MessageID     *string `json:"message_id,omitempty"`
	Channel       *string `json:"channel,omitempty"`
	ToolCallID    *string `json:"tool_call_id,omitempty"`
	CorrelationID *string `json:"correlation_id,omitempty"`
}

type RoutingContext struct {
	To             *string `json:"to,omitempty"`
	ReplyTo        *string `json:"reply_to,omitempty"`
	Channel        *string `json:"channel,omitempty"`
	ReplyChannel   *string `json:"reply_channel,omitempty"`
	AccountID      *string `json:"account_id,omitempty"`
	ReplyAccountID *string `json:"reply_account_id,omitempty"`
	ThreadID       *string `json:"thread_id,omitempty"`
	GroupID        *string `json:"group_id,omitempty"`
	GroupChannel   *string `json:"group_channel,omitempty"`
	GroupSpace     *string `json:"group_space,omitempty"`
}

type AgentCommandParams struct {
	Message           string              `json:"message"`
	AgentID           *string             `json:"agent_id,omitempty"`
	SessionID         string              `json:"session_id"`
	SessionKey        *string             `json:"session_key,omitempty"`
	Deliver           *bool               `json:"deliver,omitempty"`
	TimeoutMs         *int                `json:"timeout_ms,omitempty"`
	Policy            *RunExecutionPolicy `json:"policy,omitempty"`
	Lane              *string             `json:"lane,omitempty"`
	ExtraSystemPrompt *string             `json:"extra_system_prompt,omitempty"`
	InputProvenance   *InputProvenance    `json:"input_provenance,omitempty"`
	Routing           RoutingContext      `json:"routing"`
	Attachments       []map[string]any    `json:"attachments,omitempty"`
	IdempotencyKey    string              `json:"idempotency_key"`
	Label             *string             `json:"label,omitempty"`
	SpawnedBy         *string             `json:"spawned_by,omitempty"`
}

type ExecHost string
type ExecSecurity string
type ExecAsk string

const (
	ExecHostSandbox ExecHost = "sandbox"

	ExecSecurityDeny      ExecSecurity = "deny"
	ExecSecurityAllowlist ExecSecurity = "allowlist"
	ExecSecurityFull      ExecSecurity = "full"

	ExecAskOff    ExecAsk = "off"
	ExecAskOnMiss ExecAsk = "on-miss"
	ExecAskAlways ExecAsk = "always"
)

type RunExecutionPolicy struct {
	Host            ExecHost       `json:"host"`
	Security        ExecSecurity   `json:"security"`
	Ask             ExecAsk        `json:"ask"`
	RuntimeType     string         `json:"runtime_type"`
	WorkspaceAccess string         `json:"workspace_access"`
	NetworkEnabled  bool           `json:"network_enabled"`
	Interactive     bool           `json:"interactive"`
	Resources       map[string]any `json:"resources,omitempty"`
}

func ValidateAgentCommandParams(v *AgentCommandParams) error {
	if v == nil {
		return fmt.Errorf("payload is required")
	}
	if strings.TrimSpace(v.Message) == "" {
		return fmt.Errorf("message is required")
	}
	if strings.TrimSpace(v.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if strings.TrimSpace(v.SessionID) == "" {
		return fmt.Errorf("session_id is required")
	}
	if v.TimeoutMs != nil && *v.TimeoutMs < 0 {
		return fmt.Errorf("timeout_ms must be >= 0")
	}
	if v.Policy != nil {
		if err := ValidateRunExecutionPolicy(*v.Policy); err != nil {
			return fmt.Errorf("invalid policy: %w", err)
		}
	}
	return nil
}
