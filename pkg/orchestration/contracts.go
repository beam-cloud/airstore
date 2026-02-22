package orchestration

import (
	"encoding/json"
	"fmt"
	"strings"
)

type QueueMode string

const (
	QueueModeSteer        QueueMode = "steer"
	QueueModeFollowup     QueueMode = "followup"
	QueueModeCollect      QueueMode = "collect"
	QueueModeSteerBacklog QueueMode = "steer-backlog"
	QueueModeInterrupt    QueueMode = "interrupt"
	QueueModeQueue        QueueMode = "queue"
)

type QueueDropPolicy string

const (
	QueueDropPolicyOld       QueueDropPolicy = "old"
	QueueDropPolicyNew       QueueDropPolicy = "new"
	QueueDropPolicySummarize QueueDropPolicy = "summarize"
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
	Message           string           `json:"message"`
	AgentID           *string          `json:"agent_id,omitempty"`
	SessionID         string           `json:"session_id"`
	SessionKey        *string          `json:"session_key,omitempty"`
	Deliver           *bool            `json:"deliver,omitempty"`
	TimeoutMs         *int             `json:"timeout_ms,omitempty"`
	Lane              *string          `json:"lane,omitempty"`
	ExtraSystemPrompt *string          `json:"extra_system_prompt,omitempty"`
	InputProvenance   *InputProvenance `json:"input_provenance,omitempty"`
	Routing           RoutingContext   `json:"routing"`
	Attachments       []map[string]any `json:"attachments,omitempty"`
	IdempotencyKey    string           `json:"idempotency_key"`
	Label             *string          `json:"label,omitempty"`
	SpawnedBy         *string          `json:"spawned_by,omitempty"`
}

type ChatSendParams struct {
	SessionKey     string           `json:"session_key"`
	Message        string           `json:"message"`
	Thinking       *string          `json:"thinking,omitempty"`
	Deliver        *bool            `json:"deliver,omitempty"`
	Attachments    []map[string]any `json:"attachments,omitempty"`
	TimeoutMs      *int             `json:"timeout_ms,omitempty"`
	IdempotencyKey string           `json:"idempotency_key"`
}

type QueueSettings struct {
	Mode       QueueMode        `json:"mode"`
	DebounceMs *int             `json:"debounce_ms,omitempty"`
	Cap        *int             `json:"cap,omitempty"`
	DropPolicy *QueueDropPolicy `json:"drop_policy,omitempty"`
}

type OriginContext struct {
	Channel   *string `json:"channel,omitempty"`
	To        *string `json:"to,omitempty"`
	AccountID *string `json:"account_id,omitempty"`
	ThreadID  *string `json:"thread_id,omitempty"`
	ChatType  *string `json:"chat_type,omitempty"`
}

type FollowupRunContext struct {
	AgentID           string             `json:"agent_id"`
	SessionID         string             `json:"session_id"`
	SessionKey        *string            `json:"session_key,omitempty"`
	Provider          string             `json:"provider"`
	Model             string             `json:"model"`
	TimeoutMs         int                `json:"timeout_ms"`
	Policy            RunExecutionPolicy `json:"policy"`
	ExtraSystemPrompt *string            `json:"extra_system_prompt,omitempty"`
}

type FollowupEnvelope struct {
	Prompt      string             `json:"prompt"`
	MessageID   *string            `json:"message_id,omitempty"`
	SummaryLine *string            `json:"summary_line,omitempty"`
	EnqueuedAt  int64              `json:"enqueued_at"`
	Origin      OriginContext      `json:"origin"`
	Run         FollowupRunContext `json:"run"`
}

type ExecHost string
type ExecSecurity string
type ExecAsk string

const (
	ExecHostSandbox ExecHost = "sandbox"
	ExecHostGateway ExecHost = "gateway"
	ExecHostNode    ExecHost = "node"

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

type RunExecutionParams struct {
	RunID             string             `json:"run_id"`
	SessionID         string             `json:"session_id"`
	SessionKey        *string            `json:"session_key,omitempty"`
	AgentID           *string            `json:"agent_id,omitempty"`
	Prompt            string             `json:"prompt"`
	Provider          *string            `json:"provider,omitempty"`
	Model             *string            `json:"model,omitempty"`
	TimeoutMs         int                `json:"timeout_ms"`
	Policy            RunExecutionPolicy `json:"policy"`
	Routing           RoutingContext     `json:"routing"`
	Lane              *string            `json:"lane,omitempty"`
	ExtraSystemPrompt *string            `json:"extra_system_prompt,omitempty"`
	InputProvenance   *InputProvenance   `json:"input_provenance,omitempty"`
}

type DeliveryPlan struct {
	BaseDelivery       string  `json:"base_delivery"`
	ResolvedChannel    string  `json:"resolved_channel"`
	ResolvedTo         *string `json:"resolved_to,omitempty"`
	ResolvedAccountID  *string `json:"resolved_account_id,omitempty"`
	ResolvedThreadID   *string `json:"resolved_thread_id,omitempty"`
	DeliveryTargetMode *string `json:"delivery_target_mode,omitempty"`
}

type RunSnapshot struct {
	RunID     string  `json:"run_id"`
	Status    string  `json:"status"`
	StartedAt *int64  `json:"started_at,omitempty"`
	EndedAt   *int64  `json:"ended_at,omitempty"`
	Error     *string `json:"error,omitempty"`
	Ts        int64   `json:"ts"`
}

type ChatEvent struct {
	RunID        string         `json:"run_id"`
	SessionKey   string         `json:"session_key"`
	Seq          int            `json:"seq"`
	State        string         `json:"state"`
	Message      map[string]any `json:"message"`
	ErrorMessage *string        `json:"error_message,omitempty"`
	Usage        map[string]any `json:"usage,omitempty"`
	StopReason   *string        `json:"stop_reason,omitempty"`
}

type SpawnSubagentParams struct {
	Task                     string  `json:"task"`
	Label                    *string `json:"label,omitempty"`
	AgentID                  *string `json:"agent_id,omitempty"`
	Model                    *string `json:"model,omitempty"`
	RunTimeoutSeconds        *int    `json:"run_timeout_seconds,omitempty"`
	Mode                     *string `json:"mode,omitempty"`
	Cleanup                  *string `json:"cleanup,omitempty"`
	ExpectsCompletionMessage *bool   `json:"expects_completion_message,omitempty"`
}

type ExecApprovalRequest struct {
	ID          string  `json:"id"`
	Command     string  `json:"command"`
	Cwd         *string `json:"cwd,omitempty"`
	Host        *string `json:"host,omitempty"`
	Security    *string `json:"security,omitempty"`
	Ask         *string `json:"ask,omitempty"`
	AgentID     *string `json:"agent_id,omitempty"`
	SessionKey  *string `json:"session_key,omitempty"`
	CreatedAtMs int64   `json:"created_at_ms"`
	ExpiresAtMs int64   `json:"expires_at_ms"`
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
	return nil
}

func ValidateChatSendParams(v *ChatSendParams) error {
	if v == nil {
		return fmt.Errorf("payload is required")
	}
	if strings.TrimSpace(v.SessionKey) == "" {
		return fmt.Errorf("session_key is required")
	}
	if strings.TrimSpace(v.Message) == "" {
		return fmt.Errorf("message is required")
	}
	if strings.TrimSpace(v.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if v.TimeoutMs != nil && *v.TimeoutMs < 0 {
		return fmt.Errorf("timeout_ms must be >= 0")
	}
	return nil
}

func UnmarshalStrict(data []byte, dst any) error {
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}
