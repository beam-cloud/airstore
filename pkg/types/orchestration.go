package types

import "time"

type AgentEnvelopeKind string

const (
	AgentEnvelopeKindAgentCommand AgentEnvelopeKind = "agent_command"
	AgentEnvelopeKindRunInput     AgentEnvelopeKind = "run_input"
)

type AgentQueueMode string

const (
	AgentQueueModeSteer        AgentQueueMode = "steer"
	AgentQueueModeSteerBacklog AgentQueueMode = "steer-backlog"
	AgentQueueModeFollowup     AgentQueueMode = "followup"
	AgentQueueModeInterrupt    AgentQueueMode = "interrupt"
	AgentQueueModeQueue        AgentQueueMode = "queue"
)

type AgentEnvelopeState string

const (
	AgentEnvelopeStateAccepted   AgentEnvelopeState = "accepted"
	AgentEnvelopeStateQueued     AgentEnvelopeState = "queued"
	AgentEnvelopeStateDispatched AgentEnvelopeState = "dispatched"
	AgentEnvelopeStateDropped    AgentEnvelopeState = "dropped"
	AgentEnvelopeStateCancelled  AgentEnvelopeState = "cancelled"
)

type AgentRunStatus string

const (
	AgentRunStatusAccepted  AgentRunStatus = "accepted"
	AgentRunStatusRunning   AgentRunStatus = "running"
	AgentRunStatusOK        AgentRunStatus = "ok"
	AgentRunStatusError     AgentRunStatus = "error"
	AgentRunStatusTimeout   AgentRunStatus = "timeout"
	AgentRunStatusCancelled AgentRunStatus = "cancelled"
)

func (s AgentRunStatus) IsActive() bool {
	switch s {
	case AgentRunStatusAccepted, AgentRunStatusRunning:
		return true
	default:
		return false
	}
}

func (s AgentRunStatus) IsSteerEligible() bool {
	return s == AgentRunStatusRunning
}

func (s AgentRunStatus) IsTerminal() bool {
	switch s {
	case AgentRunStatusOK, AgentRunStatusError, AgentRunStatusTimeout, AgentRunStatusCancelled:
		return true
	default:
		return false
	}
}

type AgentAttemptStatus string

const (
	AgentAttemptStatusPending   AgentAttemptStatus = "pending"
	AgentAttemptStatusBlocked   AgentAttemptStatus = "blocked"
	AgentAttemptStatusRunning   AgentAttemptStatus = "running"
	AgentAttemptStatusOK        AgentAttemptStatus = "ok"
	AgentAttemptStatusError     AgentAttemptStatus = "error"
	AgentAttemptStatusTimeout   AgentAttemptStatus = "timeout"
	AgentAttemptStatusCancelled AgentAttemptStatus = "cancelled"
)

func (s AgentAttemptStatus) IsInFlight() bool {
	switch s {
	case AgentAttemptStatusPending, AgentAttemptStatusRunning:
		return true
	default:
		return false
	}
}

const (
	AgentAttemptStrategyPrimary = "primary"
	AgentAttemptStrategyRetry   = "retry"
)

const (
	AgentExecutionMetaKeyInstanceKey      = "instance_key"
	AgentExecutionMetaKeyRetry            = "retry"
	AgentExecutionMetaKeyRetryMaxAttempts = "retry_max_attempts"
	AgentExecutionMetaKeyRetryDelayMs     = "retry_delay_ms"
	AgentExecutionMetaKeyResources        = "resources"
)

type AgentExecutionInstanceStatus string

const (
	AgentExecutionInstanceStatusHealthy  AgentExecutionInstanceStatus = "healthy"
	AgentExecutionInstanceStatusWarning  AgentExecutionInstanceStatus = "warning"
	AgentExecutionInstanceStatusDegraded AgentExecutionInstanceStatus = "degraded"
)

type AgentProfile struct {
	ID          string         `json:"id" db:"id"`
	WorkspaceID uint           `json:"workspace_id" db:"workspace_id"`
	AgentKey    string         `json:"agent_key" db:"agent_key"`
	Name        string         `json:"name" db:"name"`
	ConfigJSON  map[string]any `json:"config_json" db:"-"`
	Active      bool           `json:"active" db:"active"`
	CreatedAt   time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at" db:"updated_at"`
}

type AgentTaskEnvelope struct {
	ID               string             `json:"id" db:"id"`
	WorkspaceID      uint               `json:"workspace_id" db:"workspace_id"`
	AgentID          *string            `json:"agent_id,omitempty" db:"agent_id"`
	Kind             AgentEnvelopeKind  `json:"kind" db:"kind"`
	QueueMode        AgentQueueMode     `json:"queue_mode" db:"queue_mode"`
	State            AgentEnvelopeState `json:"state" db:"state"`
	IdempotencyKey   string             `json:"idempotency_key" db:"idempotency_key"`
	PayloadJSON      map[string]any     `json:"payload_json" db:"-"`
	RoutingJSON      map[string]any     `json:"routing_json" db:"-"`
	ParentEnvelopeID *string            `json:"parent_envelope_id,omitempty" db:"parent_envelope_id"`
	TargetRunID      *string            `json:"target_run_id,omitempty" db:"target_run_id"`
	AcceptedAt       time.Time          `json:"accepted_at" db:"accepted_at"`
	QueuedAt         *time.Time         `json:"queued_at,omitempty" db:"queued_at"`
	DispatchedAt     *time.Time         `json:"dispatched_at,omitempty" db:"dispatched_at"`
	DroppedReason    *string            `json:"dropped_reason,omitempty" db:"dropped_reason"`
	CreatedAt        time.Time          `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at" db:"updated_at"`
}

type AgentRun struct {
	ID               string         `json:"id" db:"id"`
	WorkspaceID      uint           `json:"workspace_id" db:"workspace_id"`
	AgentID          *string        `json:"agent_id,omitempty" db:"agent_id"`
	OriginEnvelopeID string         `json:"origin_envelope_id" db:"origin_envelope_id"`
	Status           AgentRunStatus `json:"status" db:"status"`
	SessionID        string         `json:"session_id" db:"session_id"`
	SessionKey       *string        `json:"session_key,omitempty" db:"session_key"`
	Provider         *string        `json:"provider,omitempty" db:"provider"`
	Model            *string        `json:"model,omitempty" db:"model"`
	ExecHost         string         `json:"exec_host" db:"exec_host"`
	ExecSecurity     string         `json:"exec_security" db:"exec_security"`
	ExecAsk          string         `json:"exec_ask" db:"exec_ask"`
	RuntimeType      string         `json:"runtime_type" db:"runtime_type"`
	WorkspaceAccess  string         `json:"workspace_access" db:"workspace_access"`
	NetworkEnabled   bool           `json:"network_enabled" db:"network_enabled"`
	Interactive      bool           `json:"interactive" db:"interactive"`
	TimeoutMs        int            `json:"timeout_ms" db:"timeout_ms"`
	StartedAt        *time.Time     `json:"started_at,omitempty" db:"started_at"`
	EndedAt          *time.Time     `json:"ended_at,omitempty" db:"ended_at"`
	Error            *string        `json:"error,omitempty" db:"error"`
	SnapshotTS       int64          `json:"snapshot_ts" db:"snapshot_ts"`
	UsageJSON        map[string]any `json:"usage_json" db:"-"`
	DeliveryJSON     map[string]any `json:"delivery_json" db:"-"`
	CreatedAt        time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at" db:"updated_at"`
}

type AgentRunAttempt struct {
	ID                      string             `json:"id" db:"id"`
	RunID                   string             `json:"run_id" db:"run_id"`
	AttemptNo               int                `json:"attempt_no" db:"attempt_no"`
	Status                  AgentAttemptStatus `json:"status" db:"status"`
	Strategy                string             `json:"strategy" db:"strategy"`
	Provider                *string            `json:"provider,omitempty" db:"provider"`
	Model                   *string            `json:"model,omitempty" db:"model"`
	ExecHost                string             `json:"exec_host" db:"exec_host"`
	ExecSecurity            string             `json:"exec_security" db:"exec_security"`
	ExecAsk                 string             `json:"exec_ask" db:"exec_ask"`
	RuntimeType             string             `json:"runtime_type" db:"runtime_type"`
	WorkspaceAccess         string             `json:"workspace_access" db:"workspace_access"`
	NetworkEnabled          bool               `json:"network_enabled" db:"network_enabled"`
	Interactive             bool               `json:"interactive" db:"interactive"`
	ExecutionTaskExternalID *string            `json:"execution_task_external_id,omitempty" db:"execution_task_external_id"`
	StartedAt               *time.Time         `json:"started_at,omitempty" db:"started_at"`
	EndedAt                 *time.Time         `json:"ended_at,omitempty" db:"ended_at"`
	ExitCode                *int               `json:"exit_code,omitempty" db:"exit_code"`
	Error                   *string            `json:"error,omitempty" db:"error"`
	CreatedAt               time.Time          `json:"created_at" db:"created_at"`
	UpdatedAt               time.Time          `json:"updated_at" db:"updated_at"`
}

type AgentRunSnapshot struct {
	ID          int64          `json:"id" db:"id"`
	RunID       string         `json:"run_id" db:"run_id"`
	Seq         int64          `json:"seq" db:"seq"`
	Status      AgentRunStatus `json:"status" db:"status"`
	StartedAtMs *int64         `json:"started_at_ms,omitempty" db:"started_at_ms"`
	EndedAtMs   *int64         `json:"ended_at_ms,omitempty" db:"ended_at_ms"`
	Error       *string        `json:"error,omitempty" db:"error"`
	TS          int64          `json:"ts" db:"ts"`
	PayloadJSON map[string]any `json:"payload_json" db:"-"`
	CreatedAt   time.Time      `json:"created_at" db:"created_at"`
}

type AgentExecutionInstance struct {
	ID                         string                       `json:"id" db:"id"`
	InstanceKey                string                       `json:"instance_key" db:"instance_key"`
	WorkspaceID                uint                         `json:"workspace_id" db:"workspace_id"`
	AgentID                    *string                      `json:"agent_id,omitempty" db:"agent_id"`
	Lane                       *string                      `json:"lane,omitempty" db:"lane"`
	ExecutionClassKey          string                       `json:"execution_class_key" db:"execution_class_key"`
	PoolName                   string                       `json:"pool_name" db:"pool_name"`
	Active                     bool                         `json:"active" db:"active"`
	Status                     AgentExecutionInstanceStatus `json:"status" db:"status"`
	FailedAttemptThreshold     int                          `json:"failed_attempt_threshold" db:"failed_attempt_threshold"`
	DesiredDispatchConcurrency int                          `json:"desired_dispatch_concurrency" db:"desired_dispatch_concurrency"`
	RunningAttempts            int                          `json:"running_attempts" db:"running_attempts"`
	PendingAttempts            int                          `json:"pending_attempts" db:"pending_attempts"`
	StoppingAttempts           int                          `json:"stopping_attempts" db:"stopping_attempts"`
	LastEventAt                *time.Time                   `json:"last_event_at,omitempty" db:"last_event_at"`
	CreatedAt                  time.Time                    `json:"created_at" db:"created_at"`
	UpdatedAt                  time.Time                    `json:"updated_at" db:"updated_at"`
}

type ErrAgentProfileNotFound struct {
	ID string
}

func (e *ErrAgentProfileNotFound) Error() string {
	return "agent profile not found: " + e.ID
}

type ErrAgentTaskEnvelopeNotFound struct {
	ID string
}

func (e *ErrAgentTaskEnvelopeNotFound) Error() string {
	return "agent task envelope not found: " + e.ID
}

type ErrAgentRunNotFound struct {
	ID string
}

func (e *ErrAgentRunNotFound) Error() string {
	return "agent run not found: " + e.ID
}

type ErrAgentRunAttemptNotFound struct {
	ID string
}

func (e *ErrAgentRunAttemptNotFound) Error() string {
	return "agent run attempt not found: " + e.ID
}
