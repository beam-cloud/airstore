package types

import (
	"fmt"
	"time"
)

type AgentTaskKind string

const (
	AgentTaskKindAgentCommand AgentTaskKind = "agent_command"
	AgentTaskKindRunInput     AgentTaskKind = "run_input"
)

type AgentQueueMode string

const (
	AgentQueueModeSteer        AgentQueueMode = "steer"
	AgentQueueModeSteerBacklog AgentQueueMode = "steer-backlog"
	AgentQueueModeFollowup     AgentQueueMode = "followup"
	AgentQueueModeInterrupt    AgentQueueMode = "interrupt"
	AgentQueueModeQueue        AgentQueueMode = "queue"
)

type AgentTaskState string

const (
	AgentTaskStateQueued    AgentTaskState = "queued"
	AgentTaskStateRunning   AgentTaskState = "running"
	AgentTaskStateIdle      AgentTaskState = "idle"
	AgentTaskStateDone      AgentTaskState = "done"
	AgentTaskStateDropped   AgentTaskState = "dropped"
	AgentTaskStateCancelled AgentTaskState = "cancelled"
)

func (s AgentTaskState) IsDispatchable() bool {
	switch s {
	case AgentTaskStateQueued:
		return true
	default:
		return false
	}
}

func (s AgentTaskState) IsTerminal() bool {
	switch s {
	case AgentTaskStateDone, AgentTaskStateDropped, AgentTaskStateCancelled:
		return true
	default:
		return false
	}
}

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

type AgentRunEventType string

const (
	// External run-event stream events (redis/s2).
	AgentRunEventAccepted                AgentRunEventType = "accepted"
	AgentRunEventInterrupted             AgentRunEventType = "interrupted"
	AgentRunEventInputSteered            AgentRunEventType = "input_steered"
	AgentRunEventInputDispatched         AgentRunEventType = "input_dispatched"
	AgentRunEventSteerFallbackDispatched AgentRunEventType = "steer_fallback_dispatched"

	// Snapshot payload event markers.
	AgentRunEventStartRejectedTerminalRun AgentRunEventType = "start_rejected_terminal_run"
	AgentRunEventStarted                  AgentRunEventType = "started"
	AgentRunEventAttemptSuperseded        AgentRunEventType = "attempt_superseded"
	AgentRunEventRetryScheduled           AgentRunEventType = "retry_scheduled"
	AgentRunEventFinished                 AgentRunEventType = "finished"
)

const (
	AgentAttemptStrategyPrimary = "primary"
	AgentAttemptStrategyRetry   = "retry"
)

const (
	AgentTaskDropReasonInterruptMissingTarget = "interrupt_missing_target"
	AgentTaskDropReasonRunMaterializationFail = "run_materialization_failed"
	AgentTaskDropReasonRunInputMissingTarget  = "run_input_missing_target"
	AgentTaskDropReasonRunInputMissingMessage = "run_input_missing_message"
	AgentTaskDropReasonRunInputTerminalTarget = "run_input_terminal_target"
	AgentTaskDropReasonReshapedByQueueMode    = "reshaped_by_queue_mode"
)

const (
	AgentExecutionMetaKeyInstanceKey      = "instance_key"
	AgentExecutionMetaKeyRetry            = "retry"
	AgentExecutionMetaKeyRetryMaxAttempts = "retry_max_attempts"
	AgentExecutionMetaKeyRetryDelayMs     = "retry_delay_ms"
	AgentExecutionMetaKeyResources        = "resources"
	AgentExecutionMetaKeyRunID            = "run_id"
	AgentExecutionMetaKeyRunAttemptID     = "run_attempt_id"
	AgentExecutionMetaKeyOriginTaskID     = "origin_task_id"
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

// AgentTask is the high-level orchestration task (agent -> task -> run).
type AgentTask struct {
	ID             string         `json:"id" db:"id"`
	WorkspaceID    uint           `json:"workspace_id" db:"workspace_id"`
	AgentID        *string        `json:"agent_id,omitempty" db:"agent_id"`
	Kind           AgentTaskKind  `json:"kind" db:"kind"`
	QueueMode      AgentQueueMode `json:"queue_mode" db:"queue_mode"`
	State          AgentTaskState `json:"state" db:"state"`
	IdempotencyKey string         `json:"idempotency_key" db:"idempotency_key"`
	PayloadJSON    map[string]any `json:"payload_json" db:"-"`
	RoutingJSON    map[string]any `json:"routing_json" db:"-"`
	ParentTaskID   *string        `json:"parent_task_id,omitempty" db:"parent_envelope_id"`
	TargetRunID    *string        `json:"target_run_id,omitempty" db:"target_run_id"`
	AcceptedAt     time.Time      `json:"accepted_at" db:"accepted_at"`
	QueuedAt       *time.Time     `json:"queued_at,omitempty" db:"queued_at"`
	DispatchedAt   *time.Time     `json:"dispatched_at,omitempty" db:"dispatched_at"`
	DroppedReason  *string        `json:"dropped_reason,omitempty" db:"dropped_reason"`
	CreatedAt      time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at" db:"updated_at"`
}

type AgentRun struct {
	ID              string         `json:"id" db:"id"`
	WorkspaceID     uint           `json:"workspace_id" db:"workspace_id"`
	AgentID         *string        `json:"agent_id,omitempty" db:"agent_id"`
	OriginTaskID    string         `json:"origin_task_id" db:"origin_task_id"`
	Status          AgentRunStatus `json:"status" db:"status"`
	SessionID       string         `json:"session_id" db:"session_id"`
	SessionKey      *string        `json:"session_key,omitempty" db:"session_key"`
	Provider        *string        `json:"provider,omitempty" db:"provider"`
	Model           *string        `json:"model,omitempty" db:"model"`
	ExecHost        string         `json:"exec_host" db:"exec_host"`
	ExecSecurity    string         `json:"exec_security" db:"exec_security"`
	ExecAsk         string         `json:"exec_ask" db:"exec_ask"`
	RuntimeType     string         `json:"runtime_type" db:"runtime_type"`
	WorkspaceAccess string         `json:"workspace_access" db:"workspace_access"`
	NetworkEnabled  bool           `json:"network_enabled" db:"network_enabled"`
	Interactive     bool           `json:"interactive" db:"interactive"`
	TimeoutMs       int            `json:"timeout_ms" db:"timeout_ms"`
	StartedAt       *time.Time     `json:"started_at,omitempty" db:"started_at"`
	EndedAt         *time.Time     `json:"ended_at,omitempty" db:"ended_at"`
	Error           *string        `json:"error,omitempty" db:"error"`
	SnapshotTS      int64          `json:"snapshot_ts" db:"snapshot_ts"`
	UsageJSON       map[string]any `json:"usage_json" db:"-"`
	DeliveryJSON    map[string]any `json:"delivery_json" db:"-"`
	CreatedAt       time.Time      `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at" db:"updated_at"`
}

type AgentRunAttempt struct {
	ID              string             `json:"id" db:"id"`
	RunID           string             `json:"run_id" db:"run_id"`
	AttemptNo       int                `json:"attempt_no" db:"attempt_no"`
	Status          AgentAttemptStatus `json:"status" db:"status"`
	Strategy        string             `json:"strategy" db:"strategy"`
	Provider        *string            `json:"provider,omitempty" db:"provider"`
	Model           *string            `json:"model,omitempty" db:"model"`
	ExecHost        string             `json:"exec_host" db:"exec_host"`
	ExecSecurity    string             `json:"exec_security" db:"exec_security"`
	ExecAsk         string             `json:"exec_ask" db:"exec_ask"`
	RuntimeType     string             `json:"runtime_type" db:"runtime_type"`
	WorkspaceAccess string             `json:"workspace_access" db:"workspace_access"`
	NetworkEnabled  bool               `json:"network_enabled" db:"network_enabled"`
	Interactive     bool               `json:"interactive" db:"interactive"`
	ExecutionID     *string            `json:"execution_id,omitempty" db:"execution_task_external_id"`
	StartedAt       *time.Time         `json:"started_at,omitempty" db:"started_at"`
	EndedAt         *time.Time         `json:"ended_at,omitempty" db:"ended_at"`
	ExitCode        *int               `json:"exit_code,omitempty" db:"exit_code"`
	Error           *string            `json:"error,omitempty" db:"error"`
	CreatedAt       time.Time          `json:"created_at" db:"created_at"`
	UpdatedAt       time.Time          `json:"updated_at" db:"updated_at"`
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

type AgentTaskListFilter struct {
	AgentID       *string
	States        []AgentTaskState
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Limit         int
	Offset        int
}

type AgentRunListFilter struct {
	AgentID       *string
	Statuses      []AgentRunStatus
	SessionID     *string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	Limit         int
	Offset        int
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

// RunExecution represents a run worker payload.
type RunExecution struct {
	// Id is the internal ID for joins
	Id uint `json:"id" db:"id"`

	// ExternalId is the UUID exposed via API
	ExternalId string `json:"external_id" db:"external_id"`

	// WorkspaceId is the internal workspace ID (for joins)
	WorkspaceId uint `json:"workspace_id" db:"workspace_id"`

	// CreatedByMemberId is the member who created this task (for token auth)
	CreatedByMemberId *uint `json:"created_by_member_id,omitempty" db:"created_by_member_id"`

	// MemberToken is the workspace token to use for filesystem access
	// This is NOT stored in the database - it's set at creation time and passed to workers
	MemberToken string `json:"member_token,omitempty" db:"-"`

	// Status is the current run execution status
	Status RunExecutionStatus `json:"status" db:"status"`

	// Type controls run execution mode (background by default).
	Type RunExecutionType `json:"type" db:"type"`

	// Prompt is the Claude Code prompt (if this is a Claude Code task)
	Prompt string `json:"prompt,omitempty" db:"prompt"`

	// Image is the container image to use
	Image string `json:"image" db:"image"`

	// Entrypoint is the command to run
	Entrypoint []string `json:"entrypoint" db:"entrypoint"`

	// Env is environment variables for the task
	Env map[string]string `json:"env" db:"env"`

	// Resources specifies resource requirements (optional - uses defaults if nil)
	Resources *RunExecutionResources `json:"resources,omitempty" db:"-"`

	// RunAttemptID links this run execution to an orchestrated run attempt.
	RunAttemptID *string `json:"run_attempt_id,omitempty" db:"run_attempt_id"`

	// TimeoutMs applies a per-task execution timeout when set.
	TimeoutMs *int `json:"timeout_ms,omitempty" db:"timeout_ms"`

	// Execution policy fields bridged from run attempts.
	ExecHost        *string        `json:"exec_host,omitempty" db:"exec_host"`
	ExecSecurity    *string        `json:"exec_security,omitempty" db:"exec_security"`
	ExecAsk         *string        `json:"exec_ask,omitempty" db:"exec_ask"`
	RuntimeType     *string        `json:"runtime_type,omitempty" db:"runtime_type"`
	WorkspaceAccess *string        `json:"workspace_access,omitempty" db:"workspace_access"`
	NetworkEnabled  *bool          `json:"network_enabled,omitempty" db:"network_enabled"`
	ExecutionPolicy map[string]any `json:"execution_policy,omitempty" db:"-"`

	// Hook-triggered task fields (nil/defaults for manual tasks)
	HookId      *uint `json:"hook_id,omitempty" db:"hook_id"` // nil = manual, non-nil = hook-triggered
	Attempt     int   `json:"attempt" db:"attempt"`           // 1-based attempt number
	MaxAttempts int   `json:"max_attempts" db:"max_attempts"` // default 1 (manual), 3 (hook)

	// ExitCode is the exit code when complete
	ExitCode *int `json:"exit_code,omitempty" db:"exit_code"`

	// Error contains error message if failed
	Error string `json:"error,omitempty" db:"error"`

	// CreatedAt is when the task was created
	CreatedAt time.Time `json:"created_at" db:"created_at"`

	// StartedAt is when the task started running
	StartedAt *time.Time `json:"started_at,omitempty" db:"started_at"`

	// FinishedAt is when the task finished
	FinishedAt *time.Time `json:"finished_at,omitempty" db:"finished_at"`
}

// RunExecutionType represents how a run execution should execute.
type RunExecutionType string

const (
	RunExecutionTypeBackground  RunExecutionType = "background"
	RunExecutionTypeInteractive RunExecutionType = "interactive"
)

// NormalizeType applies the default run execution type when unset.
func (t *RunExecution) NormalizeType() {
	if t.Type == "" {
		t.Type = RunExecutionTypeBackground
	}
}

// IsInteractive returns true when the run execution should run in interactive mode.
func (t *RunExecution) IsInteractive() bool {
	t.NormalizeType()
	return t.Type == RunExecutionTypeInteractive
}

// IsTerminal returns true if the run execution is in a terminal state.
func (t *RunExecution) IsTerminal() bool {
	return t.Status == RunExecutionStatusComplete ||
		t.Status == RunExecutionStatusFailed ||
		t.Status == RunExecutionStatusCancelled
}

// ErrRunExecutionNotFound is returned when a run execution cannot be found
type ErrRunExecutionNotFound struct {
	ExternalId string
}

func (e *ErrRunExecutionNotFound) Error() string {
	return "run execution not found: " + e.ExternalId
}

// RunExecutionResources specifies resource requirements.
// Flow: API -> RunExecution.Resources -> SandboxConfig.Resources -> OCI spec limits
type RunExecutionResources struct {
	CPU    int64 `json:"cpu"`    // millicores (1000 = 1 CPU)
	Memory int64 `json:"memory"` // bytes
	GPU    int   `json:"gpu"`    // count
}

// Default resource limits (applied when RunExecution.Resources is nil)
const (
	DefaultRunExecutionCPU    int64 = 2000    // 2 CPUs
	DefaultRunExecutionMemory int64 = 2 << 30 // 2 GiB
)

// Maximum resource limits for validation
const (
	MaxRunExecutionCPU    int64 = 32000     // 32 CPUs
	MaxRunExecutionMemory int64 = 128 << 30 // 128 GiB
	MaxRunExecutionGPU    int   = 8         // 8 GPUs
)

// Validate checks that resource values are within acceptable bounds.
// Returns an error describing the first invalid field found.
func (r *RunExecutionResources) Validate() error {
	if r == nil {
		return nil // nil means use defaults
	}
	if r.CPU < 0 {
		return fmt.Errorf("cpu must be non-negative, got %d", r.CPU)
	}
	if r.CPU > MaxRunExecutionCPU {
		return fmt.Errorf("cpu exceeds maximum of %d millicores, got %d", MaxRunExecutionCPU, r.CPU)
	}
	if r.Memory < 0 {
		return fmt.Errorf("memory must be non-negative, got %d", r.Memory)
	}
	if r.Memory > MaxRunExecutionMemory {
		return fmt.Errorf("memory exceeds maximum of %d bytes, got %d", MaxRunExecutionMemory, r.Memory)
	}
	if r.GPU < 0 {
		return fmt.Errorf("gpu must be non-negative, got %d", r.GPU)
	}
	if r.GPU > MaxRunExecutionGPU {
		return fmt.Errorf("gpu exceeds maximum of %d, got %d", MaxRunExecutionGPU, r.GPU)
	}
	return nil
}

// GetResources returns resources with defaults applied.
func (t *RunExecution) GetResources() RunExecutionResources {
	if t.Resources != nil {
		return *t.Resources
	}
	return RunExecutionResources{CPU: DefaultRunExecutionCPU, Memory: DefaultRunExecutionMemory}
}

// RunExecutionStatus represents the current status of a run execution
type RunExecutionStatus string

const (
	RunExecutionStatusPending   RunExecutionStatus = "pending"
	RunExecutionStatusScheduled RunExecutionStatus = "scheduled"
	RunExecutionStatusRunning   RunExecutionStatus = "running"
	RunExecutionStatusComplete  RunExecutionStatus = "complete"
	RunExecutionStatusFailed    RunExecutionStatus = "failed"
	RunExecutionStatusCancelled RunExecutionStatus = "cancelled"
)

// RunExecutionState represents the current state of a run execution
type RunExecutionState struct {
	// ID is the run execution identifier
	ID string `json:"id"`

	// Status is the current status
	Status RunExecutionStatus `json:"status"`

	// SandboxID is the sandbox running this task (empty if not yet scheduled)
	SandboxID string `json:"sandbox_id,omitempty"`

	// WorkerID is the worker running this task (empty if not yet scheduled)
	WorkerID string `json:"worker_id,omitempty"`

	// ExitCode is the exit code if complete (-1 if still running)
	ExitCode int `json:"exit_code"`

	// Error contains error message if failed
	Error string `json:"error,omitempty"`

	// CreatedAt is when the task was created
	CreatedAt time.Time `json:"created_at"`

	// ScheduledAt is when the task was scheduled
	ScheduledAt time.Time `json:"scheduled_at,omitempty"`

	// StartedAt is when the task started running
	StartedAt time.Time `json:"started_at,omitempty"`

	// FinishedAt is when the task finished
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// RunExecutionResult contains the result of a completed run execution
type RunExecutionResult struct {
	// ID is the run execution identifier
	ID string `json:"id"`

	// ExitCode is the exit code of the run execution
	ExitCode int `json:"exit_code"`

	// Output is the stdout/stderr output (if captured)
	Output []byte `json:"output,omitempty"`

	// Error contains error message if failed
	Error string `json:"error,omitempty"`

	// Duration is how long the task ran
	Duration time.Duration `json:"duration"`
}

type ErrAgentProfileNotFound struct {
	ID string
}

func (e *ErrAgentProfileNotFound) Error() string {
	return "agent profile not found: " + e.ID
}

type ErrAgentTaskNotFound struct {
	ID string
}

func (e *ErrAgentTaskNotFound) Error() string {
	return "agent task not found: " + e.ID
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
