package types

import (
	"fmt"
	"time"
)

// RunExecution represents a unit of work to be executed in a sandbox (substrate run-attempt execution).
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

// IsClaudeCodeRunExecution returns true if this run execution has a prompt (Claude Code task)
func (t *RunExecution) IsClaudeCodeRunExecution() bool {
	return t.Prompt != ""
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
// Flow: API → RunExecution.Resources → SandboxConfig.Resources → OCI spec limits
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
