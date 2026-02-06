package types

import (
	"fmt"
	"time"
)

// Task represents a unit of work to be executed in a sandbox
type Task struct {
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

	// Status is the current task status
	Status TaskStatus `json:"status" db:"status"`

	// Prompt is the Claude Code prompt (if this is a Claude Code task)
	Prompt string `json:"prompt,omitempty" db:"prompt"`

	// Image is the container image to use
	Image string `json:"image" db:"image"`

	// Entrypoint is the command to run
	Entrypoint []string `json:"entrypoint" db:"entrypoint"`

	// Env is environment variables for the task
	Env map[string]string `json:"env" db:"env"`

	// Resources specifies resource requirements (optional - uses defaults if nil)
	Resources *TaskResources `json:"resources,omitempty" db:"-"`

	// Hook-triggered task fields (nil/defaults for manual tasks)
	HookId      *uint `json:"hook_id,omitempty" db:"hook_id"`   // nil = manual, non-nil = hook-triggered
	Attempt     int   `json:"attempt" db:"attempt"`              // 1-based attempt number
	MaxAttempts int   `json:"max_attempts" db:"max_attempts"`    // default 1 (manual), 3 (hook)

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

// IsClaudeCodeTask returns true if this task has a prompt (Claude Code task)
func (t *Task) IsClaudeCodeTask() bool {
	return t.Prompt != ""
}

// IsTerminal returns true if the task is in a terminal state.
func (t *Task) IsTerminal() bool {
	return t.Status == TaskStatusComplete ||
		t.Status == TaskStatusFailed ||
		t.Status == TaskStatusCancelled
}

// ErrTaskNotFound is returned when a task cannot be found
type ErrTaskNotFound struct {
	ExternalId string
}

func (e *ErrTaskNotFound) Error() string {
	return "task not found: " + e.ExternalId
}

// TaskResources specifies resource requirements.
// Flow: API → Task.Resources → SandboxConfig.Resources → OCI spec limits
type TaskResources struct {
	CPU    int64 `json:"cpu"`    // millicores (1000 = 1 CPU)
	Memory int64 `json:"memory"` // bytes
	GPU    int   `json:"gpu"`    // count
}

// Default resource limits (applied when Task.Resources is nil)
const (
	DefaultTaskCPU    int64 = 2000    // 2 CPUs
	DefaultTaskMemory int64 = 2 << 30 // 2 GiB
)

// Maximum resource limits for validation
const (
	MaxTaskCPU    int64 = 32000      // 32 CPUs
	MaxTaskMemory int64 = 128 << 30  // 128 GiB
	MaxTaskGPU    int   = 8          // 8 GPUs
)

// Validate checks that resource values are within acceptable bounds.
// Returns an error describing the first invalid field found.
func (r *TaskResources) Validate() error {
	if r == nil {
		return nil // nil means use defaults
	}
	if r.CPU < 0 {
		return fmt.Errorf("cpu must be non-negative, got %d", r.CPU)
	}
	if r.CPU > MaxTaskCPU {
		return fmt.Errorf("cpu exceeds maximum of %d millicores, got %d", MaxTaskCPU, r.CPU)
	}
	if r.Memory < 0 {
		return fmt.Errorf("memory must be non-negative, got %d", r.Memory)
	}
	if r.Memory > MaxTaskMemory {
		return fmt.Errorf("memory exceeds maximum of %d bytes, got %d", MaxTaskMemory, r.Memory)
	}
	if r.GPU < 0 {
		return fmt.Errorf("gpu must be non-negative, got %d", r.GPU)
	}
	if r.GPU > MaxTaskGPU {
		return fmt.Errorf("gpu exceeds maximum of %d, got %d", MaxTaskGPU, r.GPU)
	}
	return nil
}

// GetResources returns resources with defaults applied.
func (t *Task) GetResources() TaskResources {
	if t.Resources != nil {
		return *t.Resources
	}
	return TaskResources{CPU: DefaultTaskCPU, Memory: DefaultTaskMemory}
}

// TaskStatus represents the current status of a task
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusScheduled TaskStatus = "scheduled"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusComplete  TaskStatus = "complete"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCancelled TaskStatus = "cancelled"
)

// TaskState represents the current state of a task
type TaskState struct {
	// ID is the task identifier
	ID string `json:"id"`

	// Status is the current status
	Status TaskStatus `json:"status"`

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

// TaskResult contains the result of a completed task
type TaskResult struct {
	// ID is the task identifier
	ID string `json:"id"`

	// ExitCode is the exit code of the task
	ExitCode int `json:"exit_code"`

	// Output is the stdout/stderr output (if captured)
	Output []byte `json:"output,omitempty"`

	// Error contains error message if failed
	Error string `json:"error,omitempty"`

	// Duration is how long the task ran
	Duration time.Duration `json:"duration"`
}
