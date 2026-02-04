package types

import "time"

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

// TaskResources specifies resource requirements for a task
type TaskResources struct {
	// CPU in millicores (e.g., 1000 = 1 CPU)
	CPU int64 `json:"cpu"`

	// Memory in bytes
	Memory int64 `json:"memory"`

	// GPU count (0 = no GPU)
	GPU int `json:"gpu"`
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
