package types

import "time"

type TaskBlockerKind string

const (
	TaskBlockerKindApproval TaskBlockerKind = "approval"
	TaskBlockerKindInput    TaskBlockerKind = "input"
)

type TaskBlockerStatus string

const (
	TaskBlockerStatusOpen       TaskBlockerStatus = "open"
	TaskBlockerStatusResolved   TaskBlockerStatus = "resolved"
	TaskBlockerStatusSuperseded TaskBlockerStatus = "superseded"
)

type TaskBlocker struct {
	ID             string            `json:"id" db:"id"`
	WorkspaceID    uint              `json:"workspace_id" db:"workspace_id"`
	TaskID         string            `json:"task_id" db:"task_id"`
	RunID          *string           `json:"run_id,omitempty" db:"run_id"`
	Kind           TaskBlockerKind   `json:"kind" db:"kind"`
	InputKind      InputKind         `json:"input_kind,omitempty" db:"input_kind"`
	Status         TaskBlockerStatus `json:"status" db:"status"`
	WaitGroupID    *string           `json:"wait_group_id,omitempty" db:"wait_group_id"`
	PayloadJSON    map[string]any    `json:"payload_json,omitempty" db:"-"`
	ResolutionJSON map[string]any    `json:"resolution_json,omitempty" db:"-"`
	OutputIDs      []string          `json:"output_ids,omitempty" db:"-"`
	Revision       int               `json:"revision" db:"revision"`
	CreatedAt      time.Time         `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time         `json:"updated_at" db:"updated_at"`
	ResolvedAt     *time.Time        `json:"resolved_at,omitempty" db:"resolved_at"`
}

type TaskBlockerSpec struct {
	Kind          TaskBlockerKind
	InputKind     InputKind
	WaitGroupID   *string
	PayloadJSON   map[string]any
	OutputIDs     []string
}

type TaskBlockerResolution struct {
	Status         TaskBlockerStatus
	ResolutionJSON map[string]any
}

type TaskLiveUpdate struct {
	TaskID  string
	RunID   string
	State   AgentTaskState
	Blocker *TaskBlockerSpec
}

type TaskStateUpdate struct {
	TaskID        string
	State         AgentTaskState
	DroppedReason *string
	TargetRunID   *string
}

type CurrentRunTaskStateUpdate struct {
	TaskID        string
	ExpectedRunID string
	State         AgentTaskState
	DroppedReason *string
	TargetRunID   *string
}

type TaskBlockerOpenRequest struct {
	WorkspaceID   uint
	TaskID        string
	ExpectedRunID string
	Blocker       *TaskBlockerSpec
}

func TaskBlockerKindForInputKind(kind InputKind) TaskBlockerKind {
	switch kind {
	case InputKindApproveReject:
		return TaskBlockerKindApproval
	default:
		return TaskBlockerKindInput
	}
}

func (b *TaskBlocker) ApprovalSurface() bool {
	return b != nil && (b.Kind == TaskBlockerKindApproval || b.InputKind == InputKindApproveReject)
}

type ErrTaskBlockerNotFound struct {
	ID string
}

func (e *ErrTaskBlockerNotFound) Error() string {
	return "task blocker not found: " + e.ID
}
