package types

// DeferredToolExecutionRequest carries the context needed to execute an
// approval-gated tool call server-side and persist any resulting task output
// back onto the originating task/run.
type DeferredToolExecutionRequest struct {
	Task        *AgentTask
	WorkspaceID uint
	MemberID    uint
	ToolName    string
	Args        []string
}

func (r DeferredToolExecutionRequest) EffectiveWorkspaceID() uint {
	if r.Task != nil && r.Task.WorkspaceID != 0 {
		return r.Task.WorkspaceID
	}
	return r.WorkspaceID
}
