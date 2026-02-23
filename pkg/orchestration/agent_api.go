package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

// AgentAPI is the shared application layer for agent/task/run flows.
// Transport layers (gRPC/HTTP) should call this directly.
type AgentAPI struct {
	backend repository.BackendRepository
	runtime *AgentService
}

func NewAgentAPI(
	backend repository.BackendRepository,
	runtime *AgentService,
) *AgentAPI {
	return &AgentAPI{
		backend: backend,
		runtime: runtime,
	}
}

func (a *AgentAPI) CreateAgent(
	ctx context.Context,
	workspaceID uint,
	agentKey string,
	name string,
	config map[string]any,
	active *bool,
) (*types.AgentProfile, error) {
	if workspaceID == 0 {
		return nil, fmt.Errorf("workspace_id is required")
	}
	if strings.TrimSpace(agentKey) == "" {
		return nil, fmt.Errorf("agent_key is required")
	}
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("name is required")
	}

	isActive := true
	if active != nil {
		isActive = *active
	}

	profile := &types.AgentProfile{
		WorkspaceID: workspaceID,
		AgentKey:    strings.TrimSpace(agentKey),
		Name:        strings.TrimSpace(name),
		ConfigJSON:  config,
		Active:      isActive,
	}
	if err := a.backend.CreateAgentProfile(ctx, profile); err != nil {
		return nil, err
	}
	return profile, nil
}

func (a *AgentAPI) ListAgents(ctx context.Context, workspaceID uint) ([]*types.AgentProfile, error) {
	return a.backend.ListAgentProfiles(ctx, workspaceID)
}

func (a *AgentAPI) GetAgent(ctx context.Context, workspaceID uint, agentID string) (*types.AgentProfile, error) {
	return a.backend.GetAgentProfile(ctx, workspaceID, agentID)
}

func (a *AgentAPI) AcceptAgentCommand(
	ctx context.Context,
	workspaceID uint,
	params AgentCommandParams,
) (*types.AgentTask, bool, error) {
	if a.runtime == nil {
		return nil, false, fmt.Errorf("task service unavailable")
	}
	return a.runtime.AcceptAgentCommand(ctx, workspaceID, params)
}

func (a *AgentAPI) GetTask(ctx context.Context, workspaceID uint, taskID string) (*types.AgentTask, error) {
	return a.backend.GetTask(ctx, workspaceID, taskID)
}

func (a *AgentAPI) ListTasks(ctx context.Context, workspaceID uint, limit int) ([]*types.AgentTask, error) {
	if limit <= 0 {
		limit = 100
	}
	return a.backend.ListTasks(ctx, workspaceID, limit)
}

func (a *AgentAPI) EnqueueRunInput(
	ctx context.Context,
	workspaceID uint,
	runID string,
	queueMode types.AgentQueueMode,
	message string,
	idempotencyKey string,
) (*types.AgentTask, bool, error) {
	if a.runtime == nil {
		return nil, false, fmt.Errorf("task service unavailable")
	}
	if strings.TrimSpace(runID) == "" {
		return nil, false, fmt.Errorf("run_id is required")
	}
	idempotencyKey = normalizeGeneratedID(idempotencyKey)
	if queueMode == "" {
		queueMode = types.AgentQueueModeFollowup
	}
	queueMode = normalizeRunInputQueueMode(queueMode)
	if err := validateRunInputQueueMode(queueMode); err != nil {
		return nil, false, err
	}
	return a.runtime.AcceptRunInput(ctx, workspaceID, runID, queueMode, message, idempotencyKey)
}

func normalizeRunInputQueueMode(mode types.AgentQueueMode) types.AgentQueueMode {
	if mode == types.AgentQueueModeSteerBacklog {
		return types.AgentQueueModeSteer
	}
	return mode
}

func validateRunInputQueueMode(mode types.AgentQueueMode) error {
	switch mode {
	case types.AgentQueueModeQueue,
		types.AgentQueueModeFollowup,
		types.AgentQueueModeSteer,
		types.AgentQueueModeSteerBacklog,
		types.AgentQueueModeInterrupt:
		return nil
	default:
		return fmt.Errorf("queue_mode %q is not supported (supported: queue, followup, steer, steer-backlog, interrupt)", mode)
	}
}

func (a *AgentAPI) ListRuns(ctx context.Context, workspaceID uint, limit int) ([]*types.AgentRun, error) {
	if limit <= 0 {
		limit = 100
	}
	return a.backend.ListAgentRuns(ctx, workspaceID, limit)
}

func (a *AgentAPI) GetRun(ctx context.Context, workspaceID uint, runID string) (*types.AgentRun, error) {
	return a.backend.GetAgentRun(ctx, workspaceID, runID)
}

func (a *AgentAPI) ListRunSnapshots(ctx context.Context, workspaceID uint, runID string, limit int) ([]*types.AgentRunSnapshot, error) {
	if _, err := a.GetRun(ctx, workspaceID, runID); err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 500
	}
	return a.backend.ListAgentRunSnapshots(ctx, runID, limit)
}

func (a *AgentAPI) ListRunEvents(ctx context.Context, workspaceID uint, runID string) ([]map[string]any, error) {
	if _, err := a.GetRun(ctx, workspaceID, runID); err != nil {
		return nil, err
	}
	if a.runtime == nil || a.runtime.orchestrationStore == nil {
		return []map[string]any{}, nil
	}
	rows, err := a.runtime.orchestrationStore.ListRunEvents(ctx, runID)
	if err != nil {
		return nil, err
	}
	return decodeRunEvents(rows), nil
}

func (a *AgentAPI) CancelRun(ctx context.Context, workspaceID uint, runID string) error {
	run, err := a.GetRun(ctx, workspaceID, runID)
	if err != nil {
		return err
	}

	now := time.Now()
	errMsg := "cancelled by user"
	if err := a.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg); err != nil {
		return err
	}

	attempts, _ := a.backend.ListAgentRunAttempts(ctx, run.ID)
	for _, attempt := range attempts {
		if attempt.ExecutionID != nil && attempt.Status.IsInFlight() {
			_ = a.backend.CancelRunExecution(ctx, *attempt.ExecutionID)
		}
	}
	return nil
}

func (a *AgentAPI) CancelTask(ctx context.Context, workspaceID uint, taskID string) error {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return err
	}

	if task.TargetRunID != nil {
		if err := a.CancelRun(ctx, workspaceID, *task.TargetRunID); err != nil {
			return err
		}
	}

	if task.State == types.AgentTaskStateAccepted ||
		task.State == types.AgentTaskStateQueued ||
		task.State == types.AgentTaskStateDispatched {
		if err := a.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateCancelled, nil, task.TargetRunID); err != nil {
			return err
		}
	}

	return nil
}

func (a *AgentAPI) GetTaskLogs(ctx context.Context, workspaceID uint, taskID string) ([]common.TaskLogEntry, error) {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}

	if a.runtime == nil || a.runtime.s2 == nil || !a.runtime.s2.Enabled() || task.TargetRunID == nil {
		return []common.TaskLogEntry{}, nil
	}

	attempts, err := a.backend.ListAgentRunAttempts(ctx, *task.TargetRunID)
	if err != nil {
		return nil, err
	}
	executionID := newestExecutionID(attempts)
	if executionID == "" {
		return []common.TaskLogEntry{}, nil
	}

	logs, _, err := a.runtime.s2.ReadLogs(ctx, executionID, 0)
	if err != nil {
		return nil, err
	}
	return logs, nil
}

func newestExecutionID(attempts []*types.AgentRunAttempt) string {
	for i := len(attempts) - 1; i >= 0; i-- {
		attempt := attempts[i]
		if attempt == nil || attempt.ExecutionID == nil {
			continue
		}

		executionID := strings.TrimSpace(*attempt.ExecutionID)
		if executionID != "" {
			return executionID
		}
	}

	return ""
}

func decodeRunEvents(rows []string) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		var event map[string]any
		if err := json.Unmarshal([]byte(row), &event); err == nil {
			out = append(out, event)
		}
	}
	return out
}
