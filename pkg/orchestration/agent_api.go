package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

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

type TaskEventBatch struct {
	TaskID             string                `json:"task_id"`
	RunID              *string               `json:"run_id,omitempty"`
	Task               *types.AgentTask      `json:"task,omitempty"`
	Run                *types.AgentRun       `json:"run,omitempty"`
	Interaction        *types.RunInteraction `json:"interaction,omitempty"`
	Logs               []common.TaskLogEntry `json:"logs"`
	RunEvents          []map[string]any      `json:"run_events"`
	Outputs            []*types.TaskOutput   `json:"outputs,omitempty"`
	NextLogCursor      int64                 `json:"next_log_cursor"`
	NextRunEventCursor int                   `json:"next_run_event_cursor"`
}

type WorkspaceLiveBatch struct {
	Tasks   []*types.AgentTask  `json:"tasks"`
	Outputs []*types.TaskOutput `json:"outputs"`
}

const (
	defaultWorkspaceStreamTaskLimit   = 500
	defaultWorkspaceStreamOutputLimit = 60
)

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

	trimmedKey := strings.TrimSpace(agentKey)
	normalizedConfig, err := normalizeAgentProfileConfig(config, trimmedKey)
	if err != nil {
		return nil, err
	}

	profile := &types.AgentProfile{
		WorkspaceID: workspaceID,
		AgentKey:    trimmedKey,
		Name:        strings.TrimSpace(name),
		Role:        "generalist",
		MemoryScope: "workspace",
		ConfigJSON:  normalizedConfig,
		Active:      isActive,
	}
	if err := a.backend.CreateAgentProfile(ctx, profile); err != nil {
		return nil, err
	}
	return profile, nil
}

func (a *AgentAPI) GetDefaultConfig(agentKey string) map[string]any {
	return DefaultAgentConfig(agentKey)
}

func (a *AgentAPI) ListAgents(ctx context.Context, workspaceID uint) ([]*types.AgentProfile, error) {
	return a.backend.ListAgentProfiles(ctx, workspaceID)
}

func (a *AgentAPI) GetAgent(ctx context.Context, workspaceID uint, agentID string) (*types.AgentProfile, error) {
	return a.backend.GetAgentProfile(ctx, workspaceID, agentID)
}

func (a *AgentAPI) UpdateAgent(
	ctx context.Context,
	workspaceID uint,
	agentID string,
	name *string,
	role *string,
	memoryScope *string,
	qualityScore *float64,
	costBudgetUSD *float64,
	config map[string]any,
	active *bool,
) (*types.AgentProfile, error) {
	profile, err := a.backend.GetAgentProfile(ctx, workspaceID, agentID)
	if err != nil {
		return nil, err
	}
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed == "" {
			return nil, fmt.Errorf("name cannot be empty")
		}
		profile.Name = trimmed
	}
	if role != nil {
		profile.Role = optionalStringValue(role, "generalist")
	}
	if memoryScope != nil {
		profile.MemoryScope = optionalStringValue(memoryScope, "workspace")
	}
	if qualityScore != nil {
		profile.QualityScore = qualityScore
	}
	if costBudgetUSD != nil {
		profile.CostBudgetUSD = costBudgetUSD
	}
	if active != nil {
		profile.Active = *active
	}
	if config != nil {
		merged := cloneAnyMap(profile.ConfigJSON)
		for k, v := range config {
			merged[k] = v
		}
		normalized, err := normalizeAgentProfileConfig(merged, profile.AgentKey)
		if err != nil {
			return nil, err
		}
		profile.ConfigJSON = normalized
	}
	if err := a.backend.UpdateAgentProfile(ctx, profile); err != nil {
		return nil, err
	}
	return profile, nil
}

func (a *AgentAPI) DeleteAgent(ctx context.Context, workspaceID uint, agentID string) error {
	if workspaceID == 0 {
		return fmt.Errorf("workspace_id is required")
	}
	trimmedAgentID := strings.TrimSpace(agentID)
	if trimmedAgentID == "" {
		return fmt.Errorf("agent_id is required")
	}
	if err := a.backend.DeleteScheduledTasksByAgent(ctx, workspaceID, trimmedAgentID); err != nil {
		return fmt.Errorf("delete agent schedules: %w", err)
	}
	return a.backend.DeleteAgentProfile(ctx, workspaceID, trimmedAgentID)
}

// --- Workspace ---

func (a *AgentAPI) GetWorkspace(ctx context.Context, workspaceID uint) (*types.Workspace, error) {
	return a.backend.GetWorkspace(ctx, workspaceID)
}

// --- Channel Bindings ---

func (a *AgentAPI) ListChannelBindings(ctx context.Context, workspaceID uint, agentID *string) ([]*types.ChannelBinding, error) {
	return a.backend.ListChannelBindings(ctx, workspaceID, agentID)
}

func (a *AgentAPI) UpsertChannelBinding(ctx context.Context, binding *types.ChannelBinding) error {
	return a.backend.UpsertChannelBinding(ctx, binding)
}

func (a *AgentAPI) DeleteChannelBinding(ctx context.Context, workspaceID uint, agentID *string, channelType string) error {
	return a.backend.DeleteChannelBinding(ctx, workspaceID, agentID, channelType)
}

// --- Stats ---

func (a *AgentAPI) GetAgentStats(ctx context.Context, workspaceID uint, agentID string) (*types.AgentStats, error) {
	return a.backend.GetAgentStats(ctx, workspaceID, agentID)
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
	task, err := a.backend.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}
	return sanitizeTaskForResponse(task), nil
}

func (a *AgentAPI) ListTasks(ctx context.Context, workspaceID uint, limit int) ([]*types.AgentTask, error) {
	if limit <= 0 {
		limit = 100
	}
	tasks, err := a.backend.ListTasks(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	return sanitizeTasksForResponse(tasks), nil
}

func (a *AgentAPI) ListTasksFiltered(
	ctx context.Context,
	workspaceID uint,
	filter types.AgentTaskListFilter,
) ([]*types.AgentTask, string, bool, error) {
	limit, offset := normalizeOffsetPage(filter.Limit, filter.Offset, 50, 500)
	filter.Limit = limit + 1
	filter.Offset = offset

	tasks, err := a.backend.ListTasksFiltered(ctx, workspaceID, filter)
	if err != nil {
		return nil, "", false, err
	}
	hasMore := len(tasks) > limit
	if hasMore {
		tasks = tasks[:limit]
	}
	return sanitizeTasksForResponse(tasks), nextOffsetCursor(offset, limit, hasMore), hasMore, nil
}

func (a *AgentAPI) SubmitTaskInput(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	kind types.InputKind,
	action *types.TaskInputAction,
	message string,
	idempotencyKey string,
) (*types.AgentTask, error) {
	if a.runtime == nil {
		return nil, fmt.Errorf("task service unavailable")
	}
	if strings.TrimSpace(taskID) == "" {
		return nil, fmt.Errorf("task_id is required")
	}
	return a.runtime.AcceptTaskInput(ctx, workspaceID, taskID, kind, action, message, idempotencyKey)
}

func (a *AgentAPI) EnqueueRunInput(
	ctx context.Context,
	workspaceID uint,
	runID string,
	queueMode types.AgentQueueMode,
	message string,
	idempotencyKey string,
) (*types.AgentTask, bool, types.RunInputDeliveryOutcome, error) {
	if a.runtime == nil {
		return nil, false, "", fmt.Errorf("task service unavailable")
	}
	if strings.TrimSpace(runID) == "" {
		return nil, false, "", fmt.Errorf("run_id is required")
	}
	idempotencyKey = normalizeGeneratedID(idempotencyKey)
	queueMode = types.NormalizeRunInputQueueMode(queueMode)
	if err := types.ValidateRunInputQueueMode(queueMode); err != nil {
		return nil, false, "", err
	}
	return a.runtime.AcceptRunInput(ctx, workspaceID, runID, queueMode, message, idempotencyKey)
}

func (a *AgentAPI) ListRuns(ctx context.Context, workspaceID uint, limit int) ([]*types.AgentRun, error) {
	if limit <= 0 {
		limit = 100
	}
	runs, err := a.backend.ListAgentRuns(ctx, workspaceID, limit)
	if err != nil {
		return nil, err
	}
	return sanitizeRunsForResponse(runs), nil
}

func (a *AgentAPI) ListRunsFiltered(
	ctx context.Context,
	workspaceID uint,
	filter types.AgentRunListFilter,
) ([]*types.AgentRun, string, bool, error) {
	limit, offset := normalizeOffsetPage(filter.Limit, filter.Offset, 50, 200)
	filter.Limit = limit + 1
	filter.Offset = offset

	runs, err := a.backend.ListAgentRunsFiltered(ctx, workspaceID, filter)
	if err != nil {
		return nil, "", false, err
	}
	hasMore := len(runs) > limit
	if hasMore {
		runs = runs[:limit]
	}
	return sanitizeRunsForResponse(runs), nextOffsetCursor(offset, limit, hasMore), hasMore, nil
}

func (a *AgentAPI) GetRun(ctx context.Context, workspaceID uint, runID string) (*types.AgentRun, error) {
	run, err := a.backend.GetAgentRun(ctx, workspaceID, runID)
	if err != nil {
		return nil, err
	}
	return sanitizeRunForResponse(run), nil
}

func (a *AgentAPI) GetRunInteraction(ctx context.Context, workspaceID uint, runID string) (*types.RunInteraction, error) {
	if a.runtime == nil {
		return nil, nil
	}
	interaction, err := a.runtime.GetRunInteraction(ctx, workspaceID, runID)
	if err != nil || interaction == nil {
		return nil, err
	}
	return interaction, nil
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
	return common.RedactSensitiveMaps(decodeRunEvents(rows)), nil
}

func (a *AgentAPI) CancelRun(ctx context.Context, workspaceID uint, runID string) error {
	run, err := a.GetRun(ctx, workspaceID, runID)
	if err != nil {
		return err
	}

	// Cancel active execution(s) before marking the run terminal so the worker
	// still sees an in-flight execution and receives an immediate cancel signal.
	cancelled := false
	if a.runtime != nil && a.runtime.backend != nil {
		var cancelErr error
		cancelled, cancelErr = a.runtime.cancelInFlightRunExecutions(ctx, run.ID)
		if cancelErr != nil {
			cancelled = false
		}
	}

	if !cancelled {
		attempts, _ := a.backend.ListAgentRunAttempts(ctx, run.ID)
		for _, attempt := range attempts {
			if attempt == nil || attempt.ExecutionID == nil {
				continue
			}
			executionID := strings.TrimSpace(*attempt.ExecutionID)
			if executionID == "" {
				continue
			}
			_ = a.backend.CancelRunExecution(ctx, executionID)
			if a.runtime != nil && a.runtime.terminalIO != nil {
				_ = a.runtime.terminalIO.PublishCancel(ctx, executionID)
			}
		}
	}

	now := time.Now()
	errMsg := "cancelled by user"
	if err := a.backend.UpdateAgentRunLifecycle(ctx, run.ID, types.AgentRunStatusCancelled, nil, &now, &errMsg); err != nil {
		return err
	}

	return nil
}

func (a *AgentAPI) CancelTask(ctx context.Context, workspaceID uint, taskID string) error {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return err
	}
	if task.State.IsTerminal() {
		return &types.ErrTaskNotCancellable{ID: taskID, State: task.State}
	}

	if task.TargetRunID != nil && task.State == types.AgentTaskStateRunning {
		if err := a.CancelRun(ctx, workspaceID, *task.TargetRunID); err != nil {
			return err
		}
	}

	if err := a.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateCancelled, nil, task.TargetRunID); err != nil {
		return err
	}

	if err := a.backend.CancelPendingOutboxEventsForTask(ctx, task.ID); err != nil {
		log.Warn().Err(err).Str("task_id", task.ID).Msg("failed to cancel pending outbox events")
	}
	if a.runtime != nil {
		a.runtime.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	}

	return nil
}

func (a *AgentAPI) ArchiveTask(ctx context.Context, workspaceID uint, taskID string) error {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return err
	}
	if task.ArchivedAt != nil {
		return nil
	}
	if !task.State.IsTerminal() {
		if err := a.CancelTask(ctx, workspaceID, taskID); err != nil {
			if _, ok := err.(*types.ErrTaskNotCancellable); !ok {
				return err
			}
		}
		task, err = a.GetTask(ctx, workspaceID, taskID)
		if err != nil {
			return err
		}
		if task.ArchivedAt != nil {
			return nil
		}
		if !task.State.IsTerminal() {
			return &types.ErrTaskNotArchivable{ID: taskID, State: task.State}
		}
	}
	if err := a.backend.ArchiveTask(ctx, task.ID); err != nil {
		return err
	}
	if a.runtime != nil {
		a.runtime.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	}
	return nil
}

type TaskUpdateParams struct {
	Priority    *string        `json:"priority,omitempty"`
	BudgetUSD   *float64       `json:"budget_usd,omitempty"`
	PayloadJSON map[string]any `json:"payload_json,omitempty"`
	RoutingJSON map[string]any `json:"routing_json,omitempty"`
}

func (a *AgentAPI) UpdateTask(ctx context.Context, workspaceID uint, taskID string, params TaskUpdateParams) (*types.AgentTask, error) {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}

	if params.Priority != nil {
		p := strings.TrimSpace(*params.Priority)
		if p != "" && !types.AgentTaskPriority(p).IsValid() {
			return nil, fmt.Errorf("priority %q is not supported", p)
		}
		task.Priority = p
	}
	if params.BudgetUSD != nil {
		task.BudgetUSD = params.BudgetUSD
	}
	if params.PayloadJSON != nil {
		task.PayloadJSON = params.PayloadJSON
	}
	if params.RoutingJSON != nil {
		task.RoutingJSON = params.RoutingJSON
	}

	if err := a.backend.UpdateTask(ctx, task); err != nil {
		return nil, err
	}
	if a.runtime != nil {
		a.runtime.publishTaskUpdate(ctx, task.WorkspaceID, task.ID)
	}
	return sanitizeTaskForResponse(task), nil
}

func (a *AgentAPI) GetTaskLogs(ctx context.Context, workspaceID uint, taskID string) ([]common.TaskLogEntry, error) {
	logs, _, err := a.ListTaskLogs(ctx, workspaceID, taskID, 0)
	return logs, err
}

func (a *AgentAPI) ListTaskLogs(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	seqNum int64,
) ([]common.TaskLogEntry, int64, error) {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, seqNum, err
	}

	if a.runtime == nil || a.runtime.s2 == nil || !a.runtime.s2.Enabled() {
		return []common.TaskLogEntry{}, seqNum, nil
	}

	currentRunID := ""
	if task.TargetRunID != nil {
		currentRunID = strings.TrimSpace(*task.TargetRunID)
	}

	// When TargetRunID is nil (task is between runs after requeue or input),
	// fall back to the most recent run so we can still load history from s2.
	if currentRunID == "" {
		runs, runErr := a.backend.ListAgentRunsFiltered(ctx, workspaceID, types.AgentRunListFilter{
			TaskID: &taskID,
			Limit:  1,
		})
		if runErr != nil || len(runs) == 0 || runs[0] == nil {
			return []common.TaskLogEntry{}, seqNum, nil
		}
		currentRunID = runs[0].ID
	}
	if currentRunID == "" {
		return []common.TaskLogEntry{}, seqNum, nil
	}

	// Non-zero cursor means incremental polling for the currently bound run.
	if seqNum > 0 {
		logs, nextCursor, err := a.listTaskLogsForRun(ctx, currentRunID, seqNum)
		if err != nil {
			return nil, seqNum, err
		}
		// When the cursor rewinds (execution changed within the same run),
		// replay the full session history so the frontend gets all prior runs
		// instead of just the new execution's partial log stream.
		if nextCursor > 0 && nextCursor < seqNum {
			history, histNext, histErr := a.listTaskSessionHistoryLogs(ctx, workspaceID, task, currentRunID)
			if histErr == nil {
				history = prependTaskPromptLog(task, history)
				return common.RedactTaskLogEntries(history), histNext, nil
			}
		}
		return logs, nextCursor, nil
	}

	// Cursor zero means "hydrate history". Return logs across all runs of this
	// task session so resumed runs show the full timeline by default.
	history, nextSeq, err := a.listTaskSessionHistoryLogs(ctx, workspaceID, task, currentRunID)
	if err != nil {
		return nil, seqNum, err
	}
	history = prependTaskPromptLog(task, history)
	return common.RedactTaskLogEntries(history), nextSeq, nil
}

func (a *AgentAPI) listTaskLogsForRun(
	ctx context.Context,
	runID string,
	seqNum int64,
) ([]common.TaskLogEntry, int64, error) {
	attempts, err := a.backend.ListAgentRunAttempts(ctx, runID)
	if err != nil {
		return nil, seqNum, err
	}
	executionID := newestExecutionID(attempts)
	if executionID == "" {
		return []common.TaskLogEntry{}, seqNum, nil
	}

	logs, nextSeqNum, err := a.runtime.s2.ReadLogs(ctx, executionID, seqNum)
	if err != nil {
		return nil, seqNum, err
	}

	// Cursor is tracked client-side but log streams are per execution.
	// When a run starts a new execution (interrupt/retry), a cursor from the
	// previous execution can point past the new stream's end forever.
	// Detect that condition and recover by replaying from seq=0 once.
	if len(logs) == 0 && seqNum > 0 {
		fullLogs, fullNextSeqNum, fullErr := a.runtime.s2.ReadLogs(ctx, executionID, 0)
		if fullErr != nil {
			return nil, seqNum, fullErr
		}
		if shouldResetTaskLogCursor(seqNum, fullNextSeqNum) {
			return common.RedactTaskLogEntries(fullLogs), fullNextSeqNum, nil
		}
	}
	return common.RedactTaskLogEntries(logs), nextSeqNum, nil
}

func shouldResetTaskLogCursor(cursor int64, streamNextCursor int64) bool {
	return cursor > 0 && streamNextCursor > 0 && cursor > streamNextCursor
}

func (a *AgentAPI) listTaskSessionHistoryLogs(
	ctx context.Context,
	workspaceID uint,
	task *types.AgentTask,
	currentRunID string,
) ([]common.TaskLogEntry, int64, error) {
	currentRun, err := a.GetRun(ctx, workspaceID, currentRunID)
	if err != nil || currentRun == nil {
		return a.listTaskLogsForRun(ctx, currentRunID, 0)
	}

	sessionID := strings.TrimSpace(currentRun.SessionID)
	if sessionID == "" {
		return a.listTaskLogsForRun(ctx, currentRunID, 0)
	}

	filter := types.AgentRunListFilter{
		AgentID:   task.AgentID,
		SessionID: &sessionID,
		Limit:     500,
		Offset:    0,
	}
	runs, err := a.backend.ListAgentRunsFiltered(ctx, workspaceID, filter)
	if err != nil {
		return a.listTaskLogsForRun(ctx, currentRunID, 0)
	}

	taskRuns := make([]*types.AgentRun, 0, len(runs)+1)
	seenRunIDs := map[string]struct{}{}
	for _, run := range runs {
		if run == nil {
			continue
		}
		if strings.TrimSpace(run.OriginTaskID) != task.ID {
			continue
		}
		runID := strings.TrimSpace(run.ID)
		if runID == "" {
			continue
		}
		if _, exists := seenRunIDs[runID]; exists {
			continue
		}
		seenRunIDs[runID] = struct{}{}
		taskRuns = append(taskRuns, run)
	}
	if _, exists := seenRunIDs[currentRunID]; !exists {
		taskRuns = append(taskRuns, currentRun)
	}

	sort.Slice(taskRuns, func(i, j int) bool {
		left := taskRuns[i]
		right := taskRuns[j]
		if left == nil || right == nil {
			return left != nil
		}
		if left.CreatedAt.Equal(right.CreatedAt) {
			return strings.TrimSpace(left.ID) < strings.TrimSpace(right.ID)
		}
		return left.CreatedAt.Before(right.CreatedAt)
	})

	history := make([]common.TaskLogEntry, 0)
	seenExecutionIDs := map[string]struct{}{}
	currentNextSeq := int64(0)
	for _, run := range taskRuns {
		if run == nil {
			continue
		}
		runID := strings.TrimSpace(run.ID)
		if runID == "" {
			continue
		}
		attempts, err := a.backend.ListAgentRunAttempts(ctx, runID)
		if err != nil {
			continue
		}
		executionID := newestExecutionID(attempts)
		if executionID == "" {
			continue
		}
		if _, exists := seenExecutionIDs[executionID]; exists {
			continue
		}
		seenExecutionIDs[executionID] = struct{}{}

		logs, nextSeq, err := a.runtime.s2.ReadLogs(ctx, executionID, 0)
		if err != nil {
			continue
		}
		if len(logs) > 0 {
			history = append(history, logs...)
		}
		if runID == currentRunID {
			currentNextSeq = nextSeq
		}
	}

	return common.RedactTaskLogEntries(history), currentNextSeq, nil
}

func (a *AgentAPI) StreamTaskEvents(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	logCursor int64,
	runEventCursor int,
	cursorRunID string,
) (*TaskEventBatch, error) {
	return a.buildTaskEventBatch(ctx, workspaceID, taskID, logCursor, runEventCursor, cursorRunID)
}

func (a *AgentAPI) WorkspaceLiveBatch(ctx context.Context, workspaceID uint) (*WorkspaceLiveBatch, error) {
	tasks, _, _, err := a.ListTasksFiltered(ctx, workspaceID, types.AgentTaskListFilter{
		IncludeArchived: true,
		Limit:           defaultWorkspaceStreamTaskLimit,
	})
	if err != nil {
		return nil, err
	}
	outputs, err := a.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, types.TaskOutputListFilter{
		ExcludeArchived: true,
		Limit:           defaultWorkspaceStreamOutputLimit,
	})
	if err != nil {
		return nil, err
	}
	if outputs == nil {
		outputs = []*types.TaskOutput{}
	}
	return &WorkspaceLiveBatch{
		Tasks:   tasks,
		Outputs: outputs,
	}, nil
}

func (a *AgentAPI) SubscribeWorkspaceLive(ctx context.Context, workspaceID uint) (<-chan struct{}, func(), error) {
	if a.runtime == nil || a.runtime.orchestrationStore == nil {
		return nil, nil, fmt.Errorf("workspace live unavailable")
	}
	return a.runtime.orchestrationStore.SubscribeWorkspaceLive(ctx, workspaceID)
}

func (a *AgentAPI) SubscribeTaskLive(ctx context.Context, taskID string) (<-chan struct{}, func(), error) {
	if a.runtime == nil || a.runtime.orchestrationStore == nil {
		return nil, nil, fmt.Errorf("task live unavailable")
	}
	return a.runtime.orchestrationStore.SubscribeTaskLive(ctx, taskID)
}

func (a *AgentAPI) SubscribeRunEvents(ctx context.Context, runID string) (<-chan struct{}, func(), error) {
	if a.runtime == nil || a.runtime.orchestrationStore == nil {
		return nil, nil, fmt.Errorf("run event stream unavailable")
	}
	return a.runtime.orchestrationStore.SubscribeRunEvents(ctx, runID)
}

func (a *AgentAPI) SubscribeExecutionLogs(ctx context.Context, executionID string) (<-chan []byte, func(), error) {
	if a.runtime == nil || a.runtime.taskQueue == nil {
		return nil, nil, fmt.Errorf("execution log stream unavailable")
	}
	return a.runtime.taskQueue.SubscribeLogs(ctx, executionID)
}

func (a *AgentAPI) buildTaskEventBatch(
	ctx context.Context,
	workspaceID uint,
	taskID string,
	logCursor int64,
	runEventCursor int,
	cursorRunID string,
) (*TaskEventBatch, error) {
	task, err := a.GetTask(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}

	currentRunID := ""
	if task.TargetRunID != nil {
		currentRunID = strings.TrimSpace(*task.TargetRunID)
	}
	if boundRunID := strings.TrimSpace(cursorRunID); boundRunID != "" && boundRunID != currentRunID {
		logCursor = 0
		runEventCursor = 0
	}

	logs, nextLogCursor, err := a.ListTaskLogs(ctx, workspaceID, taskID, logCursor)
	if err != nil {
		return nil, err
	}

	var run *types.AgentRun
	var interaction *types.RunInteraction
	runEvents := []map[string]any{}
	nextRunEventCursor := runEventCursor
	if task.TargetRunID != nil {
		run, _ = a.GetRun(ctx, workspaceID, *task.TargetRunID)
		allEvents, err := a.ListRunEvents(ctx, workspaceID, *task.TargetRunID)
		if err != nil {
			return nil, err
		}
		if runEventCursor < 0 {
			runEventCursor = 0
		}
		if runEventCursor > len(allEvents) {
			runEventCursor = len(allEvents)
		}
		runEvents = allEvents[runEventCursor:]
		nextRunEventCursor = len(allEvents)

		if a.runtime != nil {
			interaction, _ = a.runtime.GetRunInteraction(ctx, workspaceID, *task.TargetRunID)
		}
	}

	outputs, err := a.backend.ListTaskOutputs(ctx, workspaceID, taskID)
	if err != nil {
		return nil, err
	}
	if outputs == nil {
		outputs = []*types.TaskOutput{}
	}

	return &TaskEventBatch{
		TaskID:             task.ID,
		RunID:              task.TargetRunID,
		Task:               task,
		Run:                run,
		Interaction:        interaction,
		Logs:               logs,
		RunEvents:          runEvents,
		Outputs:            outputs,
		NextLogCursor:      nextLogCursor,
		NextRunEventCursor: nextRunEventCursor,
	}, nil
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

func prependTaskPromptLog(task *types.AgentTask, logs []common.TaskLogEntry) []common.TaskLogEntry {
	if task == nil {
		return logs
	}
	prompt := strings.TrimSpace(stringFromPayload(task.PayloadJSON, "message"))
	if prompt == "" {
		prompt = strings.TrimSpace(stringFromPayload(task.PayloadJSON, "prompt"))
	}
	if prompt == "" {
		return logs
	}
	for _, entry := range logs {
		if strings.TrimSpace(entry.Stream) != "user" {
			continue
		}
		if strings.TrimSpace(entry.Data) == prompt {
			return logs
		}
	}

	timestamp := task.AcceptedAt.UnixMilli()
	if timestamp <= 0 {
		timestamp = task.CreatedAt.UnixMilli()
	}
	if timestamp <= 0 {
		timestamp = time.Now().UnixMilli()
	}
	promptEntry := common.TaskLogEntry{
		TaskID:    task.ID,
		Timestamp: timestamp,
		Stream:    "user",
		Data:      prompt,
		ChunkType: "task_prompt",
	}
	return append([]common.TaskLogEntry{promptEntry}, logs...)
}

func sanitizeTaskForResponse(task *types.AgentTask) *types.AgentTask {
	if task == nil {
		return nil
	}
	safe := *task
	safe.PayloadJSON = common.RedactSensitiveMap(task.PayloadJSON)
	safe.RoutingJSON = common.RedactSensitiveMap(task.RoutingJSON)
	return &safe
}

func sanitizeTasksForResponse(tasks []*types.AgentTask) []*types.AgentTask {
	if len(tasks) == 0 {
		return tasks
	}
	safe := make([]*types.AgentTask, len(tasks))
	for idx, task := range tasks {
		safe[idx] = sanitizeTaskForResponse(task)
	}
	return safe
}

func sanitizeRunForResponse(run *types.AgentRun) *types.AgentRun {
	if run == nil {
		return nil
	}
	safe := *run
	safe.UsageJSON = common.RedactSensitiveMap(run.UsageJSON)
	safe.DeliveryJSON = common.RedactSensitiveMap(run.DeliveryJSON)
	return &safe
}

func sanitizeRunsForResponse(runs []*types.AgentRun) []*types.AgentRun {
	if len(runs) == 0 {
		return runs
	}
	safe := make([]*types.AgentRun, len(runs))
	for idx, run := range runs {
		safe[idx] = sanitizeRunForResponse(run)
	}
	return safe
}

func optionalStringValue(value *string, fallback string) string {
	if value == nil {
		return fallback
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func normalizeAgentProfileConfig(config map[string]any, agentKey string) (map[string]any, error) {
	defaults := DefaultAgentConfig(agentKey)
	normalized := cloneAnyMap(config)

	runner := strings.ToLower(strings.TrimSpace(stringFromPayload(normalized, agentConfigKeyRunner)))
	provider := strings.ToLower(strings.TrimSpace(stringFromPayload(normalized, agentConfigKeyProvider)))

	if runner == "" && provider == "" {
		runner = AgentRunnerClaudeCode
		provider = providerForRunner(runner)
	}
	if runner != "" && runner != AgentRunnerClaudeCode {
		return nil, fmt.Errorf("runner %q is not supported", runner)
	}
	if provider != "" && !isClaudeCompatibleProvider(provider) {
		return nil, fmt.Errorf("provider %q is not supported", provider)
	}
	if runner == "" {
		runner = AgentRunnerClaudeCode
	}
	if provider == "" {
		provider = providerForRunner(runner)
	}

	normalized[agentConfigKeyRunner] = runner
	normalized[agentConfigKeyProvider] = provider

	for _, key := range []string{agentConfigKeyWorkspaceDir, agentConfigKeySystemPrompt} {
		if strings.TrimSpace(stringFromPayload(normalized, key)) == "" {
			normalized[key] = defaults[key]
		}
	}

	if _, hasSkills := normalized[agentConfigKeySkills]; hasSkills {
		if sp := stringFromPayload(normalized, agentConfigKeySystemPrompt); sp != "" {
			if start, end, _, ok := activeSkillsSection(sp); ok {
				cleaned := strings.TrimSpace(
					strings.TrimSpace(sp[:start]) + "\n\n" + strings.TrimSpace(sp[end:]),
				)
				if cleaned == "" {
					normalized[agentConfigKeySystemPrompt] = defaults[agentConfigKeySystemPrompt]
				} else {
					normalized[agentConfigKeySystemPrompt] = cleaned
				}
			}
		}
	}

	return normalized, nil
}

// --- Scheduled Tasks ---

func (a *AgentAPI) CreateSchedule(
	ctx context.Context,
	workspaceID uint,
	agentID string,
	cronExpr string,
	timezone string,
	prompt string,
	skillPaths []string,
	memberID *uint,
	tokenID *uint,
	encryptedToken []byte,
) (*types.ScheduledTask, error) {
	cronExpr = strings.TrimSpace(cronExpr)
	if cronExpr == "" {
		return nil, fmt.Errorf("cron_expr is required")
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, fmt.Errorf("prompt is required")
	}
	if strings.TrimSpace(agentID) == "" {
		return nil, fmt.Errorf("agent_id is required")
	}

	timezone = normalizeTimezone(timezone)

	cronExpr, err := resolveCronExpr(ctx, cronExpr, timezone)
	if err != nil {
		return nil, err
	}
	nextRun, err := NextCronTime(cronExpr, time.Now(), timezone)
	if err != nil {
		return nil, fmt.Errorf("invalid cron expression: %w", err)
	}

	if skillPaths == nil {
		skillPaths = []string{}
	}

	st := &types.ScheduledTask{
		WorkspaceID:       workspaceID,
		AgentID:           agentID,
		CronExpr:          cronExpr,
		Timezone:          timezone,
		Prompt:            prompt,
		SkillPaths:        skillPaths,
		Active:            true,
		NextRunAt:         nextRun,
		TokenID:           tokenID,
		EncryptedToken:    encryptedToken,
		CreatedByMemberID: memberID,
	}
	if err := a.backend.CreateScheduledTask(ctx, st); err != nil {
		return nil, err
	}
	return st, nil
}

// normalizeTimezone validates an IANA timezone string, falling back to UTC.
func normalizeTimezone(tz string) string {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return "UTC"
	}
	if _, err := time.LoadLocation(tz); err != nil {
		return "UTC"
	}
	return tz
}

func (a *AgentAPI) GetSchedule(ctx context.Context, workspaceID uint, externalID string) (*types.ScheduledTask, error) {
	return a.backend.GetScheduledTask(ctx, workspaceID, externalID)
}

func (a *AgentAPI) ListSchedules(ctx context.Context, workspaceID uint) ([]*types.ScheduledTask, error) {
	return a.backend.ListScheduledTasks(ctx, workspaceID)
}

func (a *AgentAPI) UpdateSchedule(
	ctx context.Context,
	workspaceID uint,
	externalID string,
	cronExpr *string,
	timezone *string,
	prompt *string,
	skillPaths *[]string,
	active *bool,
) (*types.ScheduledTask, error) {
	st, err := a.backend.GetScheduledTask(ctx, workspaceID, externalID)
	if err != nil {
		return nil, err
	}

	if timezone != nil {
		st.Timezone = normalizeTimezone(*timezone)
	}

	if cronExpr != nil {
		resolved, err := resolveCronExpr(ctx, *cronExpr, st.Timezone)
		if err != nil {
			return nil, err
		}
		st.CronExpr = resolved
	}

	if cronExpr != nil || timezone != nil {
		nextRun, err := NextCronTime(st.CronExpr, time.Now(), st.Timezone)
		if err != nil {
			return nil, fmt.Errorf("invalid cron expression: %w", err)
		}
		st.NextRunAt = nextRun
	}
	if prompt != nil {
		trimmed := strings.TrimSpace(*prompt)
		if trimmed == "" {
			return nil, fmt.Errorf("prompt cannot be empty")
		}
		st.Prompt = trimmed
	}
	if skillPaths != nil {
		st.SkillPaths = *skillPaths
	}
	if active != nil {
		st.Active = *active
	}

	if err := a.backend.UpdateScheduledTask(ctx, st); err != nil {
		return nil, err
	}
	return st, nil
}

func (a *AgentAPI) DeleteSchedule(ctx context.Context, workspaceID uint, externalID string) error {
	return a.backend.DeleteScheduledTask(ctx, workspaceID, externalID)
}

func normalizeOffsetPage(limit, offset, defaultLimit, maxLimit int) (int, int) {
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}

func nextOffsetCursor(offset, limit int, hasMore bool) string {
	if !hasMore {
		return ""
	}
	return strconv.Itoa(offset + limit)
}
