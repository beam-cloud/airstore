package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
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

type TaskEventBatch struct {
	TaskID             string                `json:"task_id"`
	RunID              *string               `json:"run_id,omitempty"`
	Task               *types.AgentTask      `json:"task,omitempty"`
	Run                *types.AgentRun       `json:"run,omitempty"`
	Logs               []common.TaskLogEntry `json:"logs"`
	RunEvents          []map[string]any      `json:"run_events"`
	NextLogCursor      int64                 `json:"next_log_cursor"`
	NextRunEventCursor int                   `json:"next_run_event_cursor"`
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

	normalizedConfig, err := normalizeAgentProfileConfig(config)
	if err != nil {
		return nil, err
	}

	profile := &types.AgentProfile{
		WorkspaceID: workspaceID,
		AgentKey:    strings.TrimSpace(agentKey),
		Name:        strings.TrimSpace(name),
		ConfigJSON:  normalizedConfig,
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

func (a *AgentAPI) ListTasksFiltered(
	ctx context.Context,
	workspaceID uint,
	filter types.AgentTaskListFilter,
) ([]*types.AgentTask, string, bool, error) {
	limit, offset := normalizeOffsetPage(filter.Limit, filter.Offset, 50, 200)
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
	return tasks, nextOffsetCursor(offset, limit, hasMore), hasMore, nil
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
	return runs, nextOffsetCursor(offset, limit, hasMore), hasMore, nil
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

	if !task.State.IsTerminal() {
		if err := a.backend.UpdateTaskState(ctx, task.ID, types.AgentTaskStateCancelled, nil, task.TargetRunID); err != nil {
			return err
		}
	}

	return nil
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

	if a.runtime == nil || a.runtime.s2 == nil || !a.runtime.s2.Enabled() || task.TargetRunID == nil {
		return []common.TaskLogEntry{}, seqNum, nil
	}

	currentRunID := strings.TrimSpace(*task.TargetRunID)
	if currentRunID == "" {
		return []common.TaskLogEntry{}, seqNum, nil
	}

	// Non-zero cursor means incremental polling for the currently bound run.
	if seqNum > 0 {
		return a.listTaskLogsForRun(ctx, currentRunID, seqNum)
	}

	// Cursor zero means "hydrate history". Return logs across all runs of this
	// task session so resumed runs show the full timeline by default.
	return a.listTaskSessionHistoryLogs(ctx, workspaceID, task, currentRunID)
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
	return logs, nextSeqNum, nil
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

	return history, currentNextSeq, nil
}

func (a *AgentAPI) StreamTaskEvents(
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
	}

	return &TaskEventBatch{
		TaskID:             task.ID,
		RunID:              task.TargetRunID,
		Task:               task,
		Run:                run,
		Logs:               logs,
		RunEvents:          runEvents,
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

func normalizeAgentProfileConfig(config map[string]any) (map[string]any, error) {
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
	return normalized, nil
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
