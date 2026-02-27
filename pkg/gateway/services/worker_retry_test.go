package services

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

type retryTestBackend struct {
	repository.BackendRepository

	runs          map[string]*types.AgentRun
	tasks         map[string]*types.AgentTask
	runExecutions map[string]*types.RunExecution
	attemptByID   map[string]*types.AgentRunAttempt
	attemptsByRun map[string][]*types.AgentRunAttempt
	snapshotSeq   map[string]int64

	nextRunID       int
	nextAttemptID   int
	nextExecutionID int
}

func newRetryTestBackend() *retryTestBackend {
	return &retryTestBackend{
		runs:          map[string]*types.AgentRun{},
		tasks:         map[string]*types.AgentTask{},
		runExecutions: map[string]*types.RunExecution{},
		attemptByID:   map[string]*types.AgentRunAttempt{},
		attemptsByRun: map[string][]*types.AgentRunAttempt{},
		snapshotSeq:   map[string]int64{},
	}
}

func (b *retryTestBackend) GetAgentRunByID(_ context.Context, runID string) (*types.AgentRun, error) {
	run, ok := b.runs[runID]
	if !ok {
		return nil, &types.ErrAgentRunNotFound{ID: runID}
	}
	return run, nil
}

func (b *retryTestBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	task, ok := b.tasks[taskID]
	if !ok {
		return nil, &types.ErrAgentTaskNotFound{ID: taskID}
	}
	return task, nil
}

func (b *retryTestBackend) GetRunExecution(_ context.Context, externalID string) (*types.RunExecution, error) {
	exec, ok := b.runExecutions[externalID]
	if !ok {
		return nil, &types.ErrRunExecutionNotFound{ExternalId: externalID}
	}
	return exec, nil
}

func (b *retryTestBackend) SetRunExecutionResult(_ context.Context, externalID string, exitCode int, errorMsg string) error {
	exec, ok := b.runExecutions[externalID]
	if !ok {
		return &types.ErrRunExecutionNotFound{ExternalId: externalID}
	}

	exec.ExitCode = &exitCode
	exec.Error = errorMsg
	if exitCode == 0 && strings.TrimSpace(errorMsg) == "" {
		exec.Status = types.RunExecutionStatusComplete
	} else {
		exec.Status = types.RunExecutionStatusFailed
	}
	return nil
}

func (b *retryTestBackend) CreateAgentRun(_ context.Context, run *types.AgentRun) error {
	copyRun := *run
	if copyRun.ID == "" {
		copyRun.ID = b.nextRunExternalID()
	}
	b.runs[copyRun.ID] = &copyRun
	run.ID = copyRun.ID
	return nil
}

func (b *retryTestBackend) UpdateTaskState(_ context.Context, taskID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error {
	task, ok := b.tasks[taskID]
	if !ok {
		return &types.ErrAgentTaskNotFound{ID: taskID}
	}
	task.State = state
	task.DroppedReason = droppedReason
	task.TargetRunID = targetRunID
	return nil
}

func (b *retryTestBackend) GetRunAttemptByExecutionID(_ context.Context, executionID string) (*types.AgentRunAttempt, error) {
	for _, attempt := range b.attemptByID {
		if attempt == nil || attempt.ExecutionID == nil {
			continue
		}
		if *attempt.ExecutionID == executionID {
			return attempt, nil
		}
	}
	return nil, &types.ErrAgentRunAttemptNotFound{ID: executionID}
}

func (b *retryTestBackend) UpdateAgentRunAttemptResult(_ context.Context, attemptID string, status types.AgentAttemptStatus, exitCode *int, endedAt time.Time, errorMsg *string) error {
	attempt, ok := b.attemptByID[attemptID]
	if !ok {
		return &types.ErrAgentRunAttemptNotFound{ID: attemptID}
	}
	attempt.Status = status
	attempt.ExitCode = exitCode
	attempt.EndedAt = &endedAt
	attempt.Error = errorMsg
	return nil
}

func (b *retryTestBackend) UpdateAgentRunLifecycle(_ context.Context, runID string, status types.AgentRunStatus, startedAt, endedAt *time.Time, errorMsg *string) error {
	run, ok := b.runs[runID]
	if !ok {
		return &types.ErrAgentRunNotFound{ID: runID}
	}
	run.Status = status
	if startedAt != nil {
		run.StartedAt = startedAt
	}
	if endedAt != nil {
		run.EndedAt = endedAt
	}
	run.Error = errorMsg
	return nil
}

func (b *retryTestBackend) SetAgentRunClaim(_ context.Context, runID string, workerID string, heartbeatAt time.Time, expiresAt time.Time) error {
	run, ok := b.runs[runID]
	if !ok {
		return &types.ErrAgentRunNotFound{ID: runID}
	}
	run.ClaimedByWorker = &workerID
	run.ClaimHeartbeatAt = &heartbeatAt
	run.ClaimExpiresAt = &expiresAt
	return nil
}

func (b *retryTestBackend) ClearAgentRunClaim(_ context.Context, runID string) error {
	run, ok := b.runs[runID]
	if !ok {
		return &types.ErrAgentRunNotFound{ID: runID}
	}
	run.ClaimedByWorker = nil
	run.ClaimHeartbeatAt = nil
	run.ClaimExpiresAt = nil
	return nil
}

func (b *retryTestBackend) ClearExpiredAgentRunClaim(_ context.Context, runID string, workerID string, expiresAt time.Time) (bool, error) {
	run, ok := b.runs[runID]
	if !ok {
		return false, &types.ErrAgentRunNotFound{ID: runID}
	}
	if run.ClaimedByWorker == nil || run.ClaimExpiresAt == nil {
		return false, nil
	}
	if *run.ClaimedByWorker != workerID {
		return false, nil
	}
	if run.ClaimExpiresAt.After(expiresAt) {
		return false, nil
	}
	run.ClaimedByWorker = nil
	run.ClaimHeartbeatAt = nil
	run.ClaimExpiresAt = nil
	return true, nil
}

func (b *retryTestBackend) RefreshAgentRunClaims(_ context.Context, workerID string, heartbeatAt time.Time, expiresAt time.Time) (int64, error) {
	var refreshed int64
	for _, run := range b.runs {
		if run == nil || run.ClaimedByWorker == nil {
			continue
		}
		if *run.ClaimedByWorker != workerID {
			continue
		}
		if !run.Status.IsActive() {
			continue
		}
		run.ClaimHeartbeatAt = &heartbeatAt
		run.ClaimExpiresAt = &expiresAt
		refreshed++
	}
	return refreshed, nil
}

func (b *retryTestBackend) ListExpiredClaimedAgentRuns(_ context.Context, now time.Time, limit int) ([]*types.AgentRun, error) {
	runs := make([]*types.AgentRun, 0)
	for _, run := range b.runs {
		if run == nil || !run.Status.IsActive() || run.ClaimedByWorker == nil || run.ClaimExpiresAt == nil {
			continue
		}
		if run.ClaimExpiresAt.Before(now) {
			runs = append(runs, run)
		}
	}
	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

func (b *retryTestBackend) ListClaimedAgentRuns(_ context.Context, limit int) ([]*types.AgentRun, error) {
	runs := make([]*types.AgentRun, 0)
	for _, run := range b.runs {
		if run == nil || !run.Status.IsActive() || run.ClaimedByWorker == nil {
			continue
		}
		runs = append(runs, run)
	}
	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

func (b *retryTestBackend) ListActiveRunsBySession(_ context.Context, workspaceID uint, sessionID string, excludeRunIDs []string, limit int) ([]*types.AgentRun, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return []*types.AgentRun{}, nil
	}
	exclude := make(map[string]struct{}, len(excludeRunIDs))
	for _, runID := range excludeRunIDs {
		runID = strings.TrimSpace(runID)
		if runID == "" {
			continue
		}
		exclude[runID] = struct{}{}
	}

	runs := make([]*types.AgentRun, 0)
	for _, run := range b.runs {
		if run == nil || run.WorkspaceID != workspaceID || !run.Status.IsActive() {
			continue
		}
		if strings.TrimSpace(run.SessionID) != sessionID {
			continue
		}
		if _, skip := exclude[run.ID]; skip {
			continue
		}
		runs = append(runs, run)
		if limit > 0 && len(runs) >= limit {
			break
		}
	}
	return runs, nil
}

func (b *retryTestBackend) ListStaleUnclaimedAgentRuns(_ context.Context, cutoff time.Time, limit int) ([]*types.AgentRun, error) {
	runs := make([]*types.AgentRun, 0)
	for _, run := range b.runs {
		if run == nil || !run.Status.IsActive() || run.ClaimedByWorker != nil {
			continue
		}
		if run.UpdatedAt.Before(cutoff) {
			runs = append(runs, run)
		}
	}
	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

func (b *retryTestBackend) CreateAgentRunAttempt(_ context.Context, attempt *types.AgentRunAttempt) error {
	copyAttempt := *attempt
	if copyAttempt.ID == "" {
		copyAttempt.ID = b.nextAttemptExternalID()
	}
	b.attemptByID[copyAttempt.ID] = &copyAttempt
	b.attemptsByRun[copyAttempt.RunID] = append(b.attemptsByRun[copyAttempt.RunID], &copyAttempt)
	attempt.ID = copyAttempt.ID
	return nil
}

func (b *retryTestBackend) EnsureWorkspaceServiceToken(_ context.Context, _ uint) (*types.Token, string, error) {
	return &types.Token{}, "svc-token", nil
}

func (b *retryTestBackend) CreateRunExecution(_ context.Context, task *types.RunExecution) error {
	copyTask := *task
	if copyTask.ExternalId == "" {
		copyTask.ExternalId = b.nextExecutionExternalID()
	}
	b.runExecutions[copyTask.ExternalId] = &copyTask
	task.ExternalId = copyTask.ExternalId
	return nil
}

func (b *retryTestBackend) BindAttemptExecutionTask(_ context.Context, attemptID, taskExternalID string) error {
	attempt, ok := b.attemptByID[attemptID]
	if !ok {
		return fmt.Errorf("attempt not found: %s", attemptID)
	}
	attempt.ExecutionID = &taskExternalID
	return nil
}

func (b *retryTestBackend) IncrementAgentRunSnapshotSeq(_ context.Context, runID string) (int64, error) {
	b.snapshotSeq[runID]++
	return b.snapshotSeq[runID], nil
}

func (b *retryTestBackend) AppendAgentRunSnapshot(_ context.Context, _ *types.AgentRunSnapshot) error {
	return nil
}

func (b *retryTestBackend) nextRunExternalID() string {
	for {
		b.nextRunID++
		id := fmt.Sprintf("run-retry-%d", b.nextRunID)
		if _, exists := b.runs[id]; !exists {
			return id
		}
	}
}

func (b *retryTestBackend) nextAttemptExternalID() string {
	b.nextAttemptID++
	return fmt.Sprintf("attempt-%d", b.nextAttemptID)
}

func (b *retryTestBackend) nextExecutionExternalID() string {
	b.nextExecutionID++
	return fmt.Sprintf("exec-retry-%d", b.nextExecutionID)
}

type capturingTaskQueue struct {
	repository.TaskQueue
	pushed         []*types.RunExecution
	failed         []string
	stateByTaskID  map[string]*types.RunExecutionState
	resultByTaskID map[string]*types.RunExecutionResult
}

func (q *capturingTaskQueue) Push(_ context.Context, task *types.RunExecution) error {
	copyTask := *task
	q.pushed = append(q.pushed, &copyTask)
	return nil
}

func (q *capturingTaskQueue) Complete(_ context.Context, taskID string, result *types.RunExecutionResult) error {
	if q.stateByTaskID == nil {
		q.stateByTaskID = map[string]*types.RunExecutionState{}
	}
	if q.resultByTaskID == nil {
		q.resultByTaskID = map[string]*types.RunExecutionResult{}
	}
	status := types.RunExecutionStatusComplete
	errText := ""
	exitCode := -1
	if result != nil {
		exitCode = result.ExitCode
		errText = strings.TrimSpace(result.Error)
		if errText != "" || result.ExitCode != 0 {
			status = types.RunExecutionStatusFailed
		}
		copyResult := *result
		q.resultByTaskID[taskID] = &copyResult
	}
	q.stateByTaskID[taskID] = &types.RunExecutionState{
		ID:         taskID,
		Status:     status,
		ExitCode:   exitCode,
		Error:      errText,
		FinishedAt: time.Now(),
	}
	return nil
}

func (q *capturingTaskQueue) Fail(_ context.Context, taskID string, _ error) error {
	q.failed = append(q.failed, taskID)
	if q.stateByTaskID == nil {
		q.stateByTaskID = map[string]*types.RunExecutionState{}
	}
	q.stateByTaskID[taskID] = &types.RunExecutionState{
		ID:         taskID,
		Status:     types.RunExecutionStatusFailed,
		FinishedAt: time.Now(),
	}
	return nil
}

func (q *capturingTaskQueue) GetState(_ context.Context, taskID string) (*types.RunExecutionState, error) {
	if q.stateByTaskID == nil {
		return nil, fmt.Errorf("state unavailable in capturing queue")
	}
	if state, ok := q.stateByTaskID[taskID]; ok {
		return state, nil
	}
	return nil, fmt.Errorf("task state not found")
}

func (q *capturingTaskQueue) GetResult(_ context.Context, taskID string) (*types.RunExecutionResult, error) {
	if q.resultByTaskID == nil {
		return nil, fmt.Errorf("result unavailable in capturing queue")
	}
	if result, ok := q.resultByTaskID[taskID]; ok {
		return result, nil
	}
	return nil, fmt.Errorf("task result not found")
}

type staticWorkerRepo struct {
	repository.WorkerRepository
	workers map[string]*types.Worker
}

func (r *staticWorkerRepo) GetWorker(_ context.Context, workerID string) (*types.Worker, error) {
	if r.workers == nil {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	worker, ok := r.workers[workerID]
	if !ok {
		return nil, &types.ErrWorkerNotFound{WorkerId: workerID}
	}
	return worker, nil
}

func seedRecoverableRunContext(
	backend *retryTestBackend,
	runID string,
	originTaskID string,
	maxAttempts int,
) *types.AgentRunAttempt {
	agentID := "agent-1"
	now := time.Now().Add(-5 * time.Minute)
	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		ExecHost:     string(orchestration.ExecHostSandbox),
		ExecSecurity: string(orchestration.ExecSecurityFull),
		ExecAsk:      string(orchestration.ExecAskOff),
		RuntimeType:  orchestration.RuntimeTypeGvisor,
		TimeoutMs:    60_000,
		UpdatedAt:    now,
		DeliveryJSON: map[string]any{
			types.AgentExecutionMetaKeyRetryMaxAttempts: maxAttempts,
			types.AgentExecutionMetaKeyRetryDelayMs:     0,
		},
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
		UpdatedAt:   now,
	}
	backend.runExecutions[runID] = &types.RunExecution{
		ExternalId:  runID,
		WorkspaceId: 42,
		Status:      types.RunExecutionStatusRunning,
		Type:        types.RunExecutionTypeBackground,
		Prompt:      "recover me",
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{"runner"},
		Env:         map[string]string{"A": "B"},
	}

	attempt := &types.AgentRunAttempt{
		ID:          fmt.Sprintf("%s-attempt-1", runID),
		RunID:       runID,
		AttemptNo:   1,
		Status:      types.AgentAttemptStatusRunning,
		ExecutionID: &runID,
	}
	backend.attemptByID[attempt.ID] = attempt
	backend.attemptsByRun[runID] = []*types.AgentRunAttempt{attempt}
	return attempt
}

func TestScheduleRetryRunCreatesNewRunAndRebindsTask(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{backend: backend, taskQueue: queue}

	agentID := "agent-1"
	originRunID := "run-1"
	originTaskID := "task-1"
	originExecutionID := "exec-1"

	backend.runs[originRunID] = &types.AgentRun{
		ID:           originRunID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		ExecHost:     string(orchestration.ExecHostSandbox),
		ExecSecurity: string(orchestration.ExecSecurityFull),
		ExecAsk:      string(orchestration.ExecAskOff),
		RuntimeType:  orchestration.RuntimeTypeGvisor,
		TimeoutMs:    60_000,
		DeliveryJSON: map[string]any{
			types.AgentExecutionMetaKeyRetryMaxAttempts: 3,
			types.AgentExecutionMetaKeyRetryDelayMs:     0,
		},
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &originRunID,
	}
	backend.runExecutions[originExecutionID] = &types.RunExecution{
		ExternalId:  originExecutionID,
		WorkspaceId: 42,
		Status:      types.RunExecutionStatusFailed,
		Type:        types.RunExecutionTypeBackground,
		Prompt:      "retry me",
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{"runner"},
		Env:         map[string]string{"A": "B"},
	}

	attempt := &types.AgentRunAttempt{
		ID:        "attempt-1",
		RunID:     originRunID,
		AttemptNo: 1,
		Status:    types.AgentAttemptStatusError,
	}
	backend.attemptByID[attempt.ID] = attempt
	backend.attemptsByRun[originRunID] = []*types.AgentRunAttempt{attempt}

	result, err := svc.scheduleRetryRun(context.Background(), attempt, originExecutionID)
	require.NoError(t, err)
	require.True(t, result.scheduled)
	require.NotEmpty(t, result.nextRunID)
	require.NotEqual(t, originRunID, result.nextRunID)
	require.Equal(t, 2, result.nextAttemptNo)

	originTask := backend.tasks[originTaskID]
	require.NotNil(t, originTask.TargetRunID)
	require.Equal(t, result.nextRunID, *originTask.TargetRunID)

	retryRun := backend.runs[result.nextRunID]
	require.NotNil(t, retryRun)
	require.Equal(t, originTaskID, retryRun.OriginTaskID)

	retryAttempts := backend.attemptsByRun[result.nextRunID]
	require.Len(t, retryAttempts, 1)
	require.Equal(t, 2, retryAttempts[0].AttemptNo)
	require.Equal(t, types.AgentAttemptStrategyRetry, retryAttempts[0].Strategy)

	require.Len(t, queue.pushed, 1)
	queuedExecution := queue.pushed[0]
	require.NotEqual(t, originExecutionID, queuedExecution.ExternalId)
	require.NotNil(t, queuedExecution.RunAttemptID)
	require.Equal(t, retryAttempts[0].ID, *queuedExecution.RunAttemptID)
	require.Equal(t, 2, queuedExecution.Attempt)
	require.Equal(t, 3, queuedExecution.MaxAttempts)
	require.Equal(t, result.nextRunID, queuedExecution.ExecutionPolicy[types.AgentExecutionMetaKeyRunID])
	require.Equal(t, retryAttempts[0].ID, queuedExecution.ExecutionPolicy[types.AgentExecutionMetaKeyRunAttemptID])
	require.Equal(t, originTaskID, queuedExecution.ExecutionPolicy[types.AgentExecutionMetaKeyOriginTaskID])
	require.Equal(t, result.nextRunID, queuedExecution.Env["AIRSTORE_RUN_ID"])
	require.Equal(t, retryAttempts[0].ID, queuedExecution.Env["AIRSTORE_RUN_ATTEMPT_ID"])
	require.Equal(t, originTaskID, queuedExecution.Env["AIRSTORE_ORIGIN_TASK_ID"])
}

func TestSetTaskResultMarksOriginTaskDoneWhenRunFinishes(t *testing.T) {
	backend := newRetryTestBackend()
	svc := &WorkerService{backend: backend, taskQueue: &capturingTaskQueue{}}

	agentID := "agent-1"
	runID := "run-1"
	originTaskID := "task-1"
	executionID := "exec-1"
	attemptID := "attempt-1"

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attemptByID[attemptID] = &types.AgentRunAttempt{
		ID:          attemptID,
		RunID:       runID,
		AttemptNo:   1,
		Status:      types.AgentAttemptStatusRunning,
		ExecutionID: &executionID,
	}

	_, err := svc.SetTaskResult(context.Background(), &pb.SetTaskResultRequest{
		TaskId:   executionID,
		ExitCode: 0,
		Error:    "",
	})
	require.NoError(t, err)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateDone, task.State)
	require.NotNil(t, task.TargetRunID)
	require.Equal(t, runID, *task.TargetRunID)
	require.Equal(t, types.AgentRunStatusOK, backend.runs[runID].Status)
}

func TestSetTaskResultMarksOriginTaskIdleWhenInteractiveRunFinishes(t *testing.T) {
	backend := newRetryTestBackend()
	svc := &WorkerService{backend: backend, taskQueue: &capturingTaskQueue{}}

	agentID := "agent-1"
	runID := "run-1"
	originTaskID := "task-1"
	executionID := "exec-1"
	attemptID := "attempt-1"

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		Interactive:  true,
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attemptByID[attemptID] = &types.AgentRunAttempt{
		ID:          attemptID,
		RunID:       runID,
		AttemptNo:   1,
		Status:      types.AgentAttemptStatusRunning,
		ExecutionID: &executionID,
	}

	_, err := svc.SetTaskResult(context.Background(), &pb.SetTaskResultRequest{
		TaskId:   executionID,
		ExitCode: 0,
		Error:    "",
	})
	require.NoError(t, err)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateIdle, task.State)
	require.NotNil(t, task.TargetRunID)
	require.Equal(t, runID, *task.TargetRunID)
	require.Equal(t, types.AgentRunStatusOK, backend.runs[runID].Status)
}

func TestSetTaskResultMarksOriginTaskCancelledWhenRunCancelled(t *testing.T) {
	backend := newRetryTestBackend()
	svc := &WorkerService{backend: backend, taskQueue: &capturingTaskQueue{}}

	agentID := "agent-1"
	runID := "run-1"
	originTaskID := "task-1"
	executionID := "exec-1"
	attemptID := "attempt-1"

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Status:     types.RunExecutionStatusRunning,
	}
	backend.attemptByID[attemptID] = &types.AgentRunAttempt{
		ID:          attemptID,
		RunID:       runID,
		AttemptNo:   1,
		Status:      types.AgentAttemptStatusRunning,
		ExecutionID: &executionID,
	}

	_, err := svc.SetTaskResult(context.Background(), &pb.SetTaskResultRequest{
		TaskId:   executionID,
		ExitCode: -1,
		Error:    "cancelled by user",
	})
	require.NoError(t, err)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateCancelled, task.State)
	require.NotNil(t, task.TargetRunID)
	require.Equal(t, runID, *task.TargetRunID)
	require.Equal(t, types.AgentRunStatusCancelled, backend.runs[runID].Status)
}

func TestMarkOriginTaskTerminalSkipsStaleCompletionAfterTaskReopen(t *testing.T) {
	backend := newRetryTestBackend()
	svc := &WorkerService{backend: backend}

	agentID := "agent-1"
	runID := "run-1"
	originTaskID := "task-1"
	endedAt := time.Now().Add(-2 * time.Minute)
	reopenedAt := endedAt.Add(5 * time.Second)

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusOK,
		EndedAt:      &endedAt,
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateQueued,
		TargetRunID: &runID,
		UpdatedAt:   reopenedAt,
	}

	err := svc.markOriginTaskTerminalIfCurrentRun(context.Background(), runID)
	require.NoError(t, err)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateQueued, task.State)
}

func TestSetTaskStartedRejectsTerminalRunMarksOriginTaskCancelled(t *testing.T) {
	backend := newRetryTestBackend()
	svc := &WorkerService{backend: backend, taskQueue: &capturingTaskQueue{}}

	agentID := "agent-1"
	runID := "run-1"
	originTaskID := "task-1"
	executionID := "exec-1"
	attemptID := "attempt-1"

	backend.runs[runID] = &types.AgentRun{
		ID:           runID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusCancelled,
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend.runExecutions[executionID] = &types.RunExecution{
		ExternalId: executionID,
		Status:     types.RunExecutionStatusPending,
	}
	backend.attemptByID[attemptID] = &types.AgentRunAttempt{
		ID:          attemptID,
		RunID:       runID,
		AttemptNo:   1,
		Status:      types.AgentAttemptStatusPending,
		ExecutionID: &executionID,
	}

	_, err := svc.SetTaskStarted(context.Background(), &pb.SetTaskStartedRequest{
		TaskId: executionID,
	})
	require.Error(t, err)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateCancelled, task.State)
	require.NotNil(t, task.TargetRunID)
	require.Equal(t, runID, *task.TargetRunID)
}

func TestRecoverOrphanedRunSchedulesRetry(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{
		backend:       backend,
		taskQueue:     queue,
		claimLeaseTTL: 30 * time.Second,
	}

	runID := "run-orphan-retry-1"
	originTaskID := "task-orphan-retry-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 3)

	recovered, retryScheduled, cleanupOnly, err := svc.recoverOrphanedRun(context.Background(), backend.runs[runID], "claim_lease_expired")
	require.NoError(t, err)
	require.True(t, recovered)
	require.True(t, retryScheduled)
	require.False(t, cleanupOnly)
	require.Contains(t, queue.failed, runID)
	require.Len(t, queue.pushed, 1)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.NotNil(t, task.TargetRunID)
	require.NotEqual(t, runID, *task.TargetRunID)
}

func TestRecoverOrphanedRunExhaustedRetriesFinalizesTask(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{
		backend:       backend,
		taskQueue:     queue,
		claimLeaseTTL: 30 * time.Second,
	}

	runID := "run-orphan-final-1"
	originTaskID := "task-orphan-final-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 1)

	recovered, retryScheduled, cleanupOnly, err := svc.recoverOrphanedRun(context.Background(), backend.runs[runID], "claim_lease_expired")
	require.NoError(t, err)
	require.True(t, recovered)
	require.False(t, retryScheduled)
	require.False(t, cleanupOnly)
	require.Len(t, queue.pushed, 0)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateDone, task.State)
	require.NotNil(t, task.TargetRunID)
	require.Equal(t, runID, *task.TargetRunID)
}

func TestRecoverOrphanedRunStaleAttemptPerformsCleanupOnly(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{
		backend:       backend,
		taskQueue:     queue,
		claimLeaseTTL: 30 * time.Second,
	}

	runID := "run-orphan-stale-attempt-1"
	originTaskID := "task-orphan-stale-attempt-1"
	attempt := seedRecoverableRunContext(backend, runID, originTaskID, 3)
	endedAt := time.Now().Add(-30 * time.Second)
	attempt.Status = types.AgentAttemptStatusError
	attempt.EndedAt = &endedAt

	recovered, retryScheduled, cleanupOnly, err := svc.recoverOrphanedRun(context.Background(), backend.runs[runID], "claim_lease_expired")
	require.NoError(t, err)
	require.True(t, recovered)
	require.False(t, retryScheduled)
	require.True(t, cleanupOnly)
	require.Contains(t, queue.failed, runID)
	require.Empty(t, queue.pushed)

	task := backend.tasks[originTaskID]
	require.NotNil(t, task)
	require.Equal(t, types.AgentTaskStateDone, task.State)
}

func TestProcessClaimedRunReconcilesTerminalQueueState(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{
		stateByTaskID:  map[string]*types.RunExecutionState{},
		resultByTaskID: map[string]*types.RunExecutionResult{},
	}
	svc := &WorkerService{
		backend:   backend,
		taskQueue: queue,
	}

	runID := "run-claimed-terminal-1"
	originTaskID := "task-claimed-terminal-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 2)
	workerID := "worker-claimed-terminal"
	claimHeartbeat := time.Now().Add(-20 * time.Second)
	claimExpires := time.Now().Add(20 * time.Second)
	backend.runs[runID].ClaimedByWorker = &workerID
	backend.runs[runID].ClaimHeartbeatAt = &claimHeartbeat
	backend.runs[runID].ClaimExpiresAt = &claimExpires

	queue.stateByTaskID[runID] = &types.RunExecutionState{
		ID:         runID,
		Status:     types.RunExecutionStatusComplete,
		ExitCode:   0,
		FinishedAt: time.Now().Add(-30 * time.Second),
	}
	queue.resultByTaskID[runID] = &types.RunExecutionResult{
		ID:       runID,
		ExitCode: 0,
	}

	outcome, err := svc.processClaimedRun(context.Background(), backend.runs[runID])
	require.NoError(t, err)
	require.True(t, outcome.detected)
	require.True(t, outcome.recovered)
	require.Equal(t, types.AgentRunStatusOK, backend.runs[runID].Status)
	require.Nil(t, backend.runs[runID].ClaimedByWorker)
	require.Equal(t, types.AgentTaskStateDone, backend.tasks[originTaskID].State)
}

func TestScheduleRetryRunBlocksOnActiveSessionConflict(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{backend: backend, taskQueue: queue}

	agentID := "agent-1"
	originRunID := "run-retry-session-conflict-1"
	conflictingRunID := "run-retry-session-conflict-2"
	originTaskID := "task-retry-session-conflict-1"
	originExecutionID := "exec-retry-session-conflict-1"
	sessionID := "session-1"

	backend.runs[originRunID] = &types.AgentRun{
		ID:           originRunID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: originTaskID,
		Status:       types.AgentRunStatusRunning,
		SessionID:    sessionID,
		ExecHost:     string(orchestration.ExecHostSandbox),
		ExecSecurity: string(orchestration.ExecSecurityFull),
		ExecAsk:      string(orchestration.ExecAskOff),
		RuntimeType:  orchestration.RuntimeTypeGvisor,
		TimeoutMs:    60_000,
		DeliveryJSON: map[string]any{
			types.AgentExecutionMetaKeyRetryMaxAttempts: 3,
			types.AgentExecutionMetaKeyRetryDelayMs:     0,
		},
	}
	backend.runs[conflictingRunID] = &types.AgentRun{
		ID:           conflictingRunID,
		WorkspaceID:  42,
		AgentID:      &agentID,
		OriginTaskID: "task-other",
		Status:       types.AgentRunStatusAccepted,
		SessionID:    sessionID,
		ExecHost:     string(orchestration.ExecHostSandbox),
		ExecSecurity: string(orchestration.ExecSecurityFull),
		ExecAsk:      string(orchestration.ExecAskOff),
		RuntimeType:  orchestration.RuntimeTypeGvisor,
		TimeoutMs:    60_000,
	}
	backend.tasks[originTaskID] = &types.AgentTask{
		ID:          originTaskID,
		WorkspaceID: 42,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &originRunID,
	}
	backend.runExecutions[originExecutionID] = &types.RunExecution{
		ExternalId:  originExecutionID,
		WorkspaceId: 42,
		Status:      types.RunExecutionStatusFailed,
		Type:        types.RunExecutionTypeBackground,
		Prompt:      "retry me",
		Image:       "ghcr.io/beam/sandbox:latest",
		Entrypoint:  []string{"runner"},
		Env:         map[string]string{"A": "B"},
	}

	attempt := &types.AgentRunAttempt{
		ID:        "attempt-session-conflict-1",
		RunID:     originRunID,
		AttemptNo: 1,
		Status:    types.AgentAttemptStatusError,
	}
	backend.attemptByID[attempt.ID] = attempt
	backend.attemptsByRun[originRunID] = []*types.AgentRunAttempt{attempt}

	result, err := svc.scheduleRetryRun(context.Background(), attempt, originExecutionID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "session ID session-1 is already in use")
	require.False(t, result.scheduled)
	require.Empty(t, queue.pushed)
}

func TestProcessClaimedRunSkipsFreshTerminalQueueState(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{
		stateByTaskID:  map[string]*types.RunExecutionState{},
		resultByTaskID: map[string]*types.RunExecutionResult{},
	}
	svc := &WorkerService{
		backend:   backend,
		taskQueue: queue,
	}

	runID := "run-claimed-fresh-1"
	originTaskID := "task-claimed-fresh-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 2)
	workerID := "worker-claimed-fresh"
	backend.runs[runID].ClaimedByWorker = &workerID

	queue.stateByTaskID[runID] = &types.RunExecutionState{
		ID:         runID,
		Status:     types.RunExecutionStatusComplete,
		ExitCode:   0,
		FinishedAt: time.Now(),
	}
	queue.resultByTaskID[runID] = &types.RunExecutionResult{
		ID:       runID,
		ExitCode: 0,
	}

	outcome, err := svc.processClaimedRun(context.Background(), backend.runs[runID])
	require.NoError(t, err)
	require.False(t, outcome.detected)
	require.False(t, outcome.recovered)
	require.Equal(t, types.AgentRunStatusRunning, backend.runs[runID].Status)
}

func TestProcessStaleUnclaimedRunRecoversWhenWorkerMissing(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{
		stateByTaskID: map[string]*types.RunExecutionState{},
	}
	svc := &WorkerService{
		backend:       backend,
		workerRepo:    &staticWorkerRepo{workers: map[string]*types.Worker{}},
		taskQueue:     queue,
		claimLeaseTTL: 30 * time.Second,
	}

	runID := "run-unclaimed-stale-1"
	originTaskID := "task-unclaimed-stale-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 2)
	backend.runs[runID].ClaimedByWorker = nil
	backend.runs[runID].Status = types.AgentRunStatusAccepted
	queue.stateByTaskID[runID] = &types.RunExecutionState{
		ID:       runID,
		Status:   types.RunExecutionStatusRunning,
		WorkerID: "missing-worker",
	}

	outcome, err := svc.processStaleUnclaimedRun(context.Background(), backend.runs[runID])
	require.NoError(t, err)
	require.True(t, outcome.detected)
	require.True(t, outcome.recovered)
	require.True(t, outcome.retryScheduled)
	require.Contains(t, queue.failed, runID)
}

func TestProcessStaleUnclaimedRunSkipsPendingQueuedState(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{
		stateByTaskID: map[string]*types.RunExecutionState{},
	}
	svc := &WorkerService{
		backend:    backend,
		workerRepo: &staticWorkerRepo{},
		taskQueue:  queue,
	}

	runID := "run-unclaimed-pending-1"
	originTaskID := "task-unclaimed-pending-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 2)
	backend.runs[runID].ClaimedByWorker = nil
	backend.runs[runID].Status = types.AgentRunStatusAccepted
	queue.stateByTaskID[runID] = &types.RunExecutionState{
		ID:     runID,
		Status: types.RunExecutionStatusPending,
	}

	outcome, err := svc.processStaleUnclaimedRun(context.Background(), backend.runs[runID])
	require.NoError(t, err)
	require.False(t, outcome.detected)
	require.False(t, outcome.recovered)
	require.False(t, outcome.retryScheduled)
	require.NotContains(t, queue.failed, runID)
}

func TestSetTaskResultIgnoresStaleCallback(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{backend: backend, taskQueue: queue}

	runID := "run-stale-callback-1"
	originTaskID := "task-stale-callback-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 2)

	endedAt := time.Now().Add(-time.Minute)
	attempt := backend.attemptsByRun[runID][0]
	attempt.Status = types.AgentAttemptStatusError
	attempt.EndedAt = &endedAt
	backend.runs[runID].Status = types.AgentRunStatusError
	backend.tasks[originTaskID].TargetRunID = &runID
	backend.tasks[originTaskID].State = types.AgentTaskStateRunning

	_, err := svc.SetTaskResult(context.Background(), &pb.SetTaskResultRequest{
		TaskId:   runID,
		ExitCode: 0,
		Error:    "",
	})
	require.NoError(t, err)
	require.Equal(t, types.AgentRunStatusError, backend.runs[runID].Status)
	require.Equal(t, types.AgentTaskStateRunning, backend.tasks[originTaskID].State)
}

func TestProcessExpiredClaimRunIdempotent(t *testing.T) {
	backend := newRetryTestBackend()
	queue := &capturingTaskQueue{}
	svc := &WorkerService{
		backend:       backend,
		taskQueue:     queue,
		claimLeaseTTL: 30 * time.Second,
	}

	runID := "run-expired-idempotent-1"
	originTaskID := "task-expired-idempotent-1"
	seedRecoverableRunContext(backend, runID, originTaskID, 1)
	workerID := "worker-1"
	expiresAt := time.Now().Add(-time.Minute)
	backend.runs[runID].ClaimedByWorker = &workerID
	backend.runs[runID].ClaimExpiresAt = &expiresAt

	firstOutcome, err := svc.processExpiredClaimRun(context.Background(), time.Now(), backend.runs[runID])
	require.NoError(t, err)
	require.True(t, firstOutcome.detected)
	require.True(t, firstOutcome.recovered)

	secondOutcome, err := svc.processExpiredClaimRun(context.Background(), time.Now(), backend.runs[runID])
	require.NoError(t, err)
	require.False(t, secondOutcome.detected)
	require.False(t, secondOutcome.recovered)
}

func TestAgentCommandParamsFromProtoMatchesHTTPShape(t *testing.T) {
	policy := map[string]any{
		"host":             "sandbox",
		"security":         "full",
		"ask":              "off",
		"runtime_type":     "gvisor",
		"workspace_access": "rw",
		"network_enabled":  true,
		"interactive":      false,
		"resources": map[string]any{
			"cpu":    "1",
			"memory": "1Gi",
		},
		"retry": map[string]any{
			"max_attempts": 4,
			"delay_ms":     250,
		},
	}
	inputProvenance := map[string]any{
		"source":         "chat",
		"message_id":     "msg-123",
		"channel":        "web",
		"tool_call_id":   "tool-abc",
		"correlation_id": "corr-xyz",
	}
	routing := map[string]any{
		"to":            "user-1",
		"reply_to":      "assistant-1",
		"channel":       "chat",
		"reply_channel": "thread",
		"account_id":    "acct-1",
		"thread_id":     "thread-1",
		"group_id":      "group-1",
	}
	attachment := map[string]any{
		"type": "text",
		"path": "/workspace/memory/input.txt",
	}

	deliver := true
	timeoutMs := int32(45_000)
	protoReq := &pb.CreateTaskRequest{
		Message:           "run this task",
		AgentId:           "agent-1",
		SessionId:         "session-1",
		SessionKey:        "session-key",
		IdempotencyKey:    "idem-1",
		Lane:              "lane-1",
		ExtraSystemPrompt: "extra prompt",
		Deliver:           &deliver,
		TimeoutMs:         &timeoutMs,
		Policy:            mustStruct(t, policy),
		InputProvenance:   mustStruct(t, inputProvenance),
		Routing:           mustStruct(t, routing),
		Attachments:       []*structpb.Struct{mustStruct(t, attachment)},
		Label:             "label-1",
		SpawnedBy:         "sdk-test",
	}

	grpcParams, err := agentCommandParamsFromProto(protoReq)
	require.NoError(t, err)

	httpBody := map[string]any{
		"message":             protoReq.Message,
		"agent_id":            protoReq.AgentId,
		"session_id":          protoReq.SessionId,
		"session_key":         protoReq.SessionKey,
		"idempotency_key":     protoReq.IdempotencyKey,
		"lane":                protoReq.Lane,
		"extra_system_prompt": protoReq.ExtraSystemPrompt,
		"deliver":             deliver,
		"timeout_ms":          int(timeoutMs),
		"policy":              policy,
		"input_provenance":    inputProvenance,
		"routing":             routing,
		"attachments":         []map[string]any{attachment},
		"label":               protoReq.Label,
		"spawned_by":          protoReq.SpawnedBy,
	}
	httpJSON, err := json.Marshal(httpBody)
	require.NoError(t, err)

	var httpParams orchestration.AgentCommandParams
	require.NoError(t, json.Unmarshal(httpJSON, &httpParams))

	grpcJSON, err := json.Marshal(grpcParams)
	require.NoError(t, err)
	httpParamsJSON, err := json.Marshal(httpParams)
	require.NoError(t, err)

	require.JSONEq(t, string(httpParamsJSON), string(grpcJSON))
}

func mustStruct(t *testing.T, values map[string]any) *structpb.Struct {
	t.Helper()
	st, err := structpb.NewStruct(values)
	require.NoError(t, err)
	return st
}
