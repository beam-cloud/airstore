package orchestration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type finalizeRunAttemptBackend struct {
	repository.BackendRepository
	run               *types.AgentRun
	task              *types.AgentTask
	attemptStatus     types.AgentAttemptStatus
	runStatus         types.AgentRunStatus
	snapshots         []*types.AgentRunSnapshot
	currentRunUpdates []types.CurrentRunTaskStateUpdate
	taskStateUpdates  []types.TaskStateUpdate
	currentRunUpdated bool
	sleepCalls        int
	sleptWakeReason   string
	upsertWatchCalls  int
	upsertWatchErr    error
	deleteWatchCalls  int
}

func (b *finalizeRunAttemptBackend) GetAgentRunByID(_ context.Context, runID string) (*types.AgentRun, error) {
	if b.run != nil && b.run.ID == runID {
		return b.run, nil
	}
	return nil, nil
}

func (b *finalizeRunAttemptBackend) GetTaskByID(_ context.Context, taskID string) (*types.AgentTask, error) {
	if b.task != nil && b.task.ID == taskID {
		return b.task, nil
	}
	return nil, nil
}

func (b *finalizeRunAttemptBackend) UpdateAgentRunAttemptResult(
	_ context.Context,
	_ string,
	status types.AgentAttemptStatus,
	_ *int,
	_ time.Time,
	_ *string,
) error {
	b.attemptStatus = status
	return nil
}

func (b *finalizeRunAttemptBackend) ClearAgentRunClaim(_ context.Context, _ string) error {
	return nil
}

func (b *finalizeRunAttemptBackend) UpdateAgentRunLifecycle(
	_ context.Context,
	_ string,
	status types.AgentRunStatus,
	_ *time.Time,
	endedAt *time.Time,
	errorMsg *string,
) error {
	b.runStatus = status
	if b.run != nil {
		b.run.Status = status
		b.run.EndedAt = endedAt
		b.run.Error = errorMsg
	}
	return nil
}

func (b *finalizeRunAttemptBackend) IncrementAgentRunSnapshotSeq(_ context.Context, _ string) (int64, error) {
	return int64(len(b.snapshots) + 1), nil
}

func (b *finalizeRunAttemptBackend) AppendAgentRunSnapshot(_ context.Context, snap *types.AgentRunSnapshot) error {
	copied := *snap
	b.snapshots = append(b.snapshots, &copied)
	return nil
}

func (b *finalizeRunAttemptBackend) UpdateTaskStateIfCurrentRun(
	_ context.Context,
	update types.CurrentRunTaskStateUpdate,
) (bool, error) {
	b.currentRunUpdates = append(b.currentRunUpdates, update)
	if !b.currentRunUpdated {
		return false, nil
	}
	if b.task != nil {
		b.task.State = update.State
		b.task.TargetRunID = update.TargetRunID
		b.task.InputKind = ""
		b.task.WaitingSummary = nil
		b.task.CurrentBlocker = nil
		b.task.CurrentBlockerID = nil
	}
	return true, nil
}

func (b *finalizeRunAttemptBackend) UpdateTaskState(
	_ context.Context,
	update types.TaskStateUpdate,
) error {
	b.taskStateUpdates = append(b.taskStateUpdates, update)
	if b.task != nil {
		b.task.State = update.State
		b.task.TargetRunID = update.TargetRunID
		b.task.InputKind = ""
		b.task.WaitingSummary = nil
		b.task.CurrentBlocker = nil
		b.task.CurrentBlockerID = nil
	}
	return nil
}

func (b *finalizeRunAttemptBackend) SleepTaskWithOutbox(
	_ context.Context,
	taskID string,
	expectedRunID string,
	_ time.Time,
	wakeReason string,
	_ []*types.TaskWakeAgendaItem,
	_ *types.OrchestrationOutboxEvent,
) (bool, error) {
	b.sleepCalls++
	b.sleptWakeReason = wakeReason
	if b.task != nil && b.task.ID == taskID {
		b.task.State = types.AgentTaskStateSleeping
		b.task.TargetRunID = nil
		b.task.WakeReason = &wakeReason
	}
	return b.run != nil && b.run.ID == expectedRunID, nil
}

func (b *finalizeRunAttemptBackend) UpsertTaskSourceWatches(_ context.Context, _ uint, _ string, _ []repository.TaskSourceWatch) error {
	b.upsertWatchCalls++
	return b.upsertWatchErr
}

func (b *finalizeRunAttemptBackend) DeleteTaskSourceWatches(_ context.Context, _ string) error {
	b.deleteWatchCalls++
	return nil
}

type failingRuntimeSourceWatchRegistrar struct {
	err          error
	cleanupCalls int
}

func (f *failingRuntimeSourceWatchRegistrar) RegisterTaskSourceWatches(
	context.Context,
	*types.AgentTask,
	*types.RunExecutionWakeSignal,
	[]*types.SourceWatchRequest,
) (*types.TaskBlockerSpec, error) {
	return nil, f.err
}

func (f *failingRuntimeSourceWatchRegistrar) CleanupTaskSourceWatches(
	context.Context,
	*types.AgentTask,
) error {
	f.cleanupCalls++
	return nil
}

func (f *failingRuntimeSourceWatchRegistrar) HasTaskSourceWatches(context.Context, *types.AgentTask) bool {
	return false
}

type emptyRuntimeSourceWatchRegistrar struct {
	cleanupCalls int
}

func (e *emptyRuntimeSourceWatchRegistrar) RegisterTaskSourceWatches(
	context.Context,
	*types.AgentTask,
	*types.RunExecutionWakeSignal,
	[]*types.SourceWatchRequest,
) (*types.TaskBlockerSpec, error) {
	return nil, nil
}

func (e *emptyRuntimeSourceWatchRegistrar) CleanupTaskSourceWatches(
	context.Context,
	*types.AgentTask,
) error {
	e.cleanupCalls++
	return nil
}

func (e *emptyRuntimeSourceWatchRegistrar) HasTaskSourceWatches(context.Context, *types.AgentTask) bool {
	return false
}

type blockerRuntimeSourceWatchRegistrar struct {
	cleanupCalls int
}

func (b *blockerRuntimeSourceWatchRegistrar) RegisterTaskSourceWatches(
	context.Context,
	*types.AgentTask,
	*types.RunExecutionWakeSignal,
	[]*types.SourceWatchRequest,
) (*types.TaskBlockerSpec, error) {
	return types.NewSourceWatchBlockerSpec(
		"Waiting for source updates.",
		"Waiting for source updates.",
		[]types.SourceWatchBlockerEntry{{
			Integration: "gmail",
			Path:        "/sources/gmail/__followup__task",
			EntityLabel: "Reply thread",
		}},
	), nil
}

func (b *blockerRuntimeSourceWatchRegistrar) CleanupTaskSourceWatches(
	context.Context,
	*types.AgentTask,
) error {
	b.cleanupCalls++
	return nil
}

func (b *blockerRuntimeSourceWatchRegistrar) HasTaskSourceWatches(context.Context, *types.AgentTask) bool {
	return true
}

func TestFinalizeRunAttemptMarksTaskErrorWhenSettlementFails(t *testing.T) {
	runID := "run-1"
	task := &types.AgentTask{
		ID:          "task-1",
		WorkspaceID: 7,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend := &finalizeRunAttemptBackend{
		run: &types.AgentRun{
			ID:           runID,
			WorkspaceID:  task.WorkspaceID,
			OriginTaskID: task.ID,
			Status:       types.AgentRunStatusRunning,
		},
		task:              task,
		currentRunUpdated: true,
	}
	registrar := &failingRuntimeSourceWatchRegistrar{err: fmt.Errorf("watch registration failed")}
	loops := &RuntimeLoops{
		backend:              backend,
		sourceWatchRegistrar: registrar,
	}
	attempt := &types.AgentRunAttempt{
		ID:     "attempt-1",
		RunID:  runID,
		Status: types.AgentAttemptStatusRunning,
	}

	err := loops.finalizeRunAttempt(context.Background(), attempt, "exec-1", 0, "", &types.RunExecutionPostRun{
		SourceWatchRequests: []*types.SourceWatchRequest{{
			Integration: string(types.SourceWeb),
			Query:       "site:example.com follow-up",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "apply source watch follow-up") {
		t.Fatalf("expected settlement error, got %v", err)
	}
	if backend.attemptStatus != types.AgentAttemptStatusOK {
		t.Fatalf("attempt status = %q, want %q", backend.attemptStatus, types.AgentAttemptStatusOK)
	}
	if backend.runStatus != types.AgentRunStatusOK {
		t.Fatalf("run status = %q, want %q", backend.runStatus, types.AgentRunStatusOK)
	}
	if len(backend.snapshots) != 1 {
		t.Fatalf("snapshot count = %d, want 1", len(backend.snapshots))
	}
	if len(backend.currentRunUpdates) != 1 {
		t.Fatalf("task state update count = %d, want 1", len(backend.currentRunUpdates))
	}
	if got := backend.currentRunUpdates[0].State; got != types.AgentTaskStateError {
		t.Fatalf("task state = %q, want %q", got, types.AgentTaskStateError)
	}
	if task.State != types.AgentTaskStateError {
		t.Fatalf("task state after failure = %q, want %q", task.State, types.AgentTaskStateError)
	}
	if registrar.cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", registrar.cleanupCalls)
	}
}

func TestFinalizeRunAttemptMarksTaskErrorWhenSourceWatchApplyReturnsNoBlocker(t *testing.T) {
	runID := "run-no-blocker"
	task := &types.AgentTask{
		ID:          "task-no-blocker",
		WorkspaceID: 17,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend := &finalizeRunAttemptBackend{
		run: &types.AgentRun{
			ID:           runID,
			WorkspaceID:  task.WorkspaceID,
			OriginTaskID: task.ID,
			Status:       types.AgentRunStatusRunning,
		},
		task:              task,
		currentRunUpdated: true,
	}
	registrar := &emptyRuntimeSourceWatchRegistrar{}
	loops := &RuntimeLoops{
		backend:              backend,
		sourceWatchRegistrar: registrar,
	}
	attempt := &types.AgentRunAttempt{
		ID:     "attempt-no-blocker",
		RunID:  runID,
		Status: types.AgentAttemptStatusRunning,
	}

	err := loops.finalizeRunAttempt(context.Background(), attempt, "exec-no-blocker", 0, "", &types.RunExecutionPostRun{
		SourceWatchRequests: []*types.SourceWatchRequest{{
			Integration: string(types.SourceWeb),
			Query:       "site:example.com follow-up",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "did not materialize any source views") {
		t.Fatalf("expected materialization error, got %v", err)
	}
	if task.State != types.AgentTaskStateError {
		t.Fatalf("task state after failure = %q, want %q", task.State, types.AgentTaskStateError)
	}
	if registrar.cleanupCalls != 1 {
		t.Fatalf("cleanup calls = %d, want 1", registrar.cleanupCalls)
	}
}

func TestFinalizeRunAttemptSleepsWhenSourceWatchFollowUpHasWakeSignal(t *testing.T) {
	runID := "run-sleep-source-watch"
	task := &types.AgentTask{
		ID:          "task-sleep-source-watch",
		WorkspaceID: 23,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend := &finalizeRunAttemptBackend{
		run: &types.AgentRun{
			ID:           runID,
			WorkspaceID:  task.WorkspaceID,
			OriginTaskID: task.ID,
			Status:       types.AgentRunStatusRunning,
		},
		task: task,
	}
	registrar := &blockerRuntimeSourceWatchRegistrar{}
	loops := &RuntimeLoops{
		backend:              backend,
		sourceWatchRegistrar: registrar,
		lifecycle:            NewTaskLifecycle(backend, nil, nil),
	}
	attempt := &types.AgentRunAttempt{
		ID:     "attempt-sleep-source-watch",
		RunID:  runID,
		Status: types.AgentAttemptStatusRunning,
	}

	err := loops.finalizeRunAttempt(context.Background(), attempt, "exec-sleep-source-watch", 0, "", &types.RunExecutionPostRun{
		WakeSignal: &types.RunExecutionWakeSignal{
			DelayMinutes: 30,
			Reason:       "Check for replies to cold outreach email sent to luke@beam.cloud",
		},
		SourceWatchRequests: []*types.SourceWatchRequest{{
			Integration: string(types.SourceGmail),
			ThreadID:    "thread-1",
			EntityLabel: "Reply from luke@beam.cloud to Beam outreach",
			Reason:      "Check for replies to cold outreach email sent to luke@beam.cloud",
		}},
	})
	if err != nil {
		t.Fatalf("finalizeRunAttempt returned error: %v", err)
	}
	if got, want := backend.upsertWatchCalls, 1; got != want {
		t.Fatalf("upsert watch calls (correlation index) = %d, want %d", got, want)
	}
	if got, want := backend.sleepCalls, 1; got != want {
		t.Fatalf("sleep calls = %d, want %d", got, want)
	}
	if got, want := task.State, types.AgentTaskStateSleeping; got != want {
		t.Fatalf("task state = %q, want %q", got, want)
	}
	if got, want := registrar.cleanupCalls, 0; got != want {
		t.Fatalf("cleanup calls = %d, want %d", got, want)
	}
	if got, want := backend.deleteWatchCalls, 0; got != want {
		t.Fatalf("delete watch calls = %d, want %d", got, want)
	}
	if got, want := backend.sleptWakeReason, "Check for replies to cold outreach email sent to luke@beam.cloud"; got != want {
		t.Fatalf("wake reason = %q, want %q", got, want)
	}
}

func TestFinalizeRunAttemptSynthesizesWakeForSourceWatchFollowUp(t *testing.T) {
	runID := "run-synth-source-watch"
	task := &types.AgentTask{
		ID:          "task-synth-source-watch",
		WorkspaceID: 29,
		State:       types.AgentTaskStateRunning,
		TargetRunID: &runID,
	}
	backend := &finalizeRunAttemptBackend{
		run: &types.AgentRun{
			ID:           runID,
			WorkspaceID:  task.WorkspaceID,
			OriginTaskID: task.ID,
			Status:       types.AgentRunStatusRunning,
		},
		task: task,
	}
	registrar := &blockerRuntimeSourceWatchRegistrar{}
	loops := &RuntimeLoops{
		backend:              backend,
		sourceWatchRegistrar: registrar,
		lifecycle:            NewTaskLifecycle(backend, nil, nil),
	}
	attempt := &types.AgentRunAttempt{
		ID:     "attempt-synth-source-watch",
		RunID:  runID,
		Status: types.AgentAttemptStatusRunning,
	}

	err := loops.finalizeRunAttempt(context.Background(), attempt, "exec-synth-source-watch", 0, "", &types.RunExecutionPostRun{
		SourceWatchRequests: []*types.SourceWatchRequest{{
			Integration: string(types.SourceGmail),
			ThreadID:    "thread-2",
			EntityLabel: "Quick question about your dev environments",
			Reason:      "Check for replies to cold outreach email sent to luke@beam.cloud",
		}},
	})
	if err != nil {
		t.Fatalf("finalizeRunAttempt returned error: %v", err)
	}
	if got, want := backend.upsertWatchCalls, 1; got != want {
		t.Fatalf("upsert watch calls (correlation index) = %d, want %d", got, want)
	}
	if got, want := backend.sleepCalls, 1; got != want {
		t.Fatalf("sleep calls = %d, want %d", got, want)
	}
	if got, want := task.State, types.AgentTaskStateSleeping; got != want {
		t.Fatalf("task state = %q, want %q", got, want)
	}
	if got, want := backend.sleptWakeReason, "Check for replies to cold outreach email sent to luke@beam.cloud"; got != want {
		t.Fatalf("wake reason = %q, want %q", got, want)
	}
	if got, want := registrar.cleanupCalls, 0; got != want {
		t.Fatalf("cleanup calls = %d, want %d", got, want)
	}
	if got, want := backend.deleteWatchCalls, 0; got != want {
		t.Fatalf("delete watch calls = %d, want %d", got, want)
	}
}

func TestSourceWatchWakePromptPinsGmailThreadForReplies(t *testing.T) {
	prompt := sourceWatchWakePrompt([]*types.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		ThreadID:    "thread-123",
		EntityLabel: "Reply from luke@beam.cloud to Beam outreach",
	}}, "")
	if !strings.Contains(prompt, "exact Gmail thread `thread-123`") {
		t.Fatalf("prompt missing exact thread guidance: %q", prompt)
	}
	if !strings.Contains(prompt, "--thread-id thread-123") {
		t.Fatalf("prompt missing Gmail thread-id guidance: %q", prompt)
	}
}

func TestFinalizeRunAttemptForcesTaskErrorWhenCurrentRunUpdateMisses(t *testing.T) {
	runID := "run-2"
	task := &types.AgentTask{
		ID:          "task-2",
		WorkspaceID: 9,
		State:       types.AgentTaskStateRunning,
	}
	backend := &finalizeRunAttemptBackend{
		run: &types.AgentRun{
			ID:           runID,
			WorkspaceID:  task.WorkspaceID,
			OriginTaskID: task.ID,
			Status:       types.AgentRunStatusRunning,
		},
		task:              task,
		currentRunUpdated: false,
	}
	registrar := &failingRuntimeSourceWatchRegistrar{err: fmt.Errorf("registration error")}
	loops := &RuntimeLoops{
		backend:              backend,
		sourceWatchRegistrar: registrar,
	}
	attempt := &types.AgentRunAttempt{
		ID:     "attempt-2",
		RunID:  runID,
		Status: types.AgentAttemptStatusRunning,
	}

	err := loops.finalizeRunAttempt(context.Background(), attempt, "exec-2", 0, "", &types.RunExecutionPostRun{
		SourceWatchRequests: []*types.SourceWatchRequest{{
			Integration: string(types.SourceWeb),
			Query:       "site:example.com missed-update",
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "apply source watch follow-up") {
		t.Fatalf("expected settlement error, got %v", err)
	}
	if len(backend.currentRunUpdates) != 1 {
		t.Fatalf("current run update count = %d, want 1", len(backend.currentRunUpdates))
	}
	if len(backend.taskStateUpdates) != 1 {
		t.Fatalf("forced task state update count = %d, want 1", len(backend.taskStateUpdates))
	}
	if got := backend.taskStateUpdates[0].State; got != types.AgentTaskStateError {
		t.Fatalf("forced task state = %q, want %q", got, types.AgentTaskStateError)
	}
	if task.State != types.AgentTaskStateError {
		t.Fatalf("task state after forced error = %q, want %q", task.State, types.AgentTaskStateError)
	}
}

func TestTaskLifecycleValidTransitionsIncludeError(t *testing.T) {
	if !isValidTransition(types.AgentTaskStateRunning, types.AgentTaskStateError) {
		t.Fatal("expected running -> error transition to be valid")
	}
	if !isValidTransition(types.AgentTaskStateWaiting, types.AgentTaskStateError) {
		t.Fatal("expected waiting -> error transition to be valid")
	}
}
