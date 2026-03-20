package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	agentsignal "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client"
	signaltypes "github.com/beam-cloud/airstore/pkg/worker/agentsignal/baml_client/types"
	"github.com/rs/zerolog/log"
)

const (
	DefaultBetweenTurnsTimeout  = 60 * time.Second
	mountFlushGracePeriod       = 10 * time.Second
	sessionLeaseTTL             = 30 * time.Second
	sessionLeaseRenewInterval   = 10 * time.Second
	runInteractionTTL           = 30 * time.Minute
	subagentPollInterval        = 10 * time.Second
	subagentMaxWait             = 30 * time.Minute
	subagentProbeTimeout        = 15 * time.Second
	terminalRingBufSize         = 256 * 1024
	approvalMessageExtractLimit = 24000
)

// subagentWaitOutcome describes why waitForSubagents returned.
type subagentWaitOutcome int

const (
	subagentNoneDetected subagentWaitOutcome = iota
	subagentFinished
	subagentMaxWaitReached
	subagentSessionCancelled
)

func (o subagentWaitOutcome) String() string {
	switch o {
	case subagentNoneDetected:
		return "none_detected"
	case subagentFinished:
		return "finished"
	case subagentMaxWaitReached:
		return "max_wait_reached"
	case subagentSessionCancelled:
		return "session_cancelled"
	default:
		return "unknown"
	}
}

// subagentProbeArgs detects real background Claude processes while excluding
// helper hooks that live under .claude/ (e.g. dump-stop-message.js) and the
// probe shell itself.
var subagentProbeArgs = []string{"/bin/sh", "-c",
	`mypid=$$; for pid in $(pgrep -f claude 2>/dev/null); do ` +
		`[ "$pid" = "$mypid" ] && continue; ` +
		`cmdline=$(tr '\0' ' ' < /proc/$pid/cmdline 2>/dev/null); ` +
		`case "$cmdline" in */.claude/*) continue;; *) exit 0;; esac; ` +
		`done; exit 1`,
}

// ---------------------------------------------------------------------------
// Interaction & task state helpers
// ---------------------------------------------------------------------------

func (w *Worker) setRunInteractionState(ctx context.Context, task types.RunExecution, state types.RunInteractionState) {
	newSessionStateBridge(w).setRunInteractionState(ctx, task, state)
}

func (w *Worker) setOriginTaskState(ctx context.Context, task types.RunExecution, update types.TaskLiveUpdate) {
	newSessionStateBridge(w).setOriginTaskState(ctx, task, update)
}

func waitingBlockerPayload(
	inputKind types.InputKind,
	waitingSummary string,
	assistantMessage string,
) map[string]any {
	return types.NewTaskBlockerPayload(inputKind, waitingSummary, assistantMessage).ToMap()
}

func buildWaitingBlockerSpec(
	task types.RunExecution,
	inputKind types.InputKind,
	waitingSummary string,
	assistantMessage string,
	outputIDs []string,
) *types.TaskBlockerSpec {
	if inputKind == "" {
		return nil
	}
	spec := &types.TaskBlockerSpec{
		Kind:        types.TaskBlockerKindForInputKind(inputKind),
		InputKind:   inputKind,
		PayloadJSON: waitingBlockerPayload(inputKind, waitingSummary, assistantMessage),
		OutputIDs:   outputIDs,
	}
	if inputKind == types.InputKindApproveReject {
		if waitGroupID := approvalWaitGroupID(task, assistantMessage); waitGroupID != "" {
			spec.WaitGroupID = &waitGroupID
		}
	}
	return spec
}

func trackedOutputIDSet(tracker *taskOutputTracker) map[string]struct{} {
	seen := make(map[string]struct{})
	if tracker == nil {
		return seen
	}
	for _, summary := range tracker.TrackedOutputSummaries() {
		if id := strings.TrimSpace(summary.OutputID); id != "" {
			seen[id] = struct{}{}
		}
	}
	return seen
}

func diffTrackedOutputIDs(tracker *taskOutputTracker, before map[string]struct{}) []string {
	if tracker == nil {
		return nil
	}
	var outputIDs []string
	for _, summary := range tracker.TrackedOutputSummaries() {
		id := strings.TrimSpace(summary.OutputID)
		if id == "" {
			continue
		}
		if _, ok := before[id]; ok {
			continue
		}
		outputIDs = append(outputIDs, id)
	}
	return outputIDs
}

// ---------------------------------------------------------------------------
// Session checkpoint
// ---------------------------------------------------------------------------

func (w *Worker) recordSessionCheckpoint(ctx context.Context, task types.RunExecution, mountSource string, env map[string]string) error {
	return newSessionStateBridge(w).recordSessionCheckpoint(ctx, task, mountSource, env)
}

// ---------------------------------------------------------------------------
// Main entry point: run an interactive task
// ---------------------------------------------------------------------------

func (w *Worker) runInteractiveTask(ctx context.Context, task types.RunExecution) (*types.RunExecutionResult, error) {
	if w.terminalIO == nil {
		return nil, fmt.Errorf("terminal transport is not configured")
	}
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		w.setRunInteractionState(c, task, types.RunInteractionStateClosed)
	}()

	sessionID := strings.TrimSpace(task.Env[agentSessionIDEnvKey])
	release, leaseErr := w.acquireSessionLease(runCtx, task, sessionID, runCancel)
	if release == nil && sessionID != "" {
		if leaseErr != nil {
			return nil, fmt.Errorf("session lease: %w", leaseErr)
		}
		return nil, fmt.Errorf("session %s already in use", sessionID)
	}
	defer release()

	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)
	env := w.sandboxManager.copyTaskEnv(task)
	if cr, ok := w.sandboxManager.ResolveRunner(task, env).(*ClaudeCodeRunner); ok {
		cr.injectEnv(env)
	}
	mountSource := w.sandboxManager.mountFilesystem(runCtx, task)
	cfg := w.sandboxManager.buildTaskSandboxConfig(task, []string{"sleep", "infinity"}, env, mountSource)

	if _, err := w.sandboxManager.Create(cfg); err != nil {
		w.sandboxManager.cleanupMount(task.ExternalId)
		return nil, fmt.Errorf("create sandbox: %w", err)
	}
	if err := w.sandboxManager.Start(sandboxID); err != nil {
		w.sandboxManager.publishStatus(ctx, task.ExternalId, types.RunExecutionStatusFailed, nil, err.Error())
		w.sandboxManager.Delete(sandboxID, true)
		w.sandboxManager.cleanupMount(task.ExternalId)
		return nil, fmt.Errorf("start sandbox: %w", err)
	}

	setupGitInsideSandbox(runCtx, w.sandboxManager.runtime, sandboxID, env)
	w.sandboxManager.publishStatus(runCtx, task.ExternalId, types.RunExecutionStatusRunning, nil, "")
	w.setRunInteractionState(runCtx, task, types.RunInteractionStateWorking)

	result := w.runInteractiveSession(runCtx, task, sandboxID, mountSource)
	w.setRunInteractionState(runCtx, task, types.RunInteractionStateClosed)

	_ = w.sandboxManager.Delete(sandboxID, true)
	time.Sleep(mountFlushGracePeriod)

	if result != nil && result.Error == "" {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := w.recordSessionCheckpoint(c, task, mountSource, env); err != nil {
			addTaskExecutionContext(log.Warn().Err(err), task).Msg("checkpoint failed")
		}
		cancel()
	}

	release()
	w.sandboxManager.cleanupMount(task.ExternalId)
	return result, nil
}

// acquireSessionLease returns a release func (call it when done) and an error.
// Returns a no-op func and nil error if sessionID is empty.
// Returns (nil, err) on infrastructure failure, (nil, nil) if the lease is contested.
func (w *Worker) acquireSessionLease(ctx context.Context, task types.RunExecution, sessionID string, onLost func()) (func(), error) {
	if sessionID == "" {
		return func() {}, nil
	}
	ownerID := fmt.Sprintf("%s:%s", strings.TrimSpace(w.workerId), task.ExternalId)
	acquired, err := w.terminalIO.AcquireSessionLease(ctx, task.WorkspaceId, sessionID, ownerID, sessionLeaseTTL)
	if err != nil {
		return nil, fmt.Errorf("acquire session lease: %w", err)
	}
	if !acquired {
		return nil, nil
	}
	leaseCtx, leaseCancel := context.WithCancel(ctx)
	go w.heartbeatSessionLease(leaseCtx, task, sessionID, ownerID, onLost)
	var once sync.Once
	return func() {
		once.Do(func() {
			leaseCancel()
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = w.terminalIO.ReleaseSessionLease(c, task.WorkspaceId, sessionID, ownerID)
		})
	}, nil
}

func (w *Worker) heartbeatSessionLease(ctx context.Context, task types.RunExecution, sessionID, ownerID string, onLost func()) {
	ticker := time.NewTicker(sessionLeaseRenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewed, err := w.terminalIO.RenewSessionLease(ctx, task.WorkspaceId, sessionID, ownerID, sessionLeaseTTL)
			if err != nil || !renewed {
				addTaskExecutionContext(log.Warn().Err(err), task).Msg("session lease lost")
				if onLost != nil {
					onLost()
				}
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Interactive session: orchestrates turns, input, subagents
// ---------------------------------------------------------------------------

func (w *Worker) runInteractiveSession(ctx context.Context, task types.RunExecution, sandboxID, mountSource string) *types.RunExecutionResult {
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()

	env := w.sandboxManager.copyTaskEnv(task)
	runner := w.sandboxManager.ResolveRunner(task, env)
	promptPlan := promptTaskPlanForRunner(runner)
	bamlEnv := w.sandboxManager.BamlEnvForRunner(runner)
	activityCh := make(chan struct{}, 1)
	var idleTimedOut atomic.Bool

	// Heartbeat
	var touchHeartbeat func()
	var checkHeartbeat func() bool
	if hr, ok := runner.(HeartbeatRunner); ok && mountSource != "" {
		if hbPath, err := hr.SetupHeartbeat(mountSource, env); err == nil {
			checkHeartbeat = func() bool { return hr.CheckHeartbeat(hbPath) }
			touchHeartbeat = func() { _ = os.WriteFile(hbPath, []byte(time.Now().Format(time.RFC3339Nano)), 0o644) }
		}
	}

	// Needs-input detection
	var needsInputRunner NeedsInputRunner
	var needsInputPath string
	if ir, ok := runner.(NeedsInputRunner); ok && mountSource != "" {
		if p, err := ir.SetupNeedsInput(mountSource, env); err == nil {
			needsInputRunner = ir
			needsInputPath = p
		}
	}

	// Idle monitor
	if timeout := w.config.Sandbox.GetInteractiveIdleTimeout(); timeout > 0 {
		go monitorInteractiveSessionIdle(sessionCtx, task.ExternalId, executionContextFromTask(task), sessionCancel, timeout, activityCh, &idleTimedOut, checkHeartbeat)
	}

	// Cancellation watcher
	cancelCleanup := w.watchTaskCancellation(sessionCtx, task, func() {
		sessionCancel()
		w.sandboxManager.Stop(sandboxID, true)
	})
	defer cancelCleanup()

	// Output writers
	outputPipeline := w.sandboxManager.taskOutputPipeline(sessionCtx, task, promptPlan)
	mirror := NewTaskStreamOutput(task.ExternalId, "stdout", outputPipeline.writers...)
	defer outputPipeline.Wait()
	defer mirror.Flush()
	tw := &terminalOutputWriter{
		ctx: sessionCtx, taskID: task.ExternalId,
		terminalIO: w.terminalIO, executionCtx: executionContextFromTask(task),
		onActivity: func() {
			signalActivity(activityCh)
			if touchHeartbeat != nil {
				touchHeartbeat()
			}
		},
		mirror: mirror, ringBuf: newRingBuffer(terminalRingBufSize),
	}

	// Build the needs-input checker
	var checkNeedsInput func(string) (bool, types.InputKind, string, string)
	if needsInputRunner != nil {
		checkNeedsInput = w.buildNeedsInputChecker(sessionCtx, task, needsInputRunner, needsInputPath, tw, bamlEnv)
	}

	start := time.Now()
	var runErr error
	var needsInput bool
	var inputKind types.InputKind
	var lastPrompt string
	var approvalOutputPersisted bool

	if tr, ok := runner.(TurnRunner); ok {
		runErr, needsInput, inputKind, lastPrompt, approvalOutputPersisted = w.runTurnSession(
			sessionCtx, task, sandboxID, tr, env, tw, activityCh, checkNeedsInput, bamlEnv, outputPipeline.tracker,
		)
	} else {
		runErr = w.runGenericPTYSession(sessionCtx, task, sandboxID, tw, activityCh)
	}

	mirror.Flush()
	outputPipeline.Wait()

	sessionAssistantMessage := ""
	if runErr == nil && tw.ringBuf != nil {
		sessionAssistantMessage = extractSessionAssistantMessage(runner, tw.ringBuf.Bytes())
		if sessionAssistantMessage != "" {
			var userMessage *string
			if trimmed := strings.TrimSpace(lastPrompt); trimmed != "" {
				userMessage = &trimmed
			}
			switch {
			case !needsInput:
				if _, err := persistFinalResponseOutput(
					sessionCtx,
					w.gatewayClient,
					task,
					outputPipeline.tracker,
					userMessage,
					sessionAssistantMessage,
					bamlEnv,
					nil,
				); err != nil {
					addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to persist final response output")
				}
			case inputKind == types.InputKindApproveReject && !approvalOutputPersisted:
				if _, err := persistApprovalResponseOutput(
					sessionCtx,
					w.gatewayClient,
					task,
					outputPipeline.tracker,
					userMessage,
					sessionAssistantMessage,
					bamlEnv,
				); err != nil {
					addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to persist approval response output")
				}
			}
		}
	}

	exitCode, errMsg, st := interactiveResult(runErr, idleTimedOut.Load())
	w.sandboxManager.publishStatus(ctx, task.ExternalId, st, &exitCode, errMsg)

	var agentMsg string
	if !needsInput && runErr == nil && needsInputRunner != nil && needsInputPath != "" {
		agentMsg = needsInputRunner.ReadLastMessage(needsInputPath)
	}
	if strings.TrimSpace(agentMsg) == "" {
		agentMsg = sessionAssistantMessage
	}

	var wakeSignal *types.RunExecutionWakeSignal
	if !needsInput && runErr == nil && agentMsg != "" {
		wakeSignal = w.classifyFollowUp(ctx, agentMsg, lastPrompt, mountSource, env, bamlEnv)
	}

	var subtaskReqs []*types.SubtaskRequest
	if wakeSignal != nil && outputPipeline.tracker != nil {
		subtaskReqs = w.classifySubtasks(ctx, outputPipeline.tracker, agentMsg, lastPrompt, bamlEnv)
	}

	return &types.RunExecutionResult{
		ID: task.ExternalId, ExitCode: exitCode, Error: errMsg,
		Duration: time.Since(start), WaitingForInput: needsInput,
		WakeSignal: wakeSignal, SubtaskRequests: subtaskReqs,
	}
}

func extractSessionAssistantMessage(runner AgentExecutionRunner, raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	if outputParser, ok := runner.(OutputParsingRunner); ok {
		if _, _, response, err := outputParser.ParseTurnOutput(raw); err == nil {
			if trimmed := strings.TrimSpace(response); trimmed != "" {
				return trimmed
			}
		}
	}
	if extractor, ok := runner.(ResponseExtractor); ok {
		if trimmed := strings.TrimSpace(extractor.ExtractResponseText(raw, 24000)); trimmed != "" {
			return trimmed
		}
	}
	return strings.TrimSpace(extractAssistantText(raw, 24000))
}

// extractAssistantText scans raw stream-json output, pulls out assistant text
// blocks, and returns the last `limit` characters of the concatenated text.
// This keeps approval and follow-up classification focused on the assistant's
// visible response instead of tool traffic.
func extractAssistantText(raw []byte, limit int) string {
	var texts []string
	totalLen := 0

	for _, line := range bytes.Split(raw, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] != '{' {
			continue
		}

		var envelope struct {
			Type    string `json:"type"`
			Message *struct {
				Content []struct {
					Type string `json:"type"`
					Text string `json:"text"`
				} `json:"content"`
			} `json:"message"`
			Result  string `json:"result"`
			IsError bool   `json:"is_error"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			continue
		}

		switch envelope.Type {
		case "assistant":
			if envelope.Message == nil {
				continue
			}
			for _, block := range envelope.Message.Content {
				if block.Type == "text" && block.Text != "" {
					texts = append(texts, block.Text)
					totalLen += len(block.Text)
				}
			}
		case "result":
			if !envelope.IsError && envelope.Result != "" {
				texts = append(texts, envelope.Result)
				totalLen += len(envelope.Result)
			}
		}
	}

	if totalLen == 0 {
		return ""
	}

	var buf bytes.Buffer
	for i, text := range texts {
		if i > 0 {
			buf.WriteString("\n\n")
		}
		buf.WriteString(text)
	}
	s := buf.String()
	if limit > 0 && len(s) > limit {
		s = s[len(s)-limit:]
		for len(s) > 0 && s[0]&0xC0 == 0x80 {
			s = s[1:]
		}
	}
	return s
}

func (w *Worker) buildNeedsInputChecker(
	ctx context.Context, task types.RunExecution,
	runner NeedsInputRunner, markerPath string,
	tw *terminalOutputWriter, bamlEnv map[string]string,
) func(string) (bool, types.InputKind, string, string) {
	return newWorkerSessionRunner(w).buildNeedsInputChecker(ctx, task, runner, markerPath, tw, bamlEnv)
}

type turnInputKindClassifier func(context.Context, string, map[string]string) types.InputKind

func classifyNeedsInputKindWithFallback(
	ctx context.Context,
	current types.InputKind,
	assistantMessage string,
	bamlEnv map[string]string,
	classify turnInputKindClassifier,
) types.InputKind {
	if current == types.InputKindApproveReject {
		return current
	}
	if strings.TrimSpace(assistantMessage) == "" || classify == nil {
		return current
	}
	if inferred := classify(ctx, assistantMessage, bamlEnv); inferred != "" {
		return inferred
	}
	return current
}

func classifyNeedsInputKindWithBAML(ctx context.Context, assistantMessage string, bamlEnv map[string]string) types.InputKind {
	assistantMessage = strings.TrimSpace(assistantMessage)
	if assistantMessage == "" {
		return ""
	}
	cls, err := agentsignal.ClassifyTurn(ctx, assistantMessage, agentsignal.WithEnv(bamlEnv))
	if err != nil || cls.Outcome != signaltypes.TurnOutcomeNEEDS_INPUT || cls.Input_kind == nil {
		return ""
	}
	return types.InputKind(strings.ToLower(string(*cls.Input_kind)))
}

func persistApprovalOutputBeforeWaiting(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
	prompt string,
	assistantMessage string,
	bamlEnv map[string]string,
) ([]string, bool) {
	return persistApprovalOutputBeforeWaitingWithFunc(
		ctx, client, task, tracker, prompt, assistantMessage, bamlEnv, persistApprovalResponseOutput,
	)
}

func persistApprovalOutputBeforeWaitingWithFunc(
	ctx context.Context,
	client taskOutputClient,
	task types.RunExecution,
	tracker *taskOutputTracker,
	prompt string,
	assistantMessage string,
	bamlEnv map[string]string,
	persist func(
		context.Context,
		taskOutputClient,
		types.RunExecution,
		*taskOutputTracker,
		*string,
		string,
		map[string]string,
	) (bool, error),
) ([]string, bool) {
	if client == nil {
		return nil, false
	}
	assistantMessage = strings.TrimSpace(assistantMessage)
	if assistantMessage == "" || len(assistantMessage) < minApprovalOutputLen {
		return nil, false
	}
	var userMessage *string
	if trimmed := strings.TrimSpace(prompt); trimmed != "" {
		userMessage = &trimmed
	}
	before := trackedOutputIDSet(tracker)
	created, err := persist(ctx, client, task, tracker, userMessage, assistantMessage, bamlEnv)
	if err != nil {
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to persist approval response output before waiting")
		return nil, false
	}
	return diffTrackedOutputIDs(tracker, before), created
}

// tryBuildApprovalSummary extracts a structured summary of what the agent is
// asking the user to approve. Approval remains a single yes/no decision over
// the whole action; per-entity granularity is expressed through subtasks.
func (w *Worker) tryBuildApprovalSummary(ctx context.Context, assistantText string, bamlEnv map[string]string) string {
	if assistantText == "" {
		return ""
	}
	summary, err := agentsignal.ExtractApprovalSummary(ctx, assistantText, agentsignal.WithEnv(bamlEnv))
	if err != nil {
		return ""
	}
	b, _ := json.Marshal(map[string]string{
		"summary": summary.Summary,
		"details": summary.Details,
	})
	return string(b)
}

// ---------------------------------------------------------------------------
// Turn session: the core turn loop
// ---------------------------------------------------------------------------

func (w *Worker) runTurnSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	activityCh chan<- struct{},
	checkNeedsInput func(string) (bool, types.InputKind, string, string),
	bamlEnv map[string]string,
	tracker *taskOutputTracker,
) (error, bool, types.InputKind, string, bool) {
	return newWorkerSessionRunner(w).runTurnSession(
		ctx,
		task,
		sandboxID,
		runner,
		env,
		stdout,
		activityCh,
		checkNeedsInput,
		bamlEnv,
		tracker,
	)
}

// ---------------------------------------------------------------------------
// Subagent monitoring
// ---------------------------------------------------------------------------

func (w *Worker) waitForSubagents(ctx context.Context, task types.RunExecution, sandboxID string, activityCh chan<- struct{}) subagentWaitOutcome {
	return newSubagentWatcher(w).waitForSubagents(ctx, task, sandboxID, activityCh)
}

func (w *Worker) waitForSubagentsWithTiming(
	ctx context.Context, task types.RunExecution, sandboxID string, activityCh chan<- struct{},
	pollInterval, maxWait, probeTimeout time.Duration,
) subagentWaitOutcome {
	return newSubagentWatcher(w).waitForSubagentsWithTiming(ctx, task, sandboxID, activityCh, pollInterval, maxWait, probeTimeout)
}

// ---------------------------------------------------------------------------
// Input: claim pending + wait for follow-up
// ---------------------------------------------------------------------------

func (w *Worker) claimPendingInput(ctx context.Context, task types.RunExecution) string {
	return newFollowupInputWaiter(w).claimPendingInput(ctx, task)
}

func (w *Worker) tryClaimInput(ctx context.Context, taskID, runID, execID string) string {
	return newFollowupInputWaiter(w).tryClaimInput(ctx, taskID, runID, execID)
}

func (w *Worker) waitForFollowupInput(ctx context.Context, task types.RunExecution, timeout time.Duration, activityCh chan<- struct{}) string {
	return newFollowupInputWaiter(w).waitForFollowupInput(ctx, task, timeout, activityCh)
}

// ---------------------------------------------------------------------------
// Turn execution
// ---------------------------------------------------------------------------

func (w *Worker) executeFirstTurn(
	ctx context.Context, task types.RunExecution, sandboxID string,
	runner TurnRunner, env map[string]string, stdout io.Writer, prompt string,
) error {
	return newWorkerSessionRunner(w).executeFirstTurn(ctx, task, sandboxID, runner, env, stdout, prompt)
}

func (w *Worker) executeTurn(
	ctx context.Context, task types.RunExecution, sandboxID string,
	runner TurnRunner, env map[string]string, stdout io.Writer,
	prompt string, mode TurnArgMode,
) error {
	return newWorkerSessionRunner(w).executeTurn(ctx, task, sandboxID, runner, env, stdout, prompt, mode)
}

func (w *Worker) runGenericPTYSession(ctx context.Context, task types.RunExecution, sandboxID string, stdout io.Writer, _ chan<- struct{}) error {
	return newWorkerSessionRunner(w).runGenericPTYSession(ctx, task, sandboxID, stdout, nil)
}

type firstTurnStrategy struct {
	mode TurnArgMode
}

func buildFirstTurnStrategies(env map[string]string) []firstTurnStrategy {
	resume := strings.ToLower(strings.TrimSpace(env[agentResumeSessionEnvKey]))
	if resume != "1" && resume != "true" && resume != "yes" && resume != "on" {
		return []firstTurnStrategy{{mode: TurnArgModeFirstStart}}
	}
	if strings.TrimSpace(env[agentSessionIDEnvKey]) != "" {
		return []firstTurnStrategy{
			{mode: TurnArgModeFirstResumeByID},
			{mode: TurnArgModeFirstResumeLatest},
		}
	}
	return []firstTurnStrategy{{mode: TurnArgModeFirstResumeLatest}}
}

type sessionStateBridge struct {
	worker *Worker
}

func newSessionStateBridge(worker *Worker) sessionStateBridge {
	return sessionStateBridge{worker: worker}
}

func (b sessionStateBridge) setRunInteractionState(ctx context.Context, task types.RunExecution, state types.RunInteractionState) {
	if b.worker == nil || b.worker.terminalIO == nil {
		return
	}
	ec := executionContextFromTask(task)
	if ec.runID == "" {
		return
	}
	activeID := ""
	if state != types.RunInteractionStateClosed {
		activeID = task.ExternalId
	}
	if err := b.worker.terminalIO.SetRunInteraction(ctx, task.WorkspaceId, ec.runID, types.RunInteraction{
		State: state, ActiveExecutionID: activeID,
	}, runInteractionTTL); err != nil {
		addTaskExecutionContext(log.Warn().Err(err).Str("state", string(state)), task).
			Msg("failed to persist run interaction state")
	}
}

func (b sessionStateBridge) setOriginTaskState(ctx context.Context, task types.RunExecution, update types.TaskLiveUpdate) {
	if b.worker == nil {
		return
	}
	ec := executionContextFromTask(task)
	if ec.originTaskID == "" || ec.runID == "" {
		return
	}
	update.TaskID = ec.originTaskID
	update.RunID = ec.runID
	if err := b.worker.gatewayClient.UpdateTaskState(ctx, update); err != nil {
		addTaskExecutionContext(log.Warn().Err(err).Str("target_state", string(update.State)), task).
			Msg("failed to update origin task state")
	}
}

func (b sessionStateBridge) recordSessionCheckpoint(ctx context.Context, task types.RunExecution, mountSource string, env map[string]string) error {
	if b.worker == nil || b.worker.terminalIO == nil {
		return nil
	}
	sessionID := strings.TrimSpace(env[agentSessionIDEnvKey])
	ec := executionContextFromTask(task)
	if sessionID == "" || ec.runID == "" {
		return nil
	}
	cp := &types.SessionCheckpoint{RunID: ec.runID, ExecutionID: task.ExternalId, UpdatedAt: time.Now().UnixMilli()}
	if err := writeClaudeSessionCheckpoint(mountSource, env, cp); err != nil {
		return err
	}
	return b.worker.terminalIO.SetSessionCheckpoint(ctx, task.WorkspaceId, sessionID, cp, 0)
}

type followupInputWaiter struct {
	worker *Worker
}

func newFollowupInputWaiter(worker *Worker) followupInputWaiter {
	return followupInputWaiter{worker: worker}
}

func (w followupInputWaiter) claimPendingInput(ctx context.Context, task types.RunExecution) string {
	ec := executionContextFromTask(task)
	if ec.originTaskID == "" || ec.runID == "" {
		return ""
	}
	return w.tryClaimInput(ctx, ec.originTaskID, ec.runID, task.ExternalId)
}

func (w followupInputWaiter) tryClaimInput(ctx context.Context, taskID, runID, execID string) string {
	if w.worker == nil {
		return ""
	}
	resp, err := w.worker.gatewayClient.ClaimTaskInput(ctx, taskID, runID, execID)
	if err != nil || !resp.Found {
		return ""
	}
	prompt := strings.TrimSpace(resp.Message)
	if prompt == "" {
		_ = w.worker.gatewayClient.AckTaskInput(ctx, resp.InputId)
		return ""
	}
	_ = w.worker.gatewayClient.AckTaskInput(ctx, resp.InputId)
	return prompt
}

func (w followupInputWaiter) waitForFollowupInput(ctx context.Context, task types.RunExecution, timeout time.Duration, activityCh chan<- struct{}) string {
	if w.worker == nil {
		return ""
	}
	ec := executionContextFromTask(task)
	if ec.originTaskID == "" || ec.runID == "" {
		<-ctx.Done()
		return ""
	}

	if prompt := w.tryClaimInput(ctx, ec.originTaskID, ec.runID, task.ExternalId); prompt != "" {
		signalActivity(activityCh)
		return prompt
	}

	var wakeCh <-chan struct{}
	if w.worker.terminalIO != nil {
		if ch, cleanup, err := w.worker.terminalIO.SubscribeInputWake(ctx, task.ExternalId); err == nil {
			wakeCh = ch
			defer cleanup()
		}
	}
	if wakeCh == nil {
		wakeCh = make(chan struct{})
	}

	var timeoutCh <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		timeoutCh = t.C
	}

	for {
		select {
		case <-ctx.Done():
			return ""
		case <-timeoutCh:
			return ""
		case <-wakeCh:
		case <-time.After(2 * time.Second):
		}
		if prompt := w.tryClaimInput(ctx, ec.originTaskID, ec.runID, task.ExternalId); prompt != "" {
			signalActivity(activityCh)
			return prompt
		}
	}
}

type subagentWatcher struct {
	worker *Worker
}

func newSubagentWatcher(worker *Worker) subagentWatcher {
	return subagentWatcher{worker: worker}
}

func (m subagentWatcher) waitForSubagents(ctx context.Context, task types.RunExecution, sandboxID string, activityCh chan<- struct{}) subagentWaitOutcome {
	return m.waitForSubagentsWithTiming(ctx, task, sandboxID, activityCh, subagentPollInterval, subagentMaxWait, subagentProbeTimeout)
}

func (m subagentWatcher) waitForSubagentsWithTiming(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	activityCh chan<- struct{},
	pollInterval, maxWait, probeTimeout time.Duration,
) subagentWaitOutcome {
	if m.worker == nil || m.worker.sandboxManager == nil {
		return subagentNoneDetected
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
	err := m.worker.sandboxManager.ExecCheck(probeCtx, sandboxID, subagentProbeArgs)
	probeCancel()
	if err != nil {
		return subagentNoneDetected
	}

	addTaskExecutionContext(log.Info(), task).Msg("waiting for subagent processes")
	deadline := time.After(maxWait)
	for {
		select {
		case <-ctx.Done():
			addTaskExecutionContext(log.Info(), task).
				Str("outcome", subagentSessionCancelled.String()).
				Msg("subagent wait ended")
			return subagentSessionCancelled
		case <-deadline:
			addTaskExecutionContext(log.Warn(), task).
				Str("outcome", subagentMaxWaitReached.String()).
				Dur("max_wait", maxWait).
				Msg("subagent wait ended")
			return subagentMaxWaitReached
		case <-time.After(pollInterval):
			signalActivity(activityCh)
			probeCtx, probeCancel := context.WithTimeout(ctx, probeTimeout)
			err := m.worker.sandboxManager.ExecCheck(probeCtx, sandboxID, subagentProbeArgs)
			probeTimedOut := probeCtx.Err() != nil && ctx.Err() == nil
			probeCancel()
			if probeTimedOut {
				addTaskExecutionContext(log.Warn(), task).Msg("subagent probe timed out, will retry")
				continue
			}
			if err != nil {
				addTaskExecutionContext(log.Info(), task).
					Str("outcome", subagentFinished.String()).
					Msg("subagent wait ended")
				return subagentFinished
			}
		}
	}
}

type workerSessionRunner struct {
	worker *Worker
}

func newWorkerSessionRunner(worker *Worker) workerSessionRunner {
	return workerSessionRunner{worker: worker}
}

func (r workerSessionRunner) buildNeedsInputChecker(
	ctx context.Context,
	task types.RunExecution,
	runner NeedsInputRunner,
	markerPath string,
	tw *terminalOutputWriter,
	bamlEnv map[string]string,
) func(string) (bool, types.InputKind, string, string) {
	return func(currentPrompt string) (bool, types.InputKind, string, string) {
		msg := runner.ReadLastMessage(markerPath)
		if msg == "" {
			return false, "", "", ""
		}
		cls, err := agentsignal.ClassifyTurn(ctx, msg, agentsignal.WithEnv(bamlEnv))
		if err != nil {
			return false, "", "", ""
		}

		if cls.Outcome != signaltypes.TurnOutcomeNEEDS_INPUT {
			return false, "", "", ""
		}

		kind := types.InputKindFreeText
		if cls.Input_kind != nil {
			kind = types.InputKind(strings.ToLower(string(*cls.Input_kind)))
		}

		assistantMessage := msg
		var summary string
		if kind == types.InputKindApproveReject {
			if tw.ringBuf != nil {
				if text := extractAssistantText(tw.ringBuf.Bytes(), approvalMessageExtractLimit); text != "" {
					assistantMessage = text
				}
			}
			if r.worker != nil {
				summary = r.worker.tryBuildApprovalSummary(ctx, assistantMessage, bamlEnv)
			}
		}
		return true, kind, summary, assistantMessage
	}
}

func (r workerSessionRunner) runTurnSession(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	activityCh chan<- struct{},
	checkNeedsInput func(string) (bool, types.InputKind, string, string),
	bamlEnv map[string]string,
	tracker *taskOutputTracker,
) (error, bool, types.InputKind, string, bool) {
	if r.worker == nil {
		return fmt.Errorf("worker is not configured"), false, "", "", false
	}
	prompt := strings.TrimSpace(task.Prompt)
	sessionEnv := cloneMap(env)
	isFirst := true

	outputParser, _ := runner.(OutputParsingRunner)
	var turnBuf bytes.Buffer
	var approvalOutputPersisted bool
	stateBridge := newSessionStateBridge(r.worker)
	subagents := newSubagentWatcher(r.worker)
	waiter := newFollowupInputWaiter(r.worker)

	for prompt != "" {
		if ctx.Err() != nil {
			return ctx.Err(), false, "", "", approvalOutputPersisted
		}

		stateBridge.setRunInteractionState(ctx, task, types.RunInteractionStateWorking)
		if !isFirst {
			stateBridge.setOriginTaskState(ctx, task, types.TaskLiveUpdate{
				State: types.AgentTaskStateRunning,
			})
		}

		turnOut := stdout
		if outputParser != nil {
			turnBuf.Reset()
			turnOut = io.MultiWriter(stdout, &turnBuf)
		}

		if isFirst {
			if err := r.executeFirstTurn(ctx, task, sandboxID, runner, sessionEnv, turnOut, prompt); err != nil {
				return err, false, "", "", approvalOutputPersisted
			}
			isFirst = false
		} else {
			if err := r.executeTurn(ctx, task, sandboxID, runner, sessionEnv, turnOut, prompt, TurnArgModeFollowup); err != nil {
				return err, false, "", "", approvalOutputPersisted
			}
		}
		signalActivity(activityCh)

		if subagents.waitForSubagents(ctx, task, sandboxID, activityCh) == subagentFinished {
			if err := r.executeTurn(
				ctx,
				task,
				sandboxID,
				runner,
				sessionEnv,
				turnOut,
				"Your background tasks / subagents have completed. Please collect and report their results.",
				TurnArgModeFollowup,
			); err != nil {
				return err, false, "", "", approvalOutputPersisted
			}
			signalActivity(activityCh)
		}

		var needsInput bool
		var inputKind types.InputKind
		var waitingSummary string
		var approvalAssistantMessage string
		var blockerOutputIDs []string

		if outputParser != nil {
			var err error
			needsInput, inputKind, approvalAssistantMessage, err = outputParser.ParseTurnOutput(turnBuf.Bytes())
			waitingSummary = approvalAssistantMessage
			if err != nil {
				addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to parse turn output")
			}
			if needsInput {
				inputKind = classifyNeedsInputKindWithFallback(
					ctx,
					inputKind,
					approvalAssistantMessage,
					bamlEnv,
					classifyNeedsInputKindWithBAML,
				)
			}
			if needsInput && inputKind == types.InputKindApproveReject {
				if summary := r.worker.tryBuildApprovalSummary(ctx, approvalAssistantMessage, bamlEnv); summary != "" {
					waitingSummary = summary
				}
			}
		} else if checkNeedsInput != nil {
			needsInput, inputKind, waitingSummary, approvalAssistantMessage = checkNeedsInput(prompt)
		}

		if !needsInput {
			if pending := waiter.claimPendingInput(ctx, task); pending != "" {
				prompt = pending
				continue
			}
			return nil, false, "", prompt, approvalOutputPersisted
		}

		if inputKind == types.InputKindApproveReject {
			blockerOutputIDs, approvalOutputPersisted = persistApprovalOutputBeforeWaiting(
				ctx, r.worker.gatewayClient, task, tracker, prompt, approvalAssistantMessage, bamlEnv,
			)
		}
		stateBridge.setRunInteractionState(ctx, task, types.RunInteractionStateWaitingForInput)
		stateBridge.setOriginTaskState(ctx, task, types.TaskLiveUpdate{
			State:   types.AgentTaskStateWaiting,
			Blocker: buildWaitingBlockerSpec(task, inputKind, waitingSummary, approvalAssistantMessage, blockerOutputIDs),
		})
		prompt = waiter.waitForFollowupInput(ctx, task, DefaultBetweenTurnsTimeout, activityCh)
		if prompt == "" {
			return nil, true, inputKind, "", approvalOutputPersisted
		}
	}
	return nil, true, "", "", approvalOutputPersisted
}

func (r workerSessionRunner) executeFirstTurn(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	prompt string,
) error {
	strategies := buildFirstTurnStrategies(env)
	for i, strategy := range strategies {
		err := r.executeTurn(ctx, task, sandboxID, runner, env, stdout, prompt, strategy.mode)
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if i == len(strategies)-1 {
			return err
		}
		addTaskExecutionContext(log.Warn().Err(err).Str("mode", string(strategy.mode)), task).
			Msg("first-turn strategy failed, trying next")
		if strategy.mode == TurnArgModeFirstResumeByID {
			delete(env, agentSessionIDEnvKey)
		}
	}
	return fmt.Errorf("no first-turn strategies")
}

func (r workerSessionRunner) executeTurn(
	ctx context.Context,
	task types.RunExecution,
	sandboxID string,
	runner TurnRunner,
	env map[string]string,
	stdout io.Writer,
	prompt string,
	mode TurnArgMode,
) error {
	if r.worker == nil || r.worker.sandboxManager == nil {
		return fmt.Errorf("sandbox manager is not configured")
	}
	return r.worker.sandboxManager.ExecPTY(ctx, sandboxID, runner.BuildTurnArgs(prompt, env, mode), env, stdout)
}

func (r workerSessionRunner) runGenericPTYSession(ctx context.Context, task types.RunExecution, sandboxID string, stdout io.Writer, _ chan<- struct{}) error {
	if r.worker == nil || r.worker.sandboxManager == nil {
		return fmt.Errorf("sandbox manager is not configured")
	}
	return r.worker.sandboxManager.AttachPTY(ctx, sandboxID, nil, stdout)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func interactiveResult(err error, idleTimedOut bool) (int, string, types.RunExecutionStatus) {
	if err == nil {
		return 0, "", types.RunExecutionStatusComplete
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if idleTimedOut {
			return 0, "", types.RunExecutionStatusComplete
		}
		return -1, "interactive session cancelled", types.RunExecutionStatusCancelled
	}
	return -1, err.Error(), types.RunExecutionStatusFailed
}

func cloneMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func signalActivity(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// ---------------------------------------------------------------------------
// Terminal output writer
// ---------------------------------------------------------------------------

// ringBuffer is a fixed-size circular byte buffer that retains the most recent
// N bytes written to it. It is only used to preserve interactive session text
// for follow-up and approval classifiers.
type ringBuffer struct {
	buf  []byte
	pos  int
	full bool
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{buf: make([]byte, size)}
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	cap := len(r.buf)
	if n >= cap {
		copy(r.buf, p[n-cap:])
		r.pos = 0
		r.full = true
		return n, nil
	}
	space := cap - r.pos
	if n <= space {
		copy(r.buf[r.pos:], p)
	} else {
		copy(r.buf[r.pos:], p[:space])
		copy(r.buf, p[space:])
	}
	r.pos = (r.pos + n) % cap
	if !r.full && r.pos < n {
		r.full = true
	}
	return n, nil
}

func (r *ringBuffer) Bytes() []byte {
	if !r.full {
		return append([]byte(nil), r.buf[:r.pos]...)
	}
	out := make([]byte, len(r.buf))
	n := copy(out, r.buf[r.pos:])
	copy(out[n:], r.buf[:r.pos])
	return out
}

type terminalOutputWriter struct {
	ctx          context.Context
	taskID       string
	terminalIO   repository.TerminalIORepository
	executionCtx taskExecutionContext
	onActivity   func()
	mirror       io.Writer
	ringBuf      *ringBuffer
}

func (w *terminalOutputWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.ringBuf != nil {
		_, _ = w.ringBuf.Write(p)
	}
	if w.mirror != nil {
		_, _ = w.mirror.Write(p)
	}
	if w.onActivity != nil {
		w.onActivity()
	}
	if err := w.terminalIO.PublishOutput(w.ctx, w.taskID, append([]byte(nil), p...)); err != nil {
		addTaskExecutionContextByID(log.Warn().Err(err), w.taskID, w.executionCtx).Msg("failed to publish terminal output")
	}
	return len(p), nil
}

// ---------------------------------------------------------------------------
// Idle monitor
// ---------------------------------------------------------------------------

func monitorInteractiveSessionIdle(
	ctx context.Context, taskID string, ec taskExecutionContext,
	cancel context.CancelFunc, timeout time.Duration,
	activityCh <-chan struct{}, idleTimedOut *atomic.Bool,
	checkHeartbeat func() bool,
) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-activityCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(timeout)
		case <-timer.C:
			if checkHeartbeat != nil && checkHeartbeat() {
				timer.Reset(timeout)
				continue
			}
			idleTimedOut.Store(true)
			addTaskExecutionContextByID(log.Info().Dur("idle_timeout", timeout), taskID, ec).
				Msg("idle timeout reached, stopping")
			cancel()
			return
		}
	}
}
