package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

func (w *Worker) runInteractiveTask(ctx context.Context, task types.Task) (*types.TaskResult, error) {
	if w.terminalIO == nil {
		return nil, fmt.Errorf("terminal transport is not configured")
	}

	sandboxID := fmt.Sprintf("task-%s", task.ExternalId)

	env := w.sandboxManager.copyTaskEnv(task)

	taskMountSource := w.sandboxManager.mountFilesystem(ctx, task)
	defer w.sandboxManager.cleanupMount(task.ExternalId)

	cfg := w.sandboxManager.buildTaskSandboxConfig(task, []string{"sleep", "infinity"}, env, taskMountSource)

	if _, err := w.sandboxManager.Create(cfg); err != nil {
		return nil, fmt.Errorf("failed to create interactive sandbox: %w", err)
	}
	defer w.sandboxManager.Delete(sandboxID, true)

	if err := w.sandboxManager.Start(sandboxID); err != nil {
		w.sandboxManager.publishStatus(ctx, task.ExternalId, types.TaskStatusFailed, nil, err.Error())
		return nil, fmt.Errorf("failed to start interactive sandbox: %w", err)
	}

	w.sandboxManager.publishStatus(ctx, task.ExternalId, types.TaskStatusRunning, nil, "")

	sessionCtx, sessionCancel := context.WithCancel(ctx)
	defer sessionCancel()
	idleTimeout := w.config.Sandbox.GetInteractiveIdleTimeout()
	var idleTimedOut atomic.Bool
	activityCh := make(chan struct{}, 1)

	if idleTimeout > 0 {
		go monitorInteractiveSessionIdle(sessionCtx, task.ExternalId, sessionCancel, idleTimeout, activityCh, &idleTimedOut)
	}

	inputCh, inputCleanup, err := w.terminalIO.SubscribeInput(sessionCtx, task.ExternalId)
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe terminal input: %w", err)
	}
	defer inputCleanup()

	stdinReader, stdinWriter := io.Pipe()
	defer stdinReader.Close()

	go forwardTerminalInput(sessionCtx, stdinWriter, inputCh, func() {
		signalActivity(activityCh)
	})

	terminalWriter := &terminalOutputWriter{
		ctx:        sessionCtx,
		taskID:     task.ExternalId,
		terminalIO: w.terminalIO,
		onActivity: func() {
			signalActivity(activityCh)
		},
		mirror: io.MultiWriter(
			NewS2Writer(sessionCtx, w.sandboxManager.s2, task.ExternalId, "stdout"),
			NewConsoleWriter(task.ExternalId, "stdout"),
		),
	}

	start := time.Now()
	runErr := w.sandboxManager.AttachPTY(sessionCtx, sandboxID, stdinReader, terminalWriter)
	exitCode, errMsg, status := interactiveResult(runErr, idleTimedOut.Load())

	w.sandboxManager.publishStatus(ctx, task.ExternalId, status, &exitCode, errMsg)
	return &types.TaskResult{
		ID:       task.ExternalId,
		ExitCode: exitCode,
		Error:    errMsg,
		Duration: time.Since(start),
	}, nil
}

func interactiveResult(err error, idleTimedOut bool) (int, string, types.TaskStatus) {
	if err == nil {
		return 0, "", types.TaskStatusComplete
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if idleTimedOut {
			return 0, "", types.TaskStatusComplete
		}
		return -1, "interactive session cancelled", types.TaskStatusCancelled
	}

	return -1, err.Error(), types.TaskStatusFailed
}

type terminalOutputWriter struct {
	ctx        context.Context
	taskID     string
	terminalIO repository.TerminalIORepository
	onActivity func()
	mirror     io.Writer
}

func forwardTerminalInput(
	ctx context.Context,
	stdinWriter *io.PipeWriter,
	inputCh <-chan []byte,
	onActivity func(),
) {
	defer stdinWriter.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-inputCh:
			if !ok {
				return
			}
			if len(data) == 0 {
				continue
			}
			if _, err := stdinWriter.Write(data); err != nil {
				return
			}
			if onActivity != nil {
				onActivity()
			}
		}
	}
}

func (w *terminalOutputWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if w.mirror != nil {
		_, _ = w.mirror.Write(p)
	}
	if w.onActivity != nil {
		w.onActivity()
	}

	if err := w.terminalIO.PublishOutput(w.ctx, w.taskID, append([]byte(nil), p...)); err != nil {
		log.Warn().Err(err).Str("task_id", w.taskID).Msg("failed to publish terminal output")
	}

	return len(p), nil
}

func signalActivity(activityCh chan<- struct{}) {
	select {
	case activityCh <- struct{}{}:
	default:
	}
}

func monitorInteractiveSessionIdle(
	ctx context.Context,
	taskID string,
	cancel context.CancelFunc,
	idleTimeout time.Duration,
	activityCh <-chan struct{},
	idleTimedOut *atomic.Bool,
) {
	timer := time.NewTimer(idleTimeout)
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
			timer.Reset(idleTimeout)
		case <-timer.C:
			idleTimedOut.Store(true)
			log.Info().
				Str("task_id", taskID).
				Dur("idle_timeout", idleTimeout).
				Msg("interactive session idle timeout reached, stopping task")
			cancel()
			return
		}
	}
}
