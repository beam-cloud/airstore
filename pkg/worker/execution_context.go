package worker

import (
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog"
)

const (
	runIDEnvKey        = "AIRSTORE_RUN_ID"
	runAttemptIDEnvKey = "AIRSTORE_RUN_ATTEMPT_ID"
	originTaskIDEnvKey = "AIRSTORE_ORIGIN_TASK_ID"
)

type taskExecutionContext struct {
	runID        string
	runAttemptID string
	originTaskID string
}

func executionContextFromTask(task types.RunExecution) taskExecutionContext {
	ctx := taskExecutionContext{}
	if task.RunAttemptID != nil {
		ctx.runAttemptID = strings.TrimSpace(*task.RunAttemptID)
	}

	if task.ExecutionPolicy != nil {
		if value, ok := task.ExecutionPolicy[types.AgentExecutionMetaKeyRunID]; ok {
			ctx.runID = anyToTrimmedString(value)
		}
		if value, ok := task.ExecutionPolicy[types.AgentExecutionMetaKeyRunAttemptID]; ok && ctx.runAttemptID == "" {
			ctx.runAttemptID = anyToTrimmedString(value)
		}
		if value, ok := task.ExecutionPolicy[types.AgentExecutionMetaKeyOriginTaskID]; ok {
			ctx.originTaskID = anyToTrimmedString(value)
		}
	}

	if ctx.runID == "" {
		ctx.runID = strings.TrimSpace(task.Env[runIDEnvKey])
	}
	if ctx.runAttemptID == "" {
		ctx.runAttemptID = strings.TrimSpace(task.Env[runAttemptIDEnvKey])
	}
	if ctx.originTaskID == "" {
		ctx.originTaskID = strings.TrimSpace(task.Env[originTaskIDEnvKey])
	}

	return ctx
}

func addTaskExecutionContext(event *zerolog.Event, task types.RunExecution) *zerolog.Event {
	if event == nil {
		return event
	}

	return addTaskExecutionContextByID(event, task.ExternalId, executionContextFromTask(task))
}

func addTaskExecutionContextByID(event *zerolog.Event, taskID string, ctx taskExecutionContext) *zerolog.Event {
	if event == nil {
		return event
	}
	if trimmedTaskID := strings.TrimSpace(taskID); trimmedTaskID != "" {
		event = event.Str("task_id", trimmedTaskID)
	}
	return addTaskExecutionContextValues(event, ctx)
}

func addTaskExecutionContextValues(event *zerolog.Event, ctx taskExecutionContext) *zerolog.Event {
	if event == nil {
		return event
	}
	if ctx.runID != "" {
		event = event.Str("run_id", ctx.runID)
	}
	if ctx.runAttemptID != "" {
		event = event.Str("run_attempt_id", ctx.runAttemptID)
	}
	if ctx.originTaskID != "" {
		event = event.Str("origin_task_id", ctx.originTaskID)
	}
	return event
}

func executionContextFromEnv(env map[string]string) taskExecutionContext {
	if len(env) == 0 {
		return taskExecutionContext{}
	}
	ctx := taskExecutionContext{
		runID:        strings.TrimSpace(env[runIDEnvKey]),
		runAttemptID: strings.TrimSpace(env[runAttemptIDEnvKey]),
		originTaskID: strings.TrimSpace(env[originTaskIDEnvKey]),
	}
	return ctx
}

func addTaskExecutionContextFromEnv(event *zerolog.Event, taskID string, env map[string]string) *zerolog.Event {
	return addTaskExecutionContextByID(event, taskID, executionContextFromEnv(env))
}

// taskOutputIDs extracts the IDs that output writers need from a task's execution policy.
type taskOutputIDs struct {
	workspaceID uint32
	taskID      string
	runID       string
	agentID     string
}

func outputIDsFromTask(task types.RunExecution) taskOutputIDs {
	ids := taskOutputIDs{workspaceID: uint32(task.WorkspaceId)}
	if task.ExecutionPolicy != nil {
		ids.taskID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyOriginTaskID])
		ids.runID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyRunID])
		ids.agentID = anyToTrimmedString(task.ExecutionPolicy[types.AgentExecutionMetaKeyAgentID])
	}
	return ids
}

func anyToTrimmedString(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case *string:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(*typed)
	case fmt.Stringer:
		return strings.TrimSpace(typed.String())
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", typed))
	}
}
