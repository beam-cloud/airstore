package hooks

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// Factory creates tasks, saves to Postgres, and pushes to the queue.
type TaskFactory struct {
	backend repository.BackendRepository
	queue   repository.TaskQueue
	image   string // default sandbox image for Claude Code tasks
}

func NewTaskFactory(backend repository.BackendRepository, queue repository.TaskQueue, defaultImage string) *TaskFactory {
	return &TaskFactory{backend: backend, queue: queue, image: defaultImage}
}

// CreateTask implements hooks.TaskCreator.
func (f *TaskFactory) CreateTask(ctx context.Context, workspaceId uint, createdByMemberId *uint, memberToken, prompt string, hookId uint, attempt, maxAttempts int) error {
	image := f.image
	if image == "" {
		return fmt.Errorf("image or prompt required")
	}

	var hid *uint
	if hookId > 0 {
		hid = &hookId
	}

	task := &types.RunExecution{
		WorkspaceId:       workspaceId,
		CreatedByMemberId: createdByMemberId,
		MemberToken:       memberToken,
		Status:            types.RunExecutionStatusPending,
		Prompt:            prompt,
		Image:             image,
		Entrypoint:        []string{},
		Env:               make(map[string]string),
		HookId:            hid,
		Attempt:           attempt,
		MaxAttempts:       maxAttempts,
	}

	if err := f.backend.CreateRunExecution(ctx, task); err != nil {
		return fmt.Errorf("create task: %w", err)
	}

	if f.queue != nil {
		if err := f.queue.Push(ctx, task); err != nil {
			log.Warn().Err(err).Str("task", task.ExternalId).Msg("task factory: queue push failed")
		}
	}

	return nil
}
