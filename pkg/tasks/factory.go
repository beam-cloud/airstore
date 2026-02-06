package tasks

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// Factory creates tasks, saves to Postgres, and pushes to the queue.
type Factory struct {
	backend repository.BackendRepository
	queue   repository.TaskQueue
	image   string // default sandbox image for Claude Code tasks
}

func NewFactory(backend repository.BackendRepository, queue repository.TaskQueue, defaultImage string) *Factory {
	return &Factory{backend: backend, queue: queue, image: defaultImage}
}

// Params describes a task to create.
type Params struct {
	WorkspaceId       uint
	CreatedByMemberId *uint
	MemberToken       string
	Prompt            string
	Image             string
	Entrypoint        []string
	Env               map[string]string
	Resources         *types.TaskResources
	HookId            *uint
	Attempt           int
	MaxAttempts       int
}

// Create builds a task, saves it to Postgres, and pushes it to the queue.
func (f *Factory) Create(ctx context.Context, p Params) (*types.Task, error) {
	image := p.Image
	if image == "" && p.Prompt != "" {
		image = f.image
	}
	if image == "" {
		return nil, fmt.Errorf("image or prompt required")
	}

	env := p.Env
	if env == nil {
		env = make(map[string]string)
	}
	entrypoint := p.Entrypoint
	if entrypoint == nil {
		entrypoint = []string{}
	}

	task := &types.Task{
		WorkspaceId:       p.WorkspaceId,
		CreatedByMemberId: p.CreatedByMemberId,
		MemberToken:       p.MemberToken,
		Status:            types.TaskStatusPending,
		Prompt:            p.Prompt,
		Image:             image,
		Entrypoint:        entrypoint,
		Env:               env,
		Resources:         p.Resources,
		HookId:            p.HookId,
		Attempt:           p.Attempt,
		MaxAttempts:       p.MaxAttempts,
	}

	if err := f.backend.CreateTask(ctx, task); err != nil {
		return nil, fmt.Errorf("create task: %w", err)
	}

	if f.queue != nil {
		if err := f.queue.Push(ctx, task); err != nil {
			log.Warn().Err(err).Str("task", task.ExternalId).Msg("task factory: queue push failed")
		}
	}

	return task, nil
}

// CreateTask implements hooks.TaskCreator.
func (f *Factory) CreateTask(ctx context.Context, workspaceId uint, createdByMemberId *uint, memberToken, prompt string, hookId uint, attempt, maxAttempts int) error {
	var hid *uint
	if hookId > 0 {
		hid = &hookId
	}
	_, err := f.Create(ctx, Params{
		WorkspaceId:       workspaceId,
		CreatedByMemberId: createdByMemberId,
		MemberToken:       memberToken,
		Prompt:            prompt,
		HookId:            hid,
		Attempt:           attempt,
		MaxAttempts:       maxAttempts,
	})
	return err
}
