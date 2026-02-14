package hooks

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

// Service handles hook CRUD. Shared by HTTP and gRPC handlers.
type Service struct {
	Store    repository.FilesystemStore
	Backend  repository.BackendRepository
	EventBus *common.EventBus
}

func (s *Service) Create(ctx context.Context, wsId uint, memberId, tokenId *uint, rawToken, path, prompt, skillPath string) (*types.Hook, error) {
	path = NormalizePath(path)

	if err := ValidateHookPath(path); err != nil {
		return nil, err
	}

	encrypted, err := EncodeToken(rawToken)
	if err != nil {
		return nil, fmt.Errorf("failed to store token")
	}

	hook := &types.Hook{
		WorkspaceId:       wsId,
		Path:              path,
		Prompt:            prompt,
		SkillPath:         skillPath,
		Active:            true,
		CreatedByMemberId: memberId,
		TokenId:           tokenId,
		EncryptedToken:    encrypted,
	}

	created, err := s.Store.CreateHook(ctx, hook)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			return nil, fmt.Errorf("a hook already exists on %s", hook.Path)
		}
		return nil, err
	}

	s.invalidateCache(wsId)
	return created, nil
}

// ValidateHookPath checks if a path is valid for hook creation.
// This is a basic sanity check blocking obvious invalid paths.
// The real validation (checking for external_id) happens on the frontend.
//
// Hooks cannot be attached to:
//   - System root directories (/tasks, /tools, /skills, /sources)
//   - Root-level source folders (/sources/gmail, /sources/github)
//
// Hooks CAN be attached to:
//   - Source view folders under sources (/sources/gmail/my-query)
//   - Top-level query folders (/my-emails)
func ValidateHookPath(path string) error {
	if types.IsSystemRootPath(path) {
		return fmt.Errorf("cannot add hook to %s", path)
	}
	if types.IsRootLevelSource(path) {
		return fmt.Errorf("cannot add hook to root-level source %s; use a source view folder instead", path)
	}
	return nil
}

func (s *Service) List(ctx context.Context, wsId uint) ([]*types.Hook, error) {
	return s.Store.ListHooks(ctx, wsId)
}

func (s *Service) Get(ctx context.Context, externalId string) (*types.Hook, error) {
	hook, err := s.Store.GetHook(ctx, externalId)
	if err != nil {
		return nil, err
	}
	if hook == nil {
		return nil, fmt.Errorf("hook not found")
	}
	return hook, nil
}

func (s *Service) Update(ctx context.Context, externalId string, prompt *string, active *bool, skillPath *string) (*types.Hook, error) {
	hook, err := s.Get(ctx, externalId)
	if err != nil {
		return nil, err
	}

	if prompt != nil {
		hook.Prompt = *prompt
	}
	if active != nil {
		hook.Active = *active
	}
	if skillPath != nil {
		hook.SkillPath = *skillPath
	}

	if err := s.Store.UpdateHook(ctx, hook); err != nil {
		return nil, err
	}

	s.invalidateCache(hook.WorkspaceId)
	return hook, nil
}

func (s *Service) Delete(ctx context.Context, externalId string) error {
	hook, err := s.Get(ctx, externalId)
	if err != nil {
		return err
	}

	if err := s.Store.DeleteHook(ctx, externalId); err != nil {
		return err
	}

	s.invalidateCache(hook.WorkspaceId)
	return nil
}

// ListRuns returns tasks associated with a hook.
func (s *Service) ListRuns(ctx context.Context, hookId uint) ([]*types.Task, error) {
	return s.Backend.ListTasksByHook(ctx, hookId)
}

func (s *Service) invalidateCache(workspaceId uint) {
	if s.EventBus == nil {
		return
	}
	s.EventBus.Emit(common.Event{
		Type: common.EventCacheInvalidate,
		Data: map[string]any{
			"scope":        "hooks",
			"workspace_id": workspaceId,
		},
	})
}
