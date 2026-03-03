package hooks

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// Service handles hook CRUD. Shared by HTTP and gRPC handlers.
type Service struct {
	Store    repository.FilesystemStore
	Backend  repository.BackendRepository
	EventBus *common.EventBus
	Seen     *SeenTracker
}

func (s *Service) Create(
	ctx context.Context,
	wsId uint,
	memberId, tokenId *uint,
	rawToken, path, prompt string,
	skillPaths []string,
	eventTypes []string,
	agentPatch *AgentConfigPatch,
) (*types.Hook, error) {
	path = NormalizePath(path)

	if err := ValidateHookPath(path); err != nil {
		return nil, err
	}
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return nil, fmt.Errorf("prompt is required")
	}
	normalizedSkills := types.NormalizeSkillPaths(skillPaths, "")

	if len(eventTypes) == 0 {
		eventTypes = []string{"fs.create"}
	}

	encrypted, err := EncodeToken(rawToken)
	if err != nil {
		return nil, fmt.Errorf("failed to store token")
	}

	hook := &types.Hook{
		WorkspaceId:       wsId,
		Path:              path,
		Prompt:            prompt,
		SkillPaths:        normalizedSkills,
		EventTypes:        eventTypes,
		Active:            true,
		CreatedByMemberId: memberId,
		TokenId:           tokenId,
		EncryptedToken:    encrypted,
	}
	hook.NormalizeSkills()

	created, err := s.Store.CreateHook(ctx, hook)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			return nil, fmt.Errorf("a hook already exists on %s", hook.Path)
		}
		return nil, err
	}

	agent, err := ResolveHookAgent(ctx, s.Backend, created.WorkspaceId, created.Path, nil, agentPatch)
	if err != nil {
		s.cleanupFailedCreate(ctx, created)
		return nil, err
	}
	agentID := agent.ID
	created.AgentId = &agentID
	if err := s.Store.UpdateHook(ctx, created); err != nil {
		s.cleanupFailedCreate(ctx, created)
		return nil, err
	}

	// Ensure a newly created (or re-created) hook observes the current snapshot
	// as fresh, instead of inheriting stale seen state from prior hook lifecycles.
	s.resetSeenState(ctx, created.WorkspaceId, created.Path)
	HydrateHookAgent(ctx, s.Backend, created)
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
	hooks, err := s.Store.ListHooks(ctx, wsId)
	if err != nil {
		return nil, err
	}
	for _, hook := range hooks {
		HydrateHookAgent(ctx, s.Backend, hook)
	}
	return hooks, nil
}

func (s *Service) Get(ctx context.Context, externalId string) (*types.Hook, error) {
	hook, err := s.Store.GetHook(ctx, externalId)
	if err != nil {
		return nil, err
	}
	if hook == nil {
		return nil, fmt.Errorf("hook not found")
	}
	HydrateHookAgent(ctx, s.Backend, hook)
	return hook, nil
}

func (s *Service) Update(
	ctx context.Context,
	externalId string,
	prompt *string,
	active *bool,
	skillPaths *[]string,
	eventTypes *[]string,
	agentPatch *AgentConfigPatch,
) (*types.Hook, error) {
	hook, err := s.Get(ctx, externalId)
	if err != nil {
		return nil, err
	}

	if prompt != nil {
		trimmed := strings.TrimSpace(*prompt)
		if trimmed == "" {
			return nil, fmt.Errorf("prompt is required")
		}
		hook.Prompt = trimmed
	}
	if active != nil {
		hook.Active = *active
	}
	if skillPaths != nil {
		hook.SkillPaths = types.NormalizeSkillPaths(*skillPaths, "")
		hook.NormalizeSkills()
	}
	if eventTypes != nil && len(*eventTypes) > 0 {
		hook.EventTypes = *eventTypes
	}
	// Empty event_types array is treated as "no change" to avoid unintentionally
	// broadening the hook to match all events (hookMatchesEvent treats len==0 as match-all).
	agent, err := ResolveHookAgent(ctx, s.Backend, hook.WorkspaceId, hook.Path, hook.AgentId, agentPatch)
	if err != nil {
		return nil, err
	}
	agentID := agent.ID
	hook.AgentId = &agentID

	if err := s.Store.UpdateHook(ctx, hook); err != nil {
		return nil, err
	}

	HydrateHookAgent(ctx, s.Backend, hook)
	s.invalidateCache(hook.WorkspaceId)
	return hook, nil
}

func (s *Service) Delete(ctx context.Context, externalId string) error {
	hook, err := s.Get(ctx, externalId)
	if err != nil {
		return err
	}

	agentID := ""
	if hook.AgentId != nil {
		agentID = strings.TrimSpace(*hook.AgentId)
	}
	deleteHookAgent := false
	if agentID != "" {
		deleteHookAgent, err = s.shouldDeleteHookAgent(ctx, hook.WorkspaceId, agentID)
		if err != nil {
			return err
		}
	}

	if err := s.Store.DeleteHook(ctx, externalId); err != nil {
		return err
	}
	s.resetSeenState(ctx, hook.WorkspaceId, hook.Path)

	if deleteHookAgent && s.Backend != nil {
		if err := s.Backend.DeleteAgentProfile(ctx, hook.WorkspaceId, agentID); err != nil {
			if _, notFound := err.(*types.ErrAgentProfileNotFound); !notFound {
				return err
			}
		}
	}

	s.invalidateCache(hook.WorkspaceId)
	return nil
}

// ListRuns returns tasks associated with a hook.
func (s *Service) ListRuns(ctx context.Context, hookId uint) ([]*types.RunExecution, error) {
	return s.Backend.ListRunExecutionsByHook(ctx, hookId)
}

func (s *Service) shouldDeleteHookAgent(ctx context.Context, workspaceID uint, agentID string) (bool, error) {
	if agentID == "" {
		return false, nil
	}

	hooks, err := s.Store.ListHooks(ctx, workspaceID)
	if err != nil {
		return false, err
	}

	references := 0
	for _, hook := range hooks {
		if hook == nil || hook.AgentId == nil {
			continue
		}
		if strings.TrimSpace(*hook.AgentId) == agentID {
			references++
		}
	}
	// We call this before deleting the current hook, so a single reference
	// means the agent belongs only to this hook and can be removed safely.
	return references <= 1, nil
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

func (s *Service) resetSeenState(ctx context.Context, workspaceID uint, path string) {
	if s.Seen == nil {
		return
	}
	if err := s.Seen.ResetPath(ctx, workspaceID, path); err != nil {
		log.Warn().
			Err(err).
			Uint("workspace_id", workspaceID).
			Str("path", path).
			Msg("failed to reset hook seen state")
	}
}

func (s *Service) cleanupFailedCreate(ctx context.Context, hook *types.Hook) {
	if s == nil || s.Store == nil || hook == nil || strings.TrimSpace(hook.ExternalId) == "" {
		return
	}
	if err := s.Store.DeleteHook(ctx, hook.ExternalId); err != nil {
		log.Warn().
			Err(err).
			Uint("workspace_id", hook.WorkspaceId).
			Str("path", hook.Path).
			Str("hook_external_id", hook.ExternalId).
			Msg("failed to clean up hook after create-side failure")
	}
}
