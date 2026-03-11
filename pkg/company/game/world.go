package game

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/company"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
)

type WorldRuntime struct {
	manager      *Manager
	agentAPI     *orchestration.AgentAPI
	backend      repository.BackendRepository
	integrations repository.IntegrationRepository
}

func NewWorldRuntime(
	s2 *common.S2Client,
	agentAPI *orchestration.AgentAPI,
	backend repository.BackendRepository,
	integrations repository.IntegrationRepository,
) *WorldRuntime {
	store := NewStore(s2, backend)
	return &WorldRuntime{
		manager:      NewManager(store),
		agentAPI:     agentAPI,
		backend:      backend,
		integrations: integrations,
	}
}

func (r *WorldRuntime) SyncWorkspace(ctx context.Context, workspaceID uint) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	snap, err := company.BuildSnapshot(ctx, workspaceID, r.agentAPI, r.backend, r.integrations)
	if err != nil {
		return nil, nil, err
	}
	world, delta, err := r.manager.SyncCompanySnapshot(ctx, worldWorkspaceID(workspaceID), snap)
	if err != nil {
		return nil, nil, err
	}
	return world, delta, nil
}

func (r *WorldRuntime) CurrentSnapshot(ctx context.Context, workspaceID uint) (*company.CompanyWorldSnapshot, error) {
	world, _, err := r.SyncWorkspace(ctx, workspaceID)
	return world, err
}

func (r *WorldRuntime) Subscribe(ctx context.Context, workspaceID uint) (<-chan StreamMessage, func(), error) {
	return r.manager.Subscribe(ctx, worldWorkspaceID(workspaceID))
}

func (r *WorldRuntime) RecordActivity(ctx context.Context, workspaceID uint, channel, message, entityID string) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	return r.manager.RecordActivity(ctx, worldWorkspaceID(workspaceID), channel, message, entityID)
}

func (r *WorldRuntime) RecordActionResults(ctx context.Context, workspaceID uint, results []company.ActionResult) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	if len(results) == 0 {
		return nil, nil, nil
	}
	envelopes := make([]ActionResultEnvelope, 0, len(results))
	for _, result := range results {
		envelopes = append(envelopes, ActionResultEnvelope{
			Type:        string(result.Action.Type),
			Description: result.Action.Description,
			Status:      string(result.Status),
			ResourceIDs: result.ResourceIDs,
			Error:       result.Error,
		})
	}
	return r.manager.RecordActionResults(ctx, worldWorkspaceID(workspaceID), envelopes)
}

func worldWorkspaceID(workspaceID uint) string {
	return fmt.Sprintf("%d", workspaceID)
}
