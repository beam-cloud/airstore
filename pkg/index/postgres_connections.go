package index

import (
	"context"
	"encoding/json"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// WorkspaceRepository defines the interface for workspace data access
type WorkspaceRepository interface {
	ListWorkspaces(ctx context.Context) ([]*types.Workspace, error)
	ListConnections(ctx context.Context, workspaceId uint) ([]types.IntegrationConnection, error)
}

// PostgresConnectionProvider implements ConnectionProvider using Postgres
type PostgresConnectionProvider struct {
	repo WorkspaceRepository
}

// NewPostgresConnectionProvider creates a new Postgres-backed connection provider
func NewPostgresConnectionProvider(repo WorkspaceRepository) *PostgresConnectionProvider {
	return &PostgresConnectionProvider{repo: repo}
}

// ListAllConnections returns all workspace connections for syncing
func (p *PostgresConnectionProvider) ListAllConnections(ctx context.Context) ([]WorkspaceConnection, error) {
	workspaces, err := p.repo.ListWorkspaces(ctx)
	if err != nil {
		return nil, err
	}

	var connections []WorkspaceConnection

	for _, ws := range workspaces {
		conns, err := p.repo.ListConnections(ctx, ws.Id)
		if err != nil {
			log.Warn().Err(err).Str("workspace_id", ws.ExternalId).Msg("failed to list connections for workspace")
			continue
		}

		for _, conn := range conns {
			// Parse credentials from JSON (stored as []byte)
			var creds types.IntegrationCredentials
			if len(conn.Credentials) > 0 {
				if err := json.Unmarshal(conn.Credentials, &creds); err != nil {
					log.Warn().Err(err).
						Str("workspace_id", ws.ExternalId).
						Str("integration", conn.IntegrationType).
						Msg("failed to parse connection credentials")
					continue
				}
			}

			// Only include connections with valid credentials
			if creds.AccessToken == "" {
				log.Debug().
					Str("workspace_id", ws.ExternalId).
					Str("integration", conn.IntegrationType).
					Msg("skipping connection without access token")
				continue
			}

			connections = append(connections, WorkspaceConnection{
				WorkspaceID:   ws.ExternalId,
				WorkspaceName: ws.Name,
				Integration:   conn.IntegrationType,
				Credentials:   &creds,
			})

			log.Debug().
				Str("workspace_id", ws.ExternalId).
				Str("workspace_name", ws.Name).
				Str("integration", conn.IntegrationType).
				Msg("found workspace connection")
		}
	}

	log.Info().Int("total_connections", len(connections)).Msg("listed all workspace connections")
	return connections, nil
}
