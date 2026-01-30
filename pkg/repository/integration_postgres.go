package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

func (r *PostgresBackend) SaveConnection(ctx context.Context, workspaceId uint, memberId *uint, integrationType string, creds *types.IntegrationCredentials, scope string) (*types.IntegrationConnection, error) {
	// Serialize credentials (TODO: encrypt before storage)
	credBytes, err := json.Marshal(creds)
	if err != nil {
		return nil, fmt.Errorf("marshal credentials: %w", err)
	}

	query := `
		INSERT INTO integration_connection (workspace_id, member_id, integration_type, credentials, scope, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (workspace_id, COALESCE(member_id, 0), integration_type)
		DO UPDATE SET credentials = EXCLUDED.credentials, scope = EXCLUDED.scope, expires_at = EXCLUDED.expires_at, updated_at = CURRENT_TIMESTAMP
		RETURNING id, external_id, workspace_id, member_id, integration_type, credentials, scope, expires_at, created_at, updated_at
	`

	var c types.IntegrationConnection
	err = r.db.QueryRowContext(ctx, query, workspaceId, memberId, integrationType, credBytes, scope, creds.ExpiresAt).Scan(
		&c.Id, &c.ExternalId, &c.WorkspaceId, &c.MemberId, &c.IntegrationType, &c.Credentials, &c.Scope, &c.ExpiresAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("save connection: %w", err)
	}
	return &c, nil
}

func (r *PostgresBackend) GetConnection(ctx context.Context, workspaceId uint, memberId uint, integrationType string) (*types.IntegrationConnection, error) {
	// Try personal connection first
	query := `
		SELECT id, external_id, workspace_id, member_id, integration_type, credentials, scope, expires_at, created_at, updated_at
		FROM integration_connection WHERE workspace_id = $1 AND member_id = $2 AND integration_type = $3
	`

	var c types.IntegrationConnection
	err := r.db.QueryRowContext(ctx, query, workspaceId, memberId, integrationType).Scan(
		&c.Id, &c.ExternalId, &c.WorkspaceId, &c.MemberId, &c.IntegrationType, &c.Credentials, &c.Scope, &c.ExpiresAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if err == nil {
		return &c, nil
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("get personal connection: %w", err)
	}

	// Fall back to shared connection
	query = `
		SELECT id, external_id, workspace_id, member_id, integration_type, credentials, scope, expires_at, created_at, updated_at
		FROM integration_connection WHERE workspace_id = $1 AND member_id IS NULL AND integration_type = $2
	`

	err = r.db.QueryRowContext(ctx, query, workspaceId, integrationType).Scan(
		&c.Id, &c.ExternalId, &c.WorkspaceId, &c.MemberId, &c.IntegrationType, &c.Credentials, &c.Scope, &c.ExpiresAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get shared connection: %w", err)
	}
	return &c, nil
}

func (r *PostgresBackend) ListConnections(ctx context.Context, workspaceId uint) ([]types.IntegrationConnection, error) {
	query := `
		SELECT id, external_id, workspace_id, member_id, integration_type, credentials, scope, expires_at, created_at, updated_at
		FROM integration_connection WHERE workspace_id = $1 ORDER BY integration_type, member_id NULLS FIRST
	`

	rows, err := r.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	defer rows.Close()

	var conns []types.IntegrationConnection
	for rows.Next() {
		var c types.IntegrationConnection
		if err := rows.Scan(&c.Id, &c.ExternalId, &c.WorkspaceId, &c.MemberId, &c.IntegrationType, &c.Credentials, &c.Scope, &c.ExpiresAt, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan connection: %w", err)
		}
		conns = append(conns, c)
	}
	return conns, rows.Err()
}

func (r *PostgresBackend) GetConnectionByExternalId(ctx context.Context, externalId string) (*types.IntegrationConnection, error) {
	query := `
		SELECT id, external_id, workspace_id, member_id, integration_type, credentials, scope, expires_at, created_at, updated_at
		FROM integration_connection WHERE external_id = $1
	`

	var c types.IntegrationConnection
	err := r.db.QueryRowContext(ctx, query, externalId).Scan(
		&c.Id, &c.ExternalId, &c.WorkspaceId, &c.MemberId, &c.IntegrationType, &c.Credentials, &c.Scope, &c.ExpiresAt, &c.CreatedAt, &c.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get connection by external id: %w", err)
	}
	return &c, nil
}

func (r *PostgresBackend) DeleteConnection(ctx context.Context, externalId string) error {
	query := `DELETE FROM integration_connection WHERE external_id = $1`
	result, err := r.db.ExecContext(ctx, query, externalId)
	if err != nil {
		return fmt.Errorf("delete connection: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DecryptCredentials parses credentials from a connection (TODO: implement encryption)
func DecryptCredentials(conn *types.IntegrationConnection) (*types.IntegrationCredentials, error) {
	var creds types.IntegrationCredentials
	if err := json.Unmarshal(conn.Credentials, &creds); err != nil {
		return nil, fmt.Errorf("unmarshal credentials: %w", err)
	}
	return &creds, nil
}
