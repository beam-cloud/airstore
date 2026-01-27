package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Workspace methods on PostgresBackend

// CreateWorkspace creates a new workspace
func (b *PostgresBackend) CreateWorkspace(ctx context.Context, name string) (*types.Workspace, error) {
	query := `
		INSERT INTO workspace (name)
		VALUES ($1)
		RETURNING id, external_id, name, created_at, updated_at
	`

	workspace := &types.Workspace{}
	err := b.db.QueryRowContext(ctx, query, name).Scan(
		&workspace.Id,
		&workspace.ExternalId,
		&workspace.Name,
		&workspace.CreatedAt,
		&workspace.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create workspace: %w", err)
	}

	return workspace, nil
}

// GetWorkspace retrieves a workspace by internal ID
func (b *PostgresBackend) GetWorkspace(ctx context.Context, id uint) (*types.Workspace, error) {
	query := `
		SELECT id, external_id, name, created_at, updated_at
		FROM workspace
		WHERE id = $1
	`

	workspace := &types.Workspace{}
	err := b.db.QueryRowContext(ctx, query, id).Scan(
		&workspace.Id,
		&workspace.ExternalId,
		&workspace.Name,
		&workspace.CreatedAt,
		&workspace.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrWorkspaceNotFound{ExternalId: fmt.Sprintf("%d", id)}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace: %w", err)
	}

	return workspace, nil
}

// GetWorkspaceByExternalId retrieves a workspace by external UUID
func (b *PostgresBackend) GetWorkspaceByExternalId(ctx context.Context, externalId string) (*types.Workspace, error) {
	query := `
		SELECT id, external_id, name, created_at, updated_at
		FROM workspace
		WHERE external_id = $1
	`

	workspace := &types.Workspace{}
	err := b.db.QueryRowContext(ctx, query, externalId).Scan(
		&workspace.Id,
		&workspace.ExternalId,
		&workspace.Name,
		&workspace.CreatedAt,
		&workspace.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrWorkspaceNotFound{ExternalId: externalId}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace by external id: %w", err)
	}

	return workspace, nil
}

// GetWorkspaceByName retrieves a workspace by name
func (b *PostgresBackend) GetWorkspaceByName(ctx context.Context, name string) (*types.Workspace, error) {
	query := `
		SELECT id, external_id, name, created_at, updated_at
		FROM workspace
		WHERE name = $1
	`

	workspace := &types.Workspace{}
	err := b.db.QueryRowContext(ctx, query, name).Scan(
		&workspace.Id,
		&workspace.ExternalId,
		&workspace.Name,
		&workspace.CreatedAt,
		&workspace.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, &types.ErrWorkspaceNotFound{Name: name}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace by name: %w", err)
	}

	return workspace, nil
}

// ListWorkspaces returns all workspaces
func (b *PostgresBackend) ListWorkspaces(ctx context.Context) ([]*types.Workspace, error) {
	query := `
		SELECT id, external_id, name, created_at, updated_at
		FROM workspace
		ORDER BY created_at DESC
	`

	rows, err := b.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to list workspaces: %w", err)
	}
	defer rows.Close()

	var workspaces []*types.Workspace
	for rows.Next() {
		workspace := &types.Workspace{}
		if err := rows.Scan(
			&workspace.Id,
			&workspace.ExternalId,
			&workspace.Name,
			&workspace.CreatedAt,
			&workspace.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan workspace: %w", err)
		}
		workspaces = append(workspaces, workspace)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workspaces: %w", err)
	}

	return workspaces, nil
}

// DeleteWorkspace removes a workspace by internal ID
func (b *PostgresBackend) DeleteWorkspace(ctx context.Context, id uint) error {
	query := `DELETE FROM workspace WHERE id = $1`

	result, err := b.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete workspace: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrWorkspaceNotFound{ExternalId: fmt.Sprintf("%d", id)}
	}

	return nil
}
