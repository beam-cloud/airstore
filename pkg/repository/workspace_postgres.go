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

// GetWorkspaceToolSettings returns all tool settings for a workspace as a lookup structure
// Tools not in the database are considered enabled by default
func (b *PostgresBackend) GetWorkspaceToolSettings(ctx context.Context, workspaceId uint) (*types.WorkspaceToolSettings, error) {
	settings := types.NewWorkspaceToolSettings(workspaceId)

	query := `
		SELECT tool_name, enabled
		FROM workspace_tool_setting
		WHERE workspace_id = $1
	`

	rows, err := b.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace tool settings: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var toolName string
		var enabled bool
		if err := rows.Scan(&toolName, &enabled); err != nil {
			return nil, fmt.Errorf("failed to scan tool setting: %w", err)
		}
		if !enabled {
			settings.DisabledTools[toolName] = true
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tool settings: %w", err)
	}

	return settings, nil
}

// GetWorkspaceToolSetting returns the setting for a specific tool in a workspace
// Returns nil if no setting exists (tool is enabled by default)
func (b *PostgresBackend) GetWorkspaceToolSetting(ctx context.Context, workspaceId uint, toolName string) (*types.WorkspaceToolSetting, error) {
	query := `
		SELECT id, workspace_id, tool_name, enabled, created_at, updated_at
		FROM workspace_tool_setting
		WHERE workspace_id = $1 AND tool_name = $2
	`

	setting := &types.WorkspaceToolSetting{}
	err := b.db.QueryRowContext(ctx, query, workspaceId, toolName).Scan(
		&setting.Id,
		&setting.WorkspaceId,
		&setting.ToolName,
		&setting.Enabled,
		&setting.CreatedAt,
		&setting.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil // No setting means enabled by default
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace tool setting: %w", err)
	}

	return setting, nil
}

// SetWorkspaceToolSetting creates or updates a tool setting for a workspace
func (b *PostgresBackend) SetWorkspaceToolSetting(ctx context.Context, workspaceId uint, toolName string, enabled bool) error {
	query := `
		INSERT INTO workspace_tool_setting (workspace_id, tool_name, enabled, updated_at)
		VALUES ($1, $2, $3, CURRENT_TIMESTAMP)
		ON CONFLICT (workspace_id, tool_name) 
		DO UPDATE SET enabled = $3, updated_at = CURRENT_TIMESTAMP
	`

	_, err := b.db.ExecContext(ctx, query, workspaceId, toolName, enabled)
	if err != nil {
		return fmt.Errorf("failed to set workspace tool setting: %w", err)
	}

	return nil
}

// ListWorkspaceToolSettings returns all tool settings for a workspace
func (b *PostgresBackend) ListWorkspaceToolSettings(ctx context.Context, workspaceId uint) ([]types.WorkspaceToolSetting, error) {
	query := `
		SELECT id, workspace_id, tool_name, enabled, created_at, updated_at
		FROM workspace_tool_setting
		WHERE workspace_id = $1
		ORDER BY tool_name
	`

	rows, err := b.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("failed to list workspace tool settings: %w", err)
	}
	defer rows.Close()

	var settings []types.WorkspaceToolSetting
	for rows.Next() {
		var setting types.WorkspaceToolSetting
		if err := rows.Scan(
			&setting.Id,
			&setting.WorkspaceId,
			&setting.ToolName,
			&setting.Enabled,
			&setting.CreatedAt,
			&setting.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("failed to scan tool setting: %w", err)
		}
		settings = append(settings, setting)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tool settings: %w", err)
	}

	return settings, nil
}
