package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

// Workspace tool methods on PostgresBackend

// nullableJSON is a helper for scanning nullable JSON columns
type nullableJSON struct {
	Data  json.RawMessage
	Valid bool
}

func (n *nullableJSON) Scan(value interface{}) error {
	if value == nil {
		n.Data = nil
		n.Valid = false
		return nil
	}
	n.Valid = true
	switch v := value.(type) {
	case []byte:
		n.Data = make(json.RawMessage, len(v))
		copy(n.Data, v)
		return nil
	case string:
		n.Data = json.RawMessage(v)
		return nil
	default:
		return fmt.Errorf("cannot scan %T into nullableJSON", value)
	}
}

// scanWorkspaceTool scans a row into a WorkspaceTool, handling nullable manifest
func scanWorkspaceTool(scanner interface{ Scan(...interface{}) error }) (*types.WorkspaceTool, error) {
	tool := &types.WorkspaceTool{}
	var manifest nullableJSON
	err := scanner.Scan(
		&tool.Id,
		&tool.ExternalId,
		&tool.WorkspaceId,
		&tool.Name,
		&tool.ProviderType,
		&tool.Config,
		&manifest,
		&tool.CreatedByMemberId,
		&tool.CreatedAt,
		&tool.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	if manifest.Valid {
		tool.Manifest = manifest.Data
	}
	return tool, nil
}

// CreateWorkspaceTool creates a new workspace tool provider
func (b *PostgresBackend) CreateWorkspaceTool(ctx context.Context, workspaceId uint, createdByMemberId *uint, name string, providerType types.WorkspaceToolProviderType, config json.RawMessage, manifest json.RawMessage) (*types.WorkspaceTool, error) {
	query := `
		INSERT INTO workspace_tool (workspace_id, name, provider_type, config, manifest, created_by_member_id)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, external_id, workspace_id, name, provider_type, config, manifest, created_by_member_id, created_at, updated_at
	`

	// Handle nil manifest - pass NULL to database
	var manifestParam interface{}
	if len(manifest) > 0 {
		manifestParam = manifest
	}

	row := b.db.QueryRowContext(ctx, query, workspaceId, name, providerType, config, manifestParam, createdByMemberId)
	tool, err := scanWorkspaceTool(row)
	if err != nil {
		// Check for unique constraint violation
		if isUniqueViolation(err) {
			return nil, &types.ErrWorkspaceToolExists{Name: name}
		}
		return nil, fmt.Errorf("failed to create workspace tool: %w", err)
	}

	return tool, nil
}

// GetWorkspaceTool retrieves a workspace tool by internal ID
func (b *PostgresBackend) GetWorkspaceTool(ctx context.Context, id uint) (*types.WorkspaceTool, error) {
	query := `
		SELECT id, external_id, workspace_id, name, provider_type, config, manifest, created_by_member_id, created_at, updated_at
		FROM workspace_tool
		WHERE id = $1
	`

	row := b.db.QueryRowContext(ctx, query, id)
	tool, err := scanWorkspaceTool(row)
	if err == sql.ErrNoRows {
		return nil, &types.ErrWorkspaceToolNotFound{ExternalId: fmt.Sprintf("%d", id)}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace tool: %w", err)
	}

	return tool, nil
}

// GetWorkspaceToolByExternalId retrieves a workspace tool by external UUID
func (b *PostgresBackend) GetWorkspaceToolByExternalId(ctx context.Context, externalId string) (*types.WorkspaceTool, error) {
	query := `
		SELECT id, external_id, workspace_id, name, provider_type, config, manifest, created_by_member_id, created_at, updated_at
		FROM workspace_tool
		WHERE external_id = $1
	`

	row := b.db.QueryRowContext(ctx, query, externalId)
	tool, err := scanWorkspaceTool(row)
	if err == sql.ErrNoRows {
		return nil, &types.ErrWorkspaceToolNotFound{ExternalId: externalId}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace tool by external id: %w", err)
	}

	return tool, nil
}

// GetWorkspaceToolByName retrieves a workspace tool by name within a workspace
func (b *PostgresBackend) GetWorkspaceToolByName(ctx context.Context, workspaceId uint, name string) (*types.WorkspaceTool, error) {
	query := `
		SELECT id, external_id, workspace_id, name, provider_type, config, manifest, created_by_member_id, created_at, updated_at
		FROM workspace_tool
		WHERE workspace_id = $1 AND name = $2
	`

	row := b.db.QueryRowContext(ctx, query, workspaceId, name)
	tool, err := scanWorkspaceTool(row)
	if err == sql.ErrNoRows {
		return nil, &types.ErrWorkspaceToolNotFound{Name: name}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get workspace tool by name: %w", err)
	}

	return tool, nil
}

// ListWorkspaceTools returns all tools for a workspace
func (b *PostgresBackend) ListWorkspaceTools(ctx context.Context, workspaceId uint) ([]*types.WorkspaceTool, error) {
	query := `
		SELECT id, external_id, workspace_id, name, provider_type, config, manifest, created_by_member_id, created_at, updated_at
		FROM workspace_tool
		WHERE workspace_id = $1
		ORDER BY name
	`

	rows, err := b.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("failed to list workspace tools: %w", err)
	}
	defer rows.Close()

	var tools []*types.WorkspaceTool
	for rows.Next() {
		tool, err := scanWorkspaceTool(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workspace tool: %w", err)
		}
		tools = append(tools, tool)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating workspace tools: %w", err)
	}

	return tools, nil
}

// UpdateWorkspaceToolManifest updates the manifest for a workspace tool
func (b *PostgresBackend) UpdateWorkspaceToolManifest(ctx context.Context, id uint, manifest json.RawMessage) error {
	query := `
		UPDATE workspace_tool
		SET manifest = $2, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`

	result, err := b.db.ExecContext(ctx, query, id, manifest)
	if err != nil {
		return fmt.Errorf("failed to update workspace tool manifest: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrWorkspaceToolNotFound{ExternalId: fmt.Sprintf("%d", id)}
	}

	return nil
}

// UpdateWorkspaceToolConfig updates the config for a workspace tool
func (b *PostgresBackend) UpdateWorkspaceToolConfig(ctx context.Context, id uint, config json.RawMessage) error {
	query := `
		UPDATE workspace_tool
		SET config = $2, manifest = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE id = $1
	`

	result, err := b.db.ExecContext(ctx, query, id, config)
	if err != nil {
		return fmt.Errorf("failed to update workspace tool config: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrWorkspaceToolNotFound{ExternalId: fmt.Sprintf("%d", id)}
	}

	return nil
}

// DeleteWorkspaceTool removes a workspace tool by internal ID
func (b *PostgresBackend) DeleteWorkspaceTool(ctx context.Context, id uint) error {
	query := `DELETE FROM workspace_tool WHERE id = $1`

	result, err := b.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete workspace tool: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrWorkspaceToolNotFound{ExternalId: fmt.Sprintf("%d", id)}
	}

	return nil
}

// DeleteWorkspaceToolByName removes a workspace tool by name within a workspace
func (b *PostgresBackend) DeleteWorkspaceToolByName(ctx context.Context, workspaceId uint, name string) error {
	query := `DELETE FROM workspace_tool WHERE workspace_id = $1 AND name = $2`

	result, err := b.db.ExecContext(ctx, query, workspaceId, name)
	if err != nil {
		return fmt.Errorf("failed to delete workspace tool: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return &types.ErrWorkspaceToolNotFound{Name: name}
	}

	return nil
}

// isUniqueViolation checks if an error is a unique constraint violation
func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// PostgreSQL unique violation error code is 23505
	return strings.Contains(errStr, "23505") ||
		strings.Contains(errStr, "unique constraint") ||
		strings.Contains(errStr, "duplicate key")
}
