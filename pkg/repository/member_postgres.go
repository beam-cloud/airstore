package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
)

func (r *PostgresBackend) CreateMember(ctx context.Context, workspaceId uint, email, name string, role types.MemberRole) (*types.WorkspaceMember, error) {
	query := `
		INSERT INTO workspace_member (workspace_id, email, name, role)
		VALUES ($1, $2, $3, $4)
		RETURNING id, external_id, workspace_id, email, name, role, created_at, updated_at
	`

	var m types.WorkspaceMember
	err := r.db.QueryRowContext(ctx, query, workspaceId, email, name, role).Scan(
		&m.Id, &m.ExternalId, &m.WorkspaceId, &m.Email, &m.Name, &m.Role, &m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("create member: %w", err)
	}
	return &m, nil
}

func (r *PostgresBackend) GetMember(ctx context.Context, externalId string) (*types.WorkspaceMember, error) {
	query := `
		SELECT id, external_id, workspace_id, email, name, role, created_at, updated_at
		FROM workspace_member WHERE external_id = $1
	`

	var m types.WorkspaceMember
	err := r.db.QueryRowContext(ctx, query, externalId).Scan(
		&m.Id, &m.ExternalId, &m.WorkspaceId, &m.Email, &m.Name, &m.Role, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get member: %w", err)
	}
	return &m, nil
}

func (r *PostgresBackend) GetMemberByEmail(ctx context.Context, workspaceId uint, email string) (*types.WorkspaceMember, error) {
	query := `
		SELECT id, external_id, workspace_id, email, name, role, created_at, updated_at
		FROM workspace_member WHERE workspace_id = $1 AND email = $2
	`

	var m types.WorkspaceMember
	err := r.db.QueryRowContext(ctx, query, workspaceId, email).Scan(
		&m.Id, &m.ExternalId, &m.WorkspaceId, &m.Email, &m.Name, &m.Role, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get member by email: %w", err)
	}
	return &m, nil
}

func (r *PostgresBackend) ListMembers(ctx context.Context, workspaceId uint) ([]types.WorkspaceMember, error) {
	query := `
		SELECT id, external_id, workspace_id, email, name, role, created_at, updated_at
		FROM workspace_member WHERE workspace_id = $1 ORDER BY created_at ASC
	`

	rows, err := r.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	defer rows.Close()

	var members []types.WorkspaceMember
	for rows.Next() {
		var m types.WorkspaceMember
		if err := rows.Scan(&m.Id, &m.ExternalId, &m.WorkspaceId, &m.Email, &m.Name, &m.Role, &m.CreatedAt, &m.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan member: %w", err)
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

func (r *PostgresBackend) UpdateMember(ctx context.Context, externalId string, name string, role types.MemberRole) (*types.WorkspaceMember, error) {
	query := `
		UPDATE workspace_member SET name = $2, role = $3, updated_at = CURRENT_TIMESTAMP
		WHERE external_id = $1
		RETURNING id, external_id, workspace_id, email, name, role, created_at, updated_at
	`

	var m types.WorkspaceMember
	err := r.db.QueryRowContext(ctx, query, externalId, name, role).Scan(
		&m.Id, &m.ExternalId, &m.WorkspaceId, &m.Email, &m.Name, &m.Role, &m.CreatedAt, &m.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("update member: %w", err)
	}
	return &m, nil
}

func (r *PostgresBackend) DeleteMember(ctx context.Context, externalId string) error {
	query := `DELETE FROM workspace_member WHERE external_id = $1`
	result, err := r.db.ExecContext(ctx, query, externalId)
	if err != nil {
		return fmt.Errorf("delete member: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
