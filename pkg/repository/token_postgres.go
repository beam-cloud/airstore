package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"golang.org/x/crypto/bcrypt"
)

// generateToken creates a cryptographically secure random token
func generateToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (r *PostgresBackend) CreateToken(ctx context.Context, workspaceId, memberId uint, name string, expiresAt *time.Time, tokenType types.TokenType) (*types.Token, string, error) {
	raw, err := generateToken()
	if err != nil {
		return nil, "", fmt.Errorf("generate token: %w", err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
	if err != nil {
		return nil, "", fmt.Errorf("hash token: %w", err)
	}

	// Default to workspace_member if not specified
	if tokenType == "" {
		tokenType = types.TokenTypeWorkspaceMember
	}

	query := `
		INSERT INTO token (workspace_id, member_id, token_hash, name, expires_at, token_type)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING id, external_id, workspace_id, member_id, token_type, token_hash, name, pool_name, expires_at, created_at, last_used_at
	`

	var t types.Token
	err = r.db.QueryRowContext(ctx, query, workspaceId, memberId, string(hash), name, expiresAt, tokenType).Scan(
		&t.Id, &t.ExternalId, &t.WorkspaceId, &t.MemberId, &t.TokenType, &t.TokenHash, &t.Name, &t.PoolName, &t.ExpiresAt, &t.CreatedAt, &t.LastUsedAt,
	)
	if err != nil {
		return nil, "", fmt.Errorf("create token: %w", err)
	}
	return &t, raw, nil
}

// CreateWorkerToken creates a cluster-level worker token (not tied to a workspace)
func (r *PostgresBackend) CreateWorkerToken(ctx context.Context, name string, poolName *string, expiresAt *time.Time) (*types.Token, string, error) {
	raw, err := generateToken()
	if err != nil {
		return nil, "", fmt.Errorf("generate token: %w", err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(raw), bcrypt.DefaultCost)
	if err != nil {
		return nil, "", fmt.Errorf("hash token: %w", err)
	}

	query := `
		INSERT INTO token (workspace_id, member_id, token_hash, name, pool_name, expires_at, token_type)
		VALUES (NULL, NULL, $1, $2, $3, $4, 'worker')
		RETURNING id, external_id, workspace_id, member_id, token_type, token_hash, name, pool_name, expires_at, created_at, last_used_at
	`

	var t types.Token
	err = r.db.QueryRowContext(ctx, query, string(hash), name, poolName, expiresAt).Scan(
		&t.Id, &t.ExternalId, &t.WorkspaceId, &t.MemberId, &t.TokenType, &t.TokenHash, &t.Name, &t.PoolName, &t.ExpiresAt, &t.CreatedAt, &t.LastUsedAt,
	)
	if err != nil {
		return nil, "", fmt.Errorf("create worker token: %w", err)
	}
	return &t, raw, nil
}

func (r *PostgresBackend) ValidateToken(ctx context.Context, rawToken string) (*types.TokenValidationResult, error) {
	// First, try to validate as a workspace token (has workspace_id and member_id)
	result, err := r.validateMemberToken(ctx, rawToken)
	if err == nil && result != nil {
		return result, nil
	}

	// Try to validate as a worker token (no workspace_id or member_id)
	return r.validateWorkerToken(ctx, rawToken)
}

func (r *PostgresBackend) validateMemberToken(ctx context.Context, rawToken string) (*types.TokenValidationResult, error) {
	query := `
		SELECT 
			t.id, t.token_hash, t.expires_at, t.token_type,
			w.id, w.external_id, w.name,
			m.id, m.external_id, m.email, m.role
		FROM token t
		JOIN workspace w ON t.workspace_id = w.id
		JOIN workspace_member m ON t.member_id = m.id
		WHERE (t.expires_at IS NULL OR t.expires_at > CURRENT_TIMESTAMP)
		  AND t.token_type = 'workspace_member'
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query tokens: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tokenId       uint
			tokenHash     string
			expiresAt     sql.NullTime
			tokenType     types.TokenType
			workspaceId   uint
			workspaceExt  string
			workspaceName string
			memberId      uint
			memberExt     string
			memberEmail   string
			memberRole    types.MemberRole
		)

		if err := rows.Scan(
			&tokenId, &tokenHash, &expiresAt, &tokenType,
			&workspaceId, &workspaceExt, &workspaceName,
			&memberId, &memberExt, &memberEmail, &memberRole,
		); err != nil {
			return nil, fmt.Errorf("scan token: %w", err)
		}

		if bcrypt.CompareHashAndPassword([]byte(tokenHash), []byte(rawToken)) != nil {
			continue
		}

		if expiresAt.Valid && expiresAt.Time.Before(time.Now()) {
			return nil, fmt.Errorf("token expired")
		}

		go func(id uint) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r.db.ExecContext(ctx, `UPDATE token SET last_used_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
		}(tokenId)

		return &types.TokenValidationResult{
			WorkspaceId:   workspaceId,
			WorkspaceExt:  workspaceExt,
			WorkspaceName: workspaceName,
			MemberId:      memberId,
			MemberExt:     memberExt,
			MemberEmail:   memberEmail,
			MemberRole:    memberRole,
			TokenType:     tokenType,
		}, nil
	}

	return nil, fmt.Errorf("invalid token")
}

func (r *PostgresBackend) validateWorkerToken(ctx context.Context, rawToken string) (*types.TokenValidationResult, error) {
	query := `
		SELECT t.id, t.token_hash, t.expires_at, t.pool_name
		FROM token t
		WHERE t.token_type = 'worker'
		  AND (t.expires_at IS NULL OR t.expires_at > CURRENT_TIMESTAMP)
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query worker tokens: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tokenId   uint
			tokenHash string
			expiresAt sql.NullTime
			poolName  sql.NullString
		)

		if err := rows.Scan(&tokenId, &tokenHash, &expiresAt, &poolName); err != nil {
			return nil, fmt.Errorf("scan worker token: %w", err)
		}

		if bcrypt.CompareHashAndPassword([]byte(tokenHash), []byte(rawToken)) != nil {
			continue
		}

		if expiresAt.Valid && expiresAt.Time.Before(time.Now()) {
			return nil, fmt.Errorf("token expired")
		}

		go func(id uint) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r.db.ExecContext(ctx, `UPDATE token SET last_used_at = CURRENT_TIMESTAMP WHERE id = $1`, id)
		}(tokenId)

		result := &types.TokenValidationResult{
			TokenType: types.TokenTypeWorker,
		}
		if poolName.Valid {
			result.PoolName = poolName.String
		}
		return result, nil
	}

	return nil, fmt.Errorf("invalid token")
}

// AuthorizeToken validates a token and returns AuthInfo for the auth system
func (r *PostgresBackend) AuthorizeToken(ctx context.Context, rawToken string) (*types.AuthInfo, error) {
	result, err := r.ValidateToken(ctx, rawToken)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("invalid token")
	}

	// Convert TokenValidationResult to AuthInfo
	info := &types.AuthInfo{
		TokenType: result.TokenType,
	}

	if result.TokenType == types.TokenTypeWorkspaceMember {
		info.Workspace = &types.WorkspaceInfo{
			Id:         result.WorkspaceId,
			ExternalId: result.WorkspaceExt,
			Name:       result.WorkspaceName,
		}
		info.Member = &types.MemberInfo{
			Id:         result.MemberId,
			ExternalId: result.MemberExt,
			Email:      result.MemberEmail,
			Role:       result.MemberRole,
		}
	} else if result.TokenType == types.TokenTypeWorker {
		info.Worker = &types.WorkerInfo{
			PoolName: result.PoolName,
		}
	}

	return info, nil
}

func (r *PostgresBackend) GetToken(ctx context.Context, externalId string) (*types.Token, error) {
	query := `
		SELECT id, external_id, workspace_id, member_id, token_type, token_hash, name, pool_name, expires_at, created_at, last_used_at
		FROM token WHERE external_id = $1
	`

	var t types.Token
	err := r.db.QueryRowContext(ctx, query, externalId).Scan(
		&t.Id, &t.ExternalId, &t.WorkspaceId, &t.MemberId, &t.TokenType, &t.TokenHash, &t.Name, &t.PoolName, &t.ExpiresAt, &t.CreatedAt, &t.LastUsedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}
	return &t, nil
}

func (r *PostgresBackend) ListTokens(ctx context.Context, workspaceId uint) ([]types.Token, error) {
	query := `
		SELECT id, external_id, workspace_id, member_id, token_type, token_hash, name, pool_name, expires_at, created_at, last_used_at
		FROM token WHERE workspace_id = $1 ORDER BY created_at DESC
	`

	rows, err := r.db.QueryContext(ctx, query, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("list tokens: %w", err)
	}
	defer rows.Close()

	var tokens []types.Token
	for rows.Next() {
		var t types.Token
		if err := rows.Scan(&t.Id, &t.ExternalId, &t.WorkspaceId, &t.MemberId, &t.TokenType, &t.TokenHash, &t.Name, &t.PoolName, &t.ExpiresAt, &t.CreatedAt, &t.LastUsedAt); err != nil {
			return nil, fmt.Errorf("scan token: %w", err)
		}
		tokens = append(tokens, t)
	}
	return tokens, rows.Err()
}

// ListWorkerTokens returns all worker tokens (cluster-level, not workspace-scoped)
func (r *PostgresBackend) ListWorkerTokens(ctx context.Context) ([]types.Token, error) {
	query := `
		SELECT id, external_id, workspace_id, member_id, token_type, token_hash, name, pool_name, expires_at, created_at, last_used_at
		FROM token WHERE token_type = 'worker' ORDER BY created_at DESC
	`

	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list worker tokens: %w", err)
	}
	defer rows.Close()

	var tokens []types.Token
	for rows.Next() {
		var t types.Token
		if err := rows.Scan(&t.Id, &t.ExternalId, &t.WorkspaceId, &t.MemberId, &t.TokenType, &t.TokenHash, &t.Name, &t.PoolName, &t.ExpiresAt, &t.CreatedAt, &t.LastUsedAt); err != nil {
			return nil, fmt.Errorf("scan worker token: %w", err)
		}
		tokens = append(tokens, t)
	}
	return tokens, rows.Err()
}

func (r *PostgresBackend) RevokeToken(ctx context.Context, externalId string) error {
	query := `DELETE FROM token WHERE external_id = $1`
	result, err := r.db.ExecContext(ctx, query, externalId)
	if err != nil {
		return fmt.Errorf("revoke token: %w", err)
	}

	n, _ := result.RowsAffected()
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}
