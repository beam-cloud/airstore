package services

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const legacyQueryCredentialMemberIDKey = "credential_member_id"

func credentialCacheKey(workspaceID, memberID uint, integration string) string {
	return fmt.Sprintf("%d:%d:%s", workspaceID, memberID, integration)
}

func (s *SourceService) resolveConnection(
	ctx context.Context,
	workspaceID uint,
	memberID uint,
	integration string,
) (*types.IntegrationConnection, error) {
	if s == nil || s.backend == nil || workspaceID == 0 {
		return nil, nil
	}
	if memberID != 0 {
		return s.backend.GetConnection(ctx, workspaceID, memberID, integration)
	}
	conns, err := s.backend.ListConnections(ctx, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list connections: %w", err)
	}
	return selectBackgroundConnection(conns, integration)
}

func (s *SourceService) backgroundConnectionMemberID(
	ctx context.Context,
	workspaceID uint,
	integration string,
) (*uint, error) {
	if meta, ok := types.GetIntegrationMeta(types.IntegrationName(integration)); ok && meta.AuthType == types.AuthNone {
		return nil, nil
	}
	conn, err := s.resolveConnection(ctx, workspaceID, 0, integration)
	if err != nil || conn == nil || conn.MemberId == nil {
		return nil, err
	}
	memberID := *conn.MemberId
	return &memberID, nil
}

func (s *SourceService) sourceWatchCredentialMemberID(
	ctx context.Context,
	task *types.AgentTask,
	integration string,
) (*uint, error) {
	if task != nil && task.WorkspaceID != 0 && task.TargetRunID != nil {
		runID := strings.TrimSpace(*task.TargetRunID)
		if runID != "" && s != nil && s.backend != nil {
			run, err := s.backend.GetAgentRunByID(ctx, runID)
			if err != nil {
				if _, ok := err.(*types.ErrAgentRunNotFound); !ok {
					return nil, fmt.Errorf("lookup originating run: %w", err)
				}
				run = nil
			}
			if run != nil && run.WorkspaceID == task.WorkspaceID && run.CreatedByMemberID != nil {
				conn, err := s.resolveConnection(ctx, task.WorkspaceID, *run.CreatedByMemberID, integration)
				if err != nil {
					return nil, fmt.Errorf("resolve run member connection: %w", err)
				}
				if conn != nil && conn.MemberId != nil {
					memberID := *conn.MemberId
					return &memberID, nil
				}
				if conn != nil {
					return nil, nil
				}
			}
		}
	}
	if task == nil {
		return nil, nil
	}
	return s.backgroundConnectionMemberID(ctx, task.WorkspaceID, integration)
}

func selectBackgroundConnection(
	conns []types.IntegrationConnection,
	integration string,
) (*types.IntegrationConnection, error) {
	var shared *types.IntegrationConnection
	personal := make([]types.IntegrationConnection, 0, 1)

	for _, conn := range conns {
		if !strings.EqualFold(strings.TrimSpace(conn.IntegrationType), strings.TrimSpace(integration)) {
			continue
		}
		connCopy := conn
		if conn.MemberId == nil {
			if shared != nil {
				return nil, fmt.Errorf("multiple shared %s connections are configured", integration)
			}
			shared = &connCopy
			continue
		}
		personal = append(personal, connCopy)
	}

	if shared != nil {
		return shared, nil
	}
	switch len(personal) {
	case 0:
		return nil, nil
	case 1:
		return &personal[0], nil
	default:
		return nil, fmt.Errorf(
			"multiple member-scoped %s connections are configured; a shared connection or explicit member selector is required",
			integration,
		)
	}
}

func (s *SourceService) loadQueryCredentials(
	ctx context.Context,
	pctx *sources.ProviderContext,
	query *types.FilesystemQuery,
) (*sources.ProviderContext, bool) {
	if query == nil {
		return pctx, false
	}
	if pctx == nil {
		pctx = &sources.ProviderContext{}
	}
	if memberID := queryCredentialMemberID(query); memberID != nil {
		pctx.MemberId = *memberID
	}
	return s.loadCredentials(ctx, pctx, query.Integration)
}

func queryCredentialMemberID(query *types.FilesystemQuery) *uint {
	if query == nil {
		return nil
	}
	if query.CredentialMemberID != nil {
		return query.CredentialMemberID
	}
	// Legacy compatibility for rows created before credential_member_id existed
	// as a first-class source-view field.
	raw := strings.TrimSpace(parseQuerySpec(query.Integration, query.QuerySpec).Metadata[legacyQueryCredentialMemberIDKey])
	if raw == "" {
		return nil
	}
	memberID, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return nil
	}
	value := uint(memberID)
	return &value
}
