package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/tools"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

type ToolService struct {
	pb.UnimplementedToolServiceServer
	registry    *tools.Registry
	resolver    *tools.WorkspaceToolResolver
	backend     repository.BackendRepository
	googleOAuth *oauth.GoogleClient
}

func NewToolService(registry *tools.Registry) *ToolService {
	return &ToolService{registry: registry}
}

func NewToolServiceWithBackend(registry *tools.Registry, backend repository.BackendRepository) *ToolService {
	resolver := tools.NewWorkspaceToolResolver(registry, backend)
	return &ToolService{registry: registry, resolver: resolver, backend: backend}
}

func NewToolServiceWithOAuth(registry *tools.Registry, backend repository.BackendRepository, googleOAuth *oauth.GoogleClient) *ToolService {
	resolver := tools.NewWorkspaceToolResolver(registry, backend)
	return &ToolService{registry: registry, resolver: resolver, backend: backend, googleOAuth: googleOAuth}
}

// Resolver returns the workspace tool resolver for use by other components
func (s *ToolService) Resolver() *tools.WorkspaceToolResolver {
	return s.resolver
}

func (s *ToolService) ListTools(ctx context.Context, req *pb.ListToolsRequest) (*pb.ListToolsResponse, error) {
	// Use resolver if available (includes workspace tools and respects disabled settings)
	if s.resolver != nil {
		resolved, err := s.resolver.ListEnabled(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("resolver list failed, falling back to registry")
		} else {
			infos := make([]*pb.ToolInfo, 0, len(resolved))
			for _, t := range resolved {
				infos = append(infos, &pb.ToolInfo{Name: t.Name, Help: t.Help})
			}
			return &pb.ListToolsResponse{Ok: true, Tools: infos}, nil
		}
	}

	// Fallback to global registry only
	names := s.registry.List()
	infos := make([]*pb.ToolInfo, 0, len(names))
	for _, name := range names {
		if p := s.registry.Get(name); p != nil {
			infos = append(infos, &pb.ToolInfo{Name: p.Name(), Help: p.Help()})
		}
	}
	return &pb.ListToolsResponse{Ok: true, Tools: infos}, nil
}

func (s *ToolService) GetToolHelp(ctx context.Context, req *pb.GetToolHelpRequest) (*pb.GetToolHelpResponse, error) {
	// Use resolver if available
	if s.resolver != nil {
		p, err := s.resolver.Get(ctx, req.Name)
		if err != nil {
			log.Warn().Err(err).Str("tool", req.Name).Msg("resolver get failed")
			return &pb.GetToolHelpResponse{Ok: false, Error: err.Error()}, nil
		}
		if p == nil {
			return &pb.GetToolHelpResponse{Ok: false, Error: "tool not found or disabled"}, nil
		}
		return &pb.GetToolHelpResponse{Ok: true, Help: p.Help()}, nil
	}

	// Fallback to registry
	p := s.registry.Get(req.Name)
	if p == nil {
		return &pb.GetToolHelpResponse{Ok: false, Error: "tool not found"}, nil
	}
	return &pb.GetToolHelpResponse{Ok: true, Help: p.Help()}, nil
}

func (s *ToolService) ExecuteTool(req *pb.ExecuteToolRequest, stream pb.ToolService_ExecuteToolServer) error {
	ctx := stream.Context()

	// Use resolver if available (respects disabled settings and includes workspace tools)
	var p tools.ToolProvider
	if s.resolver != nil {
		var err error
		p, err = s.resolver.Get(ctx, req.Name)
		if err != nil {
			log.Warn().Err(err).Str("tool", req.Name).Msg("resolver get failed")
			return stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: 1, Error: err.Error()})
		}
		if p == nil {
			log.Warn().Str("tool", req.Name).Msg("tool not found or disabled")
			return stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: 1, Error: "tool not found or disabled"})
		}
	} else {
		// Fallback to registry
		p = s.registry.Get(req.Name)
		if p == nil {
			log.Warn().Str("tool", req.Name).Msg("tool not found")
			return stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: 1, Error: "tool not found"})
		}
	}

	execCtx := s.buildExecContext(ctx, req.Name)

	var stdout, stderr bytes.Buffer
	var err error

	if execCtx != nil {
		err = p.ExecuteWithContext(ctx, execCtx, req.Args, &stdout, &stderr)
	} else {
		err = p.Execute(ctx, req.Args, &stdout, &stderr)
	}

	if stdout.Len() > 0 {
		if e := stream.Send(&pb.ExecuteToolResponse{Stream: pb.ExecuteToolResponse_STDOUT, Data: stdout.Bytes()}); e != nil {
			return e
		}
	}
	if stderr.Len() > 0 {
		if e := stream.Send(&pb.ExecuteToolResponse{Stream: pb.ExecuteToolResponse_STDERR, Data: stderr.Bytes()}); e != nil {
			return e
		}
	}

	exitCode := int32(0)
	errMsg := ""
	if err != nil {
		exitCode = 1
		errMsg = err.Error()
		log.Warn().Str("tool", req.Name).Str("error", errMsg).Msg("tool failed")
	}

	return stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: exitCode, Error: errMsg})
}

func (s *ToolService) buildExecContext(ctx context.Context, toolName string) *tools.ExecutionContext {
	rc := auth.FromContext(ctx)
	if rc == nil {
		return nil
	}

	execCtx := &tools.ExecutionContext{
		WorkspaceId:   rc.WorkspaceId,
		WorkspaceName: rc.WorkspaceName,
		MemberId:      rc.MemberId,
		MemberEmail:   rc.MemberEmail,
	}

	// No backend or workspace - return basic context
	if s.backend == nil || rc.WorkspaceId == 0 {
		return execCtx
	}

	// Check if this tool requires credentials
	if !types.RequiresAuth(types.ToolName(toolName)) {
		return execCtx
	}

	// Look up credentials (personal > shared)
	conn, err := s.backend.GetConnection(ctx, rc.WorkspaceId, rc.MemberId, toolName)
	if err != nil {
		log.Warn().Str("tool", toolName).Err(err).Msg("connection lookup failed")
		return execCtx
	}
	if conn == nil {
		return execCtx
	}

	creds, err := s.decryptCredentials(conn.Credentials)
	if err != nil {
		log.Warn().Str("tool", toolName).Err(err).Msg("credential decrypt failed")
		return execCtx
	}

	// Check if Google OAuth token needs refresh
	if oauth.IsGoogleIntegration(toolName) && oauth.NeedsRefresh(creds) && s.googleOAuth != nil {
		refreshed, err := s.googleOAuth.Refresh(ctx, creds.RefreshToken)
		if err != nil {
			log.Warn().Str("tool", toolName).Err(err).Msg("token refresh failed")
			// Continue with existing creds - they might still work
		} else {
			// Update stored credentials
			if _, err := s.backend.SaveConnection(ctx, conn.WorkspaceId, conn.MemberId, toolName, refreshed, conn.Scope); err != nil {
				log.Warn().Str("tool", toolName).Err(err).Msg("failed to persist refreshed token")
			} else {
				log.Debug().Str("tool", toolName).Msg("token refreshed successfully")
			}
			creds = refreshed
		}
	}

	execCtx.Credentials = creds
	return execCtx
}

func (s *ToolService) decryptCredentials(data []byte) (*types.IntegrationCredentials, error) {
	// TODO: implement encryption
	var creds types.IntegrationCredentials
	if err := json.Unmarshal(data, &creds); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	return &creds, nil
}
