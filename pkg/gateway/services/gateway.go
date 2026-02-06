package services

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc/metadata"
)

type GatewayService struct {
	pb.UnimplementedGatewayServiceServer
	backend        repository.BackendRepository
	fsStore        repository.FilesystemStore
	s2Client       *common.S2Client
	hooksSvc       *hooks.Service
	sourceRegistry *sources.Registry
}

func NewGatewayService(backend repository.BackendRepository, s2Client *common.S2Client, fsStore repository.FilesystemStore, eventBus *common.EventBus, sourceRegistry *sources.Registry) *GatewayService {
	return &GatewayService{
		backend:        backend,
		s2Client:       s2Client,
		fsStore:        fsStore,
		hooksSvc:       &hooks.Service{Store: fsStore, Backend: backend, EventBus: eventBus},
		sourceRegistry: sourceRegistry,
	}
}

// Workspaces

func (s *GatewayService) CreateWorkspace(ctx context.Context, req *pb.CreateWorkspaceRequest) (*pb.WorkspaceResponse, error) {
	// Workspace creation requires gateway/admin auth
	if err := auth.RequireClusterAdmin(ctx); err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.CreateWorkspace(ctx, req.Name)
	if err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.WorkspaceResponse{Ok: true, Workspace: workspaceToPb(ws)}, nil
}

func (s *GatewayService) ListWorkspaces(ctx context.Context, req *pb.ListWorkspacesRequest) (*pb.ListWorkspacesResponse, error) {
	// Listing all workspaces requires gateway/admin auth
	if err := auth.RequireClusterAdmin(ctx); err != nil {
		return &pb.ListWorkspacesResponse{Ok: false, Error: err.Error()}, nil
	}

	workspaces, err := s.backend.ListWorkspaces(ctx)
	if err != nil {
		return &pb.ListWorkspacesResponse{Ok: false, Error: err.Error()}, nil
	}

	pbWorkspaces := make([]*pb.Workspace, 0, len(workspaces))
	for _, ws := range workspaces {
		pbWorkspaces = append(pbWorkspaces, workspaceToPb(ws))
	}
	return &pb.ListWorkspacesResponse{Ok: true, Workspaces: pbWorkspaces}, nil
}

func (s *GatewayService) GetWorkspace(ctx context.Context, req *pb.GetWorkspaceRequest) (*pb.WorkspaceResponse, error) {
	// Getting a workspace requires gateway auth or workspace token for that workspace
	if err := auth.RequireWorkspaceAccess(ctx, req.Id); err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.Id)
	if err != nil {
		return &pb.WorkspaceResponse{Ok: false, Error: err.Error()}, nil
	}
	if ws == nil {
		return &pb.WorkspaceResponse{Ok: false, Error: "workspace not found"}, nil
	}
	return &pb.WorkspaceResponse{Ok: true, Workspace: workspaceToPb(ws)}, nil
}

func (s *GatewayService) DeleteWorkspace(ctx context.Context, req *pb.DeleteWorkspaceRequest) (*pb.DeleteResponse, error) {
	// Workspace deletion requires gateway/admin auth
	if err := auth.RequireClusterAdmin(ctx); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	ws, err := s.backend.GetWorkspaceByExternalId(ctx, req.Id)
	if err != nil || ws == nil {
		return &pb.DeleteResponse{Ok: false, Error: "workspace not found"}, nil
	}
	if err := s.backend.DeleteWorkspace(ctx, ws.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Members

func (s *GatewayService) AddMember(ctx context.Context, req *pb.AddMemberRequest) (*pb.MemberResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.MemberResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireAdmin(ctx); err != nil {
		return &pb.MemberResponse{Ok: false, Error: err.Error()}, nil
	}

	role := types.MemberRole(req.Role)
	if role == "" {
		role = types.RoleMember
	}

	member, err := s.backend.CreateMember(ctx, ws.Id, req.Email, req.Name, role)
	if err != nil {
		return &pb.MemberResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.MemberResponse{Ok: true, Member: memberToPb(member, ws.ExternalId)}, nil
}

func (s *GatewayService) ListMembers(ctx context.Context, req *pb.ListMembersRequest) (*pb.ListMembersResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListMembersResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireAdmin(ctx); err != nil {
		return &pb.ListMembersResponse{Ok: false, Error: err.Error()}, nil
	}

	members, err := s.backend.ListMembers(ctx, ws.Id)
	if err != nil {
		return &pb.ListMembersResponse{Ok: false, Error: err.Error()}, nil
	}

	pbMembers := make([]*pb.Member, 0, len(members))
	for _, m := range members {
		pbMembers = append(pbMembers, memberToPb(&m, ws.ExternalId))
	}
	return &pb.ListMembersResponse{Ok: true, Members: pbMembers}, nil
}

func (s *GatewayService) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.DeleteResponse, error) {
	// Removing members requires admin role
	// Note: We can't verify workspace access here since we only have the member ID
	// The admin check ensures only admins can remove members
	if err := auth.RequireAdmin(ctx); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	if err := s.backend.DeleteMember(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Tokens

func (s *GatewayService) CreateToken(ctx context.Context, req *pb.CreateTokenRequest) (*pb.CreateTokenResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireAdmin(ctx); err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}

	member, err := s.backend.GetMember(ctx, req.MemberId)
	if err != nil || member == nil {
		return &pb.CreateTokenResponse{Ok: false, Error: "member not found"}, nil
	}
	if member.WorkspaceId != ws.Id {
		return &pb.CreateTokenResponse{Ok: false, Error: "member not in workspace"}, nil
	}

	var expiresAt *time.Time
	if req.ExpiresInSeconds > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresInSeconds) * time.Second)
		expiresAt = &t
	}

	name := req.Name
	if name == "" {
		name = "API Token"
	}

	token, raw, err := s.backend.CreateToken(ctx, ws.Id, member.Id, name, expiresAt, types.TokenTypeWorkspaceMember)
	if err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.CreateTokenResponse{
		Ok:    true,
		Token: raw,
		Info:  tokenToPb(token),
	}, nil
}

func (s *GatewayService) ListTokens(ctx context.Context, req *pb.ListTokensRequest) (*pb.ListTokensResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}
	if err := auth.RequireAdmin(ctx); err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}

	tokens, err := s.backend.ListTokens(ctx, ws.Id)
	if err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}

	pbTokens := make([]*pb.Token, 0, len(tokens))
	for _, t := range tokens {
		pbTokens = append(pbTokens, tokenToPb(&t))
	}
	return &pb.ListTokensResponse{Ok: true, Tokens: pbTokens}, nil
}

func (s *GatewayService) RevokeToken(ctx context.Context, req *pb.RevokeTokenRequest) (*pb.DeleteResponse, error) {
	// Revoking tokens requires admin role
	if err := auth.RequireAdmin(ctx); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	if err := s.backend.RevokeToken(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Worker Tokens (cluster-level)

func (s *GatewayService) CreateWorkerToken(ctx context.Context, req *pb.CreateWorkerTokenRequest) (*pb.CreateTokenResponse, error) {
	// Worker tokens require gateway auth (admin token)
	if err := auth.RequireClusterAdmin(ctx); err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}

	var expiresAt *time.Time
	if req.ExpiresInSeconds > 0 {
		t := time.Now().Add(time.Duration(req.ExpiresInSeconds) * time.Second)
		expiresAt = &t
	}

	var poolName *string
	if req.PoolName != "" {
		poolName = &req.PoolName
	}

	token, raw, err := s.backend.CreateWorkerToken(ctx, req.Name, poolName, expiresAt)
	if err != nil {
		return &pb.CreateTokenResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.CreateTokenResponse{
		Ok:    true,
		Token: raw,
		Info:  tokenToPb(token),
	}, nil
}

func (s *GatewayService) ListWorkerTokens(ctx context.Context, req *pb.ListWorkerTokensRequest) (*pb.ListTokensResponse, error) {
	// Worker tokens require gateway auth (admin token)
	if err := auth.RequireClusterAdmin(ctx); err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}

	tokens, err := s.backend.ListWorkerTokens(ctx)
	if err != nil {
		return &pb.ListTokensResponse{Ok: false, Error: err.Error()}, nil
	}

	pbTokens := make([]*pb.Token, 0, len(tokens))
	for _, t := range tokens {
		pbTokens = append(pbTokens, tokenToPb(&t))
	}
	return &pb.ListTokensResponse{Ok: true, Tokens: pbTokens}, nil
}

// Connections

func (s *GatewayService) AddConnection(ctx context.Context, req *pb.AddConnectionRequest) (*pb.ConnectionResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ConnectionResponse{Ok: false, Error: err.Error()}, nil
	}

	var memberId *uint
	if req.MemberId != "" {
		// Personal connection - require admin or self
		member, err := s.backend.GetMember(ctx, req.MemberId)
		if err != nil || member == nil {
			return &pb.ConnectionResponse{Ok: false, Error: "member not found"}, nil
		}
		if member.WorkspaceId != ws.Id {
			return &pb.ConnectionResponse{Ok: false, Error: "member not in workspace"}, nil
		}

		// Check authorization: admin can create for anyone, members only for themselves
		if !auth.IsAdmin(ctx) && member.Id != auth.MemberId(ctx) {
			return &pb.ConnectionResponse{Ok: false, Error: "cannot create connection for another member"}, nil
		}

		memberId = &member.Id
	} else {
		// Shared connection - require admin
		if err := auth.RequireAdmin(ctx); err != nil {
			return &pb.ConnectionResponse{Ok: false, Error: "admin access required for shared connections"}, nil
		}
	}

	creds := &types.IntegrationCredentials{
		AccessToken: req.AccessToken,
		APIKey:      req.ApiKey,
		Extra:       req.Extra,
	}

	// Validate credentials if the provider supports it
	if s.sourceRegistry != nil {
		if provider := s.sourceRegistry.Get(req.IntegrationType); provider != nil {
			if v, ok := provider.(sources.CredentialValidator); ok {
				if err := v.ValidateCredentials(ctx, creds); err != nil {
					return &pb.ConnectionResponse{Ok: false, Error: "invalid credentials: " + err.Error()}, nil
				}
			}
		}
	}

	conn, err := s.backend.SaveConnection(ctx, ws.Id, memberId, req.IntegrationType, creds, req.Scope)
	if err != nil {
		return &pb.ConnectionResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.ConnectionResponse{Ok: true, Connection: connectionToPb(conn, ws.ExternalId)}, nil
}

func (s *GatewayService) ListConnections(ctx context.Context, req *pb.ListConnectionsRequest) (*pb.ListConnectionsResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListConnectionsResponse{Ok: false, Error: err.Error()}, nil
	}

	conns, err := s.backend.ListConnections(ctx, ws.Id)
	if err != nil {
		return &pb.ListConnectionsResponse{Ok: false, Error: err.Error()}, nil
	}

	pbConns := make([]*pb.Connection, 0, len(conns))
	for _, c := range conns {
		pbConns = append(pbConns, connectionToPb(&c, ws.ExternalId))
	}
	return &pb.ListConnectionsResponse{Ok: true, Connections: pbConns}, nil
}

func (s *GatewayService) RemoveConnection(ctx context.Context, req *pb.RemoveConnectionRequest) (*pb.DeleteResponse, error) {
	// Fetch connection to check permissions
	conn, err := s.backend.GetConnectionByExternalId(ctx, req.Id)
	if err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	if conn == nil {
		return &pb.DeleteResponse{Ok: false, Error: "connection not found"}, nil
	}

	// Check authorization: shared connections require admin, personal require admin or owner
	if conn.IsShared() {
		if !auth.IsAdmin(ctx) {
			return &pb.DeleteResponse{Ok: false, Error: "admin access required for shared connections"}, nil
		}
	} else {
		// Personal connection
		if !auth.IsAdmin(ctx) && *conn.MemberId != auth.MemberId(ctx) {
			return &pb.DeleteResponse{Ok: false, Error: "cannot delete another member's connection"}, nil
		}
	}

	if err := s.backend.DeleteConnection(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// Tasks

func (s *GatewayService) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	workspaceId := auth.WorkspaceId(ctx)
	if workspaceId == 0 {
		return &pb.ListTasksResponse{Ok: false, Error: "authentication required"}, nil
	}

	tasks, err := s.backend.ListTasks(ctx, workspaceId)
	if err != nil {
		return &pb.ListTasksResponse{Ok: false, Error: err.Error()}, nil
	}

	pbTasks := make([]*pb.Task, 0, len(tasks))
	for _, t := range tasks {
		pbTasks = append(pbTasks, taskToPb(t))
	}
	return &pb.ListTasksResponse{Ok: true, Tasks: pbTasks}, nil
}

func (s *GatewayService) GetTask(ctx context.Context, req *pb.GetTaskRequest) (*pb.TaskResponse, error) {
	rc := auth.AuthInfoFromContext(ctx)
	if rc == nil {
		return &pb.TaskResponse{Ok: false, Error: "authentication required"}, nil
	}

	task, err := s.backend.GetTask(ctx, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return &pb.TaskResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.TaskResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.TaskResponse{Ok: true, Task: taskToPb(task)}, nil
}

func (s *GatewayService) GetTaskLogs(ctx context.Context, req *pb.GetTaskLogsRequest) (*pb.GetTaskLogsResponse, error) {
	rc := auth.AuthInfoFromContext(ctx)
	if rc == nil {
		return &pb.GetTaskLogsResponse{Ok: false, Error: "authentication required"}, nil
	}

	// Verify task exists
	_, err := s.backend.GetTask(ctx, req.Id)
	if err != nil {
		if _, ok := err.(*types.ErrTaskNotFound); ok {
			return &pb.GetTaskLogsResponse{Ok: false, Error: "task not found"}, nil
		}
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}

	// Fetch logs from S2
	if s.s2Client == nil || !s.s2Client.Enabled() {
		return &pb.GetTaskLogsResponse{Ok: true, Logs: []*pb.TaskLogEntry{}}, nil
	}

	logs, _, err := s.s2Client.ReadLogs(ctx, req.Id, 0)
	if err != nil {
		return &pb.GetTaskLogsResponse{Ok: false, Error: err.Error()}, nil
	}

	pbLogs := make([]*pb.TaskLogEntry, 0, len(logs))
	for _, log := range logs {
		pbLogs = append(pbLogs, &pb.TaskLogEntry{
			TaskId:    log.TaskID,
			Timestamp: log.Timestamp,
			Stream:    log.Stream,
			Data:      log.Data,
		})
	}

	return &pb.GetTaskLogsResponse{Ok: true, Logs: pbLogs}, nil
}

// Helpers

func workspaceToPb(ws *types.Workspace) *pb.Workspace {
	return &pb.Workspace{
		Id:        ws.ExternalId,
		Name:      ws.Name,
		CreatedAt: ws.CreatedAt.Format(time.RFC3339),
		UpdatedAt: ws.UpdatedAt.Format(time.RFC3339),
	}
}

func memberToPb(m *types.WorkspaceMember, workspaceExtId string) *pb.Member {
	return &pb.Member{
		Id:          m.ExternalId,
		WorkspaceId: workspaceExtId,
		Email:       m.Email,
		Name:        m.Name,
		Role:        string(m.Role),
		CreatedAt:   m.CreatedAt.Format(time.RFC3339),
	}
}

func tokenToPb(t *types.Token) *pb.Token {
	token := &pb.Token{
		Id:        t.ExternalId,
		Name:      t.Name,
		TokenType: string(t.TokenType),
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
	}
	if t.MemberId != nil {
		// For workspace member tokens, we'd need to look up the external ID
		// For now, just use the internal ID as a string
		token.MemberId = fmt.Sprintf("%d", *t.MemberId)
	}
	if t.PoolName != nil {
		token.PoolName = *t.PoolName
	}
	if t.ExpiresAt != nil {
		token.ExpiresAt = t.ExpiresAt.Format(time.RFC3339)
	}
	if t.LastUsedAt != nil {
		token.LastUsedAt = t.LastUsedAt.Format(time.RFC3339)
	}
	return token
}

func connectionToPb(c *types.IntegrationConnection, workspaceExtId string) *pb.Connection {
	conn := &pb.Connection{
		Id:              c.ExternalId,
		WorkspaceId:     workspaceExtId,
		IntegrationType: c.IntegrationType,
		Scope:           c.Scope,
		IsShared:        c.MemberId == nil,
		CreatedAt:       c.CreatedAt.Format(time.RFC3339),
	}
	return conn
}

func taskToPb(t *types.Task) *pb.Task {
	task := &pb.Task{
		Id:        t.ExternalId,
		Status:    string(t.Status),
		Prompt:    t.Prompt,
		Image:     t.Image,
		Error:     t.Error,
		CreatedAt: t.CreatedAt.Format(time.RFC3339),
	}
	if t.ExitCode != nil {
		task.ExitCode = int32(*t.ExitCode)
		task.HasExitCode = true
	}
	if t.StartedAt != nil {
		task.StartedAt = t.StartedAt.Format(time.RFC3339)
	}
	if t.FinishedAt != nil {
		task.FinishedAt = t.FinishedAt.Format(time.RFC3339)
	}
	return task
}

// Hooks

func (s *GatewayService) CreateHook(ctx context.Context, req *pb.CreateHookRequest) (*pb.HookResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.HookResponse{Ok: false, Error: err.Error()}, nil
	}

	rawToken := extractRawToken(ctx)
	if rawToken == "" {
		return &pb.HookResponse{Ok: false, Error: "authentication token required"}, nil
	}

	hook, err := s.hooksSvc.Create(ctx, ws.Id,
		ptrIfNonZero(auth.MemberId(ctx)),
		ptrIfNonZero(auth.TokenId(ctx)),
		rawToken, req.Path, req.Prompt)
	if err != nil {
		return &pb.HookResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.HookResponse{Ok: true, Hook: hookToPb(hook, ws.ExternalId)}, nil
}

func (s *GatewayService) ListHooks(ctx context.Context, req *pb.ListHooksRequest) (*pb.ListHooksResponse, error) {
	ws, err := s.resolveWorkspace(ctx, req.WorkspaceId)
	if err != nil {
		return &pb.ListHooksResponse{Ok: false, Error: err.Error()}, nil
	}

	hks, err := s.hooksSvc.List(ctx, ws.Id)
	if err != nil {
		return &pb.ListHooksResponse{Ok: false, Error: err.Error()}, nil
	}

	pbHooks := make([]*pb.Hook, 0, len(hks))
	for _, h := range hks {
		pbHooks = append(pbHooks, hookToPb(h, ws.ExternalId))
	}
	return &pb.ListHooksResponse{Ok: true, Hooks: pbHooks}, nil
}

func (s *GatewayService) GetHook(ctx context.Context, req *pb.GetHookRequest) (*pb.HookResponse, error) {
	hook, ws, err := s.resolveHook(ctx, req.Id)
	if err != nil {
		return &pb.HookResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.HookResponse{Ok: true, Hook: hookToPb(hook, ws.ExternalId)}, nil
}

func (s *GatewayService) UpdateHook(ctx context.Context, req *pb.UpdateHookRequest) (*pb.HookResponse, error) {
	// Validate access
	if _, _, err := s.resolveHook(ctx, req.Id); err != nil {
		return &pb.HookResponse{Ok: false, Error: err.Error()}, nil
	}

	var prompt *string
	if req.Prompt != "" {
		prompt = &req.Prompt
	}
	var active *bool
	if req.HasActive {
		active = &req.Active
	}

	hook, err := s.hooksSvc.Update(ctx, req.Id, prompt, active)
	if err != nil {
		return &pb.HookResponse{Ok: false, Error: err.Error()}, nil
	}

	wsExt := ""
	if ws, _ := s.backend.GetWorkspace(ctx, hook.WorkspaceId); ws != nil {
		wsExt = ws.ExternalId
	}
	return &pb.HookResponse{Ok: true, Hook: hookToPb(hook, wsExt)}, nil
}

func (s *GatewayService) DeleteHook(ctx context.Context, req *pb.DeleteHookRequest) (*pb.DeleteResponse, error) {
	if _, _, err := s.resolveHook(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	if err := s.hooksSvc.Delete(ctx, req.Id); err != nil {
		return &pb.DeleteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.DeleteResponse{Ok: true}, nil
}

// resolveWorkspace resolves the workspace from an explicit ID or the caller's token,
// validates access, and returns the workspace.
func (s *GatewayService) resolveWorkspace(ctx context.Context, workspaceExtId string) (*types.Workspace, error) {
	wsExtId, err := auth.ResolveWorkspaceExtId(ctx, workspaceExtId)
	if err != nil {
		return nil, err
	}
	ws, err := s.backend.GetWorkspaceByExternalId(ctx, wsExtId)
	if err != nil || ws == nil {
		return nil, fmt.Errorf("workspace not found")
	}
	return ws, nil
}

// resolveHook looks up a hook by external ID, validates workspace access, and returns both.
func (s *GatewayService) resolveHook(ctx context.Context, hookExtId string) (*types.Hook, *types.Workspace, error) {
	hook, err := s.fsStore.GetHook(ctx, hookExtId)
	if err != nil {
		return nil, nil, err
	}
	if hook == nil {
		return nil, nil, fmt.Errorf("hook not found")
	}
	ws, err := s.backend.GetWorkspace(ctx, hook.WorkspaceId)
	if err != nil || ws == nil {
		return nil, nil, fmt.Errorf("workspace not found")
	}
	if err := auth.RequireWorkspaceAccess(ctx, ws.ExternalId); err != nil {
		return nil, nil, err
	}
	return hook, ws, nil
}

// extractRawToken gets the raw bearer token from gRPC metadata.
func extractRawToken(ctx context.Context) string {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	vals := md.Get("authorization")
	if len(vals) == 0 {
		return ""
	}
	return strings.TrimPrefix(vals[0], "Bearer ")
}

func hookToPb(h *types.Hook, workspaceExternalId string) *pb.Hook {
	return &pb.Hook{
		Id:          h.ExternalId,
		WorkspaceId: workspaceExternalId,
		Path:        h.Path,
		Prompt:      h.Prompt,
		Active:      h.Active,
		CreatedAt:   h.CreatedAt.Format(time.RFC3339),
		UpdatedAt:   h.UpdatedAt.Format(time.RFC3339),
	}
}

func (s *GatewayService) ListHookRuns(ctx context.Context, req *pb.ListHookRunsRequest) (*pb.ListHookRunsResponse, error) {
	hook, _, err := s.resolveHook(ctx, req.HookId)
	if err != nil {
		return &pb.ListHookRunsResponse{Ok: false, Error: err.Error()}, nil
	}

	tasks, err := s.hooksSvc.ListRuns(ctx, hook.Id)
	if err != nil {
		return &pb.ListHookRunsResponse{Ok: false, Error: err.Error()}, nil
	}

	runs := make([]*pb.HookRun, 0, len(tasks))
	for _, t := range tasks {
		run := &pb.HookRun{
			TaskId:      t.ExternalId,
			Status:      string(t.Status),
			Attempt:     int32(t.Attempt),
			MaxAttempts: int32(t.MaxAttempts),
			Error:       t.Error,
			CreatedAt:   t.CreatedAt.Format(time.RFC3339),
		}
		if t.FinishedAt != nil {
			run.FinishedAt = t.FinishedAt.Format(time.RFC3339)
		}
		runs = append(runs, run)
	}
	return &pb.ListHookRunsResponse{Ok: true, Runs: runs}, nil
}

func ptrIfNonZero(v uint) *uint {
	if v == 0 {
		return nil
	}
	return &v
}
