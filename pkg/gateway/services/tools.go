package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/tools"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/metadata"
)

type ToolService struct {
	pb.UnimplementedToolServiceServer
	registry      *tools.Registry
	resolver      *tools.WorkspaceToolResolver
	backend       repository.BackendRepository
	oauthRegistry *oauth.Registry
	recorder      instrumentation.EventRecorder
	redisClient   *common.RedisClient
}

const pendingToolCallTTL = 10 * time.Minute

// SetEventRecorder sets the product analytics event recorder.
func (s *ToolService) SetEventRecorder(r instrumentation.EventRecorder) {
	s.recorder = r
}

// SetRedisClient enables pending-tool-call storage for write gating.
func (s *ToolService) SetRedisClient(rc *common.RedisClient) {
	s.redisClient = rc
}

// PendingToolCall is stored in Redis when a write command requires approval.
type PendingToolCall struct {
	Tool        string   `json:"tool"`
	Args        []string `json:"args"`
	WorkspaceID uint     `json:"workspace_id"`
	MemberID    uint     `json:"member_id"`
	Summary     string   `json:"summary"`
	Details     string   `json:"details"`
}

func NewToolService(registry *tools.Registry) *ToolService {
	return &ToolService{registry: registry}
}

func NewToolServiceWithBackend(registry *tools.Registry, backend repository.BackendRepository) *ToolService {
	resolver := tools.NewWorkspaceToolResolver(registry, backend)
	return &ToolService{registry: registry, resolver: resolver, backend: backend}
}

func NewToolServiceWithOAuth(registry *tools.Registry, backend repository.BackendRepository, oauthRegistry *oauth.Registry) *ToolService {
	resolver := tools.NewWorkspaceToolResolver(registry, backend)
	return &ToolService{registry: registry, resolver: resolver, backend: backend, oauthRegistry: oauthRegistry}
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
				infos = append(infos, &pb.ToolInfo{Name: t.Name, Help: t.Help, LocalCommand: t.LocalCommand})
			}
			return &pb.ListToolsResponse{Ok: true, Tools: infos}, nil
		}
	}

	// Fallback to global registry only
	names := s.registry.List()
	infos := make([]*pb.ToolInfo, 0, len(names))
	for _, name := range names {
		if p := s.registry.Get(name); p != nil {
			info := &pb.ToolInfo{Name: p.Name(), Help: p.Help()}
			if lp, ok := p.(tools.LocalToolProvider); ok {
				info.LocalCommand = lp.LocalCommand()
			}
			infos = append(infos, info)
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

	if deferred, ok := s.checkWriteGate(ctx, req, stream); ok {
		if deferred {
			return nil
		}
	}

	execCtx := s.buildExecContext(ctx, req.Name)

	var stdout, stderr bytes.Buffer
	var err error

	start := time.Now()
	if execCtx != nil {
		err = p.ExecuteWithContext(ctx, execCtx, req.Args, &stdout, &stderr)
	} else {
		err = p.Execute(ctx, req.Args, &stdout, &stderr)
	}
	durationMs := time.Since(start).Milliseconds()

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

	if s.recorder != nil {
		s.recorder.Record(ctx, instrumentation.NewEvent("tool.executed", map[string]any{
			"tool_name":    req.Name,
			"exit_code":    int(exitCode),
			"success":      exitCode == 0,
			"duration_ms":  durationMs,
			"workspace_id": auth.WorkspaceExtId(ctx),
			"member_id":    auth.MemberId(ctx),
		}))
	}

	return stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: exitCode, Error: errMsg})
}

// checkWriteGate inspects whether this tool call is a write command that
// requires user approval. If approval is needed, it stores the pending call
// in Redis and returns a structured "approval required" message.
// Returns (deferred=true, ok=true) if the call was deferred.
// Returns (false, true) if execution should proceed normally.
// Returns (false, false) should never happen — ok is always true.
func (s *ToolService) checkWriteGate(ctx context.Context, req *pb.ExecuteToolRequest, stream pb.ToolService_ExecuteToolServer) (deferred, ok bool) {
	if !s.registry.IsValidWriteCommand(req.Name, req.Args) {
		return false, true
	}

	md, _ := metadata.FromIncomingContext(ctx)
	taskID := metaFirst(md, "x-airstore-task-id")
	if taskID == "" {
		return false, true
	}

	policyStr := metaFirst(md, "x-airstore-approval-policy")
	policy := types.NewApprovalPolicy(policyStr)
	if policy.AllowsWrite(types.IntegrationName(req.Name)) {
		return false, true
	}

	if s.redisClient == nil {
		log.Warn().Str("task_id", taskID).Msg("write gate: no redis client, allowing")
		return false, true
	}

	// If this tool+command was already rejected for this task, return an
	// error to the shim instead of creating another approval blocker.
	if len(req.Args) > 0 {
		rejKey := common.Keys.ToolRejection(taskID, req.Name, req.Args[0])
		if val, err := s.redisClient.Get(ctx, rejKey).Result(); err == nil && val != "" {
			log.Info().
				Str("task_id", taskID).
				Str("tool", req.Name).
				Str("command", req.Args[0]).
				Msg("write gate: tool call was previously rejected, auto-failing")
			_ = stream.Send(&pb.ExecuteToolResponse{
				Stream: pb.ExecuteToolResponse_STDERR,
				Data:   []byte("Error: This action was rejected by the user. Do not retry it.\n"),
			})
			_ = stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: 1})
			return true, true
		}
	}

	// If the user just approved a conversation-level blocker (e.g. "shall I
	// send?"), a pre-approval flag is set. Consume it and let the write
	// through — the user already expressed intent.
	preKey := common.Keys.WritePreapproval(taskID)
	if val, err := s.redisClient.GetDel(ctx, preKey).Result(); err == nil && val != "" {
		log.Info().
			Str("task_id", taskID).
			Str("tool", req.Name).
			Msg("write gate: pre-approved by prior user approval, allowing")
		return false, true
	}

	workspaceID := auth.WorkspaceId(ctx)
	memberID := auth.MemberId(ctx)

	var cmdSchema *tools.CommandSchema
	if len(req.Args) > 0 {
		cmdSchema = s.registry.GetCommandSchema(req.Name, req.Args[0])
	}
	summary, details := buildToolCallSummaryAndDetails(req.Name, req.Args, cmdSchema)

	pending := PendingToolCall{
		Tool:        req.Name,
		Args:        req.Args,
		WorkspaceID: workspaceID,
		MemberID:    memberID,
		Summary:     summary,
		Details:     details,
	}
	data, err := json.Marshal(pending)
	if err != nil {
		log.Error().Err(err).Msg("write gate: failed to marshal pending tool call")
		return false, true
	}

	key := common.Keys.PendingToolCall(taskID)
	if err := s.redisClient.Set(ctx, key, data, pendingToolCallTTL).Err(); err != nil {
		log.Error().Err(err).Str("task_id", taskID).Msg("write gate: failed to store pending tool call")
		return false, true
	}

	log.Info().
		Str("task_id", taskID).
		Str("tool", req.Name).
		Str("summary", summary).
		Msg("write gate: tool call deferred for approval")

	msg := fmt.Sprintf(
		"This action requires user approval. The task will pause for review.\nAction: %s\n",
		summary,
	)
	_ = stream.Send(&pb.ExecuteToolResponse{
		Stream: pb.ExecuteToolResponse_STDOUT,
		Data:   []byte(msg),
	})
	_ = stream.Send(&pb.ExecuteToolResponse{Done: true, ExitCode: 0})

	return true, true
}

// buildToolCallSummaryAndDetails returns a one-line summary for the blocker
// title and a rich details string that shows the full content of the action
// (email body, message text, PR description, etc.).
func buildToolCallSummaryAndDetails(toolName string, args []string, cmdSchema *tools.CommandSchema) (summary, details string) {
	if len(args) == 0 {
		return toolName, ""
	}
	subCmd := args[0]
	positional := extractPositionalArgs(args[1:], cmdSchema)

	switch toolName {
	case "gmail":
		// positional: to, subject, body [--thread-id ...] [--draft-id ...]
		to, subject, body := argAt(positional, 0), argAt(positional, 1), argAt(positional, 2)
		summary = fmt.Sprintf("Send email to %s — %s", to, subject)
		var b strings.Builder
		fmt.Fprintf(&b, "**To:** %s\n**Subject:** %s\n\n", to, subject)
		if body != "" {
			b.WriteString(body)
		}
		details = b.String()

	case "slack":
		// positional: channel, text
		channel, text := argAt(positional, 0), argAt(positional, 1)
		summary = fmt.Sprintf("Post message in #%s", channel)
		details = fmt.Sprintf("**Channel:** #%s\n\n%s", channel, text)

	case "github":
		owner, repo := argAt(positional, 0), argAt(positional, 1)
		repoFull := fmt.Sprintf("%s/%s", owner, repo)
		switch subCmd {
		case "create-pr":
			title := argAt(positional, 2)
			summary = fmt.Sprintf("Create PR in %s — %s", repoFull, title)
			details = fmt.Sprintf("**Repo:** %s\n**Title:** %s", repoFull, title)
		case "create-issue":
			title := argAt(positional, 2)
			summary = fmt.Sprintf("Create issue in %s — %s", repoFull, title)
			details = fmt.Sprintf("**Repo:** %s\n**Title:** %s", repoFull, title)
		case "comment-pr":
			number, body := argAt(positional, 2), argAt(positional, 3)
			summary = fmt.Sprintf("Comment on %s#%s", repoFull, number)
			details = fmt.Sprintf("**PR:** %s#%s\n\n%s", repoFull, number, body)
		case "review-pr":
			number := argAt(positional, 2)
			summary = fmt.Sprintf("Review %s#%s", repoFull, number)
			details = fmt.Sprintf("**PR:** %s#%s", repoFull, number)
		default:
			summary = fmt.Sprintf("github %s on %s", subCmd, repoFull)
		}

	case "linear":
		title := argAt(positional, 0)
		summary = fmt.Sprintf("Create Linear issue — %s", title)
		details = fmt.Sprintf("**Title:** %s", title)

	case "notion":
		switch subCmd {
		case "create-page":
			title := argAt(positional, 0)
			summary = fmt.Sprintf("Create Notion page — %s", title)
			details = fmt.Sprintf("**Title:** %s", title)
		case "append-paragraph":
			text := argAt(positional, 1)
			summary = "Append paragraph to Notion page"
			details = text
		default:
			summary = fmt.Sprintf("notion %s", subCmd)
		}

	default:
		summary = fmt.Sprintf("%s %s", toolName, subCmd)
	}

	return summary, details
}

// extractPositionalArgs strips flags and their values, returning only
// positional arguments. Uses the command schema (when available) to
// correctly handle boolean flags that don't consume a following token.
func extractPositionalArgs(args []string, cmdSchema *tools.CommandSchema) []string {
	boolFlags := make(map[string]bool)
	if cmdSchema != nil {
		for _, p := range cmdSchema.Params {
			if p.Type == "bool" {
				if p.Flag != "" {
					boolFlags[p.Flag] = true
				}
				if p.Short != "" {
					boolFlags[p.Short] = true
				}
			}
		}
	}

	var positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			positional = append(positional, a)
			continue
		}
		if strings.Contains(a, "=") {
			continue
		}
		if !boolFlags[a] && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
			i++
		}
	}
	return positional
}

func argAt(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}
	return ""
}

func metaFirst(md metadata.MD, key string) string {
	vals := md.Get(key)
	if len(vals) > 0 {
		return strings.TrimSpace(vals[0])
	}
	return ""
}

// ExecuteDeferred runs a previously-stored tool call server-side.
// Used by the orchestration layer after a user approves a deferred write.
// The context comes from the orchestration layer (not a gRPC handler), so
// we build the execution context from the stored workspace/member IDs
// rather than relying on auth middleware.
func (s *ToolService) ExecuteDeferred(ctx context.Context, workspaceID, memberID uint, toolName string, args []string) (stdout, stderr string, exitCode int, err error) {
	// Inject a synthetic AuthInfo so resolver lookups and credential
	// fetches work — the context comes from orchestration, not a gRPC handler.
	ctx = auth.WithAuthInfo(ctx, &types.AuthInfo{
		Workspace: &types.WorkspaceInfo{Id: workspaceID},
		Member:    &types.MemberInfo{Id: memberID},
	})

	var p tools.ToolProvider
	if s.resolver != nil {
		p, err = s.resolver.Get(ctx, toolName)
		if err != nil || p == nil {
			return "", "", 1, fmt.Errorf("tool %q not available: %w", toolName, err)
		}
	} else {
		p = s.registry.Get(toolName)
		if p == nil {
			return "", "", 1, fmt.Errorf("tool %q not found", toolName)
		}
	}

	execCtx := s.buildExecContext(ctx, toolName)

	var stdoutBuf, stderrBuf bytes.Buffer
	if execCtx != nil {
		err = p.ExecuteWithContext(ctx, execCtx, args, &stdoutBuf, &stderrBuf)
	} else {
		err = p.Execute(ctx, args, &stdoutBuf, &stderrBuf)
	}

	code := 0
	if err != nil {
		code = 1
	}
	return stdoutBuf.String(), stderrBuf.String(), code, err
}

const toolRejectionTTL = 10 * time.Minute
const writePreapprovalTTL = 10 * time.Second

// RecordToolRejection stores a rejection marker in Redis so that if the agent
// retries the same tool command, checkWriteGate returns an error instead of
// creating another approval blocker.
func (s *ToolService) RecordToolRejection(ctx context.Context, taskID, tool, command string) error {
	if s.redisClient == nil || taskID == "" || tool == "" || command == "" {
		return nil
	}
	key := common.Keys.ToolRejection(taskID, tool, command)
	return s.redisClient.Set(ctx, key, "rejected", toolRejectionTTL).Err()
}

// GrantWritePreapproval sets a short-lived flag indicating the user already
// approved a conversation-level action on this task. The next write-gate call
// for the same task consumes it and lets the tool through without a second
// approval prompt.
func (s *ToolService) GrantWritePreapproval(ctx context.Context, taskID string) error {
	if s.redisClient == nil || taskID == "" {
		return nil
	}
	key := common.Keys.WritePreapproval(taskID)
	return s.redisClient.Set(ctx, key, "1", writePreapprovalTTL).Err()
}

func (s *ToolService) buildExecContext(ctx context.Context, toolName string) *tools.ExecutionContext {
	if !auth.IsAuthenticated(ctx) {
		return nil
	}

	workspaceId := auth.WorkspaceId(ctx)
	memberId := auth.MemberId(ctx)

	execCtx := &tools.ExecutionContext{
		WorkspaceId:   workspaceId,
		WorkspaceName: auth.WorkspaceName(ctx),
		MemberId:      memberId,
		MemberEmail:   auth.MemberEmail(ctx),
	}

	// No backend or workspace - return basic context
	if s.backend == nil || workspaceId == 0 {
		return execCtx
	}

	// Check if this tool requires credentials
	if !types.RequiresAuth(types.IntegrationName(toolName)) {
		return execCtx
	}

	// Look up credentials (personal > shared)
	conn, err := s.backend.GetConnection(ctx, workspaceId, memberId, toolName)
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

	// Check if OAuth token needs refresh
	if s.oauthRegistry != nil && oauth.NeedsRefresh(creds) {
		if provider, err := s.oauthRegistry.GetProviderForIntegration(toolName); err == nil {
			refreshed, err := provider.Refresh(ctx, creds.RefreshToken)
			if err != nil {
				log.Warn().Str("tool", toolName).Str("provider", provider.Name()).Err(err).Msg("token refresh failed")
				// Continue with existing creds - they might still work
			} else {
				refreshed = oauth.MergeCredentialMetadata(refreshed, creds)
				var scopes []string
				if refreshed.Extra != nil {
					scopes = types.CSVToList(refreshed.Extra[types.CredentialMetaGrantedScopes])
				}
				refreshed = oauth.AnnotateCredentials(toolName, refreshed, scopes)

				// Update stored credentials
				if _, err := s.backend.SaveConnection(ctx, conn.WorkspaceId, conn.MemberId, toolName, refreshed, conn.Scope); err != nil {
					log.Warn().Str("tool", toolName).Err(err).Msg("failed to persist refreshed token")
				} else {
					log.Debug().Str("tool", toolName).Str("provider", provider.Name()).Msg("token refreshed successfully")
				}
				creds = refreshed
			}
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
