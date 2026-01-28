package apiv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/gateway/services"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/tools"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// FilesystemGroup handles filesystem API endpoints
type FilesystemGroup struct {
	routerGroup    *echo.Group
	backend        repository.BackendRepository
	contextService *services.ContextService
	sourceRegistry *sources.Registry
	toolRegistry   *tools.Registry
}

// NewFilesystemGroup creates a new filesystem API group
func NewFilesystemGroup(
	routerGroup *echo.Group,
	backend repository.BackendRepository,
	contextService *services.ContextService,
	sourceRegistry *sources.Registry,
	toolRegistry *tools.Registry,
) *FilesystemGroup {
	g := &FilesystemGroup{
		routerGroup:    routerGroup,
		backend:        backend,
		contextService: contextService,
		sourceRegistry: sourceRegistry,
		toolRegistry:   toolRegistry,
	}
	g.registerRoutes()
	return g
}

func (g *FilesystemGroup) registerRoutes() {
	g.routerGroup.GET("/list", g.List)
	g.routerGroup.GET("/stat", g.Stat)
	g.routerGroup.GET("/read", g.Read)
	g.routerGroup.GET("/tree", g.Tree)
}

// List returns directory contents as VirtualFile entries
func (g *FilesystemGroup) List(c echo.Context) error {
	ctx := c.Request().Context()
	path := cleanPath(c.QueryParam("path"))

	// Root directory - return virtual root folders
	if path == "" || path == "/" {
		entries := g.listRootDirectories(ctx)
		return SuccessResponse(c, types.VirtualFileListResponse{
			Path:    "/",
			Entries: entries,
		})
	}

	// Determine which service to route to
	rootDir, relPath := splitRootPath(path)

	switch rootDir {
	case "context":
		return g.listContext(c, ctx, relPath)
	case "sources":
		return g.listSources(c, ctx, relPath)
	case "tools":
		return g.listTools(c, ctx)
	default:
		return ErrorResponse(c, http.StatusNotFound, "path not found")
	}
}

// Stat returns file/directory info as VirtualFile
func (g *FilesystemGroup) Stat(c echo.Context) error {
	ctx := c.Request().Context()
	path := cleanPath(c.QueryParam("path"))

	// Root directory
	if path == "" || path == "/" {
		return SuccessResponse(c, types.NewRootFolder("", "/"))
	}

	rootDir, relPath := splitRootPath(path)

	switch rootDir {
	case "context":
		return g.statContext(c, ctx, path, relPath)
	case "sources":
		return g.statSources(c, ctx, path, relPath)
	case "tools":
		return g.statTools(c, ctx, path, relPath)
	default:
		return ErrorResponse(c, http.StatusNotFound, "path not found")
	}
}

// Read returns file contents
func (g *FilesystemGroup) Read(c echo.Context) error {
	ctx := c.Request().Context()
	path := cleanPath(c.QueryParam("path"))

	if path == "" || path == "/" {
		return ErrorResponse(c, http.StatusBadRequest, "cannot read directory")
	}

	rootDir, relPath := splitRootPath(path)

	// Parse optional offset and length
	offset, _ := strconv.ParseInt(c.QueryParam("offset"), 10, 64)
	length, _ := strconv.ParseInt(c.QueryParam("length"), 10, 64)

	switch rootDir {
	case "context":
		return g.readContext(c, ctx, relPath, offset, length)
	case "sources":
		return g.readSources(c, ctx, relPath, offset, length)
	case "tools":
		return ErrorResponse(c, http.StatusBadRequest, "tools are not readable as files")
	default:
		return ErrorResponse(c, http.StatusNotFound, "path not found")
	}
}

// Tree returns a flat listing of a subtree
func (g *FilesystemGroup) Tree(c echo.Context) error {
	ctx := c.Request().Context()
	path := cleanPath(c.QueryParam("path"))
	maxKeys, _ := strconv.ParseInt(c.QueryParam("max_keys"), 10, 32)
	if maxKeys <= 0 {
		maxKeys = 1000
	}
	continuationToken := c.QueryParam("continuation_token")

	rootDir, relPath := splitRootPath(path)

	switch rootDir {
	case "context":
		return g.treeContext(c, ctx, relPath, int32(maxKeys), continuationToken)
	case "sources":
		// Sources don't support tree listing - return empty
		return SuccessResponse(c, types.VirtualFileTreeResponse{
			Path:    path,
			Entries: []types.VirtualFile{},
		})
	case "tools":
		// Tools are flat - return same as list
		entries := g.buildToolEntries(ctx)
		return SuccessResponse(c, types.VirtualFileTreeResponse{
			Path:    path,
			Entries: entries,
		})
	default:
		return ErrorResponse(c, http.StatusNotFound, "path not found")
	}
}

// ============================================================================
// Root Directory
// ============================================================================

func (g *FilesystemGroup) listRootDirectories(ctx context.Context) []types.VirtualFile {
	entries := []types.VirtualFile{
		*types.NewRootFolder("context", "/context").
			WithMetadata("description", "Workspace context files"),
		*types.NewRootFolder("sources", "/sources").
			WithMetadata("description", "Connected integrations"),
		*types.NewRootFolder("tools", "/tools").
			WithMetadata("description", "Available tools"),
	}

	// Add child counts if possible
	if g.toolRegistry != nil {
		entries[2].ChildCount = len(g.toolRegistry.List())
	}
	if g.sourceRegistry != nil {
		entries[1].ChildCount = len(g.sourceRegistry.List())
	}

	return entries
}

// ============================================================================
// Context Service (S3-backed files)
// ============================================================================

func (g *FilesystemGroup) listContext(c echo.Context, ctx context.Context, relPath string) error {
	if g.contextService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "context service not available")
	}

	resp, err := g.contextService.ReadDir(ctx, &pb.ContextReadDirRequest{Path: relPath})
	if err != nil {
		log.Error().Err(err).Str("path", relPath).Msg("context ReadDir failed")
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if !resp.Ok {
		return ErrorResponse(c, http.StatusNotFound, resp.Error)
	}

	entries := make([]types.VirtualFile, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		vf := g.contextEntryToVirtualFile(e, relPath)
		entries = append(entries, *vf)
	}

	return SuccessResponse(c, types.VirtualFileListResponse{
		Path:    "/context/" + relPath,
		Entries: entries,
	})
}

func (g *FilesystemGroup) statContext(c echo.Context, ctx context.Context, fullPath, relPath string) error {
	if g.contextService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "context service not available")
	}

	// Root of context
	if relPath == "" {
		return SuccessResponse(c, types.NewRootFolder("context", "/context").
			WithMetadata("description", "Workspace context files"))
	}

	resp, err := g.contextService.Stat(ctx, &pb.ContextStatRequest{Path: relPath})
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if !resp.Ok {
		return ErrorResponse(c, http.StatusNotFound, resp.Error)
	}

	name := pathName(relPath)
	vf := types.NewVirtualFile(hashPath(fullPath), name, fullPath, types.VFTypeContext).
		WithFolder(resp.Info.IsDir).
		WithSize(resp.Info.Size)

	if resp.Info.Mtime > 0 {
		t := time.Unix(resp.Info.Mtime, 0)
		vf.WithModifiedAt(t)
	}
	if resp.Info.IsLink {
		vf.IsSymlink = true
	}

	return SuccessResponse(c, vf)
}

func (g *FilesystemGroup) readContext(c echo.Context, ctx context.Context, relPath string, offset, length int64) error {
	if g.contextService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "context service not available")
	}

	resp, err := g.contextService.Read(ctx, &pb.ContextReadRequest{
		Path:   relPath,
		Offset: offset,
		Length: length,
	})
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if !resp.Ok {
		return ErrorResponse(c, http.StatusNotFound, resp.Error)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", resp.Data)
}

func (g *FilesystemGroup) treeContext(c echo.Context, ctx context.Context, relPath string, maxKeys int32, continuationToken string) error {
	if g.contextService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "context service not available")
	}

	resp, err := g.contextService.ListTree(ctx, &pb.ListTreeRequest{
		Path:              relPath,
		MaxKeys:           maxKeys,
		ContinuationToken: continuationToken,
	})
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, err.Error())
	}
	if !resp.Ok {
		return ErrorResponse(c, http.StatusNotFound, resp.Error)
	}

	entries := make([]types.VirtualFile, 0, len(resp.Entries))
	for _, e := range resp.Entries {
		fullPath := "/context/" + e.Path
		isDir := e.Mode&uint32(syscall.S_IFDIR) != 0

		vf := types.NewVirtualFile(hashPath(fullPath), pathName(e.Path), fullPath, types.VFTypeContext).
			WithFolder(isDir).
			WithSize(e.Size)

		if e.Mtime > 0 {
			t := time.Unix(e.Mtime, 0)
			vf.WithModifiedAt(t)
		}
		if e.Etag != "" {
			vf.WithMetadata("etag", e.Etag)
		}

		entries = append(entries, *vf)
	}

	result := types.VirtualFileTreeResponse{
		Path:    "/context/" + relPath,
		Entries: entries,
	}

	// Add pagination info to response if truncated
	if resp.Truncated && resp.NextToken != "" {
		return c.JSON(http.StatusOK, Response{
			Success: true,
			Data: map[string]interface{}{
				"path":               result.Path,
				"entries":            result.Entries,
				"truncated":          true,
				"continuation_token": resp.NextToken,
			},
		})
	}

	return SuccessResponse(c, result)
}

func (g *FilesystemGroup) contextEntryToVirtualFile(e *pb.ContextDirEntry, parentPath string) *types.VirtualFile {
	fullPath := "/context"
	if parentPath != "" {
		fullPath += "/" + parentPath
	}
	fullPath += "/" + e.Name

	vf := types.NewVirtualFile(hashPath(fullPath), e.Name, fullPath, types.VFTypeContext).
		WithFolder(e.IsDir).
		WithSize(e.Size)

	if e.Mtime > 0 {
		t := time.Unix(e.Mtime, 0)
		vf.WithModifiedAt(t)
	}
	if e.Etag != "" {
		vf.WithMetadata("etag", e.Etag)
	}

	return vf
}

// ============================================================================
// Sources Service (Integration files)
// ============================================================================

func (g *FilesystemGroup) listSources(c echo.Context, ctx context.Context, relPath string) error {
	// Root /sources - list all integrations
	if relPath == "" {
		entries := g.buildSourceRootEntries(ctx)
		return SuccessResponse(c, types.VirtualFileListResponse{
			Path:    "/sources",
			Entries: entries,
		})
	}

	// For now, return the integration folders without deep listing
	// Deep listing would require calling SourceService which needs gRPC auth context
	integration, subPath := splitFirstPath(relPath)

	// Integration root
	if subPath == "" {
		entries := []types.VirtualFile{
			*types.NewVirtualFile(
				hashPath("/sources/"+integration+"/status.json"),
				"status.json",
				"/sources/"+integration+"/status.json",
				types.VFTypeSource,
			).WithFolder(false).WithReadOnly(true).WithMetadata("provider", integration),
		}
		return SuccessResponse(c, types.VirtualFileListResponse{
			Path:    "/sources/" + integration,
			Entries: entries,
		})
	}

	// Deep paths - return empty for now (would need gRPC context)
	return SuccessResponse(c, types.VirtualFileListResponse{
		Path:    "/sources/" + relPath,
		Entries: []types.VirtualFile{},
	})
}

func (g *FilesystemGroup) statSources(c echo.Context, ctx context.Context, fullPath, relPath string) error {
	// Root /sources
	if relPath == "" {
		return SuccessResponse(c, types.NewRootFolder("sources", "/sources").
			WithMetadata("description", "Connected integrations").
			WithChildCount(len(g.sourceRegistry.List())))
	}

	integration, subPath := splitFirstPath(relPath)

	// Check if integration exists
	if g.sourceRegistry.Get(integration) == nil {
		return ErrorResponse(c, http.StatusNotFound, "integration not found")
	}

	// Integration root
	if subPath == "" {
		vf := types.NewVirtualFile(
			hashPath(fullPath),
			integration,
			fullPath,
			types.VFTypeSource,
		).WithFolder(true).WithReadOnly(true).WithMetadata("provider", integration)
		return SuccessResponse(c, vf)
	}

	// status.json
	if subPath == "status.json" {
		vf := types.NewVirtualFile(
			hashPath(fullPath),
			"status.json",
			fullPath,
			types.VFTypeSource,
		).WithFolder(false).WithReadOnly(true).WithMetadata("provider", integration)
		return SuccessResponse(c, vf)
	}

	// Other paths - return generic source file
	vf := types.NewVirtualFile(
		hashPath(fullPath),
		pathName(subPath),
		fullPath,
		types.VFTypeSource,
	).WithReadOnly(true).WithMetadata("provider", integration)
	return SuccessResponse(c, vf)
}

func (g *FilesystemGroup) readSources(c echo.Context, ctx context.Context, relPath string, offset, length int64) error {
	if relPath == "" {
		return ErrorResponse(c, http.StatusBadRequest, "cannot read directory")
	}

	integration, subPath := splitFirstPath(relPath)

	// status.json - generate dynamically
	if subPath == "status.json" {
		// Check if integration is connected (would need auth context for full check)
		connected := false // Default to not connected without auth context
		data := sources.GenerateStatusJSON(integration, connected, "", "")
		return c.Blob(http.StatusOK, "application/json", data)
	}

	// Other source files would need gRPC context
	return ErrorResponse(c, http.StatusNotImplemented, "source file reading requires workspace authentication")
}

func (g *FilesystemGroup) buildSourceRootEntries(ctx context.Context) []types.VirtualFile {
	if g.sourceRegistry == nil {
		return []types.VirtualFile{}
	}

	integrations := g.sourceRegistry.List()
	entries := make([]types.VirtualFile, 0, len(integrations))

	for _, name := range integrations {
		vf := types.NewVirtualFile(
			hashPath("/sources/"+name),
			name,
			"/sources/"+name,
			types.VFTypeSource,
		).WithFolder(true).WithReadOnly(true).WithMetadata("provider", name)
		entries = append(entries, *vf)
	}

	return entries
}

// ============================================================================
// Tools Service
// ============================================================================

func (g *FilesystemGroup) listTools(c echo.Context, ctx context.Context) error {
	entries := g.buildToolEntries(ctx)
	return SuccessResponse(c, types.VirtualFileListResponse{
		Path:    "/tools",
		Entries: entries,
	})
}

func (g *FilesystemGroup) statTools(c echo.Context, ctx context.Context, fullPath, relPath string) error {
	// Root /tools
	if relPath == "" {
		return SuccessResponse(c, types.NewRootFolder("tools", "/tools").
			WithMetadata("description", "Available tools").
			WithChildCount(len(g.toolRegistry.List())))
	}

	// Specific tool
	toolName := relPath
	provider := g.toolRegistry.Get(toolName)
	if provider == nil {
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	vf := types.NewVirtualFile(
		"tool-"+toolName,
		toolName,
		fullPath,
		types.VFTypeTool,
	).WithFolder(false).WithReadOnly(true).
		WithMetadata("description", provider.Help()).
		WithMetadata("name", provider.Name())

	return SuccessResponse(c, vf)
}

func (g *FilesystemGroup) buildToolEntries(ctx context.Context) []types.VirtualFile {
	if g.toolRegistry == nil {
		return []types.VirtualFile{}
	}

	names := g.toolRegistry.List()
	entries := make([]types.VirtualFile, 0, len(names))

	for _, name := range names {
		provider := g.toolRegistry.Get(name)
		if provider == nil {
			continue
		}

		vf := types.NewVirtualFile(
			"tool-"+name,
			name,
			"/tools/"+name,
			types.VFTypeTool,
		).WithFolder(false).WithReadOnly(true).
			WithMetadata("description", provider.Help()).
			WithMetadata("name", provider.Name())

		entries = append(entries, *vf)
	}

	return entries
}

// ============================================================================
// Helpers
// ============================================================================

// cleanPath removes leading/trailing slashes and normalizes the path
func cleanPath(path string) string {
	return strings.Trim(path, "/")
}

// splitRootPath splits a path into root directory and relative path
// e.g., "context/foo/bar" -> ("context", "foo/bar")
func splitRootPath(path string) (root, relPath string) {
	path = cleanPath(path)
	parts := strings.SplitN(path, "/", 2)
	root = parts[0]
	if len(parts) > 1 {
		relPath = parts[1]
	}
	return
}

// splitFirstPath splits a path into first component and rest
func splitFirstPath(path string) (first, rest string) {
	parts := strings.SplitN(path, "/", 2)
	first = parts[0]
	if len(parts) > 1 {
		rest = parts[1]
	}
	return
}

// pathName returns the last component of a path
func pathName(path string) string {
	if path == "" {
		return ""
	}
	parts := strings.Split(strings.TrimSuffix(path, "/"), "/")
	return parts[len(parts)-1]
}

// hashPath creates a stable ID from a path
func hashPath(path string) string {
	h := sha256.Sum256([]byte(path))
	return hex.EncodeToString(h[:8])
}

// FilesystemAuthConfig holds auth configuration for the filesystem API
type FilesystemAuthConfig struct {
	AdminToken string
	Backend    repository.BackendRepository
}

// NewFilesystemAuthMiddleware creates middleware that validates workspace access
// It accepts both admin tokens and workspace tokens
func NewFilesystemAuthMiddleware(cfg FilesystemAuthConfig) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			workspaceID := c.Param("workspace_id")
			if workspaceID == "" {
				return ErrorResponse(c, http.StatusBadRequest, "workspace_id required")
			}

			// Extract token from Authorization header
			authHeader := c.Request().Header.Get("Authorization")
			token := strings.TrimPrefix(authHeader, "Bearer ")

			var rc *auth.RequestContext

			// Check if it's an admin token
			if cfg.AdminToken != "" && token == cfg.AdminToken {
				// Admin token - get workspace from URL
				workspace, err := cfg.Backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceID)
				if err != nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}

				rc = &auth.RequestContext{
					WorkspaceId:   workspace.Id,
					WorkspaceExt:  workspace.ExternalId,
					WorkspaceName: workspace.Name,
					IsGatewayAuth: true,
				}
			} else if token != "" && cfg.Backend != nil {
				// Try as workspace token
				result, err := cfg.Backend.ValidateToken(c.Request().Context(), token)
				if err != nil || result == nil {
					return ErrorResponse(c, http.StatusUnauthorized, "invalid token")
				}

				// Verify the token belongs to the requested workspace
				if result.WorkspaceExt != workspaceID {
					return ErrorResponse(c, http.StatusForbidden, "token does not have access to this workspace")
				}

				rc = &auth.RequestContext{
					WorkspaceId:   result.WorkspaceId,
					WorkspaceExt:  result.WorkspaceExt,
					WorkspaceName: result.WorkspaceName,
					MemberId:      result.MemberId,
					MemberExt:     result.MemberExt,
					MemberEmail:   result.MemberEmail,
					MemberRole:    result.MemberRole,
					IsGatewayAuth: false,
				}
			} else if cfg.AdminToken == "" {
				// No admin token configured - allow unauthenticated access (local mode)
				workspace, err := cfg.Backend.GetWorkspaceByExternalId(c.Request().Context(), workspaceID)
				if err != nil {
					return ErrorResponse(c, http.StatusNotFound, "workspace not found")
				}

				rc = &auth.RequestContext{
					WorkspaceId:   workspace.Id,
					WorkspaceExt:  workspace.ExternalId,
					WorkspaceName: workspace.Name,
					IsGatewayAuth: true,
				}
			} else {
				return ErrorResponse(c, http.StatusUnauthorized, "authorization required")
			}

			// Add auth context to request
			ctx := auth.WithContext(c.Request().Context(), rc)
			c.SetRequest(c.Request().WithContext(ctx))

			log.Debug().
				Str("workspace_id", workspaceID).
				Uint("workspace_internal_id", rc.WorkspaceId).
				Bool("is_gateway_auth", rc.IsGatewayAuth).
				Msg("filesystem auth validated")

			return next(c)
		}
	}
}

// logRequest logs the request details for debugging
func logRequest(c echo.Context, operation string) {
	log.Debug().
		Str("operation", operation).
		Str("path", c.QueryParam("path")).
		Str("workspace_id", c.Param("workspace_id")).
		Msg("filesystem API request")
}

// WithAuthContext is a helper to wrap handlers that need auth context
func WithAuthContext(ctx context.Context, workspaceID string, workspaceExt string) context.Context {
	rc := &auth.RequestContext{
		WorkspaceExt:  workspaceExt,
		IsGatewayAuth: true,
	}
	return auth.WithContext(ctx, rc)
}

// handleError converts service errors to HTTP responses
func handleError(c echo.Context, err error, notFoundMsg string) error {
	if err == nil {
		return nil
	}
	errStr := err.Error()
	if strings.Contains(errStr, "not found") || strings.Contains(errStr, "NoSuchKey") {
		return ErrorResponse(c, http.StatusNotFound, notFoundMsg)
	}
	return ErrorResponse(c, http.StatusInternalServerError, errStr)
}

// validatePath ensures the path is valid and safe
func validatePath(path string) error {
	if strings.Contains(path, "..") {
		return fmt.Errorf("invalid path: contains '..'")
	}
	return nil
}
