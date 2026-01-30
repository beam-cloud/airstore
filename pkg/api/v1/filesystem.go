package apiv1

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	sourceService  *services.SourceService
	sourceRegistry *sources.Registry
	toolRegistry   *tools.Registry
	toolResolver   *tools.WorkspaceToolResolver
}

// NewFilesystemGroup creates a new filesystem API group
func NewFilesystemGroup(
	routerGroup *echo.Group,
	backend repository.BackendRepository,
	contextService *services.ContextService,
	sourceService *services.SourceService,
	sourceRegistry *sources.Registry,
	toolRegistry *tools.Registry,
) *FilesystemGroup {
	// Create resolver for workspace-aware tool resolution
	var resolver *tools.WorkspaceToolResolver
	if backend != nil {
		resolver = tools.NewWorkspaceToolResolver(toolRegistry, backend)
	}

	g := &FilesystemGroup{
		routerGroup:    routerGroup,
		backend:        backend,
		contextService: contextService,
		sourceService:  sourceService,
		sourceRegistry: sourceRegistry,
		toolRegistry:   toolRegistry,
		toolResolver:   resolver,
	}
	g.registerRoutes()
	return g
}

func (g *FilesystemGroup) registerRoutes() {
	g.routerGroup.GET("/list", g.List)
	g.routerGroup.GET("/stat", g.Stat)
	g.routerGroup.GET("/read", g.Read)
	g.routerGroup.GET("/tree", g.Tree)

	// Tool settings endpoints
	g.routerGroup.GET("/tools", g.ListToolSettings)
	g.routerGroup.GET("/tools/:tool_name", g.GetToolSetting)
	g.routerGroup.PUT("/tools/:tool_name", g.UpdateToolSetting)

	// Workspace tool provider CRUD endpoints
	g.routerGroup.GET("/tools/providers", g.ListToolProviders)
	g.routerGroup.POST("/tools/providers", g.CreateToolProvider)
	g.routerGroup.GET("/tools/providers/:name", g.GetToolProvider)
	g.routerGroup.PUT("/tools/providers/:name", g.UpdateToolProvider)
	g.routerGroup.DELETE("/tools/providers/:name", g.DeleteToolProvider)

	// Smart query endpoints
	g.routerGroup.POST("/queries", g.CreateQuery)
	g.routerGroup.GET("/queries", g.GetQuery)           // ?path=...
	g.routerGroup.PUT("/queries/:id", g.UpdateQuery)    // :id = external_id
	g.routerGroup.DELETE("/queries/:id", g.DeleteQuery) // :id = external_id
}

// List returns directory contents as VirtualFile entries
func (g *FilesystemGroup) List(c echo.Context) error {
	ctx := c.Request().Context()
	path := cleanPath(c.QueryParam("path"))
	showHidden := c.QueryParam("show_hidden") == "true"
	refresh := c.QueryParam("refresh") == "true"

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
		return g.listSources(c, ctx, relPath, refresh)
	case "tools":
		return g.listTools(c, ctx, showHidden)
	default:
		return ErrorResponse(c, http.StatusNotFound, "path not found")
	}
}

// Stat returns file/directory info as VirtualFile
func (g *FilesystemGroup) Stat(c echo.Context) error {
	ctx := c.Request().Context()
	path := cleanPath(c.QueryParam("path"))
	showHidden := c.QueryParam("show_hidden") == "true"

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
		return g.statTools(c, ctx, path, relPath, showHidden)
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
	showHidden := c.QueryParam("show_hidden") == "true"

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
		entries := g.buildToolEntries(ctx, showHidden)
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
		*types.NewRootFolder(types.DirNameContext, types.PathContext).
			WithMetadata("description", "Workspace context files"),
		*types.NewRootFolder(types.DirNameSources, types.PathSources).
			WithMetadata("description", "Connected integrations"),
		*types.NewRootFolder(types.DirNameTools, types.PathTools).
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
		Path:    types.ContextPath(relPath),
		Entries: entries,
	})
}

func (g *FilesystemGroup) statContext(c echo.Context, ctx context.Context, fullPath, relPath string) error {
	if g.contextService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "context service not available")
	}

	// Root of context
	if relPath == "" {
		return SuccessResponse(c, types.NewRootFolder(types.DirNameContext, types.PathContext).
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
		fullPath := types.ContextPath(e.Path)
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
		Path:    types.ContextPath(relPath),
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
	var fullPath string
	if parentPath != "" {
		fullPath = types.ContextPath(types.JoinPath(parentPath, e.Name))
	} else {
		fullPath = types.ContextPath(e.Name)
	}

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

func (g *FilesystemGroup) listSources(c echo.Context, ctx context.Context, relPath string, refresh bool) error {
	// Root /sources - list all integrations
	if relPath == "" {
		entries := g.buildSourceRootEntries(ctx)
		return SuccessResponse(c, types.VirtualFileListResponse{
			Path:    types.PathSources,
			Entries: entries,
		})
	}

	// If refresh requested, force re-execute the smart query
	if refresh && g.sourceService != nil {
		queryPath := types.SourcePath(relPath)
		if _, err := g.sourceService.RefreshSmartQuery(ctx, queryPath); err != nil {
			// Log but don't fail - might not be a smart query folder
			log.Debug().Err(err).Str("path", queryPath).Msg("refresh: not a smart query or refresh failed")
		}
	}

	// Use SourceService to list directory contents (it handles credentials & caching)
	if g.sourceService != nil {
		resp, err := g.sourceService.ReadDir(ctx, &pb.SourceReadDirRequest{
			Path: relPath,
		})
		if err != nil {
			log.Error().Err(err).Str("path", relPath).Msg("source readdir failed")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to list source directory")
		}
		if !resp.Ok {
			log.Warn().Str("error", resp.Error).Str("path", relPath).Msg("source readdir returned error")
			// Return empty list rather than error for better UX
			return SuccessResponse(c, types.VirtualFileListResponse{
				Path:    types.SourcePath(relPath),
				Entries: []types.VirtualFile{},
			})
		}

		// Build a map of query paths to external_ids for smart queries
		// This allows us to include the external_id in VirtualFile metadata
		queryExternalIds := make(map[string]string)
		queryGuidance := make(map[string]string)
		integration, _ := splitFirstPath(relPath)
		parentPath := types.SourcePath(relPath)
		if relPath == integration {
			// Listing integration root - fetch all smart queries
			queriesResp, err := g.sourceService.ListSmartQueries(ctx, &pb.ListSmartQueriesRequest{
				ParentPath: parentPath,
			})
			if err == nil && queriesResp.Ok {
				for _, q := range queriesResp.Queries {
					queryExternalIds[q.Path] = q.ExternalId
					queryGuidance[q.Path] = q.Guidance
				}
			}
		}

		// Convert protobuf entries to VirtualFile
		entries := make([]types.VirtualFile, 0, len(resp.Entries))
		for _, e := range resp.Entries {
			entryPath := types.SourcePath(types.JoinPath(relPath, e.Name))

			vf := types.NewVirtualFile(
				hashPath(entryPath),
				e.Name,
				entryPath,
				types.VFTypeSource,
			).WithFolder(e.IsDir).WithReadOnly(true).WithMetadata(types.MetaKeyProvider, integration)

			// Add external_id and guidance if this is a smart query
			if extId, ok := queryExternalIds[entryPath]; ok {
				vf = vf.WithMetadata(types.MetaKeyExternalID, extId)
			}
			if guidance, ok := queryGuidance[entryPath]; ok {
				vf = vf.WithMetadata(types.MetaKeyGuidance, guidance)
			}

			// Add result_id for query result files (origin pointer for re-fetching)
			if e.ResultId != "" {
				vf = vf.WithMetadata("result_id", e.ResultId)
				vf = vf.WithMetadata("query_path", types.SourcePath(relPath))
			}

			if e.Size > 0 {
				vf = vf.WithSize(e.Size)
			}
			if e.Mtime > 0 {
				t := time.Unix(e.Mtime, 0)
				vf = vf.WithModifiedAt(t)
			}
			if e.IsDir && e.ChildCount > 0 {
				vf = vf.WithChildCount(int(e.ChildCount))
			}

			entries = append(entries, *vf)
		}

		return SuccessResponse(c, types.VirtualFileListResponse{
			Path:    types.SourcePath(relPath),
			Entries: entries,
		})
	}

	// Fallback: if no source service, return minimal entries
	integration, subPath := splitFirstPath(relPath)

	// Integration root
	if subPath == "" {
		statusPath := types.SourcePath(types.JoinPath(integration, "status.json"))
		entries := []types.VirtualFile{
			*types.NewVirtualFile(
				hashPath(statusPath),
				"status.json",
				statusPath,
				types.VFTypeSource,
			).WithFolder(false).WithReadOnly(true).WithMetadata(types.MetaKeyProvider, integration),
		}
		return SuccessResponse(c, types.VirtualFileListResponse{
			Path:    types.SourcePath(integration),
			Entries: entries,
		})
	}

	// Deep paths - return empty
	return SuccessResponse(c, types.VirtualFileListResponse{
		Path:    types.SourcePath(relPath),
		Entries: []types.VirtualFile{},
	})
}

func (g *FilesystemGroup) statSources(c echo.Context, ctx context.Context, fullPath, relPath string) error {
	// Root /sources
	if relPath == "" {
		return SuccessResponse(c, types.NewRootFolder(types.DirNameSources, types.PathSources).
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
		).WithFolder(true).WithReadOnly(true).WithMetadata(types.MetaKeyProvider, integration)
		return SuccessResponse(c, vf)
	}

	// status.json
	if subPath == "status.json" {
		vf := types.NewVirtualFile(
			hashPath(fullPath),
			"status.json",
			fullPath,
			types.VFTypeSource,
		).WithFolder(false).WithReadOnly(true).WithMetadata(types.MetaKeyProvider, integration)
		return SuccessResponse(c, vf)
	}

	// Other paths - return generic source file
	vf := types.NewVirtualFile(
		hashPath(fullPath),
		pathName(subPath),
		fullPath,
		types.VFTypeSource,
	).WithReadOnly(true).WithMetadata(types.MetaKeyProvider, integration)
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

	// Use SourceService.Read for all other source files
	if g.sourceService != nil {
		resp, err := g.sourceService.Read(ctx, &pb.SourceReadRequest{
			Path:   relPath,
			Offset: offset,
			Length: length,
		})
		if err != nil {
			log.Error().Err(err).Str("path", relPath).Msg("source read failed")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to read source file")
		}
		if !resp.Ok {
			if strings.Contains(resp.Error, "not found") {
				return ErrorResponse(c, http.StatusNotFound, resp.Error)
			}
			return ErrorResponse(c, http.StatusBadRequest, resp.Error)
		}

		// Determine content type based on file extension
		contentType := "text/plain; charset=utf-8"
		if strings.HasSuffix(relPath, ".json") {
			contentType = "application/json"
		}

		return c.Blob(http.StatusOK, contentType, resp.Data)
	}

	return ErrorResponse(c, http.StatusServiceUnavailable, "source service not available")
}

func (g *FilesystemGroup) buildSourceRootEntries(ctx context.Context) []types.VirtualFile {
	if g.sourceRegistry == nil {
		return []types.VirtualFile{}
	}

	integrations := g.sourceRegistry.List()
	entries := make([]types.VirtualFile, 0, len(integrations))

	for _, name := range integrations {
		entryPath := types.SourcePath(name)
		vf := types.NewVirtualFile(
			hashPath(entryPath),
			name,
			entryPath,
			types.VFTypeSource,
		).WithFolder(true).WithReadOnly(true).WithMetadata(types.MetaKeyProvider, name)
		entries = append(entries, *vf)
	}

	return entries
}

// ============================================================================
// Tools Service
// ============================================================================

func (g *FilesystemGroup) listTools(c echo.Context, ctx context.Context, showHidden bool) error {
	entries := g.buildToolEntries(ctx, showHidden)
	return SuccessResponse(c, types.VirtualFileListResponse{
		Path:    types.PathTools,
		Entries: entries,
	})
}

func (g *FilesystemGroup) statTools(c echo.Context, ctx context.Context, fullPath, relPath string, showHidden bool) error {
	// Root /tools
	if relPath == "" {
		// Count tools using resolver if available
		toolCount := 0
		if g.toolResolver != nil {
			resolved, err := g.toolResolver.List(ctx)
			if err == nil {
				for _, t := range resolved {
					if showHidden || t.Enabled {
						toolCount++
					}
				}
			}
		} else if g.toolRegistry != nil {
			toolCount = len(g.toolRegistry.List())
		}
		return SuccessResponse(c, types.NewRootFolder(types.DirNameTools, types.PathTools).
			WithMetadata("description", "Available tools").
			WithChildCount(toolCount))
	}

	// Specific tool - use resolver if available
	toolName := relPath

	if g.toolResolver != nil {
		// Get all tools to find the one we need (includes enabled status)
		resolved, err := g.toolResolver.List(ctx)
		if err != nil {
			log.Warn().Err(err).Str("tool", toolName).Msg("resolver list failed")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to list tools")
		}

		for _, t := range resolved {
			if t.Name == toolName {
				// Check if tool is disabled - return not found unless showHidden is true
				if !t.Enabled && !showHidden {
					return ErrorResponse(c, http.StatusNotFound, "tool not found")
				}

				vf := types.NewVirtualFile(
					"tool-"+toolName,
					toolName,
					fullPath,
					types.VFTypeTool,
				).WithFolder(false).WithReadOnly(true).
					WithMetadata("description", t.Help).
					WithMetadata("name", t.Name).
					WithMetadata("enabled", t.Enabled).
					WithMetadata("hidden", !t.Enabled).
					WithMetadata("origin", string(t.Origin))

				if t.Origin == types.ToolOriginWorkspace && t.ExternalId != "" {
					vf = vf.WithMetadata("workspace_tool_external_id", t.ExternalId)
				}

				return SuccessResponse(c, vf)
			}
		}
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	// Fallback to registry
	provider := g.toolRegistry.Get(toolName)
	if provider == nil {
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	// Get workspace ID from context for filtering
	var workspaceId uint
	if rc := auth.FromContext(ctx); rc != nil {
		workspaceId = rc.WorkspaceId
	}

	// Get tool settings for this workspace
	var settings *types.WorkspaceToolSettings
	if workspaceId > 0 && g.backend != nil {
		var err error
		settings, err = g.backend.GetWorkspaceToolSettings(ctx, workspaceId)
		if err != nil {
			log.Warn().Err(err).Uint("workspace_id", workspaceId).Msg("failed to get tool settings")
			settings = nil
		}
	}

	isDisabled := settings != nil && settings.IsDisabled(toolName)

	// Check if tool is disabled - return not found unless showHidden is true
	if isDisabled && !showHidden {
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	vf := types.NewVirtualFile(
		"tool-"+toolName,
		toolName,
		fullPath,
		types.VFTypeTool,
	).WithFolder(false).WithReadOnly(true).
		WithMetadata("description", provider.Help()).
		WithMetadata("name", provider.Name()).
		WithMetadata("enabled", !isDisabled).
		WithMetadata("hidden", isDisabled).
		WithMetadata("origin", string(types.ToolOriginGlobal))

	return SuccessResponse(c, vf)
}

func (g *FilesystemGroup) buildToolEntries(ctx context.Context, showHidden bool) []types.VirtualFile {
	// Use resolver if available (includes workspace tools)
	if g.toolResolver != nil {
		resolved, err := g.toolResolver.List(ctx)
		if err != nil {
			log.Warn().Err(err).Msg("resolver list failed, falling back to registry")
		} else {
			entries := make([]types.VirtualFile, 0, len(resolved))
			for _, t := range resolved {
				// Skip disabled tools unless showHidden is true
				if !t.Enabled && !showHidden {
					continue
				}

				entryPath := types.ToolsPath(t.Name)
				vf := types.NewVirtualFile(
					"tool-"+t.Name,
					t.Name,
					entryPath,
					types.VFTypeTool,
				).WithFolder(false).WithReadOnly(true).
					WithMetadata("description", t.Help).
					WithMetadata("name", t.Name).
					WithMetadata("enabled", t.Enabled).
					WithMetadata(types.MetaKeyHidden, !t.Enabled).
					WithMetadata("origin", string(t.Origin))

				// Add external_id for workspace tools (needed for delete operations)
				if t.Origin == types.ToolOriginWorkspace && t.ExternalId != "" {
					vf = vf.WithMetadata("workspace_tool_external_id", t.ExternalId)
				}

				entries = append(entries, *vf)
			}
			return entries
		}
	}

	// Fallback to global registry only
	if g.toolRegistry == nil {
		return []types.VirtualFile{}
	}

	// Get workspace ID from context for filtering
	var workspaceId uint
	if rc := auth.FromContext(ctx); rc != nil {
		workspaceId = rc.WorkspaceId
	}

	// Get tool settings for this workspace
	var settings *types.WorkspaceToolSettings
	if workspaceId > 0 && g.backend != nil {
		var err error
		settings, err = g.backend.GetWorkspaceToolSettings(ctx, workspaceId)
		if err != nil {
			log.Warn().Err(err).Uint("workspace_id", workspaceId).Msg("failed to get tool settings, showing all tools")
			settings = nil
		}
	}

	names := g.toolRegistry.List()
	entries := make([]types.VirtualFile, 0, len(names))

	for _, name := range names {
		isDisabled := settings != nil && settings.IsDisabled(name)

		// Skip disabled tools unless showHidden is true
		if isDisabled && !showHidden {
			continue
		}

		provider := g.toolRegistry.Get(name)
		if provider == nil {
			continue
		}

		entryPath := types.ToolsPath(name)
		vf := types.NewVirtualFile(
			"tool-"+name,
			name,
			entryPath,
			types.VFTypeTool,
		).WithFolder(false).WithReadOnly(true).
			WithMetadata("description", provider.Help()).
			WithMetadata("name", provider.Name()).
			WithMetadata("enabled", !isDisabled).
			WithMetadata(types.MetaKeyHidden, isDisabled).
			WithMetadata("origin", string(types.ToolOriginGlobal))

		entries = append(entries, *vf)
	}

	return entries
}

// ============================================================================
// Tool Settings API
// ============================================================================

// ToolSettingResponse represents a tool with its enabled state
type ToolSettingResponse struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
	Origin      string `json:"origin,omitempty"`
	ExternalId  string `json:"external_id,omitempty"`
}

// UpdateToolSettingRequest represents a request to update tool settings
type UpdateToolSettingRequest struct {
	Enabled bool `json:"enabled"`
}

// ListToolSettings returns all tools with their enabled/disabled state for the workspace
func (g *FilesystemGroup) ListToolSettings(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "list_tool_settings")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Use resolver if available (includes workspace tools)
	if g.toolResolver != nil {
		resolved, err := g.toolResolver.List(ctx)
		if err != nil {
			log.Error().Err(err).Msg("failed to list tools via resolver")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to list tools")
		}

		toolSettings := make([]ToolSettingResponse, 0, len(resolved))
		for _, t := range resolved {
			toolSettings = append(toolSettings, ToolSettingResponse{
				Name:        t.Name,
				Description: t.Help,
				Enabled:     t.Enabled,
				Origin:      string(t.Origin),
				ExternalId:  t.ExternalId,
			})
		}

		return SuccessResponse(c, map[string]interface{}{
			"tools": toolSettings,
		})
	}

	// Fallback to registry only
	settings, err := g.backend.GetWorkspaceToolSettings(ctx, rc.WorkspaceId)
	if err != nil {
		log.Error().Err(err).Msg("failed to get tool settings")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get tool settings")
	}

	// Build response with all tools and their state
	names := g.toolRegistry.List()
	toolSettings := make([]ToolSettingResponse, 0, len(names))

	for _, name := range names {
		provider := g.toolRegistry.Get(name)
		if provider == nil {
			continue
		}

		toolSettings = append(toolSettings, ToolSettingResponse{
			Name:        name,
			Description: provider.Help(),
			Enabled:     settings.IsEnabled(name),
			Origin:      string(types.ToolOriginGlobal),
		})
	}

	return SuccessResponse(c, map[string]interface{}{
		"tools": toolSettings,
	})
}

// GetToolSetting returns the setting for a specific tool
func (g *FilesystemGroup) GetToolSetting(c echo.Context) error {
	ctx := c.Request().Context()
	toolName := c.Param("tool_name")
	logRequest(c, "get_tool_setting")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Use resolver if available (includes workspace tools)
	if g.toolResolver != nil {
		resolved, err := g.toolResolver.List(ctx)
		if err != nil {
			log.Error().Err(err).Msg("failed to list tools via resolver")
			return ErrorResponse(c, http.StatusInternalServerError, "failed to list tools")
		}

		for _, t := range resolved {
			if t.Name == toolName {
				return SuccessResponse(c, ToolSettingResponse{
					Name:        t.Name,
					Description: t.Help,
					Enabled:     t.Enabled,
					Origin:      string(t.Origin),
					ExternalId:  t.ExternalId,
				})
			}
		}
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	// Fallback: check if tool exists in registry
	provider := g.toolRegistry.Get(toolName)
	if provider == nil {
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	// Get tool setting
	setting, err := g.backend.GetWorkspaceToolSetting(ctx, rc.WorkspaceId, toolName)
	if err != nil {
		log.Error().Err(err).Msg("failed to get tool setting")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get tool setting")
	}

	// Default to enabled if no setting exists
	enabled := true
	if setting != nil {
		enabled = setting.Enabled
	}

	return SuccessResponse(c, ToolSettingResponse{
		Name:        toolName,
		Description: provider.Help(),
		Enabled:     enabled,
		Origin:      string(types.ToolOriginGlobal),
	})
}

// UpdateToolSetting updates the enabled/disabled state of a tool
func (g *FilesystemGroup) UpdateToolSetting(c echo.Context) error {
	ctx := c.Request().Context()
	toolName := c.Param("tool_name")
	logRequest(c, "update_tool_setting")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Check if tool exists using resolver or registry
	var toolExists bool
	var toolHelp string
	var toolOrigin string

	if g.toolResolver != nil {
		if g.toolResolver.Has(ctx, toolName) {
			toolExists = true
			// Get help text
			if g.toolResolver.IsGlobal(toolName) {
				toolOrigin = string(types.ToolOriginGlobal)
				if p := g.toolRegistry.Get(toolName); p != nil {
					toolHelp = p.Help()
				}
			} else {
				toolOrigin = string(types.ToolOriginWorkspace)
				// For workspace tools, help will be minimal
				toolHelp = "Workspace tool: " + toolName
			}
		}
	} else {
		provider := g.toolRegistry.Get(toolName)
		if provider != nil {
			toolExists = true
			toolHelp = provider.Help()
			toolOrigin = string(types.ToolOriginGlobal)
		}
	}

	if !toolExists {
		return ErrorResponse(c, http.StatusNotFound, "tool not found")
	}

	// Parse request body
	var req UpdateToolSettingRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	// Update tool setting
	if err := g.backend.SetWorkspaceToolSetting(ctx, rc.WorkspaceId, toolName, req.Enabled); err != nil {
		log.Error().Err(err).Msg("failed to update tool setting")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to update tool setting")
	}

	log.Info().
		Uint("workspace_id", rc.WorkspaceId).
		Str("tool_name", toolName).
		Bool("enabled", req.Enabled).
		Msg("tool setting updated")

	return SuccessResponse(c, ToolSettingResponse{
		Name:        toolName,
		Description: toolHelp,
		Enabled:     req.Enabled,
		Origin:      toolOrigin,
	})
}

// ============================================================================
// Workspace Tool Provider API
// ============================================================================

// CreateToolProviderRequest represents a request to create a workspace tool provider
type CreateToolProviderRequest struct {
	Name           string                 `json:"name"`
	ProviderType   string                 `json:"provider_type"` // "mcp"
	MCP            *types.MCPServerConfig `json:"mcp,omitempty"`
	SkipValidation bool                   `json:"skip_validation,omitempty"` // Skip connection validation
}

// ToolProviderResponse represents a workspace tool provider in API responses
type ToolProviderResponse struct {
	ExternalId   string `json:"external_id"`
	Name         string `json:"name"`
	ProviderType string `json:"provider_type"`
	ToolCount    int    `json:"tool_count,omitempty"`
	Warning      string `json:"warning,omitempty"` // Validation warning if any
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
}

// ListToolProviders returns all workspace-defined tool providers
func (g *FilesystemGroup) ListToolProviders(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "list_tool_providers")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// List workspace tools from database
	workspaceTools, err := g.backend.ListWorkspaceTools(ctx, rc.WorkspaceId)
	if err != nil {
		log.Error().Err(err).Msg("failed to list workspace tools")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list tool providers")
	}

	providers := make([]ToolProviderResponse, 0, len(workspaceTools))
	for _, wt := range workspaceTools {
		providers = append(providers, ToolProviderResponse{
			ExternalId:   wt.ExternalId,
			Name:         wt.Name,
			ProviderType: string(wt.ProviderType),
			ToolCount:    getManifestToolCount(wt.Manifest),
			CreatedAt:    wt.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
			UpdatedAt:    wt.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
		})
	}

	return SuccessResponse(c, map[string]interface{}{
		"providers": providers,
	})
}

// CreateToolProvider creates a new workspace tool provider (MCP server)
func (g *FilesystemGroup) CreateToolProvider(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "create_tool_provider")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Require admin or member role for creating tools
	if !auth.CanWrite(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "insufficient permissions")
	}

	// Parse request body
	var req CreateToolProviderRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	// Validate request
	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}
	if strings.ContainsAny(req.Name, "/\\") {
		return ErrorResponse(c, http.StatusBadRequest, "name cannot contain path separators")
	}
	if len(req.Name) > 100 {
		return ErrorResponse(c, http.StatusBadRequest, "name is too long (max 100 characters)")
	}
	if req.ProviderType != "mcp" {
		return ErrorResponse(c, http.StatusBadRequest, "provider_type must be 'mcp'")
	}
	if req.MCP == nil {
		return ErrorResponse(c, http.StatusBadRequest, "mcp configuration is required")
	}
	if req.MCP.URL == "" && req.MCP.Command == "" {
		return ErrorResponse(c, http.StatusBadRequest, "mcp.url or mcp.command is required")
	}

	// Check if name conflicts with a global tool
	if g.toolRegistry.Has(req.Name) {
		return ErrorResponse(c, http.StatusConflict, "name conflicts with a global tool")
	}

	// Serialize config
	configJSON, err := json.Marshal(req.MCP)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid mcp configuration")
	}

	// Validate MCP connection and get manifest (unless skipped)
	var manifest []byte
	var toolCount int
	var validationError string

	if !req.SkipValidation {
		manifest, toolCount, err = g.validateAndGetManifest(ctx, req.Name, req.MCP)
		if err != nil {
			log.Warn().Err(err).Str("name", req.Name).Msg("MCP validation failed")
			validationError = err.Error()
			// Don't fail - allow creation with warning
		}
	}

	// Create workspace tool in database
	var memberIdPtr *uint
	if rc.MemberId > 0 {
		memberIdPtr = &rc.MemberId
	}

	wt, err := g.backend.CreateWorkspaceTool(
		ctx,
		rc.WorkspaceId,
		memberIdPtr,
		req.Name,
		types.ProviderTypeMCP,
		configJSON,
		manifest,
	)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceToolExists); ok {
			return ErrorResponse(c, http.StatusConflict, "tool provider already exists")
		}
		log.Error().Err(err).Msg("failed to create workspace tool")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to create tool provider")
	}

	// Invalidate resolver cache for this workspace
	if g.toolResolver != nil {
		g.toolResolver.InvalidateWorkspace(rc.WorkspaceId)
	}

	// Audit log - tool provider creation
	log.Info().
		Uint("workspace_id", rc.WorkspaceId).
		Uint("member_id", rc.MemberId).
		Str("member_email", rc.MemberEmail).
		Str("name", req.Name).
		Str("provider_type", req.ProviderType).
		Bool("is_remote", req.MCP.IsRemote()).
		Int("tool_count", toolCount).
		Bool("validation_skipped", req.SkipValidation).
		Str("validation_error", validationError).
		Msg("audit: workspace tool provider created")

	resp := ToolProviderResponse{
		ExternalId:   wt.ExternalId,
		Name:         wt.Name,
		ProviderType: string(wt.ProviderType),
		ToolCount:    toolCount,
		CreatedAt:    wt.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:    wt.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}

	// Add warning if validation failed
	if validationError != "" {
		resp.Warning = fmt.Sprintf("Connection validation failed: %s. The tool was created but may not work until the server is accessible.", validationError)
	}

	return c.JSON(http.StatusCreated, Response{
		Success: true,
		Data:    resp,
	})
}

// GetToolProvider retrieves a workspace tool provider by name
func (g *FilesystemGroup) GetToolProvider(c echo.Context) error {
	ctx := c.Request().Context()
	name := c.Param("name")
	logRequest(c, "get_tool_provider")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Get tool from database
	wt, err := g.backend.GetWorkspaceToolByName(ctx, rc.WorkspaceId, name)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceToolNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "tool provider not found")
		}
		log.Error().Err(err).Msg("failed to get workspace tool")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get tool provider")
	}

	// Parse MCP config for response
	cfg, _ := wt.GetMCPConfig()

	resp := map[string]interface{}{
		"external_id":   wt.ExternalId,
		"name":          wt.Name,
		"provider_type": string(wt.ProviderType),
		"created_at":    wt.CreatedAt.Format("2006-01-02T15:04:05Z07:00"),
		"updated_at":    wt.UpdatedAt.Format("2006-01-02T15:04:05Z07:00"),
	}

	if cfg != nil {
		// Return full config for editing (member already has write access to view)
		resp["mcp"] = cfg
	}

	return c.JSON(http.StatusOK, Response{
		Success: true,
		Data:    resp,
	})
}

// UpdateToolProvider updates a workspace tool provider's config
func (g *FilesystemGroup) UpdateToolProvider(c echo.Context) error {
	ctx := c.Request().Context()
	name := c.Param("name")
	logRequest(c, "update_tool_provider")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Require write access
	if !auth.CanWrite(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "insufficient permissions")
	}

	// Get existing tool
	wt, err := g.backend.GetWorkspaceToolByName(ctx, rc.WorkspaceId, name)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceToolNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "tool provider not found")
		}
		log.Error().Err(err).Msg("failed to get workspace tool")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get tool provider")
	}

	// Parse request body
	var req CreateToolProviderRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	// Validate MCP config
	if req.MCP == nil {
		return ErrorResponse(c, http.StatusBadRequest, "mcp configuration is required")
	}
	if req.MCP.URL == "" && req.MCP.Command == "" {
		return ErrorResponse(c, http.StatusBadRequest, "mcp.url or mcp.command is required")
	}

	// Serialize new config
	configJSON, err := json.Marshal(req.MCP)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid mcp configuration")
	}

	// Invalidate resolver cache before update
	if g.toolResolver != nil {
		g.toolResolver.Invalidate(rc.WorkspaceId, name)
	}

	// Update in database
	if err := g.backend.UpdateWorkspaceToolConfig(ctx, wt.Id, configJSON); err != nil {
		log.Error().Err(err).Msg("failed to update workspace tool")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to update tool provider")
	}

	// Audit log
	log.Info().
		Uint("workspace_id", rc.WorkspaceId).
		Uint("member_id", rc.MemberId).
		Str("name", name).
		Msg("audit: workspace tool provider updated")

	return c.JSON(http.StatusOK, Response{
		Success: true,
		Data: map[string]interface{}{
			"name":    name,
			"message": "tool provider updated",
		},
	})
}

// DeleteToolProvider deletes a workspace tool provider
func (g *FilesystemGroup) DeleteToolProvider(c echo.Context) error {
	ctx := c.Request().Context()
	name := c.Param("name")
	logRequest(c, "delete_tool_provider")

	// Get workspace ID from auth context
	rc := auth.FromContext(ctx)
	if rc == nil {
		return ErrorResponse(c, http.StatusUnauthorized, "unauthorized")
	}

	// Require admin or member role for deleting tools
	if !auth.CanWrite(ctx) {
		return ErrorResponse(c, http.StatusForbidden, "insufficient permissions")
	}

	// Check if tool exists
	_, err := g.backend.GetWorkspaceToolByName(ctx, rc.WorkspaceId, name)
	if err != nil {
		if _, ok := err.(*types.ErrWorkspaceToolNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "tool provider not found")
		}
		log.Error().Err(err).Msg("failed to get workspace tool")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get tool provider")
	}

	// Invalidate resolver cache before deletion
	if g.toolResolver != nil {
		g.toolResolver.Invalidate(rc.WorkspaceId, name)
	}

	// Delete from database
	if err := g.backend.DeleteWorkspaceToolByName(ctx, rc.WorkspaceId, name); err != nil {
		if _, ok := err.(*types.ErrWorkspaceToolNotFound); ok {
			return ErrorResponse(c, http.StatusNotFound, "tool provider not found")
		}
		log.Error().Err(err).Msg("failed to delete workspace tool")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to delete tool provider")
	}

	// Audit log - tool provider deletion
	log.Info().
		Uint("workspace_id", rc.WorkspaceId).
		Uint("member_id", rc.MemberId).
		Str("member_email", rc.MemberEmail).
		Str("name", name).
		Msg("audit: workspace tool provider deleted")

	return SuccessResponse(c, nil)
}

// validateAndGetManifest validates an MCP server by connecting and listing tools
func (g *FilesystemGroup) validateAndGetManifest(ctx context.Context, name string, cfg *types.MCPServerConfig) ([]byte, int, error) {
	// Create a temporary client to validate
	var client tools.MCPClient
	if cfg.IsRemote() {
		client = tools.NewMCPRemoteClient(name, *cfg)
	} else {
		client = tools.NewMCPStdioClient(name, *cfg)
	}

	// Start with timeout
	startCtx, cancel := context.WithTimeout(ctx, tools.MCPInitTimeout)
	defer cancel()

	if err := client.Start(startCtx); err != nil {
		return nil, 0, fmt.Errorf("failed to start: %w", err)
	}
	defer client.Close()

	// List tools
	toolList, err := client.ListTools(startCtx)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to list tools: %w", err)
	}

	// Serialize manifest
	manifest, err := json.Marshal(map[string]interface{}{
		"tools": toolList,
	})
	if err != nil {
		return nil, len(toolList), nil
	}

	return manifest, len(toolList), nil
}

// getManifestToolCount extracts the tool count from a manifest
func getManifestToolCount(manifest []byte) int {
	if len(manifest) == 0 {
		return 0
	}
	var m struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(manifest, &m); err != nil {
		return 0
	}
	return len(m.Tools)
}

// CreateQueryRequest represents a request to create a smart query
type CreateQueryRequest struct {
	Integration  string `json:"integration"`   // e.g., "gmail", "gdrive"
	Name         string `json:"name"`          // Folder/file name
	Guidance     string `json:"guidance"`      // Optional user guidance for LLM
	OutputFormat string `json:"output_format"` // "folder" or "file"
	FileExt      string `json:"file_ext"`      // For files: ".json", ".md"
}

// UpdateQueryRequest represents a request to update a smart query
type UpdateQueryRequest struct {
	Name     string `json:"name"`     // New name (optional)
	Guidance string `json:"guidance"` // New guidance (optional)
}

// SmartQueryResponse represents a smart query in API responses
type SmartQueryResponse struct {
	ExternalID   string `json:"external_id"`
	Integration  string `json:"integration"`
	Path         string `json:"path"`
	Name         string `json:"name"`
	QuerySpec    string `json:"query_spec"`
	Guidance     string `json:"guidance"`
	OutputFormat string `json:"output_format"`
	FileExt      string `json:"file_ext"`
	CacheTTL     int    `json:"cache_ttl"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

// CreateQuery creates a new smart query via LLM inference
func (g *FilesystemGroup) CreateQuery(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "create_query")

	if g.sourceService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "source service not available")
	}

	var req CreateQueryRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	if req.Integration == "" {
		return ErrorResponse(c, http.StatusBadRequest, "integration is required")
	}
	if req.Name == "" {
		return ErrorResponse(c, http.StatusBadRequest, "name is required")
	}
	if req.OutputFormat == "" {
		req.OutputFormat = "folder"
	}

	resp, err := g.sourceService.CreateSmartQuery(ctx, &pb.CreateSmartQueryRequest{
		Integration:  req.Integration,
		Name:         req.Name,
		Guidance:     req.Guidance,
		OutputFormat: req.OutputFormat,
		FileExt:      req.FileExt,
	})
	if err != nil {
		log.Error().Err(err).Msg("failed to create smart query")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to create query")
	}
	if !resp.Ok {
		return ErrorResponse(c, http.StatusBadRequest, resp.Error)
	}

	return SuccessResponse(c, protoQueryToResponse(resp.Query))
}

// GetQuery retrieves a smart query by path
func (g *FilesystemGroup) GetQuery(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "get_query")

	if g.sourceService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "source service not available")
	}

	path := c.QueryParam("path")
	if path == "" {
		return ErrorResponse(c, http.StatusBadRequest, "path is required")
	}

	resp, err := g.sourceService.GetSmartQuery(ctx, &pb.GetSmartQueryRequest{
		Path: path,
	})
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("failed to get smart query")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to get query")
	}
	if !resp.Ok {
		return ErrorResponse(c, http.StatusBadRequest, resp.Error)
	}
	if resp.Query == nil {
		return ErrorResponse(c, http.StatusNotFound, "query not found")
	}

	return SuccessResponse(c, protoQueryToResponse(resp.Query))
}

// UpdateQuery updates an existing smart query by external_id
func (g *FilesystemGroup) UpdateQuery(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "update_query")

	if g.sourceService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "source service not available")
	}

	externalId := c.Param("id")
	if externalId == "" {
		return ErrorResponse(c, http.StatusBadRequest, "id is required")
	}

	var req UpdateQueryRequest
	if err := c.Bind(&req); err != nil {
		return ErrorResponse(c, http.StatusBadRequest, "invalid request body")
	}

	resp, err := g.sourceService.UpdateSmartQuery(ctx, &pb.UpdateSmartQueryRequest{
		ExternalId: externalId,
		Name:       req.Name,
		Guidance:   req.Guidance,
	})
	if err != nil {
		log.Error().Err(err).Str("external_id", externalId).Msg("failed to update smart query")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to update query")
	}
	if !resp.Ok {
		if strings.Contains(resp.Error, "not found") {
			return ErrorResponse(c, http.StatusNotFound, resp.Error)
		}
		return ErrorResponse(c, http.StatusBadRequest, resp.Error)
	}

	return SuccessResponse(c, protoQueryToResponse(resp.Query))
}

// DeleteQuery removes a smart query by external_id
func (g *FilesystemGroup) DeleteQuery(c echo.Context) error {
	ctx := c.Request().Context()
	logRequest(c, "delete_query")

	if g.sourceService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "source service not available")
	}

	externalId := c.Param("id")
	if externalId == "" {
		return ErrorResponse(c, http.StatusBadRequest, "id is required")
	}

	resp, err := g.sourceService.DeleteSmartQuery(ctx, &pb.DeleteSmartQueryRequest{
		ExternalId: externalId,
	})
	if err != nil {
		log.Error().Err(err).Str("external_id", externalId).Msg("failed to delete smart query")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to delete query")
	}
	if !resp.Ok {
		if strings.Contains(resp.Error, "not found") {
			return ErrorResponse(c, http.StatusNotFound, resp.Error)
		}
		return ErrorResponse(c, http.StatusBadRequest, resp.Error)
	}

	return SuccessResponse(c, map[string]bool{"deleted": true})
}

// protoQueryToResponse converts a pb.SmartQuery to SmartQueryResponse
func protoQueryToResponse(q *pb.SmartQuery) *SmartQueryResponse {
	if q == nil {
		return nil
	}
	return &SmartQueryResponse{
		ExternalID:   q.ExternalId,
		Integration:  q.Integration,
		Path:         q.Path,
		Name:         q.Name,
		QuerySpec:    q.QuerySpec,
		Guidance:     q.Guidance,
		OutputFormat: q.OutputFormat,
		FileExt:      q.FileExt,
		CacheTTL:     int(q.CacheTtl),
		CreatedAt:    q.CreatedAt,
		UpdatedAt:    q.UpdatedAt,
	}
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
