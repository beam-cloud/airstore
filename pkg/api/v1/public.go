package apiv1

import (
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/labstack/echo/v4"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

// PublicGroup handles unauthenticated public access to workspace content.
type PublicGroup struct {
	backend      repository.BackendRepository
	storage      *clients.StorageClient
	bucketPrefix string
}

// NewPublicGroup registers public routes that do NOT require authentication.
func NewPublicGroup(g *echo.Group, backend repository.BackendRepository, storage *clients.StorageClient, bucketPrefix string) {
	p := &PublicGroup{
		backend:      backend,
		storage:      storage,
		bucketPrefix: bucketPrefix,
	}

	// Raw file content: GET /r/{slug}/{path...}
	g.GET("/r/:slug/*", p.RawRead)

	// Workspace metadata: GET /api/v1/public/{slug}
	g.GET("/api/v1/public/:slug", p.WorkspaceMeta)

	// List directory: GET /api/v1/public/{slug}/list?path=...
	g.GET("/api/v1/public/:slug/list", p.ListDir)
}

// RawRead serves raw file content from a public workspace.
// GET /r/{slug}/{path...}
func (p *PublicGroup) RawRead(c echo.Context) error {
	slug := c.Param("slug")
	path := c.Param("*")

	ws, err := p.resolvePublicWorkspace(c, slug)
	if err != nil {
		return err
	}

	// Read from workspace storage
	bucket := types.WorkspaceBucketName(p.bucketPrefix, ws.ExternalId)
	key := "/" + strings.TrimPrefix(path, "/")

	content, err := p.storage.Download(c.Request().Context(), bucket, key)
	if err != nil {
		return c.String(http.StatusNotFound, "file not found")
	}

	// Set content type based on extension
	contentType := mime.TypeByExtension(filepath.Ext(path))
	if contentType == "" {
		contentType = "text/plain; charset=utf-8"
	}

	return c.Blob(http.StatusOK, contentType, content)
}

// WorkspaceMeta returns public metadata about a workspace.
// GET /api/v1/public/{slug}
func (p *PublicGroup) WorkspaceMeta(c echo.Context) error {
	slug := c.Param("slug")

	ws, err := p.resolvePublicWorkspace(c, slug)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"name":       ws.Name,
		"slug":       ws.Slug,
		"created_at": ws.CreatedAt,
	})
}

// ListDir lists a directory in a public workspace.
// GET /api/v1/public/{slug}/list?path=/skills
func (p *PublicGroup) ListDir(c echo.Context) error {
	slug := c.Param("slug")
	path := c.QueryParam("path")
	if path == "" {
		path = "/"
	}

	ws, err := p.resolvePublicWorkspace(c, slug)
	if err != nil {
		return err
	}

	bucket := types.WorkspaceBucketName(p.bucketPrefix, ws.ExternalId)
	prefix := strings.TrimPrefix(path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	result, err := p.storage.ListObjects(c.Request().Context(), bucket, prefix, 1000)
	if err != nil {
		return c.JSON(http.StatusOK, map[string]interface{}{
			"entries": []interface{}{},
		})
	}

	entries := make([]map[string]interface{}, 0)
	for _, obj := range result.Contents {
		name := strings.TrimPrefix(*obj.Key, prefix)
		if name == "" {
			continue
		}
		entries = append(entries, map[string]interface{}{
			"name":  name,
			"size":  obj.Size,
			"mtime": obj.LastModified,
		})
	}
	for _, cp := range result.CommonPrefixes {
		name := strings.TrimPrefix(*cp.Prefix, prefix)
		name = strings.TrimSuffix(name, "/")
		if name == "" {
			continue
		}
		entries = append(entries, map[string]interface{}{
			"name":   name,
			"is_dir": true,
		})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"entries": entries,
	})
}

// resolvePublicWorkspace looks up a workspace by slug and verifies it's public.
func (p *PublicGroup) resolvePublicWorkspace(c echo.Context, slug string) (*types.Workspace, error) {
	ws, err := p.backend.GetWorkspaceBySlug(c.Request().Context(), slug)
	if err != nil || ws == nil {
		return nil, c.String(http.StatusNotFound, "workspace not found")
	}

	if ws.Visibility != types.VisibilityPublic {
		return nil, c.String(http.StatusNotFound, "workspace not found")
	}

	return ws, nil
}
