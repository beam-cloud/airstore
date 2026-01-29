package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	driveAPIBase = "https://www.googleapis.com/drive/v3"
)

// GDriveProvider implements sources.Provider for Google Drive integration.
// It exposes Google Drive resources as a read-only filesystem under /sources/gdrive/
type GDriveProvider struct {
	httpClient *http.Client
}

// NewGDriveProvider creates a new Google Drive source provider
func NewGDriveProvider() *GDriveProvider {
	return &GDriveProvider{
		httpClient: &http.Client{Timeout: 60 * time.Second}, // Longer timeout for file downloads
	}
}

func (g *GDriveProvider) Name() string {
	return types.ToolGDrive.String()
}

// Stat returns file/directory attributes
func (g *GDriveProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return g.statViews(ctx, pctx, parts[1:])
	case "files":
		return g.statFiles(ctx, pctx, parts[1:])
	case "shared":
		return g.statShared(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// ReadDir lists directory contents
func (g *GDriveProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if path == "" {
		return []sources.DirEntry{
			{Name: "views", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "files", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
			{Name: "shared", Mode: sources.ModeDir, IsDir: true, Mtime: sources.NowUnix()},
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return g.readdirViews(ctx, pctx, parts[1:])
	case "files":
		return g.readdirFiles(ctx, pctx, parts[1:])
	case "shared":
		return g.readdirShared(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// Read reads file content
func (g *GDriveProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "views":
		return g.readViews(ctx, pctx, parts[1:], offset, length)
	case "files":
		return g.readFiles(ctx, pctx, parts[1:], offset, length)
	case "shared":
		return g.readShared(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// Readlink is not supported for Drive
func (g *GDriveProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// --- Views ---
// /views/recent.json - recently modified files
// /views/starred.json - starred files
// /views/storage.json - storage quota info

func (g *GDriveProvider) statViews(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	switch parts[0] {
	case "recent.json", "starred.json", "storage.json":
		data, err := g.getViewData(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GDriveProvider) readdirViews(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		return []sources.DirEntry{
			{Name: "recent.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "starred.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "storage.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil
	}
	return nil, sources.ErrNotDir
}

func (g *GDriveProvider) readViews(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 0 {
		return nil, sources.ErrIsDir
	}

	data, err := g.getViewData(ctx, pctx, parts[0])
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

func (g *GDriveProvider) getViewData(ctx context.Context, pctx *sources.ProviderContext, view string) ([]byte, error) {
	token := pctx.Credentials.AccessToken

	switch view {
	case "recent.json":
		return g.fetchRecent(ctx, token)
	case "starred.json":
		return g.fetchStarred(ctx, token)
	case "storage.json":
		return g.fetchStorage(ctx, token)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- Files ---
// /files/<fileid>/meta.json - file metadata
// /files/<fileid>/content - file content (binary)

func (g *GDriveProvider) statFiles(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	token := pctx.Credentials.AccessToken

	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		// /files/<fileid> - directory
		return sources.DirInfo(), nil
	case 2:
		// /files/<fileid>/<file>
		fileId, file := parts[0], parts[1]
		switch file {
		case "meta.json":
			data, err := g.fetchFileMeta(ctx, token, fileId)
			if err != nil {
				return nil, err
			}
			return sources.FileInfoFromBytes(data), nil
		case "content":
			// Get file size from metadata
			meta, err := g.getFileMeta(ctx, token, fileId)
			if err != nil {
				return nil, err
			}
			size := int64(0)
			if s, ok := meta["size"].(string); ok {
				fmt.Sscanf(s, "%d", &size)
			}
			return &sources.FileInfo{
				Size:  size,
				Mode:  sources.ModeFile,
				Mtime: sources.NowUnix(),
			}, nil
		default:
			return nil, sources.ErrNotFound
		}
	default:
		return nil, sources.ErrNotFound
	}
}

func (g *GDriveProvider) readdirFiles(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	token := pctx.Credentials.AccessToken

	switch len(parts) {
	case 0:
		// List files in My Drive root
		files, err := g.listFiles(ctx, token, "'root' in parents and trashed = false", 100)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(files))
		for _, f := range files {
			if id, ok := f["id"].(string); ok {
				isDir := f["mimeType"] == "application/vnd.google-apps.folder"
				entries = append(entries, sources.DirEntry{
					Name:  id,
					Mode:  sources.ModeDir, // All files show as directories with meta.json/content inside
					IsDir: !isDir,          // Actually, let's keep as dirs for consistency
					Mtime: sources.NowUnix(),
				})
			}
		}
		return entries, nil

	case 1:
		// List files in a file directory (meta.json, content)
		return []sources.DirEntry{
			{Name: "meta.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "content", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (g *GDriveProvider) readFiles(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 2 {
		return nil, sources.ErrIsDir
	}

	token := pctx.Credentials.AccessToken
	fileId, file := parts[0], parts[1]

	switch file {
	case "meta.json":
		data, err := g.fetchFileMeta(ctx, token, fileId)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	case "content":
		return g.downloadFile(ctx, token, fileId, offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// --- Shared ---
// /shared/<fileid>/... - shared with me files

func (g *GDriveProvider) statShared(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	// Same structure as files
	return g.statFiles(ctx, pctx, parts)
}

func (g *GDriveProvider) readdirShared(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	token := pctx.Credentials.AccessToken

	switch len(parts) {
	case 0:
		// List shared with me files
		files, err := g.listFiles(ctx, token, "sharedWithMe = true and trashed = false", 100)
		if err != nil {
			return nil, err
		}

		entries := make([]sources.DirEntry, 0, len(files))
		for _, f := range files {
			if id, ok := f["id"].(string); ok {
				entries = append(entries, sources.DirEntry{
					Name:  id,
					Mode:  sources.ModeDir,
					IsDir: true,
					Mtime: sources.NowUnix(),
				})
			}
		}
		return entries, nil

	case 1:
		return []sources.DirEntry{
			{Name: "meta.json", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
			{Name: "content", Mode: sources.ModeFile, Mtime: sources.NowUnix()},
		}, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (g *GDriveProvider) readShared(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	return g.readFiles(ctx, pctx, parts, offset, length)
}

// --- API methods ---

func (g *GDriveProvider) fetchRecent(ctx context.Context, token string) ([]byte, error) {
	files, err := g.listFiles(ctx, token, "trashed = false", 50)
	if err != nil {
		return nil, err
	}

	// Simplify
	result := make([]map[string]any, 0, len(files))
	for _, f := range files {
		result = append(result, map[string]any{
			"id":           f["id"],
			"name":         f["name"],
			"mimeType":     f["mimeType"],
			"size":         f["size"],
			"modifiedTime": f["modifiedTime"],
			"webViewLink":  f["webViewLink"],
		})
	}

	return jsonMarshal(map[string]any{
		"files": result,
		"count": len(result),
	})
}

func (g *GDriveProvider) fetchStarred(ctx context.Context, token string) ([]byte, error) {
	files, err := g.listFiles(ctx, token, "starred = true and trashed = false", 100)
	if err != nil {
		return nil, err
	}

	result := make([]map[string]any, 0, len(files))
	for _, f := range files {
		result = append(result, map[string]any{
			"id":           f["id"],
			"name":         f["name"],
			"mimeType":     f["mimeType"],
			"size":         f["size"],
			"modifiedTime": f["modifiedTime"],
		})
	}

	return jsonMarshal(map[string]any{
		"files": result,
		"count": len(result),
	})
}

func (g *GDriveProvider) fetchStorage(ctx context.Context, token string) ([]byte, error) {
	var result map[string]any
	if err := g.request(ctx, token, "/about?fields=storageQuota,user", &result); err != nil {
		return nil, err
	}

	return jsonMarshal(result)
}

func (g *GDriveProvider) listFiles(ctx context.Context, token, query string, pageSize int) ([]map[string]any, error) {
	path := fmt.Sprintf("/files?q=%s&pageSize=%d&orderBy=modifiedTime desc&fields=files(id,name,mimeType,size,modifiedTime,webViewLink,parents)",
		query, pageSize)

	var result map[string]any
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	files, ok := result["files"].([]any)
	if !ok {
		return nil, nil
	}

	fileList := make([]map[string]any, 0, len(files))
	for _, f := range files {
		if file, ok := f.(map[string]any); ok {
			fileList = append(fileList, file)
		}
	}
	return fileList, nil
}

func (g *GDriveProvider) getFileMeta(ctx context.Context, token, fileId string) (map[string]any, error) {
	path := fmt.Sprintf("/files/%s?fields=id,name,mimeType,size,modifiedTime,createdTime,webViewLink,parents,owners,shared", fileId)

	var result map[string]any
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	return result, nil
}

func (g *GDriveProvider) fetchFileMeta(ctx context.Context, token, fileId string) ([]byte, error) {
	meta, err := g.getFileMeta(ctx, token, fileId)
	if err != nil {
		return nil, err
	}
	return jsonMarshal(meta)
}

func (g *GDriveProvider) downloadFile(ctx context.Context, token, fileId string, offset, length int64) ([]byte, error) {
	// First check if it's a Google Doc (needs export)
	meta, err := g.getFileMeta(ctx, token, fileId)
	if err != nil {
		return nil, err
	}

	mimeType, _ := meta["mimeType"].(string)

	// Handle Google Docs export
	var url string
	switch mimeType {
	case "application/vnd.google-apps.document":
		url = fmt.Sprintf("%s/files/%s/export?mimeType=text/plain", driveAPIBase, fileId)
	case "application/vnd.google-apps.spreadsheet":
		url = fmt.Sprintf("%s/files/%s/export?mimeType=text/csv", driveAPIBase, fileId)
	case "application/vnd.google-apps.presentation":
		url = fmt.Sprintf("%s/files/%s/export?mimeType=text/plain", driveAPIBase, fileId)
	default:
		url = fmt.Sprintf("%s/files/%s?alt=media", driveAPIBase, fileId)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	// Add Range header for partial reads
	if offset > 0 || length > 0 {
		end := ""
		if length > 0 {
			end = fmt.Sprintf("%d", offset+length-1)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%s", offset, end))
	}

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("drive API: %s - %s", resp.Status, string(body))
	}

	// Read with limit
	if length > 0 {
		data := make([]byte, length)
		n, err := io.ReadFull(resp.Body, data)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			return data[:n], nil
		}
		if err != nil {
			return nil, err
		}
		return data, nil
	}

	return io.ReadAll(resp.Body)
}

func (g *GDriveProvider) request(ctx context.Context, token, path string, result any) error {
	url := driveAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		var apiErr struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		json.Unmarshal(body, &apiErr)
		if apiErr.Error.Message != "" {
			return fmt.Errorf("drive API: %s", apiErr.Error.Message)
		}
		return fmt.Errorf("drive API: %s", resp.Status)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}
