package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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

// Search executes a Google Drive search query and returns results
// The query uses Google Drive query syntax (e.g., "name contains 'invoice'", "mimeType='application/pdf'")
func (g *GDriveProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	if limit <= 0 {
		limit = 50
	}

	token := pctx.Credentials.AccessToken

	fields := "nextPageToken,incompleteSearch,files(id,name,mimeType,size,modifiedTime,webViewLink)"
	files, err := g.listFilesPaged(ctx, token, query, limit, fields, true)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return []sources.SearchResult{}, nil
	}

	results := make([]sources.SearchResult, 0, len(files))
	for _, f := range files {
		file := f

		id, _ := file["id"].(string)
		name, _ := file["name"].(string)
		mimeType, _ := file["mimeType"].(string)
		size, _ := file["size"].(string)
		modifiedTime, _ := file["modifiedTime"].(string)

		// Determine if it's a folder
		isDir := mimeType == "application/vnd.google-apps.folder"
		var mode uint32 = sources.ModeFile
		if isDir {
			mode = sources.ModeDir
		}

		// Parse size
		var sizeInt int64
		fmt.Sscanf(size, "%d", &sizeInt)

		// Parse modified time
		mtime := sources.NowUnix()
		if t, err := time.Parse(time.RFC3339, modifiedTime); err == nil {
			mtime = t.Unix()
		}

		// Generate a unique filename (name may not be unique in Drive)
		filename := name
		if id != "" && len(id) >= 8 {
			// Append short ID if there could be duplicates
			ext := ""
			if idx := strings.LastIndex(name, "."); idx > 0 {
				ext = name[idx:]
				name = name[:idx]
			}
			filename = fmt.Sprintf("%s_%s%s", name, id[:8], ext)
		}

		results = append(results, sources.SearchResult{
			Name:    filename,
			Id:      id,
			Mode:    mode,
			Size:    sizeInt,
			Mtime:   mtime,
			Preview: mimeType,
		})
	}

	return results, nil
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
	fields := "files(id,name,mimeType,size,modifiedTime,webViewLink,parents)"
	path := g.buildFilesListPath(query, pageSize, "modifiedTime desc", fields, "", false)

	var result driveFileListResponse
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, fmt.Errorf("drive files.list failed (q=%q): %w", query, err)
	}

	return result.Files, nil
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
		return nil, driveAPIError(resp.Status, resp.StatusCode, body)
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
		return driveAPIError(resp.Status, resp.StatusCode, body)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ============================================================================
// QueryExecutor implementation
// ============================================================================

// ExecuteQuery runs a Google Drive search query and returns results with generated filenames.
// This implements the sources.QueryExecutor interface for filesystem queries.
func (g *GDriveProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) ([]sources.QueryResult, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}

	token := pctx.Credentials.AccessToken

	fields := "nextPageToken,incompleteSearch,files(id,name,mimeType,size,modifiedTime,createdTime,webViewLink)"
	files, err := g.listFilesPaged(ctx, token, spec.Query, limit, fields, true)
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return []sources.QueryResult{}, nil
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat("gdrive")
	}

	results := make([]sources.QueryResult, 0, len(files))
	for _, f := range files {
		file := f

		id, _ := file["id"].(string)
		fullName, _ := file["name"].(string)
		mimeType, _ := file["mimeType"].(string)
		sizeStr, _ := file["size"].(string)
		modifiedTime, _ := file["modifiedTime"].(string)
		createdTime, _ := file["createdTime"].(string)

		// Parse size
		var size int64
		fmt.Sscanf(sizeStr, "%d", &size)

		// Parse modified time
		mtime := sources.NowUnix()
		if t, err := time.Parse(time.RFC3339, modifiedTime); err == nil {
			mtime = t.Unix()
		}

		// Parse dates for metadata
		modDate := ""
		if t, err := time.Parse(time.RFC3339, modifiedTime); err == nil {
			modDate = t.Format("2006-01-02")
		}
		createdDate := ""
		if t, err := time.Parse(time.RFC3339, createdTime); err == nil {
			createdDate = t.Format("2006-01-02")
		}

		// Split name into basename + extension.
		// NOTE: We store basename in metadata["name"] and extension in metadata["ext"] to avoid
		// accidentally duplicating extensions when the filename format appends {id}.
		baseName := fullName
		nameExt := ""
		if idx := strings.LastIndex(fullName, "."); idx > 0 {
			baseName = fullName[:idx]
			nameExt = fullName[idx:]
		}

		// Build metadata map
		metadata := map[string]string{
			"id":        id,
			"name":      baseName,
			"mime_type": mimeType,
			"date":      modDate,
			"created":   createdDate,
		}

		// Determine extension based on mime type or existing extension
		ext := nameExt
		switch mimeType {
		// Google Docs types: we export these to text/csv, so enforce the exported extension.
		case "application/vnd.google-apps.document":
			ext = ".txt"
		case "application/vnd.google-apps.spreadsheet":
			ext = ".csv"
		case "application/vnd.google-apps.presentation":
			ext = ".txt"
		case "application/vnd.google-apps.folder":
			ext = "" // folder, no extension
		}
		metadata["ext"] = ext

		// Generate filename
		filename := g.FormatFilename(filenameFormat, metadata)

		results = append(results, sources.QueryResult{
			ID:       id,
			Filename: filename,
			Metadata: metadata,
			Size:     size,
			Mtime:    mtime,
		})
	}

	return results, nil
}

// ReadResult fetches the content of a Drive file by its file ID.
// This implements the sources.QueryExecutor interface.
func (g *GDriveProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if pctx.Credentials == nil || pctx.Credentials.AccessToken == "" {
		return nil, sources.ErrNotConnected
	}
	return g.downloadFile(ctx, pctx.Credentials.AccessToken, resultID, 0, 0)
}

// FormatFilename generates a filename from metadata using a format template.
// Supported placeholders: {id}, {name}, {date}, {created}, {mime_type}, {ext}
// - {name} is the basename (no extension)
// - {ext} is the extension (includes leading dot, e.g. ".pdf")
// This implements the sources.QueryExecutor interface.
func (g *GDriveProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{name}_{id}"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		var safeValue string
		if key == "ext" {
			// Extensions are already validated, don't sanitize (preserve leading dot)
			safeValue = value
		} else {
			// Sanitize the value for filesystem use
			safeValue = sanitizeFolderName(value)
			// Truncate long values (except id)
			if key != "id" && len(safeValue) > 50 {
				safeValue = safeValue[:50]
			}
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	// If there's an extension in metadata, ensure it's appended
	if ext, ok := metadata["ext"]; ok && ext != "" && !strings.HasSuffix(result, ext) {
		// Only append if format didn't explicitly include it
		if !strings.Contains(format, "{ext}") {
			result += ext
		}
	}

	// Ensure filename is not empty
	if result == "" {
		if id, ok := metadata["id"]; ok {
			result = id
		} else {
			result = "unknown"
		}
	}

	return result
}

// Compile-time interface check for QueryExecutor
var _ sources.QueryExecutor = (*GDriveProvider)(nil)

type driveFileListResponse struct {
	Files            []map[string]any `json:"files"`
	NextPageToken    string           `json:"nextPageToken"`
	IncompleteSearch bool             `json:"incompleteSearch"`
}

func (g *GDriveProvider) buildFilesListPath(query string, pageSize int, orderBy, fields, pageToken string, includeAllDrives bool) string {
	params := url.Values{}
	if query != "" {
		params.Set("q", query)
	}
	if pageSize > 0 {
		params.Set("pageSize", strconv.Itoa(pageSize))
	}
	if orderBy != "" {
		params.Set("orderBy", orderBy)
	}
	if fields != "" {
		params.Set("fields", fields)
	}
	if pageToken != "" {
		params.Set("pageToken", pageToken)
	}

	// Default Drive search space.
	params.Set("spaces", "drive")

	if includeAllDrives {
		params.Set("corpora", "allDrives")
		params.Set("includeItemsFromAllDrives", "true")
		params.Set("supportsAllDrives", "true")
	}

	u := url.URL{
		Path:     "/files",
		RawQuery: params.Encode(),
	}
	return u.RequestURI()
}

func (g *GDriveProvider) listFilesPaged(ctx context.Context, token, query string, limit int, fields string, includeAllDrives bool) ([]map[string]any, error) {
	if limit <= 0 {
		limit = 50
	}

	out := make([]map[string]any, 0, limit)
	pageToken := ""
	for len(out) < limit {
		pageSize := limit - len(out)
		if pageSize > 1000 {
			pageSize = 1000
		}

		path := g.buildFilesListPath(query, pageSize, "modifiedTime desc", fields, pageToken, includeAllDrives)

		var result driveFileListResponse
		if err := g.request(ctx, token, path, &result); err != nil {
			return nil, fmt.Errorf("drive files.list failed (q=%q, all_drives=%t): %w", query, includeAllDrives, err)
		}

		if len(result.Files) == 0 {
			break
		}
		out = append(out, result.Files...)
		if len(out) >= limit {
			out = out[:limit]
			break
		}

		if result.NextPageToken == "" {
			break
		}
		pageToken = result.NextPageToken
	}

	return out, nil
}

func driveAPIError(status string, statusCode int, body []byte) error {
	if msg := parseDriveAPIErrorMessage(body); msg != "" {
		return fmt.Errorf("drive API: %s (HTTP %d)", msg, statusCode)
	}

	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 2048 {
		snippet = snippet[:2048] + "..."
	}
	if snippet != "" {
		return fmt.Errorf("drive API: %s - %s", status, snippet)
	}
	return fmt.Errorf("drive API: %s", status)
}

func parseDriveAPIErrorMessage(body []byte) string {
	var apiErr struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error.Message != "" {
		return apiErr.Error.Message
	}
	return ""
}
