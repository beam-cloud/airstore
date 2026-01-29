package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	driveAPIBase = "https://www.googleapis.com/drive/v3"
)

// Text-extractable MIME types
var textMimeTypes = map[string]bool{
	"text/plain":                             true,
	"text/html":                              true,
	"text/css":                               true,
	"text/javascript":                        true,
	"application/json":                       true,
	"application/xml":                        true,
	"text/xml":                               true,
	"text/markdown":                          true,
	"text/csv":                               true,
	"application/x-yaml":                     true,
	"application/javascript":                 true,
}

// Google Docs export MIME types
var googleDocsExportTypes = map[string]string{
	"application/vnd.google-apps.document":     "text/plain",
	"application/vnd.google-apps.spreadsheet":  "text/csv",
	"application/vnd.google-apps.presentation": "text/plain",
}

// GDriveIndexProvider implements IndexProvider for Google Drive.
// It syncs files from Drive into the local index.
type GDriveIndexProvider struct {
	httpClient *http.Client
	apiCalls   int64
}

// NewGDriveIndexProvider creates a new Google Drive index provider
func NewGDriveIndexProvider() *GDriveIndexProvider {
	return &GDriveIndexProvider{
		httpClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Integration returns the integration name
func (g *GDriveIndexProvider) Integration() string {
	return types.ToolGDrive.String()
}

// SyncInterval returns the recommended sync interval
func (g *GDriveIndexProvider) SyncInterval() time.Duration {
	return 60 * time.Second // Drive changes less frequently
}

// Sync fetches files from Drive and updates the index
func (g *GDriveIndexProvider) Sync(ctx context.Context, store index.IndexStore, creds *types.IntegrationCredentials, since time.Time) error {
	if creds == nil || creds.AccessToken == "" {
		return fmt.Errorf("no credentials provided")
	}

	token := creds.AccessToken
	log.Info().Time("since", since).Msg("starting gdrive index sync")

	// List ALL files (paginated)
	files, err := g.listAllFiles(ctx, token)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	if len(files) == 0 {
		log.Info().Msg("no files to sync")
		return nil
	}

	// Filter out files we already have that haven't changed
	var newFiles []driveFile
	for _, file := range files {
		existing, err := store.Get(ctx, g.Integration(), file.ID)
		if err == nil && existing != nil {
			// Check if file was modified since we last indexed it
			if !file.ModifiedTime.After(existing.ModTime) {
				continue // File hasn't changed, skip it
			}
		}
		newFiles = append(newFiles, file)
	}

	log.Info().
		Int("total_files", len(files)).
		Int("already_indexed", len(files)-len(newFiles)).
		Int("new_or_modified", len(newFiles)).
		Msg("gdrive sync - filtering unchanged files")

	if len(newFiles) == 0 {
		log.Info().Msg("all files already indexed and unchanged")
		return nil
	}

	// Sync files with concurrency limit
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 5) // Reduced concurrency
	errors := make(chan error, len(newFiles))

	for _, file := range newFiles {
		wg.Add(1)
		go func(f driveFile) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := g.syncFile(ctx, store, token, f); err != nil {
				log.Warn().Err(err).Str("file_id", f.ID).Str("name", f.Name).Msg("failed to sync file")
				errors <- err
			}
		}(file)
	}

	wg.Wait()
	close(errors)

	errorCount := 0
	for range errors {
		errorCount++
	}

	log.Info().
		Int("synced", len(newFiles)-errorCount).
		Int("errors", errorCount).
		Int64("api_calls", atomic.LoadInt64(&g.apiCalls)).
		Msg("gdrive sync completed")

	return nil
}

// driveFile holds Drive file metadata
type driveFile struct {
	ID           string
	Name         string
	MimeType     string
	Size         int64
	ModifiedTime time.Time
	Parents      []string
	Path         string // Computed path
}

// listAllFiles lists files from Drive
func (g *GDriveIndexProvider) listAllFiles(ctx context.Context, token string) ([]driveFile, error) {
	query := "trashed=false"
	encodedQuery := url.QueryEscape(query)

	var allFiles []driveFile
	pageToken := ""

	for {
		path := fmt.Sprintf("/files?q=%s&pageSize=1000&orderBy=modifiedTime desc&fields=nextPageToken,files(id,name,mimeType,size,modifiedTime,parents)", encodedQuery)
		if pageToken != "" {
			path += "&pageToken=" + pageToken
		}

		var result map[string]any
		if err := g.request(ctx, token, path, &result); err != nil {
			return nil, err
		}

		rawFiles, _ := result["files"].([]any)

		for _, f := range rawFiles {
			fileMap, ok := f.(map[string]any)
			if !ok {
				continue
			}

			file := driveFile{
				ID:       getString(fileMap, "id"),
				Name:     getString(fileMap, "name"),
				MimeType: getString(fileMap, "mimeType"),
			}

			if size, ok := fileMap["size"].(string); ok {
				fmt.Sscanf(size, "%d", &file.Size)
			}
			if size, ok := fileMap["size"].(float64); ok {
				file.Size = int64(size)
			}

			if modTime, ok := fileMap["modifiedTime"].(string); ok {
				file.ModifiedTime, _ = time.Parse(time.RFC3339, modTime)
			}

			if parents, ok := fileMap["parents"].([]any); ok {
				for _, p := range parents {
					if ps, ok := p.(string); ok {
						file.Parents = append(file.Parents, ps)
					}
				}
			}

			// Build simplified path
			file.Path = "files/" + sanitizeFilename(file.Name)

			allFiles = append(allFiles, file)
		}

		log.Info().
			Int("page_count", len(rawFiles)).
			Int("total_so_far", len(allFiles)).
			Msg("fetched drive files page")

		// Check for next page
		nextPageToken, _ := result["nextPageToken"].(string)
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
	}

	return allFiles, nil
}

// syncFile syncs a single file to the index
func (g *GDriveIndexProvider) syncFile(ctx context.Context, store index.IndexStore, token string, file driveFile) error {
	// Determine if this is a directory (folder)
	isDir := file.MimeType == "application/vnd.google-apps.folder"

	// Determine if we should extract text
	shouldExtractText := false
	var textContent string

	if !isDir {
		// Check if it's a text-extractable file
		if textMimeTypes[file.MimeType] && file.Size < 1024*1024 { // < 1MB for text files
			shouldExtractText = true
		} else if exportMime, ok := googleDocsExportTypes[file.MimeType]; ok {
			// Google Docs can be exported as text
			content, err := g.exportGoogleDoc(ctx, token, file.ID, exportMime)
			if err == nil && len(content) < 1024*1024 {
				textContent = string(content)
			}
		}
	}

	// For text files, fetch content
	if shouldExtractText && textContent == "" {
		content, err := g.fetchFileContent(ctx, token, file.ID, file.Size)
		if err == nil {
			textContent = string(content)
		}
	}

	// Build metadata
	metadata := map[string]any{
		"id":        file.ID,
		"name":      file.Name,
		"mime_type": file.MimeType,
		"size":      file.Size,
		"parents":   file.Parents,
	}
	metadataJSON, _ := json.Marshal(metadata)

	entryType := index.EntryTypeFile
	if isDir {
		entryType = index.EntryTypeDir
	}

	// Determine content ref for large/binary files
	contentRef := ""
	if !isDir && textContent == "" && file.Size > 0 {
		contentRef = file.ID // Reference for fetching on-demand
	}

	entry := &index.IndexEntry{
		Integration: types.ToolGDrive.String(),
		EntityID:    file.ID,
		Path:        file.Path,
		ParentPath:  "files",
		Name:        sanitizeFilename(file.Name),
		Type:        entryType,
		Size:        file.Size,
		ModTime:     file.ModifiedTime,
		Title:       file.Name,
		Body:        textContent,
		Metadata:    string(metadataJSON),
		ContentRef:  contentRef,
	}

	if err := store.Upsert(ctx, entry); err != nil {
		return err
	}

	// Create virtual directory entries for parent paths
	ensureParentDirs(ctx, store, types.ToolGDrive.String(), file.Path)

	return nil
}

// fetchFileContent fetches the raw content of a file
func (g *GDriveIndexProvider) fetchFileContent(ctx context.Context, token, fileID string, size int64) ([]byte, error) {
	// Limit to 1MB for indexing
	if size > 1024*1024 {
		return nil, fmt.Errorf("file too large for indexing")
	}

	url := fmt.Sprintf("%s/files/%s?alt=media", driveAPIBase, fileID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("drive API error %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// exportGoogleDoc exports a Google Doc as text
func (g *GDriveIndexProvider) exportGoogleDoc(ctx context.Context, token, fileID, mimeType string) ([]byte, error) {
	url := fmt.Sprintf("%s/files/%s/export?mimeType=%s", driveAPIBase, fileID, mimeType)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("drive export error %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// FetchContent retrieves file content for an entity ID
func (g *GDriveIndexProvider) FetchContent(ctx context.Context, creds *types.IntegrationCredentials, entityID string) ([]byte, error) {
	if creds == nil || creds.AccessToken == "" {
		return nil, fmt.Errorf("no credentials provided")
	}

	token := creds.AccessToken

	// Get file metadata first to check type
	var result map[string]any
	path := fmt.Sprintf("/files/%s?fields=id,name,mimeType,size", entityID)
	if err := g.request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	mimeType := getString(result, "mimeType")

	// Check if it's a Google Doc that needs export
	if exportMime, ok := googleDocsExportTypes[mimeType]; ok {
		return g.exportGoogleDoc(ctx, token, entityID, exportMime)
	}

	// Fetch raw content
	url := fmt.Sprintf("%s/files/%s?alt=media", driveAPIBase, entityID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("drive API error %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// request makes an HTTP request to the Drive API
func (g *GDriveIndexProvider) request(ctx context.Context, token, path string, result any) error {
	atomic.AddInt64(&g.apiCalls, 1)

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

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drive API error %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

