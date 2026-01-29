package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	DriveAPIBase = "https://www.googleapis.com/drive/v3"
)

// Text-extractable MIME types
var TextMimeTypes = map[string]bool{
	"text/plain":             true,
	"text/html":              true,
	"text/css":               true,
	"text/javascript":        true,
	"application/json":       true,
	"application/xml":        true,
	"text/xml":               true,
	"text/markdown":          true,
	"text/csv":               true,
	"application/x-yaml":     true,
	"application/javascript": true,
}

// Google Docs export MIME types
var GoogleDocsExportTypes = map[string]string{
	"application/vnd.google-apps.document":     "text/plain",
	"application/vnd.google-apps.spreadsheet":  "text/csv",
	"application/vnd.google-apps.presentation": "text/plain",
}

// API call counter for metrics
var driveAPICallCount int64

// GetDriveAPICallCount returns the current API call count
func GetDriveAPICallCount() int64 {
	return atomic.LoadInt64(&driveAPICallCount)
}

// ResetDriveAPICallCount resets the API call counter
func ResetDriveAPICallCount() {
	atomic.StoreInt64(&driveAPICallCount, 0)
}

// DriveFile represents a Google Drive file
type DriveFile struct {
	ID           string
	Name         string
	MimeType     string
	Size         int64
	ModifiedTime time.Time
	Parents      []string
	WebViewLink  string
	IsFolder     bool
}

// DriveClient provides shared Google Drive API functionality
type DriveClient struct {
	HTTPClient *http.Client
}

// NewDriveClient creates a new Google Drive API client
func NewDriveClient() *DriveClient {
	return &DriveClient{
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

// Integration returns the integration name
func (c *DriveClient) Integration() types.ToolName {
	return types.ToolGDrive
}

// Request makes a GET request to the Drive API
func (c *DriveClient) Request(ctx context.Context, token, path string, result any) error {
	atomic.AddInt64(&driveAPICallCount, 1)

	url := DriveAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drive API request failed (path=%s): %w", path, driveAPIError(resp.Status, resp.StatusCode, body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// ListFiles lists files from Drive
func (c *DriveClient) ListFiles(ctx context.Context, token, query string, maxResults int) ([]*DriveFile, error) {
	if maxResults <= 0 {
		maxResults = 50
	}

	fields := "nextPageToken,files(id,name,mimeType,size,modifiedTime,webViewLink,parents)"
	includeAllDrives := true

	files := make([]*DriveFile, 0, maxResults)
	pageToken := ""
	for len(files) < maxResults {
		pageSize := maxResults - len(files)
		if pageSize > 1000 {
			pageSize = 1000
		}

		path := buildFilesListRequestURI(query, pageSize, "modifiedTime desc", fields, pageToken, includeAllDrives)

		var result driveFileListResponse
		if err := c.Request(ctx, token, path, &result); err != nil {
			return nil, err
		}

		if len(result.Files) == 0 {
			break
		}

		for _, fileMap := range result.Files {
			file := c.ParseFile(fileMap)
			if file != nil {
				files = append(files, file)
				if len(files) >= maxResults {
					break
				}
			}
		}

		if result.NextPageToken == "" || len(files) >= maxResults {
			break
		}
		pageToken = result.NextPageToken
	}

	if len(files) > maxResults {
		files = files[:maxResults]
	}
	return files, nil
}

// GetFile fetches metadata for a single file
func (c *DriveClient) GetFile(ctx context.Context, token, fileID string) (*DriveFile, error) {
	path := fmt.Sprintf("/files/%s?fields=id,name,mimeType,size,modifiedTime,createdTime,webViewLink,parents", fileID)

	var result map[string]any
	if err := c.Request(ctx, token, path, &result); err != nil {
		return nil, err
	}

	return c.ParseFile(result), nil
}

// ParseFile extracts structured data from a Drive API response
func (c *DriveClient) ParseFile(fileMap map[string]any) *DriveFile {
	file := &DriveFile{
		ID:       getString(fileMap, "id"),
		Name:     getString(fileMap, "name"),
		MimeType: getString(fileMap, "mimeType"),
	}

	// Parse size
	if size, ok := fileMap["size"].(string); ok {
		fmt.Sscanf(size, "%d", &file.Size)
	}
	if size, ok := fileMap["size"].(float64); ok {
		file.Size = int64(size)
	}

	// Parse modified time
	if modTime, ok := fileMap["modifiedTime"].(string); ok {
		file.ModifiedTime, _ = time.Parse(time.RFC3339, modTime)
	}

	// Parse parents
	if parents, ok := fileMap["parents"].([]any); ok {
		for _, p := range parents {
			if ps, ok := p.(string); ok {
				file.Parents = append(file.Parents, ps)
			}
		}
	}

	file.WebViewLink = getString(fileMap, "webViewLink")
	file.IsFolder = file.MimeType == "application/vnd.google-apps.folder"

	return file
}

// DownloadFile downloads file content
func (c *DriveClient) DownloadFile(ctx context.Context, token, fileID string) ([]byte, error) {
	url := fmt.Sprintf("%s/files/%s?alt=media", DriveAPIBase, fileID)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, driveAPIError(resp.Status, resp.StatusCode, body)
	}

	return io.ReadAll(resp.Body)
}

// ExportGoogleDoc exports a Google Doc as text
func (c *DriveClient) ExportGoogleDoc(ctx context.Context, token, fileID, mimeType string) ([]byte, error) {
	url := fmt.Sprintf("%s/files/%s/export?mimeType=%s", DriveAPIBase, fileID, url.QueryEscape(mimeType))
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return nil, driveAPIError(resp.Status, resp.StatusCode, body)
	}

	return io.ReadAll(resp.Body)
}

// IsTextExtractable returns true if the file's text content can be extracted
func (c *DriveClient) IsTextExtractable(mimeType string) bool {
	return TextMimeTypes[mimeType]
}

// IsGoogleDoc returns true if the file is a Google Docs type that can be exported
func (c *DriveClient) IsGoogleDoc(mimeType string) bool {
	_, ok := GoogleDocsExportTypes[mimeType]
	return ok
}

// GetExportMimeType returns the export MIME type for a Google Doc
func (c *DriveClient) GetExportMimeType(mimeType string) string {
	return GoogleDocsExportTypes[mimeType]
}

// GetFileContent fetches file content, handling Google Docs export automatically
func (c *DriveClient) GetFileContent(ctx context.Context, token string, file *DriveFile) ([]byte, error) {
	log.Debug().Str("file_id", file.ID).Str("mime_type", file.MimeType).Msg("fetching drive file content")

	// Handle Google Docs export
	if exportMime := c.GetExportMimeType(file.MimeType); exportMime != "" {
		return c.ExportGoogleDoc(ctx, token, file.ID, exportMime)
	}

	// Direct download
	return c.DownloadFile(ctx, token, file.ID)
}

type driveFileListResponse struct {
	Files            []map[string]any `json:"files"`
	NextPageToken    string           `json:"nextPageToken"`
	IncompleteSearch bool             `json:"incompleteSearch"`
}

func buildFilesListRequestURI(query string, pageSize int, orderBy, fields, pageToken string, includeAllDrives bool) string {
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
