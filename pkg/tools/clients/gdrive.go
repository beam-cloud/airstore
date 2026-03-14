package clients

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/textproto"
	"net/url"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	gdriveAPIBase         = "https://www.googleapis.com/drive/v3"
	gdriveUploadAPIBase   = "https://www.googleapis.com/upload/drive/v3"
	gdriveCmdCreateFolder = "create-folder"
	gdriveCmdWriteFile    = "write-file"
	gdriveCmdUpdateFile   = "update-file"
)

type GDriveClient struct {
	api *oauthHTTPClient
}

func NewGDriveClient() *GDriveClient {
	return &GDriveClient{
		api: newOAuthHTTPClient("gdrive", gdriveAPIBase, nil),
	}
}

func (g *GDriveClient) Name() types.IntegrationName {
	return types.GDrive
}

func (g *GDriveClient) Execute(ctx context.Context, command string, args map[string]any, creds *types.IntegrationCredentials, stdout, _ io.Writer) error {
	return ExecuteOAuthCommand(ctx, "gdrive", command, args, creds, map[string]OAuthCommandHandler{
		gdriveCmdCreateFolder: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "name")
			if err != nil {
				return nil, err
			}
			parentID := GetStringArg(args, "parent_id", "")
			return g.createFolder(ctx, token, required["name"], parentID)
		},
		gdriveCmdWriteFile: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "name")
			if err != nil {
				return nil, err
			}
			data, err := resolveContent(args)
			if err != nil {
				return nil, err
			}
			parentID := GetStringArg(args, "parent_id", "")
			mimeType := GetStringArg(args, "mime_type", "text/plain")
			return g.writeFile(ctx, token, required["name"], data, parentID, mimeType)
		},
		gdriveCmdUpdateFile: func(ctx context.Context, token string, args map[string]any) (any, error) {
			required, err := RequireStringArgs(args, "file_id")
			if err != nil {
				return nil, err
			}
			data, err := resolveContent(args)
			if err != nil {
				return nil, err
			}
			mimeType := GetStringArg(args, "mime_type", "text/plain")
			return g.updateFile(ctx, token, required["file_id"], data, mimeType)
		},
	}, stdout)
}

// resolveContent extracts file content from either "content" (text) or
// "content_base64" (base64-encoded binary). Exactly one must be provided.
func resolveContent(args map[string]any) ([]byte, error) {
	text := GetStringArg(args, "content", "")
	b64 := GetStringArg(args, "content_base64", "")

	if text == "" && b64 == "" {
		return nil, fmt.Errorf("content or content_base64 is required")
	}
	if text != "" && b64 != "" {
		return nil, fmt.Errorf("provide content or content_base64, not both")
	}
	if b64 != "" {
		data, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			return nil, fmt.Errorf("decode content_base64: %w", err)
		}
		return data, nil
	}
	return []byte(text), nil
}

func (g *GDriveClient) createFolder(ctx context.Context, token, name, parentID string) (map[string]any, error) {
	payload := map[string]any{
		"name":     name,
		"mimeType": "application/vnd.google-apps.folder",
	}
	if parentID != "" {
		payload["parents"] = []string{parentID}
	}

	var result map[string]any
	if err := g.api.RequestJSON(ctx, token, "POST", "/files?fields=id,name,webViewLink,parents", payload, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (g *GDriveClient) writeFile(ctx context.Context, token, name string, content []byte, parentID, mimeType string) (map[string]any, error) {
	metadata := map[string]any{
		"name": name,
	}
	if parentID != "" {
		metadata["parents"] = []string{parentID}
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	metaHeader := textproto.MIMEHeader{}
	metaHeader.Set("Content-Type", "application/json; charset=UTF-8")
	metaPart, err := writer.CreatePart(metaHeader)
	if err != nil {
		return nil, fmt.Errorf("create metadata part: %w", err)
	}
	metaJSON, err := json.Marshal(metadata)
	if err != nil {
		return nil, fmt.Errorf("marshal metadata: %w", err)
	}
	if _, err := metaPart.Write(metaJSON); err != nil {
		return nil, fmt.Errorf("write metadata part: %w", err)
	}

	contentHeader := textproto.MIMEHeader{}
	contentHeader.Set("Content-Type", mimeType)
	contentPart, err := writer.CreatePart(contentHeader)
	if err != nil {
		return nil, fmt.Errorf("create content part: %w", err)
	}
	if _, err := contentPart.Write(content); err != nil {
		return nil, fmt.Errorf("write content part: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart writer: %w", err)
	}

	uploadURL := gdriveUploadAPIBase + "/files?uploadType=multipart&fields=id,name,mimeType,webViewLink,parents"
	var result map[string]any
	contentType := "multipart/related; boundary=" + writer.Boundary()
	if err := g.api.RequestRaw(ctx, token, "POST", uploadURL, contentType, bytes.NewReader(body.Bytes()), &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (g *GDriveClient) updateFile(ctx context.Context, token, fileID string, content []byte, mimeType string) (map[string]any, error) {
	escapedID := url.PathEscape(fileID)
	uploadURL := gdriveUploadAPIBase + "/files/" + escapedID + "?uploadType=media&fields=id,name,mimeType,webViewLink,parents"

	var result map[string]any
	if err := g.api.RequestRaw(ctx, token, "PATCH", uploadURL, mimeType, bytes.NewReader(content), &result); err != nil {
		return nil, err
	}
	return result, nil
}
