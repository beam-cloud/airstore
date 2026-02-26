package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

type oauthHTTPClient struct {
	apiName        string
	baseURL        string
	httpClient     *http.Client
	defaultHeaders map[string]string
}

func newOAuthHTTPClient(apiName, baseURL string, defaultHeaders map[string]string) *oauthHTTPClient {
	headers := make(map[string]string, len(defaultHeaders))
	for k, v := range defaultHeaders {
		headers[k] = v
	}
	return &oauthHTTPClient{
		apiName:        apiName,
		baseURL:        strings.TrimRight(baseURL, "/"),
		httpClient:     &http.Client{Timeout: 30 * time.Second},
		defaultHeaders: headers,
	}
}

func (c *oauthHTTPClient) RequestJSON(ctx context.Context, token, method, path string, payload any, result any) error {
	var rawBody io.Reader
	contentType := ""
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		rawBody = bytes.NewReader(data)
		contentType = "application/json"
	}
	return c.RequestRaw(ctx, token, method, path, contentType, rawBody, result)
}

func (c *oauthHTTPClient) RequestRaw(ctx context.Context, token, method, path, contentType string, body io.Reader, result any) error {
	url := c.resolveURL(path)
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for key, value := range c.defaultHeaders {
		req.Header.Set(key, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		raw, _ := io.ReadAll(resp.Body)
		if msg := extractAPIErrorMessage(raw); msg != "" {
			return fmt.Errorf("%s API: %s", c.apiName, msg)
		}
		return fmt.Errorf("%s API: %s", c.apiName, resp.Status)
	}

	if result == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(result)
}

func (c *oauthHTTPClient) resolveURL(path string) string {
	if strings.HasPrefix(path, "https://") || strings.HasPrefix(path, "http://") {
		return path
	}
	return c.baseURL + path
}

func extractAPIErrorMessage(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		msg := strings.TrimSpace(string(raw))
		if len(msg) > 200 {
			return msg[:200]
		}
		return msg
	}

	if msg := getString(obj, "message"); msg != "" {
		return msg
	}

	if errorsAny, ok := obj["errors"].([]any); ok && len(errorsAny) > 0 {
		if firstErr, ok := errorsAny[0].(map[string]any); ok {
			if msg := getString(firstErr, "message"); msg != "" {
				return msg
			}
		}
	}

	if errVal, ok := obj["error"]; ok {
		switch e := errVal.(type) {
		case string:
			if strings.TrimSpace(e) != "" {
				return strings.TrimSpace(e)
			}
		case map[string]any:
			if msg := getString(e, "message"); msg != "" {
				return msg
			}
		}
	}
	return ""
}
