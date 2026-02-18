package desktop

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	staticfiles "github.com/beam-cloud/airstore/pkg/desktop/static"
	"github.com/beam-cloud/airstore/pkg/mount"
	"github.com/beam-cloud/airstore/pkg/types"
)

type desktopServer struct {
	app      *App
	http     *http.Server
	listener net.Listener
	baseURL  string

	staticFS  fs.FS
	staticMux http.Handler
	indexHTML []byte
	proxyV1   *httputil.ReverseProxy

	gatewayBaseURL string
	client         *http.Client

	workspaceMu sync.Mutex
	workspaceID string
}

type apiError struct {
	Error string `json:"error"`
}

type desktopEntry struct {
	Name       string     `json:"name"`
	Path       string     `json:"path"`
	IsDir      bool       `json:"is_dir"`
	Size       int64      `json:"size,omitempty"`
	ModifiedAt *time.Time `json:"modified_at,omitempty"`
	Type       string     `json:"type,omitempty"`
}

type gatewayEnvelope struct {
	Success bool            `json:"success"`
	Data    json.RawMessage `json:"data"`
	Error   string          `json:"error"`
}

func startDesktopServer(app *App) (*desktopServer, error) {
	distFS, err := fs.Sub(staticfiles.Files, "dist")
	if err != nil {
		return nil, err
	}
	indexHTML, err := fs.ReadFile(distFS, "index.html")
	if err != nil {
		return nil, err
	}
	if err := verifyEmbeddedIndexAssets(distFS, indexHTML); err != nil {
		return nil, err
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}

	server := &desktopServer{
		app:            app,
		listener:       ln,
		baseURL:        "http://" + ln.Addr().String(),
		staticFS:       distFS,
		staticMux:      http.FileServer(http.FS(distFS)),
		indexHTML:      indexHTML,
		gatewayBaseURL: normalizeHTTPURL(app.cfg.GatewayHTTPAddr),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	server.proxyV1 = buildGatewayProxy(server.gatewayBaseURL, app.cfg.Token)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/desktop/status", server.handleStatus)
	mux.HandleFunc("/api/desktop/mount/toggle", server.handleMountToggle)
	mux.HandleFunc("/api/desktop/open-folder", server.handleOpenFolder)
	mux.HandleFunc("/api/desktop/fs/list", server.handleList)
	mux.HandleFunc("/api/desktop/fs/search", server.handleSearch)
	mux.HandleFunc("/api/desktop/fs/mkdir", server.handleMkdir)
	mux.HandleFunc("/api/desktop/fs/rename", server.handleRename)
	mux.HandleFunc("/api/desktop/fs/delete", server.handleDelete)
	mux.Handle("/api/v1/", http.HandlerFunc(server.handleProxyV1))
	mux.HandleFunc("/", server.handleStatic)

	server.http = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		_ = server.http.Serve(ln)
	}()

	return server, nil
}

func (s *desktopServer) shutdown(ctx context.Context) error {
	if s.http == nil {
		return nil
	}
	return s.http.Shutdown(ctx)
}

func buildGatewayProxy(gatewayBaseURL, token string) *httputil.ReverseProxy {
	target, err := url.Parse(gatewayBaseURL)
	if err != nil {
		return nil
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		if token != "" && req.Header.Get("Authorization") == "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	return proxy
}

func normalizeHTTPURL(rawAddr string) string {
	value := strings.TrimSpace(rawAddr)
	switch {
	case value == "":
		return "http://localhost:1994"
	case strings.HasPrefix(value, "http://"), strings.HasPrefix(value, "https://"):
		return value
	default:
		return "http://" + value
	}
}

var embeddedAssetRefPattern = regexp.MustCompile(`(?:src|href)=["'](/assets/[^"']+)["']`)

func verifyEmbeddedIndexAssets(distFS fs.FS, indexHTML []byte) error {
	matches := embeddedAssetRefPattern.FindAllSubmatch(indexHTML, -1)
	if len(matches) == 0 {
		return errors.New("embedded index.html references no /assets/* files")
	}

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		relPath := strings.TrimPrefix(string(match[1]), "/")
		if relPath == "" {
			return errors.New("embedded index.html has an empty asset reference")
		}
		info, err := fs.Stat(distFS, relPath)
		if err != nil {
			return fmt.Errorf("embedded asset missing: %s: %w", relPath, err)
		}
		if info.IsDir() || info.Size() == 0 {
			return fmt.Errorf("embedded asset invalid: %s", relPath)
		}
	}
	return nil
}

func (s *desktopServer) handleStatic(w http.ResponseWriter, r *http.Request) {
	cleanPath := path.Clean("/" + r.URL.Path)
	relativePath := strings.TrimPrefix(cleanPath, "/")
	if relativePath == "" {
		s.serveIndex(w)
		return
	}
	if _, err := fs.Stat(s.staticFS, relativePath); err == nil {
		s.staticMux.ServeHTTP(w, r)
		return
	}
	// For missing static assets (js/css/images), return 404 instead of index.html.
	// This avoids hard-to-debug white screens when index.html and asset filenames drift.
	if path.Ext(relativePath) != "" {
		http.NotFound(w, r)
		return
	}
	s.serveIndex(w)
}

func (s *desktopServer) serveIndex(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(s.indexHTML)
}

func (s *desktopServer) handleProxyV1(w http.ResponseWriter, r *http.Request) {
	if s.proxyV1 == nil {
		writeAPIError(w, http.StatusBadGateway, "gateway proxy unavailable")
		return
	}
	s.proxyV1.ServeHTTP(w, r)
}

func (s *desktopServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	currentState := s.app.mgr.State()
	payload := map[string]any{
		"mounted":           currentState == mount.Mounted,
		"mount_state":       currentState.String(),
		"mount_point":       s.app.cfg.MountPoint,
		"gateway_addr":      s.app.mgr.GatewayAddr(),
		"gateway_http_addr": s.gatewayBaseURL,
		"window_visible":    s.app.isWindowVisible(),
		"autostart_enabled": IsAutostartEnabled(),
	}
	if uiError := strings.TrimSpace(s.app.getUIError()); uiError != "" {
		payload["ui_error"] = uiError
	}
	if err := s.app.mgr.Err(); err != nil {
		payload["last_error"] = err.Error()
	}
	if workspaceID, err := s.resolveWorkspaceID(r.Context()); err == nil {
		payload["workspace_id"] = workspaceID
	}

	writeJSON(w, http.StatusOK, payload)
}

func (s *desktopServer) handleMountToggle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	s.app.toggleMount()
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *desktopServer) handleOpenFolder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	if err := s.app.openFolder(); err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *desktopServer) handleList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workspaceID, err := s.resolveWorkspaceID(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	var upstream struct {
		Path    string              `json:"path"`
		Entries []types.VirtualFile `json:"entries"`
	}
	query := url.Values{}
	query.Set("path", defaultPath(r.URL.Query().Get("path")))
	if err := s.gatewayRequest(
		r.Context(),
		http.MethodGet,
		fmt.Sprintf("/workspaces/%s/fs/list", workspaceID),
		query,
		nil,
		&upstream,
	); err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"path":    defaultPath(upstream.Path),
		"entries": mapVirtualEntries(upstream.Entries),
	})
}

func (s *desktopServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workspaceID, err := s.resolveWorkspaceID(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	query := url.Values{}
	query.Set("q", strings.TrimSpace(r.URL.Query().Get("q")))
	if limit := strings.TrimSpace(r.URL.Query().Get("limit")); limit != "" {
		query.Set("limit", limit)
	}

	var upstream struct {
		Query   string              `json:"query"`
		Results []types.VirtualFile `json:"results"`
	}
	if err := s.gatewayRequest(
		r.Context(),
		http.MethodGet,
		fmt.Sprintf("/workspaces/%s/fs/search", workspaceID),
		query,
		nil,
		&upstream,
	); err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"query":   upstream.Query,
		"results": mapVirtualEntries(upstream.Results),
	})
}

func (s *desktopServer) handleMkdir(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workspaceID, err := s.resolveWorkspaceID(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	var req struct {
		Path string `json:"path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	cleanPath := strings.TrimSpace(req.Path)
	if cleanPath == "" {
		writeAPIError(w, http.StatusBadRequest, "path is required")
		return
	}

	if err := s.gatewayRequest(
		r.Context(),
		http.MethodPost,
		fmt.Sprintf("/workspaces/%s/fs/mkdir", workspaceID),
		nil,
		map[string]string{"path": cleanPath},
		nil,
	); err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *desktopServer) handleRename(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workspaceID, err := s.resolveWorkspaceID(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	var req struct {
		OldPath string `json:"old_path"`
		NewPath string `json:"new_path"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	oldPath := strings.TrimSpace(req.OldPath)
	newPath := strings.TrimSpace(req.NewPath)
	if oldPath == "" || newPath == "" {
		writeAPIError(w, http.StatusBadRequest, "old_path and new_path are required")
		return
	}

	if err := s.gatewayRequest(
		r.Context(),
		http.MethodPost,
		fmt.Sprintf("/workspaces/%s/fs/rename", workspaceID),
		nil,
		map[string]string{
			"old_path": oldPath,
			"new_path": newPath,
		},
		nil,
	); err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *desktopServer) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		writeAPIError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	workspaceID, err := s.resolveWorkspaceID(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}

	query := url.Values{}
	targetPath := strings.TrimSpace(r.URL.Query().Get("path"))
	if targetPath == "" || targetPath == "/" {
		writeAPIError(w, http.StatusBadRequest, "path is required")
		return
	}
	query.Set("path", targetPath)
	if err := s.gatewayRequest(
		r.Context(),
		http.MethodDelete,
		fmt.Sprintf("/workspaces/%s/fs/delete", workspaceID),
		query,
		nil,
		nil,
	); err != nil {
		writeAPIError(w, http.StatusBadGateway, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *desktopServer) resolveWorkspaceID(ctx context.Context) (string, error) {
	s.workspaceMu.Lock()
	if s.workspaceID != "" {
		id := s.workspaceID
		s.workspaceMu.Unlock()
		return id, nil
	}
	s.workspaceMu.Unlock()

	var whoami struct {
		WorkspaceID string `json:"workspace_id"`
	}
	if err := s.gatewayRequest(ctx, http.MethodGet, "/auth/whoami", nil, nil, &whoami); err == nil && whoami.WorkspaceID != "" {
		s.workspaceMu.Lock()
		s.workspaceID = whoami.WorkspaceID
		s.workspaceMu.Unlock()
		return whoami.WorkspaceID, nil
	}

	var workspaces []struct {
		ExternalID string `json:"external_id"`
	}
	if err := s.gatewayRequest(ctx, http.MethodGet, "/workspaces", nil, nil, &workspaces); err != nil {
		return "", fmt.Errorf("resolve workspace failed: %w", err)
	}
	if len(workspaces) == 0 || workspaces[0].ExternalID == "" {
		return "", errors.New("no workspace available for token")
	}

	s.workspaceMu.Lock()
	s.workspaceID = workspaces[0].ExternalID
	s.workspaceMu.Unlock()
	return workspaces[0].ExternalID, nil
}

func (s *desktopServer) gatewayRequest(
	ctx context.Context,
	method string,
	route string,
	query url.Values,
	body any,
	out any,
) error {
	endpoint, err := url.Parse(s.gatewayBaseURL + "/api/v1" + route)
	if err != nil {
		return err
	}
	if query != nil {
		endpoint.RawQuery = query.Encode()
	}

	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), payload)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token := strings.TrimSpace(s.app.cfg.Token); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if len(rawBody) == 0 {
		if resp.StatusCode >= 400 {
			return fmt.Errorf("gateway error: %s", resp.Status)
		}
		return nil
	}

	var envelope gatewayEnvelope
	if err := json.Unmarshal(rawBody, &envelope); err != nil {
		if resp.StatusCode >= 400 {
			return fmt.Errorf("gateway error: %s", resp.Status)
		}
		return nil
	}

	if resp.StatusCode >= 400 || !envelope.Success {
		message := strings.TrimSpace(envelope.Error)
		if message == "" {
			message = fmt.Sprintf("gateway error: %s", resp.Status)
		}
		return errors.New(message)
	}

	if out == nil || len(envelope.Data) == 0 {
		return nil
	}
	return json.Unmarshal(envelope.Data, out)
}

func mapVirtualEntries(entries []types.VirtualFile) []desktopEntry {
	out := make([]desktopEntry, 0, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			name = path.Base(entry.Path)
		}
		if name == "." || name == "/" {
			name = entry.Path
		}
		out = append(out, desktopEntry{
			Name:       name,
			Path:       defaultPath(entry.Path),
			IsDir:      entry.IsFolder,
			Size:       entry.Size,
			ModifiedAt: entry.ModifiedAt,
			Type:       string(entry.Type),
		})
	}
	return out
}

func defaultPath(value string) string {
	if strings.TrimSpace(value) == "" {
		return "/"
	}
	if strings.HasPrefix(value, "/") {
		return value
	}
	return "/" + value
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, message string) {
	if strings.TrimSpace(message) == "" {
		message = http.StatusText(status)
	}
	writeJSON(w, status, apiError{Error: message})
}

func isClosedNetworkError(err error) bool {
	return err != nil && (errors.Is(err, net.ErrClosed) || strings.Contains(err.Error(), "use of closed network connection"))
}
