package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// MCPRemoteClient manages communication with a remote MCP server via HTTP/SSE.
// It implements the JSON-RPC 2.0 protocol used by MCP over HTTP transport.
// Supports both SSE transport and Streamable HTTP transport.
type MCPRemoteClient struct {
	name       string
	config     types.MCPServerConfig
	httpClient *http.Client

	// Transport mode
	useHTTPTransport bool // true = Streamable HTTP, false = SSE

	// Session ID for Streamable HTTP transport
	sessionId string

	// SSE connection (only used for SSE transport)
	sseResp   *http.Response
	sseReader *bufio.Reader

	// Message endpoint (discovered from SSE, or same as URL for HTTP)
	messageEndpoint string

	reqID    atomic.Int64
	mu       sync.Mutex
	pending  map[int64]chan *jsonRPCResponse
	closed   bool
	closedMu sync.RWMutex

	// Cached capabilities after initialization
	serverInfo   *MCPServerInfo
	capabilities *MCPCapabilities
	tools        []MCPToolInfo
}

// NewMCPRemoteClient creates a new HTTP/SSE-based MCP client for the given server configuration.
// The connection is not established until Start() is called.
func NewMCPRemoteClient(name string, config types.MCPServerConfig) *MCPRemoteClient {
	return &MCPRemoteClient{
		name:    name,
		config:  config,
		pending: make(map[int64]chan *jsonRPCResponse),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Name returns the server name
func (c *MCPRemoteClient) Name() string {
	return c.name
}

// Start connects to the remote MCP server and initializes the connection.
// Supports both SSE transport (default) and Streamable HTTP transport.
func (c *MCPRemoteClient) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	transport := c.config.GetTransport()
	c.useHTTPTransport = transport == types.MCPTransportHTTP

	// For HTTP transport, use the URL directly as the message endpoint
	if c.useHTTPTransport {
		c.messageEndpoint = c.config.URL
	} else {
		// SSE transport: connect to SSE endpoint
		if err := c.connectSSE(ctx); err != nil {
			return err
		}
	}

	// Initialize the connection (unlock for call)
	c.mu.Unlock()
	err := c.initialize(ctx)
	c.mu.Lock()
	if err != nil {
		c.closeInternal()
		return fmt.Errorf("initialize: %w", err)
	}

	return nil
}

// connectSSE establishes the SSE connection (for SSE transport)
func (c *MCPRemoteClient) connectSSE(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, "GET", c.config.URL, nil)
	if err != nil {
		return fmt.Errorf("create SSE request: %w", err)
	}

	// Set SSE headers
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	// Add auth headers
	c.addAuthHeaders(req)

	// Connect (use a client without timeout for SSE)
	sseClient := &http.Client{} // No timeout for SSE streaming
	resp, err := sseClient.Do(req)
	if err != nil {
		return fmt.Errorf("SSE connect: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("SSE connect failed: %s", resp.Status)
	}

	c.sseResp = resp
	c.sseReader = bufio.NewReader(resp.Body)

	// Read the endpoint event to get message URL
	if err := c.readEndpointEvent(ctx); err != nil {
		c.sseResp.Body.Close()
		return fmt.Errorf("read endpoint: %w", err)
	}

	// Start SSE reader goroutine
	go c.readSSEEvents()

	return nil
}

// addAuthHeaders adds authentication headers to an HTTP request
func (c *MCPRemoteClient) addAuthHeaders(req *http.Request) {
	if c.config.Auth == nil {
		return
	}

	// Bearer token
	if c.config.Auth.Token != "" {
		token := os.ExpandEnv(c.config.Auth.Token)
		req.Header.Set("Authorization", "Bearer "+token)
	}

	// Custom headers
	for name, value := range c.config.Auth.Headers {
		expanded := os.ExpandEnv(value)
		req.Header.Set(name, expanded)
	}
}

// readEndpointEvent reads the initial SSE event that contains the message endpoint URL.
// MCP SSE servers send an "endpoint" event first with the URL to POST messages to.
func (c *MCPRemoteClient) readEndpointEvent(ctx context.Context) error {
	// Read SSE events until we get the endpoint
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		event, data, err := c.readSSEEvent()
		if err != nil {
			return fmt.Errorf("read SSE event: %w", err)
		}

		if event == "endpoint" {
			// The data is the message endpoint URL
			c.messageEndpoint = strings.TrimSpace(data)

			// Handle relative URLs
			if strings.HasPrefix(c.messageEndpoint, "/") {
				// Extract base URL from SSE URL
				baseURL := c.config.URL
				if idx := strings.Index(baseURL, "://"); idx != -1 {
					if slashIdx := strings.Index(baseURL[idx+3:], "/"); slashIdx != -1 {
						baseURL = baseURL[:idx+3+slashIdx]
					}
				}
				c.messageEndpoint = baseURL + c.messageEndpoint
			}

			return nil
		}
	}
}

// readSSEEvent reads a single SSE event from the stream
func (c *MCPRemoteClient) readSSEEvent() (event string, data string, err error) {
	var dataLines []string
	event = "message" // default event type

	for {
		line, err := c.sseReader.ReadString('\n')
		if err != nil {
			return "", "", err
		}

		line = strings.TrimRight(line, "\r\n")

		// Empty line marks end of event
		if line == "" {
			if len(dataLines) > 0 {
				return event, strings.Join(dataLines, "\n"), nil
			}
			continue
		}

		// Parse field
		if strings.HasPrefix(line, "event:") {
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data:"))
		} else if strings.HasPrefix(line, ":") {
			// Comment, ignore
		}
	}
}

// readSSEEvents continuously reads SSE events and routes responses
func (c *MCPRemoteClient) readSSEEvents() {
	for {
		c.closedMu.RLock()
		closed := c.closed
		c.closedMu.RUnlock()
		if closed {
			return
		}

		event, data, err := c.readSSEEvent()
		if err != nil {
			return
		}

		// Handle message events (JSON-RPC responses)
		if event == "message" {
			var resp jsonRPCResponse
			if err := json.Unmarshal([]byte(data), &resp); err != nil {
				continue
			}

			// Route response to waiting caller
			c.mu.Lock()
			if ch, ok := c.pending[resp.ID]; ok {
				ch <- &resp
			}
			c.mu.Unlock()
		}
	}
}

// initialize sends the MCP initialize request
func (c *MCPRemoteClient) initialize(ctx context.Context) error {
	params := map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo": map[string]string{
			"name":    "airstore",
			"version": "1.0.0",
		},
	}

	result, err := c.call(ctx, "initialize", params)
	if err != nil {
		return err
	}

	var initResult struct {
		ProtocolVersion string           `json:"protocolVersion"`
		Capabilities    *MCPCapabilities `json:"capabilities"`
		ServerInfo      *MCPServerInfo   `json:"serverInfo"`
	}
	if err := json.Unmarshal(result, &initResult); err != nil {
		return fmt.Errorf("parse init result: %w", err)
	}

	c.serverInfo = initResult.ServerInfo
	c.capabilities = initResult.Capabilities

	// Send initialized notification
	if err := c.notify(ctx, "notifications/initialized", nil); err != nil {
		return fmt.Errorf("send initialized: %w", err)
	}

	return nil
}

// ListTools fetches the list of available tools from the server
func (c *MCPRemoteClient) ListTools(ctx context.Context) ([]MCPToolInfo, error) {
	result, err := c.call(ctx, "tools/list", nil)
	if err != nil {
		return nil, err
	}

	var listResult struct {
		Tools []MCPToolInfo `json:"tools"`
	}
	if err := json.Unmarshal(result, &listResult); err != nil {
		return nil, fmt.Errorf("parse tools list: %w", err)
	}

	c.tools = listResult.Tools
	return listResult.Tools, nil
}

// CallTool invokes a tool on the MCP server.
// Implements MCPToolExecutor interface.
func (c *MCPRemoteClient) CallTool(ctx context.Context, name string, args map[string]any) (MCPToolResult, error) {
	params := map[string]any{
		"name":      name,
		"arguments": args,
	}

	result, err := c.call(ctx, "tools/call", params)
	if err != nil {
		return MCPToolResult{}, err
	}

	var toolResult MCPToolResult
	if err := json.Unmarshal(result, &toolResult); err != nil {
		return MCPToolResult{}, fmt.Errorf("parse tool result: %w", err)
	}

	return toolResult, nil
}

// Tools returns the cached list of tools
func (c *MCPRemoteClient) Tools() []MCPToolInfo {
	return c.tools
}

// ServerInfo returns the server information from initialization
func (c *MCPRemoteClient) ServerInfo() *MCPServerInfo {
	return c.serverInfo
}

// call sends a JSON-RPC request via HTTP POST and waits for the response.
// For SSE transport, the response comes via SSE stream.
// For HTTP transport, the response comes directly in the HTTP response body.
func (c *MCPRemoteClient) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	c.closedMu.RLock()
	if c.closed {
		c.closedMu.RUnlock()
		return nil, fmt.Errorf("client closed")
	}
	c.closedMu.RUnlock()

	id := c.reqID.Add(1)

	req := jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      id,
		Method:  method,
		Params:  params,
	}

	// Send request via HTTP POST
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.messageEndpoint, bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	c.addAuthHeaders(httpReq)

	// For HTTP transport, get response directly from HTTP response body
	if c.useHTTPTransport {
		// Streamable HTTP requires accepting both JSON and SSE
		httpReq.Header.Set("Accept", "application/json, text/event-stream")

		// Include session ID if we have one (required after initialization)
		if c.sessionId != "" {
			httpReq.Header.Set("Mcp-Session-Id", c.sessionId)
		}

		return c.callHTTP(ctx, httpReq, method, id)
	}

	// For SSE transport, wait for response via SSE stream
	return c.callSSE(ctx, httpReq, method, id)
}

// callHTTP sends request and reads response from HTTP body (Streamable HTTP transport)
// The server may respond with application/json or text/event-stream depending on the request
func (c *MCPRemoteClient) callHTTP(ctx context.Context, httpReq *http.Request, method string, id int64) (json.RawMessage, error) {
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("request failed: %s - %s", resp.Status, string(body))
	}

	// Capture session ID from response headers (set during initialization)
	if sessionId := resp.Header.Get("Mcp-Session-Id"); sessionId != "" {
		c.sessionId = sessionId
	}

	// Handle SSE response (streaming)
	contentType := resp.Header.Get("Content-Type")
	if strings.HasPrefix(contentType, "text/event-stream") {
		return c.readSSEResponse(resp, method, id)
	}

	// Handle JSON response (simple)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	return rpcResp.Result, nil
}

// readSSEResponse reads a JSON-RPC response from an SSE stream (for Streamable HTTP)
func (c *MCPRemoteClient) readSSEResponse(resp *http.Response, method string, id int64) (json.RawMessage, error) {
	reader := bufio.NewReader(resp.Body)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return nil, fmt.Errorf("unexpected end of SSE stream")
			}
			return nil, fmt.Errorf("read SSE: %w", err)
		}

		line = strings.TrimRight(line, "\r\n")

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}

		// Parse data lines
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimPrefix(line, "data:")
			data = strings.TrimSpace(data)

			if data == "" {
				continue
			}

			var rpcResp jsonRPCResponse
			if err := json.Unmarshal([]byte(data), &rpcResp); err != nil {
				continue
			}

			// Check if this is the response we're waiting for
			if rpcResp.ID == id {
				if rpcResp.Error != nil {
					return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
				}
				return rpcResp.Result, nil
			}
		}
	}
}

// callSSE sends request and waits for response via SSE stream
func (c *MCPRemoteClient) callSSE(ctx context.Context, httpReq *http.Request, method string, id int64) (json.RawMessage, error) {
	respChan := make(chan *jsonRPCResponse, 1)

	c.mu.Lock()
	c.pending[id] = respChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("request failed: %s", resp.Status)
	}

	// Wait for response via SSE
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case rpcResp := <-respChan:
		if rpcResp == nil {
			return nil, fmt.Errorf("connection closed")
		}
		if rpcResp.Error != nil {
			return nil, fmt.Errorf("RPC error %d: %s", rpcResp.Error.Code, rpcResp.Error.Message)
		}
		return rpcResp.Result, nil
	}
}

// notify sends a JSON-RPC notification via HTTP POST (no response expected)
func (c *MCPRemoteClient) notify(ctx context.Context, method string, params any) error {
	c.closedMu.RLock()
	if c.closed {
		c.closedMu.RUnlock()
		return fmt.Errorf("client closed")
	}
	c.closedMu.RUnlock()

	// Notifications have no ID
	notif := struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  any    `json:"params,omitempty"`
	}{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
	}

	data, err := json.Marshal(notif)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.messageEndpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	c.addAuthHeaders(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send notification: %w", err)
	}
	resp.Body.Close()

	return nil
}

// Close shuts down the MCP client and closes the SSE connection
func (c *MCPRemoteClient) Close() error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return nil
	}
	c.closed = true
	c.closedMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closeInternal()
}

// closeInternal performs the actual close operations (must hold c.mu)
func (c *MCPRemoteClient) closeInternal() error {
	// Close pending channels
	for _, ch := range c.pending {
		close(ch)
	}
	c.pending = make(map[int64]chan *jsonRPCResponse)

	// Close SSE connection
	if c.sseResp != nil {
		c.sseResp.Body.Close()
	}

	return nil
}

// Ensure MCPRemoteClient implements MCPClient and MCPToolExecutor interfaces
var (
	_ MCPClient       = (*MCPRemoteClient)(nil)
	_ MCPToolExecutor = (*MCPRemoteClient)(nil)
)
