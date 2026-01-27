package tools

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// MCPClient is the interface for MCP server clients (both stdio and remote).
type MCPClient interface {
	Name() string
	Start(ctx context.Context) error
	ListTools(ctx context.Context) ([]MCPToolInfo, error)
	CallTool(ctx context.Context, name string, args map[string]any) (MCPToolResult, error)
	Tools() []MCPToolInfo
	ServerInfo() *MCPServerInfo
	Close() error
}

// MCPStdioClient manages communication with an MCP server process via stdio.
// It implements the JSON-RPC 2.0 protocol used by MCP.
type MCPStdioClient struct {
	name   string
	config types.MCPServerConfig
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout *bufio.Reader
	stderr io.ReadCloser

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

// JSON-RPC 2.0 types
type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int64  `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int64           `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

// NewMCPStdioClient creates a new stdio-based MCP client for the given server configuration.
// The server process is not started until Start() is called.
func NewMCPStdioClient(name string, config types.MCPServerConfig) *MCPStdioClient {
	return &MCPStdioClient{
		name:    name,
		config:  config,
		pending: make(map[int64]chan *jsonRPCResponse),
	}
}

// Name returns the server name
func (c *MCPStdioClient) Name() string {
	return c.name
}

// Start launches the MCP server process and initializes the connection.
func (c *MCPStdioClient) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Build command
	c.cmd = exec.CommandContext(ctx, c.config.Command, c.config.Args...)

	log.Debug().
		Str("server", c.name).
		Str("command", c.config.Command).
		Strs("args", c.config.Args).
		Msg("starting MCP server process")

	// Set working directory if specified
	if c.config.WorkingDir != "" {
		c.cmd.Dir = c.config.WorkingDir
	}

	// Set environment
	c.cmd.Env = os.Environ()
	for k, v := range c.config.Env {
		// Expand environment variables in values
		expanded := os.ExpandEnv(v)
		c.cmd.Env = append(c.cmd.Env, fmt.Sprintf("%s=%s", k, expanded))
	}

	// Inject auth credentials as environment variables
	if c.config.Auth != nil {
		if c.config.Auth.Token != "" {
			envName := "MCP_AUTH_TOKEN"
			if c.config.Auth.TokenEnv != "" {
				envName = c.config.Auth.TokenEnv
			}
			// Expand environment variables in token value
			expanded := os.ExpandEnv(c.config.Auth.Token)
			c.cmd.Env = append(c.cmd.Env, fmt.Sprintf("%s=%s", envName, expanded))
			log.Debug().Str("server", c.name).Str("env", envName).Msg("injected auth token")
		}
		for name, value := range c.config.Auth.Headers {
			// Convert header name to env var format: Authorization -> MCP_AUTH_HEADER_AUTHORIZATION
			envName := fmt.Sprintf("MCP_AUTH_HEADER_%s", strings.ToUpper(strings.ReplaceAll(name, "-", "_")))
			expanded := os.ExpandEnv(value)
			c.cmd.Env = append(c.cmd.Env, fmt.Sprintf("%s=%s", envName, expanded))
			log.Debug().Str("server", c.name).Str("env", envName).Msg("injected auth header")
		}
	}

	// Setup pipes
	var err error
	c.stdin, err = c.cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("stdin pipe: %w", err)
	}

	stdoutPipe, err := c.cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("stdout pipe: %w", err)
	}
	c.stdout = bufio.NewReader(stdoutPipe)

	c.stderr, err = c.cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("stderr pipe: %w", err)
	}

	// Start the process
	if err := c.cmd.Start(); err != nil {
		return fmt.Errorf("start process: %w", err)
	}

	log.Debug().
		Str("server", c.name).
		Int("pid", c.cmd.Process.Pid).
		Msg("MCP server process started")

	// Start stderr reader to capture any error output
	go c.readStderr()

	// Start response reader goroutine
	go c.readResponses()

	// Initialize the connection (unlock for call)
	log.Debug().Str("server", c.name).Msg("sending MCP initialize request")
	c.mu.Unlock()
	err = c.initialize(ctx)
	c.mu.Lock()
	if err != nil {
		c.closeInternal()
		return fmt.Errorf("initialize: %w", err)
	}

	log.Debug().Str("server", c.name).Msg("MCP server initialized successfully")
	return nil
}

// readStderr reads and logs stderr output from the MCP server
func (c *MCPStdioClient) readStderr() {
	scanner := bufio.NewScanner(c.stderr)
	for scanner.Scan() {
		line := scanner.Text()
		log.Debug().
			Str("server", c.name).
			Str("stderr", line).
			Msg("MCP server stderr")
	}
}

// initialize sends the MCP initialize request
func (c *MCPStdioClient) initialize(ctx context.Context) error {
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
	if err := c.notify("notifications/initialized", nil); err != nil {
		return fmt.Errorf("send initialized: %w", err)
	}

	return nil
}

// ListTools fetches the list of available tools from the server
func (c *MCPStdioClient) ListTools(ctx context.Context) ([]MCPToolInfo, error) {
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
func (c *MCPStdioClient) CallTool(ctx context.Context, name string, args map[string]any) (MCPToolResult, error) {
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
func (c *MCPStdioClient) Tools() []MCPToolInfo {
	return c.tools
}

// ServerInfo returns the server information from initialization
func (c *MCPStdioClient) ServerInfo() *MCPServerInfo {
	return c.serverInfo
}

// call sends a JSON-RPC request and waits for the response
func (c *MCPStdioClient) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
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

	respChan := make(chan *jsonRPCResponse, 1)

	c.mu.Lock()
	c.pending[id] = respChan
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
	}()

	// Send request
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	log.Debug().
		Str("server", c.name).
		Str("method", method).
		Int64("id", id).
		Msg("sending MCP request")

	c.mu.Lock()
	_, err = fmt.Fprintf(c.stdin, "%s\n", data)
	c.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("write request: %w", err)
	}

	// Wait for response
	select {
	case <-ctx.Done():
		log.Debug().
			Str("server", c.name).
			Str("method", method).
			Int64("id", id).
			Msg("MCP request timed out waiting for response")
		return nil, ctx.Err()
	case resp := <-respChan:
		if resp == nil {
			return nil, fmt.Errorf("connection closed")
		}
		if resp.Error != nil {
			log.Debug().
				Str("server", c.name).
				Str("method", method).
				Int("code", resp.Error.Code).
				Str("error", resp.Error.Message).
				Msg("MCP request returned error")
			return nil, fmt.Errorf("RPC error %d: %s", resp.Error.Code, resp.Error.Message)
		}
		log.Debug().
			Str("server", c.name).
			Str("method", method).
			Int64("id", id).
			Msg("MCP request completed")
		return resp.Result, nil
	}
}

// notify sends a JSON-RPC notification (no response expected)
func (c *MCPStdioClient) notify(method string, params any) error {
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

	c.mu.Lock()
	defer c.mu.Unlock()
	_, err = fmt.Fprintf(c.stdin, "%s\n", data)
	return err
}

// readResponses reads JSON-RPC responses from stdout
func (c *MCPStdioClient) readResponses() {
	for {
		c.closedMu.RLock()
		closed := c.closed
		c.closedMu.RUnlock()
		if closed {
			return
		}

		line, err := c.stdout.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				// Log error but don't crash
			}
			return
		}

		var resp jsonRPCResponse
		if err := json.Unmarshal(line, &resp); err != nil {
			// Could be a notification or invalid JSON, skip
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

// Close shuts down the MCP client and terminates the server process
func (c *MCPStdioClient) Close() error {
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
func (c *MCPStdioClient) closeInternal() error {
	// Close pending channels
	for _, ch := range c.pending {
		close(ch)
	}
	c.pending = make(map[int64]chan *jsonRPCResponse)

	// Close stdin to signal the process
	if c.stdin != nil {
		c.stdin.Close()
	}

	// Kill the process if still running
	if c.cmd != nil && c.cmd.Process != nil {
		c.cmd.Process.Kill()
		c.cmd.Wait()
	}

	return nil
}

// Ensure MCPStdioClient implements MCPClient and MCPToolExecutor interfaces
var (
	_ MCPClient       = (*MCPStdioClient)(nil)
	_ MCPToolExecutor = (*MCPStdioClient)(nil)
)
