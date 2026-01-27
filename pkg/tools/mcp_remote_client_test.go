package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// mockMCPServer simulates a remote MCP server with SSE transport
type mockMCPServer struct {
	t           *testing.T
	server      *httptest.Server
	sseClients  map[chan string]bool
	sseClientMu sync.Mutex
	tools       []MCPToolInfo
}

func newMockMCPServer(t *testing.T) *mockMCPServer {
	m := &mockMCPServer{
		t:          t,
		sseClients: make(map[chan string]bool),
		tools: []MCPToolInfo{
			{
				Name:        "echo",
				Description: "Echo a message",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"message":{"type":"string"}},"required":["message"]}`),
			},
			{
				Name:        "add",
				Description: "Add two numbers",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}`),
			},
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", m.handleSSE)
	mux.HandleFunc("/message", m.handleMessage)

	m.server = httptest.NewServer(mux)
	return m
}

func (m *mockMCPServer) URL() string {
	return m.server.URL + "/sse"
}

func (m *mockMCPServer) Close() {
	m.server.Close()
	m.sseClientMu.Lock()
	for ch := range m.sseClients {
		close(ch)
	}
	m.sseClientMu.Unlock()
}

func (m *mockMCPServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}

	// Send endpoint event first
	fmt.Fprintf(w, "event: endpoint\ndata: %s/message\n\n", m.server.URL)
	flusher.Flush()

	// Create channel for this client
	ch := make(chan string, 10)
	m.sseClientMu.Lock()
	m.sseClients[ch] = true
	m.sseClientMu.Unlock()

	defer func() {
		m.sseClientMu.Lock()
		delete(m.sseClients, ch)
		m.sseClientMu.Unlock()
	}()

	// Send messages from channel until context is done
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", msg)
			flusher.Flush()
		}
	}
}

func (m *mockMCPServer) handleMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var req jsonRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Handle different methods
	var result any
	var rpcErr *jsonRPCError

	switch req.Method {
	case "initialize":
		result = map[string]any{
			"protocolVersion": "2024-11-05",
			"capabilities":    map[string]any{},
			"serverInfo": map[string]string{
				"name":    "mock-server",
				"version": "1.0.0",
			},
		}

	case "notifications/initialized":
		// No response for notifications
		w.WriteHeader(http.StatusOK)
		return

	case "tools/list":
		result = map[string]any{
			"tools": m.tools,
		}

	case "tools/call":
		params, ok := req.Params.(map[string]any)
		if !ok {
			rpcErr = &jsonRPCError{Code: -32602, Message: "Invalid params"}
		} else {
			toolName := params["name"].(string)
			args := params["arguments"].(map[string]any)

			switch toolName {
			case "echo":
				result = MCPToolResult{
					Content: []MCPContent{{Type: "text", Text: args["message"].(string)}},
				}
			case "add":
				a := int(args["a"].(float64))
				b := int(args["b"].(float64))
				result = MCPToolResult{
					Content: []MCPContent{{Type: "text", Text: fmt.Sprintf("%d", a+b)}},
				}
			default:
				rpcErr = &jsonRPCError{Code: -32601, Message: "Unknown tool"}
			}
		}

	default:
		rpcErr = &jsonRPCError{Code: -32601, Message: "Method not found"}
	}

	// Build response
	resp := jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
	}
	if rpcErr != nil {
		resp.Error = rpcErr
	} else {
		resultJSON, _ := json.Marshal(result)
		resp.Result = resultJSON
	}

	// Send response via SSE
	respJSON, _ := json.Marshal(resp)
	m.broadcast(string(respJSON))

	w.WriteHeader(http.StatusOK)
}

func (m *mockMCPServer) broadcast(msg string) {
	m.sseClientMu.Lock()
	defer m.sseClientMu.Unlock()
	for ch := range m.sseClients {
		select {
		case ch <- msg:
		default:
		}
	}
}

func TestMCPRemoteClient_Connect(t *testing.T) {
	server := newMockMCPServer(t)
	defer server.Close()

	client := NewMCPRemoteClient("test", types.MCPServerConfig{
		URL: server.URL(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("failed to start client: %v", err)
	}
	defer client.Close()

	if client.Name() != "test" {
		t.Errorf("expected name 'test', got %s", client.Name())
	}

	info := client.ServerInfo()
	if info == nil || info.Name != "mock-server" {
		t.Errorf("unexpected server info: %+v", info)
	}
}

func TestMCPRemoteClient_ListTools(t *testing.T) {
	server := newMockMCPServer(t)
	defer server.Close()

	client := NewMCPRemoteClient("test", types.MCPServerConfig{
		URL: server.URL(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("failed to start client: %v", err)
	}
	defer client.Close()

	tools, err := client.ListTools(ctx)
	if err != nil {
		t.Fatalf("failed to list tools: %v", err)
	}

	if len(tools) != 2 {
		t.Errorf("expected 2 tools, got %d", len(tools))
	}

	// Check cached tools
	cached := client.Tools()
	if len(cached) != 2 {
		t.Errorf("expected 2 cached tools, got %d", len(cached))
	}
}

func TestMCPRemoteClient_CallTool(t *testing.T) {
	server := newMockMCPServer(t)
	defer server.Close()

	client := NewMCPRemoteClient("test", types.MCPServerConfig{
		URL: server.URL(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("failed to start client: %v", err)
	}
	defer client.Close()

	// Test echo
	result, err := client.CallTool(ctx, "echo", map[string]any{"message": "hello world"})
	if err != nil {
		t.Fatalf("failed to call echo: %v", err)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "hello world" {
		t.Errorf("unexpected echo result: %+v", result)
	}

	// Test add
	result, err = client.CallTool(ctx, "add", map[string]any{"a": 3.0, "b": 4.0})
	if err != nil {
		t.Fatalf("failed to call add: %v", err)
	}
	if len(result.Content) != 1 || result.Content[0].Text != "7" {
		t.Errorf("unexpected add result: %+v", result)
	}
}

func TestMCPRemoteClient_Auth(t *testing.T) {
	var receivedAuth string
	var receivedHeaders map[string]string

	// Server that checks auth
	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		receivedHeaders = make(map[string]string)
		for k, v := range r.Header {
			if strings.HasPrefix(k, "X-") {
				receivedHeaders[k] = v[0]
			}
		}

		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: endpoint\ndata: /message\n\n")
		// Keep connection open briefly
		time.Sleep(100 * time.Millisecond)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := NewMCPRemoteClient("test", types.MCPServerConfig{
		URL: server.URL + "/sse",
		Auth: &types.MCPAuthConfig{
			Token: "test-token-123",
			Headers: map[string]string{
				"X-Custom-Header": "custom-value",
			},
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start will fail because mock doesn't implement full protocol, but auth headers will be sent
	client.Start(ctx)
	client.Close()

	if receivedAuth != "Bearer test-token-123" {
		t.Errorf("expected 'Bearer test-token-123', got '%s'", receivedAuth)
	}

	if receivedHeaders["X-Custom-Header"] != "custom-value" {
		t.Errorf("expected custom header, got %v", receivedHeaders)
	}
}

func TestMCPRemoteClient_IsRemote(t *testing.T) {
	tests := []struct {
		name     string
		config   types.MCPServerConfig
		expected bool
	}{
		{
			name:     "remote with URL",
			config:   types.MCPServerConfig{URL: "https://example.com/sse"},
			expected: true,
		},
		{
			name:     "local with command",
			config:   types.MCPServerConfig{Command: "npx", Args: []string{"-y", "server"}},
			expected: false,
		},
		{
			name:     "empty config",
			config:   types.MCPServerConfig{},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.config.IsRemote(); got != tt.expected {
				t.Errorf("IsRemote() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestMCPRemoteClient_CloseIdempotent(t *testing.T) {
	server := newMockMCPServer(t)
	defer server.Close()

	client := NewMCPRemoteClient("test", types.MCPServerConfig{
		URL: server.URL(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("failed to start client: %v", err)
	}

	// Close multiple times should not panic
	client.Close()
	client.Close()
	client.Close()
}
