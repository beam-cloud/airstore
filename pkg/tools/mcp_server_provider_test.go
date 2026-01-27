package tools

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// mockExecutor captures tool calls for testing
type mockExecutor struct {
	lastTool string
	lastArgs map[string]any
	result   MCPToolResult
	err      error
}

func (m *mockExecutor) CallTool(ctx context.Context, name string, args map[string]any) (MCPToolResult, error) {
	m.lastTool = name
	m.lastArgs = args
	return m.result, m.err
}

func TestParseToolArgs_SimpleString(t *testing.T) {
	provider := NewMCPServerProvider("test", nil)
	provider.AddTool("echo", "Echo a message", json.RawMessage(`{
		"type": "object",
		"properties": {
			"message": {"type": "string", "description": "Message to echo"}
		},
		"required": ["message"]
	}`))

	tool := provider.tools["echo"]

	// Positional arg
	args, err := provider.parseToolArgs(tool, []string{"hello world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["message"] != "hello world" {
		t.Errorf("expected 'hello world', got %v", args["message"])
	}

	// JSON object
	args, err = provider.parseToolArgs(tool, []string{`{"message": "hello"}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["message"] != "hello" {
		t.Errorf("expected 'hello', got %v", args["message"])
	}

	// Flag style
	args, err = provider.parseToolArgs(tool, []string{"--message=hello"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["message"] != "hello" {
		t.Errorf("expected 'hello', got %v", args["message"])
	}
}

func TestParseToolArgs_MultipleStrings(t *testing.T) {
	provider := NewMCPServerProvider("git", nil)
	provider.AddTool("commit", "Make a commit", json.RawMessage(`{
		"type": "object",
		"properties": {
			"message": {"type": "string"},
			"author": {"type": "string"}
		},
		"required": ["message", "author"]
	}`))

	tool := provider.tools["commit"]

	// Two positional args
	args, err := provider.parseToolArgs(tool, []string{"fix bug", "john@example.com"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["message"] != "fix bug" {
		t.Errorf("expected 'fix bug', got %v", args["message"])
	}
	if args["author"] != "john@example.com" {
		t.Errorf("expected 'john@example.com', got %v", args["author"])
	}

	// JSON
	args, err = provider.parseToolArgs(tool, []string{`{"message": "fix", "author": "jane"}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["message"] != "fix" || args["author"] != "jane" {
		t.Errorf("unexpected args: %v", args)
	}
}

func TestParseToolArgs_ArrayParameter(t *testing.T) {
	provider := NewMCPServerProvider("memory", nil)
	provider.AddTool("add_observations", "Add observations", json.RawMessage(`{
		"type": "object",
		"properties": {
			"observations": {
				"type": "array",
				"items": {
					"type": "object",
					"properties": {
						"entityName": {"type": "string"},
						"contents": {"type": "array", "items": {"type": "string"}}
					}
				}
			}
		},
		"required": ["observations"]
	}`))

	tool := provider.tools["add_observations"]

	// JSON array - should be wrapped into "observations"
	args, err := provider.parseToolArgs(tool, []string{`[{"entityName": "foo", "contents": ["bar"]}]`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obs, ok := args["observations"].([]any)
	if !ok {
		t.Fatalf("expected observations to be array, got %T", args["observations"])
	}
	if len(obs) != 1 {
		t.Errorf("expected 1 observation, got %d", len(obs))
	}

	// Full JSON object
	args, err = provider.parseToolArgs(tool, []string{`{"observations": [{"entityName": "test", "contents": ["data"]}]}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obs, ok = args["observations"].([]any)
	if !ok {
		t.Fatalf("expected observations to be array, got %T", args["observations"])
	}
	if len(obs) != 1 {
		t.Errorf("expected 1 observation, got %d", len(obs))
	}
}

func TestParseToolArgs_ObjectParameter(t *testing.T) {
	provider := NewMCPServerProvider("api", nil)
	provider.AddTool("request", "Make API request", json.RawMessage(`{
		"type": "object",
		"properties": {
			"config": {
				"type": "object",
				"properties": {
					"url": {"type": "string"},
					"method": {"type": "string"}
				}
			}
		},
		"required": ["config"]
	}`))

	tool := provider.tools["request"]

	// JSON object - should be wrapped into "config"
	args, err := provider.parseToolArgs(tool, []string{`{"url": "http://example.com", "method": "GET"}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	config, ok := args["config"].(map[string]any)
	if !ok {
		t.Fatalf("expected config to be object, got %T", args["config"])
	}
	if config["url"] != "http://example.com" {
		t.Errorf("expected url, got %v", config["url"])
	}

	// Full JSON with key
	args, err = provider.parseToolArgs(tool, []string{`{"config": {"url": "http://test.com"}}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	config, ok = args["config"].(map[string]any)
	if !ok {
		t.Fatalf("expected config to be object, got %T", args["config"])
	}
	if config["url"] != "http://test.com" {
		t.Errorf("expected url, got %v", config["url"])
	}
}

func TestParseToolArgs_IntegerAndBoolean(t *testing.T) {
	provider := NewMCPServerProvider("search", nil)
	provider.AddTool("find", "Search for items", json.RawMessage(`{
		"type": "object",
		"properties": {
			"query": {"type": "string"},
			"limit": {"type": "integer"},
			"recursive": {"type": "boolean"}
		},
		"required": ["query"]
	}`))

	tool := provider.tools["find"]

	// Mixed args
	args, err := provider.parseToolArgs(tool, []string{"test", "--limit=10", "--recursive=true"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["query"] != "test" {
		t.Errorf("expected 'test', got %v", args["query"])
	}
	if args["limit"] != 10 {
		t.Errorf("expected 10, got %v (type %T)", args["limit"], args["limit"])
	}
	if args["recursive"] != true {
		t.Errorf("expected true, got %v", args["recursive"])
	}

	// JSON
	args, err = provider.parseToolArgs(tool, []string{`{"query": "foo", "limit": 5, "recursive": false}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// JSON numbers come back as float64
	if limit, ok := args["limit"].(float64); !ok || limit != 5 {
		t.Errorf("expected 5, got %v", args["limit"])
	}
}

func TestParseToolArgs_NoSchema(t *testing.T) {
	provider := NewMCPServerProvider("custom", nil)
	provider.AddTool("run", "Run command", json.RawMessage(`{}`))

	tool := provider.tools["run"]

	// Should still accept JSON
	args, err := provider.parseToolArgs(tool, []string{`{"foo": "bar"}`})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if args["foo"] != "bar" {
		t.Errorf("expected 'bar', got %v", args["foo"])
	}
}

func TestParseToolArgs_FilesystemServer(t *testing.T) {
	// Real-world example: @modelcontextprotocol/server-filesystem
	provider := NewMCPServerProvider("filesystem", nil)
	provider.AddTool("read_file", "Read file contents", json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {"type": "string", "description": "Path to the file"}
		},
		"required": ["path"]
	}`))
	provider.AddTool("write_file", "Write to file", json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {"type": "string"},
			"content": {"type": "string"}
		},
		"required": ["path", "content"]
	}`))
	provider.AddTool("list_directory", "List directory", json.RawMessage(`{
		"type": "object",
		"properties": {
			"path": {"type": "string"}
		},
		"required": ["path"]
	}`))

	// read_file with positional
	args, _ := provider.parseToolArgs(provider.tools["read_file"], []string{"/tmp/test.txt"})
	if args["path"] != "/tmp/test.txt" {
		t.Errorf("expected path, got %v", args["path"])
	}

	// write_file with two positional
	args, _ = provider.parseToolArgs(provider.tools["write_file"], []string{"/tmp/out.txt", "hello world"})
	if args["path"] != "/tmp/out.txt" || args["content"] != "hello world" {
		t.Errorf("unexpected args: %v", args)
	}

	// list_directory with JSON
	args, _ = provider.parseToolArgs(provider.tools["list_directory"], []string{`{"path": "/home"}`})
	if args["path"] != "/home" {
		t.Errorf("expected path, got %v", args["path"])
	}
}

func TestParseToolArgs_GitHubServer(t *testing.T) {
	// Real-world example: @modelcontextprotocol/server-github
	provider := NewMCPServerProvider("github", nil)
	provider.AddTool("create_issue", "Create GitHub issue", json.RawMessage(`{
		"type": "object",
		"properties": {
			"owner": {"type": "string"},
			"repo": {"type": "string"},
			"title": {"type": "string"},
			"body": {"type": "string"},
			"labels": {"type": "array", "items": {"type": "string"}}
		},
		"required": ["owner", "repo", "title"]
	}`))

	tool := provider.tools["create_issue"]

	// Positional + flags
	args, _ := provider.parseToolArgs(tool, []string{"myorg", "myrepo", "Bug report", "--body=Details here"})
	if args["owner"] != "myorg" || args["repo"] != "myrepo" || args["title"] != "Bug report" {
		t.Errorf("unexpected args: %v", args)
	}
	if args["body"] != "Details here" {
		t.Errorf("expected body, got %v", args["body"])
	}

	// Full JSON with array
	args, _ = provider.parseToolArgs(tool, []string{`{"owner": "org", "repo": "repo", "title": "Test", "labels": ["bug", "urgent"]}`})
	labels, ok := args["labels"].([]any)
	if !ok || len(labels) != 2 {
		t.Errorf("expected labels array, got %v", args["labels"])
	}
}

func TestParseToolArgs_BraveSearchServer(t *testing.T) {
	// Real-world example: @modelcontextprotocol/server-brave-search
	provider := NewMCPServerProvider("brave", nil)
	provider.AddTool("brave_web_search", "Search the web", json.RawMessage(`{
		"type": "object",
		"properties": {
			"query": {"type": "string", "description": "Search query"},
			"count": {"type": "integer", "description": "Number of results"}
		},
		"required": ["query"]
	}`))

	tool := provider.tools["brave_web_search"]

	args, _ := provider.parseToolArgs(tool, []string{"golang tutorials", "--count=20"})
	if args["query"] != "golang tutorials" {
		t.Errorf("expected query, got %v", args["query"])
	}
	if args["count"] != 20 {
		t.Errorf("expected 20, got %v", args["count"])
	}
}

func TestToolHelp_SimpleString(t *testing.T) {
	provider := NewMCPServerProvider("test", nil)
	provider.AddTool("echo", "Echo a message", json.RawMessage(`{
		"type": "object",
		"properties": {
			"message": {"type": "string", "description": "The message to echo"}
		},
		"required": ["message"]
	}`))

	help := provider.toolHelp(provider.tools["echo"])

	// Should have positional syntax for simple string
	if !bytes.Contains([]byte(help), []byte("<message>")) {
		t.Errorf("help should show <message>, got:\n%s", help)
	}
	if !bytes.Contains([]byte(help), []byte("string, required")) {
		t.Errorf("help should show type info, got:\n%s", help)
	}
}

func TestToolHelp_ArrayParameter(t *testing.T) {
	provider := NewMCPServerProvider("memory", nil)
	provider.AddTool("add_observations", "Add observations", json.RawMessage(`{
		"type": "object",
		"properties": {
			"observations": {
				"type": "array",
				"description": "List of observations to add"
			}
		},
		"required": ["observations"]
	}`))

	help := provider.toolHelp(provider.tools["add_observations"])

	// Should recommend JSON format for array
	if !bytes.Contains([]byte(help), []byte("<json>")) {
		t.Errorf("help should show <json> for array param, got:\n%s", help)
	}
	if !bytes.Contains([]byte(help), []byte("array, required")) {
		t.Errorf("help should show array type, got:\n%s", help)
	}
	// Should have JSON example
	if !bytes.Contains([]byte(help), []byte(`"observations"`)) {
		t.Errorf("help should show JSON example with observations key, got:\n%s", help)
	}
}

func TestToolHelp_MixedParameters(t *testing.T) {
	provider := NewMCPServerProvider("api", nil)
	provider.AddTool("request", "Make request", json.RawMessage(`{
		"type": "object",
		"properties": {
			"url": {"type": "string", "description": "Target URL"},
			"method": {"type": "string", "description": "HTTP method"},
			"headers": {"type": "object", "description": "Request headers"},
			"timeout": {"type": "integer", "description": "Timeout in seconds"}
		},
		"required": ["url"]
	}`))

	help := provider.toolHelp(provider.tools["request"])

	// Should show url as required
	if !bytes.Contains([]byte(help), []byte("url (string, required)")) {
		t.Errorf("help should show url as required, got:\n%s", help)
	}
	// Should show optional params
	if !bytes.Contains([]byte(help), []byte("optional")) {
		t.Errorf("help should show optional params, got:\n%s", help)
	}
}

func TestExecuteWithContext_EndToEnd(t *testing.T) {
	exec := &mockExecutor{
		result: MCPToolResult{
			Content: []MCPContent{{Type: "text", Text: "success"}},
		},
	}
	provider := NewMCPServerProvider("test", exec)
	provider.AddTool("greet", "Greet user", json.RawMessage(`{
		"type": "object",
		"properties": {
			"name": {"type": "string"}
		},
		"required": ["name"]
	}`))

	var stdout, stderr bytes.Buffer
	err := provider.ExecuteWithContext(context.Background(), nil, []string{"greet", "Alice"}, &stdout, &stderr)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exec.lastTool != "greet" {
		t.Errorf("expected tool 'greet', got %s", exec.lastTool)
	}
	if exec.lastArgs["name"] != "Alice" {
		t.Errorf("expected name 'Alice', got %v", exec.lastArgs["name"])
	}
	if !bytes.Contains(stdout.Bytes(), []byte("success")) {
		t.Errorf("expected success output, got %s", stdout.String())
	}
}

func TestExecuteWithContext_Help(t *testing.T) {
	provider := NewMCPServerProvider("test", nil)
	provider.AddTool("foo", "Do foo", json.RawMessage(`{}`))

	var stdout, stderr bytes.Buffer

	// Server help
	provider.ExecuteWithContext(context.Background(), nil, []string{"--help"}, &stdout, &stderr)
	if !bytes.Contains(stdout.Bytes(), []byte("COMMANDS")) {
		t.Errorf("expected server help, got %s", stdout.String())
	}

	// Tool help
	stdout.Reset()
	provider.ExecuteWithContext(context.Background(), nil, []string{"foo", "--help"}, &stdout, &stderr)
	if !bytes.Contains(stdout.Bytes(), []byte("NAME")) {
		t.Errorf("expected tool help, got %s", stdout.String())
	}
}

func TestExecuteWithContext_UnknownCommand(t *testing.T) {
	provider := NewMCPServerProvider("test", nil)
	provider.AddTool("foo", "Do foo", json.RawMessage(`{}`))

	var stdout, stderr bytes.Buffer
	err := provider.ExecuteWithContext(context.Background(), nil, []string{"bar"}, &stdout, &stderr)

	if err == nil {
		t.Error("expected error for unknown command")
	}
	if !bytes.Contains(stderr.Bytes(), []byte("Unknown command")) {
		t.Errorf("expected unknown command error, got %s", stderr.String())
	}
}
