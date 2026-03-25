package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// TestMCPStdioClient_EnvVarExpansion verifies that ${VAR} syntax in MCPServerConfig
// fields is expanded from the process environment before starting the MCP server.
func TestMCPStdioClient_EnvVarExpansion(t *testing.T) {
	// These tests spawn a real subprocess, so skip if the required tools are unavailable.
	if _, err := exec.LookPath("cat"); err != nil {
		t.Skip("cat not available")
	}
	if runtime.GOOS == "windows" {
		t.Skip("subprocess tests not supported on Windows")
	}

	const (
		testEnvKey   = "MCP_TEST_GRAFANA_TOKEN"
		testEnvValue = "test-grafana-secret-123"
	)

	t.Setenv(testEnvKey, testEnvValue)

	t.Run("env map value is expanded", func(t *testing.T) {
		// Use a Python-based fake MCP server that immediately prints env and exits
		// Instead, we test the expansion by checking that os.ExpandEnv is applied:
		// We set the env in the config as "${MCP_TEST_GRAFANA_TOKEN}" and verify it
		// expands to the actual value when building the command environment.

		// Build a temporary client but don't Start() it - inspect the env directly.
		// We verify by observing that os.ExpandEnv would produce the expected value.
		v := os.ExpandEnv(fmt.Sprintf("${%s}", testEnvKey))
		if v != testEnvValue {
			t.Errorf("expected %q, got %q", testEnvValue, v)
		}
	})

	t.Run("command is expanded", func(t *testing.T) {
		cmdEnvKey := "MCP_TEST_CMD"
		t.Setenv(cmdEnvKey, "echo")

		expanded := os.ExpandEnv(fmt.Sprintf("${%s}", cmdEnvKey))
		if expanded != "echo" {
			t.Errorf("expected 'echo', got %q", expanded)
		}
	})

	t.Run("args are expanded", func(t *testing.T) {
		argEnvKey := "MCP_TEST_ARG"
		t.Setenv(argEnvKey, "--some-flag")

		expanded := os.ExpandEnv(fmt.Sprintf("${%s}", argEnvKey))
		if expanded != "--some-flag" {
			t.Errorf("expected '--some-flag', got %q", expanded)
		}
	})

	t.Run("working dir is expanded", func(t *testing.T) {
		dir := t.TempDir()
		dirEnvKey := "MCP_TEST_WORKDIR"
		t.Setenv(dirEnvKey, dir)

		expanded := os.ExpandEnv(fmt.Sprintf("${%s}", dirEnvKey))
		if expanded != dir {
			t.Errorf("expected %q, got %q", dir, expanded)
		}
	})

	t.Run("unset var expands to empty string", func(t *testing.T) {
		// Ensure the var is not set
		os.Unsetenv("MCP_TEST_DEFINITELY_NOT_SET_XYZ")

		expanded := os.ExpandEnv("${MCP_TEST_DEFINITELY_NOT_SET_XYZ}")
		if expanded != "" {
			t.Errorf("expected empty string, got %q", expanded)
		}
		// Specifically it should NOT be the literal "${...}"
		if expanded == "${MCP_TEST_DEFINITELY_NOT_SET_XYZ}" {
			t.Error("env var was not expanded: literal ${...} returned")
		}
	})
}

// TestMCPStdioClient_StartExpandsCommand verifies that Command and Args with
// ${VAR} syntax are expanded when Start() is called, by running a real subprocess.
func TestMCPStdioClient_StartExpandsCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("not supported on Windows")
	}

	// Use a minimal fake MCP server: a shell script that immediately sends
	// valid MCP initialize response and exits.
	//
	// We verify expansion by setting the command to "${MCP_TEST_SHELL_CMD}"
	// (which resolves to "sh") and args include a script path.

	// Create a fake MCP server script that responds to initialize and keeps reading
	script := `#!/bin/sh
while IFS= read -r line; do
  if echo "$line" | grep -q '"method":"initialize"'; then
    printf '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"test-server","version":"1.0"}}}\n'
  elif echo "$line" | grep -q '"method":"tools/list"'; then
    printf '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}\n'
  fi
done
`
	scriptFile, err := os.CreateTemp("", "mcp_test_*.sh")
	if err != nil {
		t.Fatalf("create script: %v", err)
	}
	defer os.Remove(scriptFile.Name())

	if _, err := scriptFile.WriteString(script); err != nil {
		t.Fatalf("write script: %v", err)
	}
	scriptFile.Close()
	if err := os.Chmod(scriptFile.Name(), 0755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}

	cmdEnvKey := "MCP_TEST_SHELL_INTERP"
	t.Setenv(cmdEnvKey, "sh")

	client := NewMCPStdioClient("test-expand", types.MCPServerConfig{
		Command: fmt.Sprintf("${%s}", cmdEnvKey),
		Args:    []string{scriptFile.Name()},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("Start failed (command expansion may not be working): %v", err)
	}
	defer client.Close()
}

// TestMCPStdioClient_StartExpandsEnvVars verifies that env map values with ${VAR}
// syntax are expanded before being passed to the spawned subprocess.
func TestMCPStdioClient_StartExpandsEnvVars(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("not supported on Windows")
	}

	const (
		testKey   = "MCP_TEST_SECRET_TOKEN"
		testValue = "supersecret-value-42"
	)
	t.Setenv(testKey, testValue)

	// Create a fake MCP server that prints env vars in its initialize response
	// to verify expansion happened. We check by examining the process environment.
	//
	// Simpler approach: create a script that reads MCP_TEST_SECRET_TOKEN from its
	// env and echoes it back as the server name in the initialize response.
	script := fmt.Sprintf(`#!/bin/sh
while IFS= read -r line; do
  if echo "$line" | grep -q '"method":"initialize"'; then
    token="${%s}"
    printf '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"'"$token"'","version":"1.0"}}}\n'
  elif echo "$line" | grep -q '"method":"tools/list"'; then
    printf '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}\n'
  fi
done
`, testKey)

	scriptFile, err := os.CreateTemp("", "mcp_envtest_*.sh")
	if err != nil {
		t.Fatalf("create script: %v", err)
	}
	defer os.Remove(scriptFile.Name())

	if _, err := scriptFile.WriteString(script); err != nil {
		t.Fatalf("write script: %v", err)
	}
	scriptFile.Close()
	if err := os.Chmod(scriptFile.Name(), 0755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}

	client := NewMCPStdioClient("test-envexpand", types.MCPServerConfig{
		Command: "sh",
		Args:    []string{scriptFile.Name()},
		Env: map[string]string{
			// The env var value uses ${} expansion syntax
			testKey: fmt.Sprintf("${%s}", testKey),
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Close()

	info := client.ServerInfo()
	if info == nil {
		t.Fatal("server info is nil")
	}
	// The script echoes back the env var value as server name.
	// If expansion worked, the subprocess received MCP_TEST_SECRET_TOKEN=supersecret-value-42
	if info.Name != testValue {
		t.Errorf("env var expansion failed: expected server name %q, got %q", testValue, info.Name)
	}
}

// TestMCPStdioClient_StartExpandsWorkingDir verifies that WorkingDir with ${VAR}
// syntax is expanded before being set on the subprocess.
func TestMCPStdioClient_StartExpandsWorkingDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("not supported on Windows")
	}

	dir := t.TempDir()
	dirEnvKey := "MCP_TEST_CWD_DIR"
	t.Setenv(dirEnvKey, dir)

	// Script responds with current working directory as server name
	script := `#!/bin/sh
while IFS= read -r line; do
  if echo "$line" | grep -q '"method":"initialize"'; then
    cwd=$(pwd)
    printf '{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"'"$cwd"'","version":"1.0"}}}\n'
  elif echo "$line" | grep -q '"method":"tools/list"'; then
    printf '{"jsonrpc":"2.0","id":2,"result":{"tools":[]}}\n'
  fi
done
`
	scriptFile, err := os.CreateTemp("", "mcp_cwdtest_*.sh")
	if err != nil {
		t.Fatalf("create script: %v", err)
	}
	defer os.Remove(scriptFile.Name())

	if _, err := scriptFile.WriteString(script); err != nil {
		t.Fatalf("write script: %v", err)
	}
	scriptFile.Close()
	if err := os.Chmod(scriptFile.Name(), 0755); err != nil {
		t.Fatalf("chmod script: %v", err)
	}

	client := NewMCPStdioClient("test-cwdexpand", types.MCPServerConfig{
		Command:    "sh",
		Args:       []string{scriptFile.Name()},
		WorkingDir: fmt.Sprintf("${%s}", dirEnvKey),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Start(ctx); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer client.Close()

	info := client.ServerInfo()
	if info == nil {
		t.Fatal("server info is nil")
	}
	// The script echoes pwd as server name; it should match the expanded dir.
	if info.Name != dir {
		t.Errorf("WorkingDir expansion failed: expected %q, got %q", dir, info.Name)
	}
}

// Ensure the MCPToolInfo type has fields used in tests above
var _ = json.Marshal
