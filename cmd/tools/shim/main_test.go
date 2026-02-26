package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigUsesExplicitConfigPathFirst(t *testing.T) {
	explicitConfigPath := writeConfigFile(t, Config{
		GatewayAddr: "explicit.gateway.internal:1993",
		Token:       "explicit-token",
	})

	mountRoot := t.TempDir()
	shimPath := filepath.Join(mountRoot, "tools", "wikipedia")
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		t.Fatalf("mkdir tools dir: %v", err)
	}
	if err := os.WriteFile(shimPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write shim argv path: %v", err)
	}
	_ = writeMountConfig(t, mountRoot, Config{
		GatewayAddr: "argv.gateway.internal:1993",
		Token:       "argv-token",
	})

	setArgv(t, shimPath)
	t.Setenv(configPathEnvKey, explicitConfigPath)
	t.Setenv(gatewayEnvKey, "")
	t.Setenv(tokenEnvKey, "")

	cfg := loadConfig()
	if cfg.GatewayAddr != "explicit.gateway.internal:1993" {
		t.Fatalf("expected explicit config gateway, got %q", cfg.GatewayAddr)
	}
	if cfg.Token != "explicit-token" {
		t.Fatalf("expected explicit config token, got %q", cfg.Token)
	}
}

func TestLoadConfigFallsBackToShimRelativeConfig(t *testing.T) {
	mountRoot := t.TempDir()
	shimPath := filepath.Join(mountRoot, "tools", "wikipedia")
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		t.Fatalf("mkdir tools dir: %v", err)
	}
	if err := os.WriteFile(shimPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write shim argv path: %v", err)
	}
	_ = writeMountConfig(t, mountRoot, Config{
		GatewayAddr: "argv.gateway.internal:1993",
		Token:       "argv-token",
	})

	setArgv(t, shimPath)
	t.Setenv(configPathEnvKey, "")
	t.Setenv(gatewayEnvKey, "")
	t.Setenv(tokenEnvKey, "")

	cfg := loadConfig()
	if cfg.GatewayAddr != "argv.gateway.internal:1993" {
		t.Fatalf("expected argv-relative config gateway, got %q", cfg.GatewayAddr)
	}
	if cfg.Token != "argv-token" {
		t.Fatalf("expected argv-relative config token, got %q", cfg.Token)
	}
}

func TestLoadConfigEnvOverridesAppliedLast(t *testing.T) {
	configPath := writeConfigFile(t, Config{
		GatewayAddr: "file.gateway.internal:1993",
		Token:       "file-token",
	})

	setArgv(t, filepath.Join(t.TempDir(), "tools", "wikipedia"))
	t.Setenv(configPathEnvKey, configPath)
	t.Setenv(gatewayEnvKey, "env.gateway.internal:1993")
	t.Setenv(tokenEnvKey, "env-token")

	cfg := loadConfig()
	if cfg.GatewayAddr != "env.gateway.internal:1993" {
		t.Fatalf("expected env gateway override, got %q", cfg.GatewayAddr)
	}
	if cfg.Token != "env-token" {
		t.Fatalf("expected env token override, got %q", cfg.Token)
	}
}

func TestLoadConfigInvalidExplicitPathFallsBackToShimRelativeConfig(t *testing.T) {
	mountRoot := t.TempDir()
	shimPath := filepath.Join(mountRoot, "tools", "wikipedia")
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		t.Fatalf("mkdir tools dir: %v", err)
	}
	if err := os.WriteFile(shimPath, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatalf("write shim argv path: %v", err)
	}
	_ = writeMountConfig(t, mountRoot, Config{
		GatewayAddr: "argv.gateway.internal:1993",
		Token:       "argv-token",
	})

	setArgv(t, shimPath)
	t.Setenv(configPathEnvKey, filepath.Join(t.TempDir(), "missing-config.json"))
	t.Setenv(gatewayEnvKey, "")
	t.Setenv(tokenEnvKey, "")

	cfg := loadConfig()
	if cfg.GatewayAddr != "argv.gateway.internal:1993" {
		t.Fatalf("expected fallback to argv-relative config, got %q", cfg.GatewayAddr)
	}
	if cfg.Token != "argv-token" {
		t.Fatalf("expected fallback token from argv-relative config, got %q", cfg.Token)
	}
}

func setArgv(t *testing.T, argv0 string) {
	t.Helper()
	old := os.Args
	os.Args = []string{argv0}
	t.Cleanup(func() { os.Args = old })
}

func writeMountConfig(t *testing.T, mountRoot string, cfg Config) string {
	t.Helper()
	configDir := filepath.Join(mountRoot, ".airstore")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	configPath := filepath.Join(configDir, "config")
	writeConfig(t, configPath, cfg)
	return configPath
}

func writeConfigFile(t *testing.T, cfg Config) string {
	t.Helper()
	configPath := filepath.Join(t.TempDir(), "config.json")
	writeConfig(t, configPath, cfg)
	return configPath
}

func writeConfig(t *testing.T, path string, cfg Config) {
	t.Helper()
	encoded, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

