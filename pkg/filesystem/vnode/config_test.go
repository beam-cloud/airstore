package vnode

import (
	"encoding/json"
	"io/fs"
	"syscall"
	"testing"
)

func TestConfigVNodeReaddirIncludesConfigAndToolShim(t *testing.T) {
	vn := NewConfigVNode("gateway.internal:1993", "token-123", []byte("shim-bytes"))

	entries, err := vn.Readdir(ConfigDir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	found := map[string]bool{}
	for _, e := range entries {
		found[e.Name] = true
		if e.Mode&syscall.S_IFREG == 0 {
			t.Fatalf("expected regular file mode for %s", e.Name)
		}
	}
	if !found["config"] {
		t.Fatalf("missing config entry")
	}
	if !found["tool-shim"] {
		t.Fatalf("missing tool-shim entry")
	}
}

func TestConfigVNodeReadConfigAndToolShim(t *testing.T) {
	shimData := []byte("shim-bytes")
	vn := NewConfigVNode("gateway.internal:1993", "token-123", shimData)

	configBuf := make([]byte, 4096)
	n, err := vn.Read(ConfigFile, configBuf, 0, 0)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(configBuf[:n], &cfg); err != nil {
		t.Fatalf("decode config json: %v", err)
	}
	if cfg.GatewayAddr != "gateway.internal:1993" {
		t.Fatalf("unexpected gateway addr: %q", cfg.GatewayAddr)
	}
	if cfg.Token != "token-123" {
		t.Fatalf("unexpected token: %q", cfg.Token)
	}

	shimBuf := make([]byte, len(shimData))
	n, err = vn.Read(ConfigToolShim, shimBuf, 0, 0)
	if err != nil {
		t.Fatalf("read tool shim: %v", err)
	}
	if n != len(shimData) {
		t.Fatalf("expected %d shim bytes, got %d", len(shimData), n)
	}
	if string(shimBuf) != string(shimData) {
		t.Fatalf("tool shim bytes mismatch")
	}
}

func TestConfigVNodeReadNotFound(t *testing.T) {
	vn := NewConfigVNode("gateway.internal:1993", "", nil)
	buf := make([]byte, 16)
	if _, err := vn.Read("/.airstore/missing", buf, 0, 0); err != fs.ErrNotExist {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}
}

