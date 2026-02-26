package vnode

import (
	"bytes"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestToolsVNode_Getattr_Dir(t *testing.T) {
	tv := &ToolsVNode{
		shim:    []byte("test binary"),
		tools:   []string{"tool1", "tool2"},
		toolSet: map[string]bool{"tool1": true, "tool2": true},
	}

	info, err := tv.Getattr(ToolsPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Mode&syscall.S_IFDIR == 0 {
		t.Error("expected directory mode")
	}
}

func TestToolsVNode_Getattr_Tool(t *testing.T) {
	shimData := []byte("test binary data")
	tv := &ToolsVNode{
		shim:    shimData,
		tools:   []string{"mytool"},
		toolSet: map[string]bool{"mytool": true},
	}

	info, err := tv.Getattr("/tools/mytool")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Size != int64(len(gatewayToolWrapper)) {
		t.Errorf("expected wrapper size %d, got %d", len(gatewayToolWrapper), info.Size)
	}

	// Should be executable
	if info.Mode&0111 == 0 {
		t.Error("expected executable mode")
	}
}

func TestToolsVNode_Getattr_SharedShim(t *testing.T) {
	shimData := []byte("test binary data")
	tv := &ToolsVNode{
		shim:    shimData,
		tools:   []string{"mytool"},
		toolSet: map[string]bool{"mytool": true},
	}

	info, err := tv.Getattr(toolsShimPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Size != int64(len(shimData)) {
		t.Errorf("expected size %d, got %d", len(shimData), info.Size)
	}
}

func TestToolsVNode_Getattr_NotFound(t *testing.T) {
	tv := &ToolsVNode{
		shim:    []byte("test"),
		tools:   []string{},
		toolSet: map[string]bool{},
	}

	_, err := tv.Getattr("/tools/nonexistent")
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestToolsVNode_Readdir(t *testing.T) {
	tv := &ToolsVNode{
		shim:    []byte("test"),
		tools:   []string{"tool1", "tool2", "tool3"},
		toolSet: map[string]bool{"tool1": true, "tool2": true, "tool3": true},
	}

	entries, err := tv.Readdir(ToolsPath)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(entries) != 3 {
		t.Errorf("expected 3 entries, got %d", len(entries))
	}

	// Verify entry names
	names := make(map[string]bool)
	for _, e := range entries {
		names[e.Name] = true
		// Each should be a regular file
		if e.Mode&syscall.S_IFREG == 0 {
			t.Errorf("expected regular file mode for %s", e.Name)
		}
	}

	for _, expected := range []string{"tool1", "tool2", "tool3"} {
		if !names[expected] {
			t.Errorf("missing expected tool: %s", expected)
		}
	}
}

func TestToolsVNode_Readdir_NotDir(t *testing.T) {
	tv := &ToolsVNode{
		shim:    []byte("test"),
		tools:   []string{"tool1"},
		toolSet: map[string]bool{"tool1": true},
	}

	_, err := tv.Readdir("/tools/tool1")
	if err != syscall.ENOTDIR {
		t.Errorf("expected ENOTDIR, got %v", err)
	}
}

func TestToolsVNode_Open(t *testing.T) {
	tv := &ToolsVNode{
		shim:    []byte("test"),
		tools:   []string{"tool1"},
		toolSet: map[string]bool{"tool1": true},
	}

	// Opening a tool should succeed
	_, err := tv.Open("/tools/tool1", 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// Opening the directory should fail with EISDIR
	_, err = tv.Open(ToolsPath, 0)
	if err != syscall.EISDIR {
		t.Errorf("expected EISDIR, got %v", err)
	}

	// Opening nonexistent tool should fail
	_, err = tv.Open("/tools/nonexistent", 0)
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestToolsVNode_Read(t *testing.T) {
	shimData := []byte("hello world shim binary")
	tv := &ToolsVNode{
		shim:    shimData,
		tools:   []string{"tool1"},
		toolSet: map[string]bool{"tool1": true},
	}

	// Gateway tools serve a wrapper script.
	buf := make([]byte, 5)
	n, err := tv.Read("/tools/tool1", buf, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes, got %d", n)
	}
	if string(buf) != "#!/bi" {
		t.Errorf("expected wrapper prefix, got %q", buf)
	}

	// Read with offset into wrapper body.
	n, err = tv.Read("/tools/tool1", buf, 6, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(buf[:n]) != "/sh\ns" {
		t.Errorf("expected wrapper bytes, got %q", buf[:n])
	}

	// Read past end
	n, err = tv.Read("/tools/tool1", buf, int64(len(tv.toolBinary("tool1"))+10), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes past end, got %d", n)
	}
}

func TestToolsVNode_Read_SharedShim(t *testing.T) {
	shimData := []byte("hello world shim binary")
	tv := &ToolsVNode{
		shim:    shimData,
		tools:   []string{"tool1"},
		toolSet: map[string]bool{"tool1": true},
	}

	buf := make([]byte, len(shimData))
	n, err := tv.Read(toolsShimPath, buf, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(shimData) {
		t.Fatalf("expected %d bytes, got %d", len(shimData), n)
	}
	if !bytes.Equal(buf[:n], shimData) {
		t.Fatalf("expected shared shim bytes to match")
	}
}

func TestGatewayToolWrapperExecutesCopiedShim(t *testing.T) {
	tv := &ToolsVNode{
		shim: []byte("#!/bin/sh\nprintf 'argv0=%s\\n' \"$0\"\nprintf 'gateway=%s\\n' \"${AIRSTORE_GATEWAY:-}\"\n"),
		tools:   []string{"wikipedia"},
		toolSet: map[string]bool{"wikipedia": true},
	}

	mountRoot := t.TempDir()
	toolsDir := filepath.Join(mountRoot, "tools")
	if err := os.MkdirAll(toolsDir, 0o755); err != nil {
		t.Fatalf("mkdir tools: %v", err)
	}

	wrapperPath := filepath.Join(toolsDir, "wikipedia")
	if err := os.WriteFile(wrapperPath, tv.toolBinary("wikipedia"), 0o755); err != nil {
		t.Fatalf("write wrapper: %v", err)
	}
	shimPath := filepath.Join(toolsDir, toolsShimName)
	if err := os.WriteFile(shimPath, tv.shim, 0o755); err != nil {
		t.Fatalf("write shim: %v", err)
	}

	tmpDir := t.TempDir()
	cmd := exec.Command(wrapperPath, "--help")
	cmd.Env = append(os.Environ(),
		"TMPDIR="+tmpDir,
		"GATEWAY_ADDR=gateway.test.internal:1993",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("wrapper execution failed: %v output=%s", err, string(out))
	}

	output := string(out)
	if !strings.Contains(output, "gateway=gateway.test.internal:1993") {
		t.Fatalf("expected wrapper to map GATEWAY_ADDR to AIRSTORE_GATEWAY, got: %s", output)
	}
	if !strings.Contains(output, ".airstore-shims/wikipedia") {
		t.Fatalf("expected wrapper to exec via tool-named symlink, got: %s", output)
	}
}

func TestToolsVNode_Read_NotFound(t *testing.T) {
	tv := &ToolsVNode{
		shim:    []byte("test"),
		tools:   []string{},
		toolSet: map[string]bool{},
	}

	buf := make([]byte, 10)
	_, err := tv.Read("/tools/nonexistent", buf, 0, 0)
	if err != fs.ErrNotExist {
		t.Errorf("expected ErrNotExist, got %v", err)
	}
}

func TestToolsVNode_Prefix(t *testing.T) {
	tv := &ToolsVNode{}
	if tv.Prefix() != ToolsPath {
		t.Errorf("expected prefix %q, got %q", ToolsPath, tv.Prefix())
	}
}

func TestSameWrapperBytes(t *testing.T) {
	if !sameWrapperBytes(map[string][]byte{}, map[string][]byte{}) {
		t.Fatal("expected empty wrapper maps to match")
	}

	if !sameWrapperBytes(
		map[string][]byte{"tool": []byte("#!/bin/sh\nexec foo \"$@\"\n")},
		map[string][]byte{"tool": []byte("#!/bin/sh\nexec foo \"$@\"\n")},
	) {
		t.Fatal("expected identical wrappers to match")
	}

	if sameWrapperBytes(
		map[string][]byte{"tool": []byte("#!/bin/sh\nexec foo \"$@\"\n")},
		map[string][]byte{"tool": []byte("#!/bin/sh\nexec bar \"$@\"\n")},
	) {
		t.Fatal("expected wrapper-content change to be detected")
	}

	if sameWrapperBytes(
		map[string][]byte{"tool": []byte("#!/bin/sh\nexec foo \"$@\"\n")},
		map[string][]byte{},
	) {
		t.Fatal("expected missing wrapper key to be detected")
	}
}

func TestToolNameFromPath(t *testing.T) {
	tests := []struct {
		path string
		want string
		ok   bool
	}{
		{path: "/tools/wikipedia", want: "wikipedia", ok: true},
		{path: "/tools/.airstore-shim", want: ".airstore-shim", ok: true},
		{path: "/tools", want: "", ok: false},
		{path: "/tools/", want: "", ok: false},
		{path: "/tools/a/b", want: "", ok: false},
		{path: "/source/wikipedia", want: "", ok: false},
	}

	for _, tc := range tests {
		got, ok := toolNameFromPath(tc.path)
		if ok != tc.ok || got != tc.want {
			t.Fatalf("toolNameFromPath(%q) = (%q, %v), want (%q, %v)", tc.path, got, ok, tc.want, tc.ok)
		}
	}
}
