package vnode

import (
	"io/fs"
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

	if info.Size != int64(len(shimData)) {
		t.Errorf("expected size %d, got %d", len(shimData), info.Size)
	}

	// Should be executable
	if info.Mode&0111 == 0 {
		t.Error("expected executable mode")
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

	// Read from start
	buf := make([]byte, 5)
	n, err := tv.Read("/tools/tool1", buf, 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 5 {
		t.Errorf("expected 5 bytes, got %d", n)
	}
	if string(buf) != "hello" {
		t.Errorf("expected 'hello', got %q", buf)
	}

	// Read with offset
	n, err = tv.Read("/tools/tool1", buf, 6, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(buf[:n]) != "world" {
		t.Errorf("expected 'world', got %q", buf[:n])
	}

	// Read past end
	n, err = tv.Read("/tools/tool1", buf, int64(len(shimData)+10), 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes past end, got %d", n)
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
