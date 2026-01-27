package vnode

import (
	"context"
	"io/fs"
	"strings"
	"syscall"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ToolsVNode implements VirtualNode for the /tools directory.
// It serves tool binaries directly via FUSE.
type ToolsVNode struct {
	ReadOnlyBase // Embeds read-only defaults for write operations

	gatewayAddr string
	shim        []byte          // shim binary served via FUSE
	modTime     time.Time       // stable timestamps for getattr
	tools       []string        // immutable after construction - no locks needed
	toolSet     map[string]bool
}

// NewToolsVNode creates a new ToolsVNode.
func NewToolsVNode(gatewayAddr string, shimBinary []byte) *ToolsVNode {
	modTime := time.Now()

	t := &ToolsVNode{
		gatewayAddr: gatewayAddr,
		shim:        shimBinary,
		modTime:     modTime,
		tools:       []string{},
		toolSet:     make(map[string]bool),
	}

	// Fetch tools synchronously at startup - cache is immutable after this
	if tools := t.fetchTools(); tools != nil {
		t.tools = tools
		for _, name := range tools {
			t.toolSet[name] = true
		}
	}

	return t
}

// Prefix returns the path prefix this node handles
func (t *ToolsVNode) Prefix() string {
	return ToolsPath
}

// Getattr returns file attributes for paths under /tools
func (t *ToolsVNode) Getattr(path string) (*FileInfo, error) {
	if path == ToolsPath {
		info := NewDirInfo(toolsIno())
		info.Atime = t.modTime
		info.Mtime = t.modTime
		info.Ctime = t.modTime
		return info, nil
	}

	name := strings.TrimPrefix(path, ToolsPathPrefix)
	if name == "" || strings.Contains(name, "/") {
		return nil, fs.ErrNotExist
	}

	if !t.hasTool(name) {
		return nil, fs.ErrNotExist
	}

	info := NewExecFileInfo(toolIno(name), int64(len(t.shim)))
	info.Atime = t.modTime
	info.Mtime = t.modTime
	info.Ctime = t.modTime
	return info, nil
}

// Readdir returns entries in /tools directory
func (t *ToolsVNode) Readdir(path string) ([]DirEntry, error) {
	if path != ToolsPath {
		return nil, syscall.ENOTDIR
	}

	tools := t.getTools()
	entries := make([]DirEntry, 0, len(tools))
	for _, name := range tools {
		entries = append(entries, DirEntry{
			Name: name,
			Mode: syscall.S_IFREG | 0755,
			Ino:  toolIno(name),
		})
	}

	return entries, nil
}

// Open opens a tool file
func (t *ToolsVNode) Open(path string, flags int) (FileHandle, error) {
	if path == ToolsPath {
		return 0, syscall.EISDIR
	}

	name := strings.TrimPrefix(path, ToolsPathPrefix)
	if !t.hasTool(name) {
		return 0, fs.ErrNotExist
	}

	return 0, nil
}

// Read reads bytes from a tool binary (served from memory)
func (t *ToolsVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	name := strings.TrimPrefix(path, ToolsPathPrefix)
	if !t.hasTool(name) {
		return 0, fs.ErrNotExist
	}
	if off >= int64(len(t.shim)) {
		return 0, nil
	}
	return copy(buf, t.shim[off:]), nil
}

// hasTool checks if a tool is registered.
// Lock-free O(1) lookup - toolSet is immutable after construction.
func (t *ToolsVNode) hasTool(name string) bool {
	return t.toolSet[name]
}

// getTools returns the list of registered tools.
// Lock-free - tools slice is immutable after construction.
func (t *ToolsVNode) getTools() []string {
	return t.tools
}

// fetchTools queries the gateway for registered tools.
// Only called at startup during construction.
func (t *ToolsVNode) fetchTools() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(
		t.gatewayAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil
	}
	defer conn.Close()

	client := pb.NewToolServiceClient(conn)
	resp, err := client.ListTools(ctx, &pb.ListToolsRequest{})
	if err != nil {
		return nil
	}

	if !resp.Ok {
		return nil
	}

	tools := make([]string, len(resp.Tools))
	for i, tool := range resp.Tools {
		tools[i] = tool.Name
	}
	return tools
}

func toolsIno() uint64 {
	return PathIno(ToolsPath)
}

func toolIno(name string) uint64 {
	return PathIno(ToolsPathPrefix + name)
}
