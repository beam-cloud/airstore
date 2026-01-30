package vnode

import (
	"context"
	"io/fs"
	"strings"
	"sync"
	"syscall"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const (
	// ToolsCacheTTL is the time-to-live for the tools cache
	ToolsCacheTTL = 5 * time.Second
)

// ToolsVNode implements VirtualNode for the /tools directory.
// It serves tool binaries directly via FUSE.
// Tools are cached with a TTL to allow dynamic updates without remount.
type ToolsVNode struct {
	ReadOnlyBase // Embeds read-only defaults for write operations

	gatewayAddr string
	token       string          // Auth token for gRPC calls
	shim        []byte          // shim binary served via FUSE
	modTime     time.Time       // stable timestamps for getattr

	// Cache with TTL
	mu           sync.RWMutex
	tools        []string
	toolSet      map[string]bool
	lastFetch    time.Time
	cacheModTime time.Time // Updated when tool set changes
}

// NewToolsVNode creates a new ToolsVNode.
func NewToolsVNode(gatewayAddr string, token string, shimBinary []byte) *ToolsVNode {
	modTime := time.Now()

	t := &ToolsVNode{
		gatewayAddr:  gatewayAddr,
		token:        token,
		shim:         shimBinary,
		modTime:      modTime,
		tools:        []string{},
		toolSet:      make(map[string]bool),
		cacheModTime: modTime,
	}

	// Initial fetch - non-blocking if it fails
	t.refreshCache()

	return t
}

// Prefix returns the path prefix this node handles
func (t *ToolsVNode) Prefix() string {
	return ToolsPath
}

// Getattr returns file attributes for paths under /tools
func (t *ToolsVNode) Getattr(path string) (*FileInfo, error) {
	if path == ToolsPath {
		t.maybeRefresh()
		info := NewDirInfo(toolsIno())
		mtime := t.getCacheModTime()
		info.Atime = mtime
		info.Mtime = mtime
		info.Ctime = mtime
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
	mtime := t.getCacheModTime()
	info.Atime = mtime
	info.Mtime = mtime
	info.Ctime = mtime
	return info, nil
}

// Readdir returns entries in /tools directory
func (t *ToolsVNode) Readdir(path string) ([]DirEntry, error) {
	if path != ToolsPath {
		return nil, syscall.ENOTDIR
	}

	t.maybeRefresh()
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
func (t *ToolsVNode) hasTool(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.toolSet[name]
}

// getTools returns the list of registered tools.
func (t *ToolsVNode) getTools() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]string, len(t.tools))
	copy(result, t.tools)
	return result
}

// getCacheModTime returns the cache modification time
func (t *ToolsVNode) getCacheModTime() time.Time {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.cacheModTime
}

// maybeRefresh refreshes the cache if TTL has expired
func (t *ToolsVNode) maybeRefresh() {
	t.mu.RLock()
	needsRefresh := time.Since(t.lastFetch) > ToolsCacheTTL
	t.mu.RUnlock()

	if needsRefresh {
		go t.refreshCache()
	}
}

// refreshCache fetches the latest tools from the gateway
func (t *ToolsVNode) refreshCache() {
	tools := t.fetchTools()
	if tools == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if the tool set has changed
	changed := len(tools) != len(t.tools)
	if !changed {
		newSet := make(map[string]bool, len(tools))
		for _, name := range tools {
			newSet[name] = true
		}
		for _, name := range t.tools {
			if !newSet[name] {
				changed = true
				break
			}
		}
	}

	// Update cache
	t.tools = tools
	t.toolSet = make(map[string]bool, len(tools))
	for _, name := range tools {
		t.toolSet[name] = true
	}
	t.lastFetch = time.Now()

	// Update modTime if tools changed (helps OS notice changes)
	if changed {
		t.cacheModTime = time.Now()
	}
}

// fetchTools queries the gateway for registered tools.
func (t *ToolsVNode) fetchTools() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add auth token if available
	if t.token != "" {
		md := metadata.Pairs("authorization", "Bearer "+t.token)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

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
