package vnode

import (
	"context"
	"io/fs"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	// ToolsCacheTTL is the time-to-live for the tools cache
	ToolsCacheTTL = 5 * time.Second
)

// ToolsVNode implements VirtualNode for the /tools directory.
// It serves tool binaries via FUSE, choosing between two modes per tool:
//   - Gateway tools: served as the compiled gRPC shim binary
//   - Local tools:   served as a shell wrapper that execs the CLI directly
type ToolsVNode struct {
	ReadOnlyBase

	gatewayAddr string
	token       string
	bearerToken string
	shim        []byte    // gRPC shim binary for gateway-executed tools
	modTime     time.Time // stable timestamp for initial getattr

	mu            sync.RWMutex
	tools         []string          // ordered tool names
	toolSet       map[string]bool   // fast membership check
	localWrappers map[string][]byte // tool → wrapper script (nil = use shim)
	lastFetch     time.Time
	cacheModTime  time.Time
}

// NewToolsVNode creates a new ToolsVNode.
func NewToolsVNode(gatewayAddr string, token string, shimBinary []byte) *ToolsVNode {
	modTime := time.Now()

	t := &ToolsVNode{
		gatewayAddr:   gatewayAddr,
		token:         token,
		bearerToken:   BearerToken(token),
		shim:          shimBinary,
		modTime:       modTime,
		tools:         []string{},
		toolSet:       make(map[string]bool),
		localWrappers: make(map[string][]byte),
		cacheModTime:  modTime,
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

	info := NewExecFileInfo(toolIno(name), int64(len(t.toolBinary(name))))
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

	data := t.toolBinary(name)
	if off >= int64(len(data)) {
		return 0, nil
	}
	return copy(buf, data[off:]), nil
}

// hasTool checks if a tool is registered.
func (t *ToolsVNode) hasTool(name string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.toolSet[name]
}

// toolBinary returns the bytes to serve for a given tool:
// a shell wrapper for local tools, the gRPC shim for gateway tools.
func (t *ToolsVNode) toolBinary(name string) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if w, ok := t.localWrappers[name]; ok {
		return w
	}
	return t.shim
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

type toolEntry struct {
	name         string
	localCommand string
}

// refreshCache fetches the latest tools from the gateway and rebuilds the cache.
func (t *ToolsVNode) refreshCache() {
	entries := t.fetchTools()
	if entries == nil {
		return
	}

	names := make([]string, len(entries))
	set := make(map[string]bool, len(entries))
	wrappers := make(map[string][]byte)
	for i, e := range entries {
		names[i] = e.name
		set[e.name] = true
		if e.localCommand != "" {
			wrappers[e.name] = []byte("#!/bin/sh\nexec " + e.localCommand + " \"$@\"\n")
		}
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	changed := !sameKeys(set, t.toolSet)
	t.tools = names
	t.toolSet = set
	t.localWrappers = wrappers
	t.lastFetch = time.Now()
	if changed {
		t.cacheModTime = time.Now()
	}
}

func sameKeys(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

// fetchTools queries the gateway for registered tools.
func (t *ToolsVNode) fetchTools() []toolEntry {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if t.bearerToken != "" {
		md := metadata.Pairs("authorization", t.bearerToken)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	conn, err := grpc.NewClient(
		t.gatewayAddr,
		grpc.WithTransportCredentials(common.TransportCredentials(t.gatewayAddr)),
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

	entries := make([]toolEntry, len(resp.Tools))
	for i, tool := range resp.Tools {
		entries[i] = toolEntry{name: tool.Name, localCommand: tool.LocalCommand}
	}
	return entries
}

func toolsIno() uint64 {
	return PathIno(ToolsPath)
}

func toolIno(name string) uint64 {
	return PathIno(ToolsPathPrefix + name)
}
