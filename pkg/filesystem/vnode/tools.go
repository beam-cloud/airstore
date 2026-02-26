package vnode

import (
	"bytes"
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

	toolsShimName = ".airstore-shim"
	toolsShimPath = ToolsPathPrefix + toolsShimName
)

var gatewayToolWrapper = []byte(`#!/bin/sh
set -eu

TOOLS_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" 2>/dev/null && pwd || true)
SHIM_SRC="$TOOLS_DIR/.airstore-shim"
if [ ! -r "$SHIM_SRC" ]; then
	SHIM_SRC="/workspace/tools/.airstore-shim"
fi
if [ ! -r "$SHIM_SRC" ]; then
	printf 'airstore shim not found (tried %s and /workspace/tools/.airstore-shim)\n' "$TOOLS_DIR/.airstore-shim" >&2
	exit 127
fi

CACHE_DIR="${TMPDIR:-/tmp}/.airstore-shims"
SHIM_BIN="$CACHE_DIR/airstore-tool-shim"
TOOL_NAME="${0##*/}"
TOOL_LINK="$CACHE_DIR/$TOOL_NAME"

mkdir -p "$CACHE_DIR"
if [ ! -x "$SHIM_BIN" ]; then
	TMP="$SHIM_BIN.$$"
	if command -v cp >/dev/null 2>&1; then
		cp "$SHIM_SRC" "$TMP"
	else
		cat "$SHIM_SRC" > "$TMP"
	fi
	chmod 755 "$TMP"
	mv -f "$TMP" "$SHIM_BIN"
fi

ln -sf "$SHIM_BIN" "$TOOL_LINK"

# Worker sandboxes export GATEWAY_ADDR. The shim reads AIRSTORE_GATEWAY.
if [ -n "${GATEWAY_ADDR:-}" ] && [ -z "${AIRSTORE_GATEWAY:-}" ]; then
	export AIRSTORE_GATEWAY="$GATEWAY_ADDR"
fi

exec "$TOOL_LINK" "$@"
`)

// ToolsVNode implements VirtualNode for the /tools directory.
// It serves executable wrappers for tools:
//   - Gateway tools: wrapper that copies an embedded shim to local /tmp and execs it
//   - Local tools:   wrapper that execs the local CLI directly
type ToolsVNode struct {
	ReadOnlyBase

	gatewayAddr string
	bearerToken string
	shim        []byte    // gRPC shim binary for gateway-executed tools

	mu            sync.RWMutex
	tools         []string          // ordered tool names
	toolSet       map[string]bool   // fast membership check
	localWrappers map[string][]byte // tool → wrapper script (missing key = gateway wrapper)
	lastFetch     time.Time
	cacheModTime  time.Time
}

// NewToolsVNode creates a new ToolsVNode.
func NewToolsVNode(gatewayAddr string, token string, shimBinary []byte) *ToolsVNode {
	modTime := time.Now()

	t := &ToolsVNode{
		gatewayAddr:   gatewayAddr,
		bearerToken:   BearerToken(token),
		shim:          shimBinary,
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

	if path == toolsShimPath {
		info := NewFileInfo(toolIno(toolsShimName), int64(len(t.shim)), 0444)
		mtime := t.getCacheModTime()
		info.Atime = mtime
		info.Mtime = mtime
		info.Ctime = mtime
		return info, nil
	}

	name, ok := toolNameFromPath(path)
	if !ok {
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

	if path == toolsShimPath {
		return 0, nil
	}

	name, ok := toolNameFromPath(path)
	if !ok || !t.hasTool(name) {
		return 0, fs.ErrNotExist
	}

	return 0, nil
}

// Read reads bytes from a tool binary (served from memory)
func (t *ToolsVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	if path == toolsShimPath {
		if off >= int64(len(t.shim)) {
			return 0, nil
		}
		return copy(buf, t.shim[off:]), nil
	}

	name, ok := toolNameFromPath(path)
	if !ok || !t.hasTool(name) {
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
// a shell wrapper for local tools, a gateway shim bootstrap wrapper otherwise.
func (t *ToolsVNode) toolBinary(name string) []byte {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if w, ok := t.localWrappers[name]; ok {
		return w
	}
	return gatewayToolWrapper
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

	// Bump cache mtime when either the tool names or wrapper bytes change.
	// This lets timestamp-based clients notice localCommand updates.
	changed := !sameKeys(set, t.toolSet) || !sameWrapperBytes(wrappers, t.localWrappers)
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

func sameWrapperBytes(a, b map[string][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || !bytes.Equal(va, vb) {
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

func toolNameFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, ToolsPathPrefix) {
		return "", false
	}
	name := strings.TrimPrefix(path, ToolsPathPrefix)
	if name == "" || strings.Contains(name, "/") {
		return "", false
	}
	return name, true
}
