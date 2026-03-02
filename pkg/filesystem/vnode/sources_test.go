package vnode

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
)

type sourceServiceClientStub struct {
	pb.SourceServiceClient
	readFn func(context.Context, *pb.SourceReadRequest, ...grpc.CallOption) (*pb.SourceReadResponse, error)
}

func (s *sourceServiceClientStub) Read(
	ctx context.Context,
	req *pb.SourceReadRequest,
	opts ...grpc.CallOption,
) (*pb.SourceReadResponse, error) {
	if s.readFn != nil {
		return s.readFn(ctx, req, opts...)
	}
	return &pb.SourceReadResponse{Ok: false, Error: "stub read not configured"}, nil
}

func TestSourcesVNode_Getattr_MaterializedResultUsesOpenContentSize(t *testing.T) {
	v := &SourcesVNode{
		client:      nil,
		token:       "",
		results:     make(map[string]*cachedQueryResult),
		queries:     make(map[string]*cachedQuery),
		openContent: make(map[string]*cachedContent),
		openHandles: make(map[FileHandle]string),
		stats:       make(map[string]*cachedStat),
	}

	queryPath := "/sources/gmail/coreweave-emails"
	q := &types.SourceView{
		Path:         queryPath,
		OutputFormat: types.ViewOutputFolder,
		CreatedAt:    time.Unix(1700000000, 0),
		UpdatedAt:    time.Unix(1700000100, 0),
	}
	v.setCachedQuery(queryPath, q)

	entryMtime := int64(1733875200) // 2024-12-10T00:00:00Z
	v.setCachedResults(queryPath, []*pb.SourceDirEntry{
		{Name: "example.txt", Size: 999, Mtime: entryMtime},
	})

	// Simulate prefetched content (step 1). Getattr should use the
	// actual content length (123), not the metadata size (999).
	filePath := queryPath + "/example.txt"
	v.prefetchContent(filePath, make([]byte, 123), nil)

	info, err := v.Getattr(filePath)
	if err != nil {
		t.Fatalf("Getattr returned error: %v", err)
	}
	if info == nil {
		t.Fatalf("Getattr returned nil info")
	}
	if info.Size != 123 {
		t.Fatalf("expected size=123, got %d", info.Size)
	}
	if got := info.Mtime.Unix(); got != entryMtime {
		t.Fatalf("expected mtime=%d, got %d", entryMtime, got)
	}
}

func TestSourcesVNode_Getattr_MaterializedResultUsesReaddirCache(t *testing.T) {
	v := &SourcesVNode{
		client:      nil,
		token:       "",
		results:     make(map[string]*cachedQueryResult),
		queries:     make(map[string]*cachedQuery),
		openContent: make(map[string]*cachedContent),
		openHandles: make(map[FileHandle]string),
		stats:       make(map[string]*cachedStat),
	}

	queryPath := "/sources/gmail/coreweave-emails"
	q := &types.SourceView{
		Path:         queryPath,
		OutputFormat: types.ViewOutputFolder,
		CreatedAt:    time.Unix(1700000000, 0),
		UpdatedAt:    time.Unix(1700000100, 0),
	}
	v.setCachedQuery(queryPath, q)

	entryMtime := int64(1733875200)
	v.setCachedResults(queryPath, []*pb.SourceDirEntry{
		{Name: "example.txt", Size: 456, Mtime: entryMtime},
	})

	// No compression — should use readdir metadata cache (step 2).
	info, err := v.Getattr(queryPath + "/example.txt")
	if err != nil {
		t.Fatalf("Getattr returned error: %v", err)
	}
	if info.Size != 456 {
		t.Fatalf("expected size=456, got %d", info.Size)
	}
	if got := info.Mtime.Unix(); got != entryMtime {
		t.Fatalf("expected mtime=%d, got %d", entryMtime, got)
	}
}

func TestSourcesVNode_Getattr_QueryMetaFileUsesQueryUpdatedAt(t *testing.T) {
	v := &SourcesVNode{
		client:  nil,
		token:   "",
		results: make(map[string]*cachedQueryResult),
		queries: make(map[string]*cachedQuery),
	}

	queryPath := "/sources/gmail/coreweave-emails"
	updated := time.Unix(1700000100, 0)
	q := &types.SourceView{
		Path:         queryPath,
		OutputFormat: types.ViewOutputFolder,
		CreatedAt:    time.Unix(1700000000, 0),
		UpdatedAt:    updated,
	}
	v.setCachedQuery(queryPath, q)

	info, err := v.Getattr(queryPath + "/.query.as")
	if err != nil {
		t.Fatalf("Getattr returned error: %v", err)
	}
	if info == nil {
		t.Fatalf("Getattr returned nil info")
	}
	if got := info.Mtime.Unix(); got != updated.Unix() {
		t.Fatalf("expected .query.as mtime=%d, got %d", updated.Unix(), got)
	}
}

func TestSourcesVNode_OpenDirectoryReturnsEISDIR(t *testing.T) {
	v := &SourcesVNode{
		results:      make(map[string]*cachedQueryResult),
		queries:      make(map[string]*cachedQuery),
		integrations: make(map[string]*cachedIntegration),
		stats:        make(map[string]*cachedStat),
		openContent:  make(map[string]*cachedContent),
		openHandles:  make(map[FileHandle]string),
		recentDirs:   make(map[string]time.Time),
	}
	// Seed the integration cache so Getattr returns a directory for /sources/gmail.
	v.setCachedIntegration("gmail", time.Now().Unix())

	_, err := v.Open("/sources/gmail", 0)
	if !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("Open directory: expected EISDIR, got %v", err)
	}

	_, err = v.Open(SourcesPath, 0)
	if !errors.Is(err, syscall.EISDIR) {
		t.Fatalf("Open /sources root: expected EISDIR, got %v", err)
	}
}

func TestSourcesVNode_MutationOpsAreReadOnly(t *testing.T) {
	v := &SourcesVNode{}
	path := "/sources/gmail/unread-emails"

	_, err := v.Create(path+"/new.txt", 0, 0644)
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Create: expected ErrReadOnly, got %v", err)
	}
	if err := v.Mkdir(path, 0755); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Mkdir: expected ErrReadOnly, got %v", err)
	}
	if _, err := v.Write(path+"/new.txt", []byte("x"), 0, 0); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Write: expected ErrReadOnly, got %v", err)
	}
	if err := v.Truncate(path+"/new.txt", 0, 0); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Truncate: expected ErrReadOnly, got %v", err)
	}
	if err := v.Rmdir(path); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Rmdir: expected ErrReadOnly, got %v", err)
	}
	if err := v.Unlink(path + "/new.txt"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Unlink: expected ErrReadOnly, got %v", err)
	}
	if err := v.Rename(path+"/a", path+"/b"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Rename: expected ErrReadOnly, got %v", err)
	}
	if err := v.Symlink(path+"/target", path+"/link"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Symlink: expected ErrReadOnly, got %v", err)
	}
}

func TestSourcesVNode_OpenMaterializedResultPrefetchesWhenCachedSizeUnknown(t *testing.T) {
	content := []byte("hello from materialized source result")
	readCalls := 0
	client := &sourceServiceClientStub{
		readFn: func(_ context.Context, req *pb.SourceReadRequest, _ ...grpc.CallOption) (*pb.SourceReadResponse, error) {
			readCalls++
			if req.Path == "" {
				t.Fatalf("expected non-empty read path")
			}
			return &pb.SourceReadResponse{
				Ok:   true,
				Data: content,
			}, nil
		},
	}

	v := &SourcesVNode{
		client:      client,
		results:     make(map[string]*cachedQueryResult),
		queries:     make(map[string]*cachedQuery),
		openContent: make(map[string]*cachedContent),
		openHandles: make(map[FileHandle]string),
		stats:       make(map[string]*cachedStat),
	}

	queryPath := "/sources/gmail/coreweave-emails"
	filePath := queryPath + "/example.txt"
	q := &types.SourceView{
		Path:         queryPath,
		OutputFormat: types.ViewOutputFolder,
		CreatedAt:    time.Unix(1700000000, 0),
		UpdatedAt:    time.Unix(1700000100, 0),
	}
	v.setCachedQuery(queryPath, q)
	v.setCachedResults(queryPath, []*pb.SourceDirEntry{
		{
			Name:  "example.txt",
			Size:  0, // Unknown/placeholder metadata must not block real reads.
			Mode:  syscall.S_IFREG | 0444,
			Mtime: time.Now().Unix(),
		},
	})
	v.cacheStatFromEntry(filePath, &pb.SourceDirEntry{
		Name:  "example.txt",
		Size:  0, // Unknown cached stat should not short-circuit Getattr.
		Mode:  syscall.S_IFREG | 0444,
		Mtime: time.Now().Unix(),
	})

	fh, err := v.Open(filePath, 0)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	if readCalls != 1 {
		t.Fatalf("expected exactly one content prefetch read, got %d", readCalls)
	}

	info, err := v.Getattr(filePath)
	if err != nil {
		t.Fatalf("Getattr returned error: %v", err)
	}
	if info == nil {
		t.Fatalf("Getattr returned nil info")
	}
	if info.Size != int64(len(content)) {
		t.Fatalf("expected size=%d, got %d", len(content), info.Size)
	}

	buf := make([]byte, len(content)+8)
	n, err := v.Read(filePath, buf, 0, fh)
	if err != nil {
		t.Fatalf("Read returned error: %v", err)
	}
	if got := string(buf[:n]); got != string(content) {
		t.Fatalf("expected read content %q, got %q", string(content), got)
	}
}

func TestSourcesVNodeCleanupExpiredCaches(t *testing.T) {
	v := &SourcesVNode{
		results:      make(map[string]*cachedQueryResult),
		queries:      make(map[string]*cachedQuery),
		integrations: make(map[string]*cachedIntegration),
		stats:        make(map[string]*cachedStat),
		openContent:  make(map[string]*cachedContent),
		openHandles:  make(map[FileHandle]string),
		recentDirs:   make(map[string]time.Time),
		stopRefresh:  make(chan struct{}),
	}

	expired := time.Now().Add(-time.Hour)
	fresh := time.Now().Add(time.Hour)

	v.results["expired"] = &cachedQueryResult{expiresAt: expired}
	v.results["fresh"] = &cachedQueryResult{expiresAt: fresh}

	v.queries["expired"] = &cachedQuery{expiresAt: expired}
	v.queries["fresh"] = &cachedQuery{expiresAt: fresh}

	v.integrations["expired"] = &cachedIntegration{expiresAt: expired}
	v.integrations["fresh"] = &cachedIntegration{expiresAt: fresh}

	v.stats["expired"] = &cachedStat{expiresAt: expired}
	v.stats["fresh"] = &cachedStat{expiresAt: fresh}

	v.openContent["stale"] = &cachedContent{refs: 0, cachedAt: time.Now().Add(-time.Hour)}
	v.openContent["active"] = &cachedContent{refs: 1, cachedAt: time.Now().Add(-time.Hour)}

	v.cleanupExpiredCaches()

	if len(v.results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(v.results))
	}
	if _, ok := v.results["fresh"]; !ok {
		t.Fatal("fresh result was evicted")
	}

	if len(v.queries) != 1 {
		t.Fatalf("expected 1 query, got %d", len(v.queries))
	}

	if len(v.integrations) != 1 {
		t.Fatalf("expected 1 integration, got %d", len(v.integrations))
	}

	if len(v.stats) != 1 {
		t.Fatalf("expected 1 stat, got %d", len(v.stats))
	}

	if len(v.openContent) != 1 {
		t.Fatalf("expected 1 openContent (refs>0), got %d", len(v.openContent))
	}
	if _, ok := v.openContent["active"]; !ok {
		t.Fatal("active content with refs>0 was evicted")
	}
}
