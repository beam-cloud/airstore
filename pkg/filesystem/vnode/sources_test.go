package vnode

import (
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

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
	q := &types.SmartQuery{
		Path:         queryPath,
		OutputFormat: types.SmartQueryOutputFolder,
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
	q := &types.SmartQuery{
		Path:         queryPath,
		OutputFormat: types.SmartQueryOutputFolder,
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
	q := &types.SmartQuery{
		Path:         queryPath,
		OutputFormat: types.SmartQueryOutputFolder,
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
