package vnode

import (
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

func TestSourcesVNode_Getattr_MaterializedResultUsesCachedMtime(t *testing.T) {
	v := &SourcesVNode{
		client:  nil, // should not be used when cache is populated
		token:   "",
		results: make(map[string]*cachedQueryResult),
		queries: make(map[string]*cachedQuery),
	}

	queryPath := "/sources/gmail/coreweave-emails"
	q := &types.SmartQuery{
		Path:         queryPath,
		OutputFormat: types.SmartQueryOutputFolder,
		CreatedAt:    time.Unix(1700000000, 0),
		UpdatedAt:    time.Unix(1700000100, 0),
	}
	v.setCachedQuery(queryPath, q)
	if cached, found := v.getCachedQuery(queryPath); !found || cached == nil {
		t.Fatalf("expected query to be cached for %q (found=%v, cached_nil=%v, cache_len=%d)", queryPath, found, cached == nil, len(v.queries))
	}

	entryMtime := int64(1733875200) // 2024-12-10T00:00:00Z
	v.setCachedResults(queryPath, []*pb.SourceDirEntry{
		{
			Name:  "example.txt",
			Size:  123,
			Mtime: entryMtime,
		},
	})

	info, err := v.Getattr(queryPath + "/example.txt")
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

func TestSourcesVNode_Getattr_QueryMetaFileUsesQueryUpdatedAt(t *testing.T) {
	v := &SourcesVNode{
		client:  nil, // should not be used when cache is populated
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
	if cached, found := v.getCachedQuery(queryPath); !found || cached == nil {
		t.Fatalf("expected query to be cached for %q (found=%v, cached_nil=%v, cache_len=%d)", queryPath, found, cached == nil, len(v.queries))
	}

	info, err := v.Getattr(queryPath + "/.query")
	if err != nil {
		t.Fatalf("Getattr returned error: %v", err)
	}
	if info == nil {
		t.Fatalf("Getattr returned nil info")
	}
	if got := info.Mtime.Unix(); got != updated.Unix() {
		t.Fatalf("expected .query mtime=%d, got %d", updated.Unix(), got)
	}
}

