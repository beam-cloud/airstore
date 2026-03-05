package vnode

import pb "github.com/beam-cloud/airstore/proto"

// ReadAttribution carries read-path metadata used by mount-level access logs.
// It is emitted per logical read and can be attached by vnodes that know where
// bytes came from (cache vs backend) and, for source reads, token-cost details.
type ReadAttribution struct {
	CacheSource      string
	Integration      string
	SourceURI        string
	QueryPath        string
	ResultID         string
	Strategy         string
	Outcome          string
	OriginalBytes    int
	CompressedBytes  int
	OriginalTokens   int
	CompressedTokens int
	CompressionMs    int64
	FetchMs          int64 // e2e content fetch duration (e.g. time to fetch from Gmail during Open)
}

const (
	CacheSourceUnknown        = "unknown"
	CacheSourceBackendRPC     = "backend_rpc"
	CacheSourceOpenContent    = "open_content"
	CacheSourceContentCache   = "content_cache"
	CacheSourcePrefetch       = "prefetch"
	CacheSourceDirtyBuffer    = "dirty_buffer"
	CacheSourceSynthetic      = "synthetic"
	CacheSourceMetadata       = "metadata"
	CacheSourceLegacyMetadata = "legacy_metadata"
)

// ReadAttributionNode is an optional vnode extension that returns attribution
// metadata alongside read bytes.
type ReadAttributionNode interface {
	ReadWithAttribution(path string, buf []byte, off int64, fh FileHandle) (int, *ReadAttribution, error)
}

func AttributionForCache(cacheSource string) *ReadAttribution {
	return &ReadAttribution{CacheSource: cacheSource}
}

func AttributionFromCostHint(cacheSource string, hint *pb.SourceReadCostHint) *ReadAttribution {
	attr := AttributionForCache(cacheSource)
	if hint == nil {
		return attr
	}
	attr.Integration = hint.Integration
	attr.SourceURI = hint.SourceUri
	attr.QueryPath = hint.QueryPath
	attr.ResultID = hint.ResultId
	attr.Strategy = hint.Strategy
	attr.Outcome = hint.Outcome
	attr.OriginalBytes = int(hint.OriginalBytes)
	attr.CompressedBytes = int(hint.CompressedBytes)
	attr.OriginalTokens = int(hint.OriginalTokens)
	attr.CompressedTokens = int(hint.CompressedTokens)
	attr.CompressionMs = hint.CompressionMs
	return attr
}
