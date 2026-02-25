package compression

import "context"

// CompressionStats is populated by readWithCompression and passed back to
// the HTTP handler via context so that real token counts can be emitted as
// response headers. The caller pre-allocates the struct and puts a pointer
// on the context; the compression layer fills it in.
type CompressionStats struct {
	OriginalBytes    int
	CompressedBytes  int
	OriginalTokens   int
	CompressedTokens int
	Strategy         string
}

type compressionStatsKey struct{}

// WithCompressionStats returns a derived context carrying a pointer to an
// empty CompressionStats. The caller retains the pointer and reads it after
// the service call completes.
func WithCompressionStats(ctx context.Context) (context.Context, *CompressionStats) {
	stats := &CompressionStats{}
	return context.WithValue(ctx, compressionStatsKey{}, stats), stats
}

// GetCompressionStats returns the stats pointer stored on the context, or nil.
func GetCompressionStats(ctx context.Context) *CompressionStats {
	v := ctx.Value(compressionStatsKey{})
	if v == nil {
		return nil
	}
	return v.(*CompressionStats)
}

// SetCompressionStats fills the CompressionStats stored on the context (if
// present). Safe to call with a nil result.
func SetCompressionStats(ctx context.Context, originalBytes int, result *CompressionResult, strategy string) {
	stats := GetCompressionStats(ctx)
	if stats == nil {
		return
	}
	stats.OriginalBytes = originalBytes
	stats.Strategy = strategy
	if result != nil {
		stats.CompressedBytes = len(result.Data)
		stats.OriginalTokens = result.OriginalTokens
		stats.CompressedTokens = result.CompressedTokens
	}
}
