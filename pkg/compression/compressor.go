package compression

import (
	"context"
	"fmt"
	"time"
)

type CompressionStrategy string

const (
	CompressionStrategyStrip       CompressionStrategy = "strip"
	CompressionStrategyPassthrough CompressionStrategy = "passthrough"
)

func ParseStrategy(s string) (CompressionStrategy, error) {
	st := CompressionStrategy(s)
	if st.Valid() {
		return st, nil
	}
	return "", fmt.Errorf("unknown compression strategy %q (valid: strip, passthrough)", s)
}

func (s CompressionStrategy) Valid() bool {
	switch s {
	case CompressionStrategyStrip, CompressionStrategyPassthrough:
		return true
	}
	return false
}

func (s CompressionStrategy) String() string { return string(s) }

type Outcome string

const (
	OutcomeCompressed  Outcome = "compressed"
	OutcomeCacheHit    Outcome = "cache_hit"
	OutcomePassthrough Outcome = "passthrough"
	OutcomeTimeout     Outcome = "timeout"
	OutcomeError       Outcome = "error"
	OutcomeSkipped     Outcome = "skipped"
)

// CompressionResult is returned by every Compress call.
type CompressionResult struct {
	Data             []byte
	OriginalTokens   int
	CompressedTokens int
	Strategy         CompressionStrategy
	Outcome          Outcome
	DurationMs       int64
}

// ContentMeta provides context about the content being compressed.
type ContentMeta struct {
	Integration string // Source type: types.SourceGmail, types.SourceGitHub, etc.
	QueryPath   string
	ResultID    string
	Filename    string
	MimeHint    string
}

// ContextCompressor transforms raw content into a smaller representation.
// Implementations must be safe for concurrent use.
type ContextCompressor interface {
	Name() CompressionStrategy
	Compress(ctx context.Context, content []byte, meta ContentMeta) (*CompressionResult, error)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Config holds all compression settings.
type Config struct {
	Strategy             CompressionStrategy `yaml:"strategy"`
	CacheEnabled         bool                `yaml:"cacheEnabled"`
	TokenThreshold       int                 `yaml:"tokenThreshold"`
	MaxContentBytes      int                 `yaml:"maxContentBytes"`
	TokenEncoding        string              `yaml:"tokenEncoding"`
	Timeout              time.Duration       `yaml:"timeout"`
	ContentCacheMaxBytes int64               `yaml:"contentCacheMaxBytes"` // cache only
	ContentCacheTTL      time.Duration       `yaml:"contentCacheTTL"`      // cache only
}

func DefaultConfig() Config {
	return Config{
		Strategy:             CompressionStrategyStrip,
		TokenEncoding:        "cl100k_base",
		ContentCacheMaxBytes: 10 * 1024 * 1024,
		ContentCacheTTL:      5 * time.Minute,
	}
}

func (c Config) DefaultTimeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return 2 * time.Second
}

func tokenEncoding(cfg Config) string {
	if cfg.TokenEncoding != "" {
		return cfg.TokenEncoding
	}
	return "cl100k_base"
}

func newTokenCounter(cfg Config) *TokenCounter {
	tc, _ := NewTokenCounter(tokenEncoding(cfg))
	return tc
}

func NewCompressor(strategy CompressionStrategy, cfg Config) (ContextCompressor, error) {
	switch strategy {
	case CompressionStrategyStrip:
		return NewStripCompressor(cfg), nil
	case CompressionStrategyPassthrough:
		return newPassthroughCompressor(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported compression strategy %q", strategy)
	}
}

// Passthrough — returns content unchanged, still counts tokens for metrics.
type passthrough struct{ counter *TokenCounter }

func newPassthroughCompressor(cfg Config) *passthrough {
	return &passthrough{counter: newTokenCounter(cfg)}
}

func (p *passthrough) Name() CompressionStrategy { return CompressionStrategyPassthrough }

func (p *passthrough) Compress(_ context.Context, content []byte, _ ContentMeta) (*CompressionResult, error) {
	start := time.Now()
	tokens := p.counter.Count(content)
	return &CompressionResult{
		Data:             content,
		OriginalTokens:   tokens,
		CompressedTokens: tokens,
		Strategy:         CompressionStrategyPassthrough,
		Outcome:          OutcomePassthrough,
		DurationMs:       time.Since(start).Milliseconds(),
	}, nil
}
