package compression

import (
	"context"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

type Strategy string

const (
	StrategyStrip       Strategy = "strip"
	StrategyPassthrough Strategy = "passthrough"
)

func ParseStrategy(s string) (Strategy, error) {
	st := Strategy(s)
	if st.Valid() {
		return st, nil
	}
	return "", fmt.Errorf("unknown compression strategy %q (valid: strip, passthrough)", s)
}

func (s Strategy) Valid() bool {
	switch s {
	case StrategyStrip, StrategyPassthrough:
		return true
	}
	return false
}

func (s Strategy) String() string { return string(s) }

// ---------------------------------------------------------------------------
// Outcome
// ---------------------------------------------------------------------------

type Outcome string

const (
	OutcomeCompressed  Outcome = "compressed"
	OutcomeCacheHit    Outcome = "cache_hit"
	OutcomePassthrough Outcome = "passthrough"
	OutcomeTimeout     Outcome = "timeout"
	OutcomeError       Outcome = "error"
	OutcomeSkipped     Outcome = "skipped"
)

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

// CompressionResult is returned by every Compress call.
type CompressionResult struct {
	Data             []byte
	OriginalTokens   int
	CompressedTokens int
	Strategy         Strategy
	Outcome          Outcome
	DurationMs       int64
}

// ContentMeta provides context about the content being compressed.
type ContentMeta struct {
	Integration string // "gmail", "github", etc.
	QueryPath   string
	ResultID    string
	Filename    string
	MimeHint    string
}

// ContextCompressor transforms raw content into a smaller representation.
// Implementations must be safe for concurrent use.
type ContextCompressor interface {
	Name() Strategy
	Compress(ctx context.Context, content []byte, meta ContentMeta) (*CompressionResult, error)
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Config holds all compression settings.
type Config struct {
	Strategy             Strategy      `yaml:"strategy"`
	TokenThreshold       int           `yaml:"tokenThreshold"`
	MaxContentBytes      int           `yaml:"maxContentBytes"`
	TokenEncoding        string        `yaml:"tokenEncoding"`
	Timeout              time.Duration `yaml:"timeout"`
	ContentCacheMaxBytes int64         `yaml:"contentCacheMaxBytes"`
	ContentCacheTTL      time.Duration `yaml:"contentCacheTTL"`
}

func DefaultConfig() Config {
	return Config{
		Strategy:             StrategyStrip,
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

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

func NewCompressor(strategy Strategy, cfg Config) (ContextCompressor, error) {
	switch strategy {
	case StrategyStrip:
		return NewStripCompressor(cfg), nil
	case StrategyPassthrough:
		return newPassthroughCompressor(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported compression strategy %q", strategy)
	}
}

// ---------------------------------------------------------------------------
// Passthrough — returns content unchanged, still counts tokens for metrics.
// ---------------------------------------------------------------------------

type passthrough struct{ counter *TokenCounter }

func newPassthroughCompressor(cfg Config) *passthrough {
	return &passthrough{counter: newTokenCounter(cfg)}
}

func (p *passthrough) Name() Strategy { return StrategyPassthrough }

func (p *passthrough) Compress(_ context.Context, content []byte, _ ContentMeta) (*CompressionResult, error) {
	start := time.Now()
	tokens := p.counter.Count(content)
	return &CompressionResult{
		Data:             content,
		OriginalTokens:   tokens,
		CompressedTokens: tokens,
		Strategy:         StrategyPassthrough,
		Outcome:          OutcomePassthrough,
		DurationMs:       time.Since(start).Milliseconds(),
	}, nil
}
