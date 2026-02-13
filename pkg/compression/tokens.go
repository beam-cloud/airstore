package compression

import (
	"sync"

	tiktoken "github.com/pkoukk/tiktoken-go"
)

// TokenCounter counts tokens using a tiktoken encoding.
// A nil *TokenCounter is safe to use — it falls back to EstimateTokens.
type TokenCounter struct {
	enc *tiktoken.Tiktoken
}

var (
	defaultCounter     *TokenCounter
	defaultCounterOnce sync.Once
)

// NewTokenCounter creates a counter for the given encoding name.
// Common encodings: "cl100k_base" (GPT-4), "o200k_base" (GPT-4o).
func NewTokenCounter(encoding string) (*TokenCounter, error) {
	enc, err := tiktoken.GetEncoding(encoding)
	if err != nil {
		return nil, err
	}
	return &TokenCounter{enc: enc}, nil
}

// Count returns the number of tokens in content.
// Safe to call on a nil receiver (falls back to EstimateTokens).
func (tc *TokenCounter) Count(content []byte) int {
	if tc == nil || tc.enc == nil {
		return EstimateTokens(content)
	}
	return len(tc.enc.Encode(string(content), nil, nil))
}

// DefaultTokenCounter returns a shared counter using cl100k_base.
// Lazily initialized on first call.
func DefaultTokenCounter() *TokenCounter {
	defaultCounterOnce.Do(func() {
		tc, err := NewTokenCounter("cl100k_base")
		if err != nil {
			return // defaultCounter stays nil — Count is nil-safe
		}
		defaultCounter = tc
	})
	return defaultCounter
}

// EstimateTokens is a fast fallback when no tiktoken encoding is available.
// Approximation: ~4 characters per token.
func EstimateTokens(content []byte) int {
	n := len(content)
	if n == 0 {
		return 0
	}
	return (n + 3) / 4
}
