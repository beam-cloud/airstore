package sources

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// RateLimiter provides per-integration, per-workspace rate limiting
// to prevent hammering upstream APIs when multiple agents are active.
type RateLimiter struct {
	mu       sync.Mutex
	limiters map[string]*tokenBucket
	config   RateLimitConfig
}

// RateLimitConfig configures the rate limiter
type RateLimitConfig struct {
	// RequestsPerSecond is the rate limit per integration per workspace
	RequestsPerSecond float64

	// BurstSize is the maximum burst size
	BurstSize int

	// CleanupInterval is how often to clean up stale limiters
	CleanupInterval time.Duration
}

// DefaultRateLimitConfig returns sensible defaults
func DefaultRateLimitConfig() RateLimitConfig {
	return RateLimitConfig{
		RequestsPerSecond: 10,            // 10 requests per second per integration
		BurstSize:         20,            // Allow bursts up to 20
		CleanupInterval:   5 * time.Minute,
	}
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(config RateLimitConfig) *RateLimiter {
	if config.RequestsPerSecond <= 0 {
		config.RequestsPerSecond = 10
	}
	if config.BurstSize <= 0 {
		config.BurstSize = 20
	}
	if config.CleanupInterval <= 0 {
		config.CleanupInterval = 5 * time.Minute
	}

	rl := &RateLimiter{
		limiters: make(map[string]*tokenBucket),
		config:   config,
	}

	// Start cleanup goroutine
	go rl.cleanupLoop()

	return rl
}

// limiterKey generates a key for the rate limiter
func limiterKey(workspaceId uint, integration string) string {
	return fmt.Sprintf("%d:%s", workspaceId, integration)
}

// Allow checks if a request is allowed under the rate limit.
// Returns true if allowed, false if rate limited.
func (r *RateLimiter) Allow(workspaceId uint, integration string) bool {
	key := limiterKey(workspaceId, integration)

	r.mu.Lock()
	limiter, ok := r.limiters[key]
	if !ok {
		limiter = newTokenBucket(r.config.RequestsPerSecond, r.config.BurstSize)
		r.limiters[key] = limiter
	}
	r.mu.Unlock()

	return limiter.allow()
}

// Wait waits until a request is allowed or context is cancelled.
// Returns nil if allowed, context error if cancelled/timed out.
func (r *RateLimiter) Wait(ctx context.Context, workspaceId uint, integration string) error {
	key := limiterKey(workspaceId, integration)

	r.mu.Lock()
	limiter, ok := r.limiters[key]
	if !ok {
		limiter = newTokenBucket(r.config.RequestsPerSecond, r.config.BurstSize)
		r.limiters[key] = limiter
	}
	r.mu.Unlock()

	return limiter.wait(ctx)
}

// cleanupLoop periodically removes stale limiters
func (r *RateLimiter) cleanupLoop() {
	ticker := time.NewTicker(r.config.CleanupInterval)
	for range ticker.C {
		r.cleanup()
	}
}

func (r *RateLimiter) cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()

	staleThreshold := time.Now().Add(-r.config.CleanupInterval)
	for key, limiter := range r.limiters {
		if limiter.lastUsed.Before(staleThreshold) {
			delete(r.limiters, key)
		}
	}
}

// tokenBucket implements a simple token bucket rate limiter
type tokenBucket struct {
	mu       sync.Mutex
	rate     float64   // tokens per second
	burst    int       // max tokens
	tokens   float64   // current tokens
	lastUsed time.Time // last request time
	lastFill time.Time // last token fill time
}

func newTokenBucket(rate float64, burst int) *tokenBucket {
	return &tokenBucket{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst), // Start full
		lastFill: time.Now(),
		lastUsed: time.Now(),
	}
}

func (tb *tokenBucket) allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	tb.refill()
	tb.lastUsed = time.Now()

	if tb.tokens >= 1 {
		tb.tokens--
		return true
	}
	return false
}

func (tb *tokenBucket) wait(ctx context.Context) error {
	for {
		tb.mu.Lock()
		tb.refill()
		tb.lastUsed = time.Now()

		if tb.tokens >= 1 {
			tb.tokens--
			tb.mu.Unlock()
			return nil
		}

		// Calculate wait time for next token
		waitTime := time.Duration(float64(time.Second) / tb.rate)
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
			// Try again
		}
	}
}

func (tb *tokenBucket) refill() {
	now := time.Now()
	elapsed := now.Sub(tb.lastFill).Seconds()
	tb.lastFill = now

	tb.tokens += elapsed * tb.rate
	if tb.tokens > float64(tb.burst) {
		tb.tokens = float64(tb.burst)
	}
}

// ErrRateLimited is returned when a request is rate limited
var ErrRateLimited = fmt.Errorf("rate limited")
