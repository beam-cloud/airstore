package hooks

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/redis/go-redis/v9"
)

const seenKeyTTL = 24 * time.Hour

// SeenTracker detects new query result IDs by diffing against the previous set.
// Usage: Compare (read-only) → act on new IDs → Commit (update stored set).
// This two-phase approach ensures the stored set only advances after the caller
// has successfully processed the new IDs.
type SeenTracker struct {
	rdb *common.RedisClient
}

func NewSeenTracker(rdb *common.RedisClient) *SeenTracker {
	return &SeenTracker{rdb: rdb}
}

// Compare returns IDs in current that weren't in the previous set at key.
// Does NOT modify the stored set -- call Commit after successful processing.
// Returns nil on first call (empty stored set) to avoid a false-positive flood.
func (t *SeenTracker) Compare(ctx context.Context, key string, current []string) ([]string, error) {
	if len(current) == 0 {
		return nil, nil
	}

	old, err := t.rdb.SMembers(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// First call: no previous set. Return nil to avoid flooding.
	if len(old) == 0 {
		return nil, nil
	}

	oldSet := make(map[string]struct{}, len(old))
	for _, id := range old {
		oldSet[id] = struct{}{}
	}

	var newIDs []string
	for _, id := range current {
		if _, seen := oldSet[id]; !seen {
			newIDs = append(newIDs, id)
		}
	}
	return newIDs, nil
}

// Commit replaces the stored set with current and refreshes the TTL.
// Call only after the caller has successfully acted on the new IDs from Compare.
func (t *SeenTracker) Commit(ctx context.Context, key string, current []string) error {
	// Zero results: clear the stored set so stale IDs don't suppress
	// future events when results reappear.
	if len(current) == 0 {
		return t.rdb.Del(ctx, key).Err()
	}

	pipe := t.rdb.Pipeline()
	pipe.Del(ctx, key)

	args := make([]any, len(current))
	for i, id := range current {
		args[i] = id
	}
	pipe.SAdd(ctx, key, args...)
	pipe.Expire(ctx, key, seenKeyTTL)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

// TrySetCooldown attempts to set a cooldown key via SETNX. Returns true if
// the key was set (no active cooldown), false if a cooldown is already active.
// Used to prevent duplicate event emissions from multiple gateway replicas.
func (t *SeenTracker) TrySetCooldown(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return t.rdb.SetNX(ctx, key, "1", ttl).Result()
}

// Diff is a convenience that combines Compare + Commit in one call.
// Use Compare + Commit separately when you need to confirm delivery before advancing.
func (t *SeenTracker) Diff(ctx context.Context, key string, current []string) ([]string, error) {
	newIDs, err := t.Compare(ctx, key, current)
	if err != nil {
		return nil, err
	}

	// Always commit (even if no new IDs) to refresh TTL and update the set.
	if err := t.Commit(ctx, key, current); err != nil {
		return nil, err
	}

	return newIDs, nil
}
