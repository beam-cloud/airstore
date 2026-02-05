package common

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// SeenTracker tracks set membership and returns what's new.
// Knows nothing about hooks, workspaces, or queries -- just operates on
// a Redis key and a set of strings.
type SeenTracker struct {
	rdb *RedisClient
}

func NewSeenTracker(rdb *RedisClient) *SeenTracker {
	return &SeenTracker{rdb: rdb}
}

// Diff returns IDs in current that weren't in the previous set stored at key.
// Atomically replaces the stored set with current (bounded to len(current)).
// Returns nil on first call (empty previous set) to avoid a false-positive flood.
func (t *SeenTracker) Diff(ctx context.Context, key string, current []string) ([]string, error) {
	if len(current) == 0 {
		return nil, nil
	}

	// Pipeline: read old set, then replace atomically
	pipe := t.rdb.Pipeline()
	oldCmd := pipe.SMembers(ctx, key)
	pipe.Del(ctx, key)
	args := make([]any, len(current))
	for i, id := range current {
		args[i] = id
	}
	pipe.SAdd(ctx, key, args...)

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	old := oldCmd.Val()
	if len(old) == 0 {
		return nil, nil // first execution: populate only, don't report everything as new
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
