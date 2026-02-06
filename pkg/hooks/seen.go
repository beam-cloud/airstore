package hooks

import (
	"context"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/redis/go-redis/v9"
)

// SeenTracker detects new query result IDs by diffing against the previous set.
type SeenTracker struct {
	rdb *common.RedisClient
}

func NewSeenTracker(rdb *common.RedisClient) *SeenTracker {
	return &SeenTracker{rdb: rdb}
}

// Diff returns IDs in current that weren't in the previous set at key.
// Replaces the stored set with current (pipelined, not strictly atomic).
// Returns nil on first call to avoid a false-positive flood.
func (t *SeenTracker) Diff(ctx context.Context, key string, current []string) ([]string, error) {
	if len(current) == 0 {
		return nil, nil
	}

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
