package hooks

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
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
// Returns nil on first call (empty stored set) to seed the baseline without
// triggering a flood of events for pre-existing results.
func (t *SeenTracker) Compare(ctx context.Context, key string, current []string) ([]string, error) {
	if len(current) == 0 {
		return nil, nil
	}

	old, err := t.rdb.SMembers(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// First call: no previous set. Seed the baseline — the caller should
	// Commit() to store these IDs, and future polls will detect changes.
	if len(old) == 0 {
		log.Debug().Str("key", key).Int("current", len(current)).
			Msg("seen tracker: first call, seeding baseline")
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

	log.Debug().Str("key", key).Int("previous", len(old)).Int("current", len(current)).Int("new", len(newIDs)).
		Msg("seen tracker: compare complete")
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
