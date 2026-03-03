package hooks

import (
	"context"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const seenInitSuffix = ":init"

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
// On first call, returns current as "new" so the first observed snapshot can
// trigger a source.change event immediately.
func (t *SeenTracker) Compare(ctx context.Context, key string, current []string) ([]string, error) {
	if len(current) == 0 {
		return nil, nil
	}

	initKey := seenInitKey(key)
	initialized, err := t.rdb.Exists(ctx, initKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	old, err := t.rdb.SMembers(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// First call for this key: treat current IDs as new so hooks can run on
	// the initial observed snapshot.
	if initialized == 0 {
		log.Debug().Str("key", key).Int("current", len(current)).
			Msg("seen tracker: first call, bootstrap emit")
		return append([]string(nil), current...), nil
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

// Commit replaces the stored set with current and marks the key as initialized.
// Call only after the caller has successfully acted on the new IDs from Compare.
func (t *SeenTracker) Commit(ctx context.Context, key string, current []string) error {
	initKey := seenInitKey(key)

	// Zero results: clear the stored set so stale IDs don't suppress
	// future events when results reappear.
	pipe := t.rdb.Pipeline()
	if len(current) == 0 {
		pipe.Del(ctx, key)
		pipe.Set(ctx, initKey, "1", 0)
		_, err := pipe.Exec(ctx)
		if err != nil && err != redis.Nil {
			return err
		}
		return nil
	}

	pipe.Del(ctx, key)

	args := make([]any, len(current))
	for i, id := range current {
		args[i] = id
	}
	pipe.SAdd(ctx, key, args...)
	pipe.Set(ctx, initKey, "1", 0)

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return err
	}
	return nil
}

// Reset clears both the tracked ID set and initialization marker for a key.
// The next Compare call for this key will behave like a first observation.
func (t *SeenTracker) Reset(ctx context.Context, key string) error {
	initKey := seenInitKey(key)
	if err := t.rdb.Del(ctx, key, initKey).Err(); err != nil && err != redis.Nil {
		return err
	}
	return nil
}

// ResetPath clears seen state for a workspace + hook path.
func (t *SeenTracker) ResetPath(ctx context.Context, workspaceID uint, path string) error {
	normalizedPath := NormalizePath(path)
	key := common.Keys.HookSeen(workspaceID, types.GeneratePathID(normalizedPath))
	return t.Reset(ctx, key)
}

func seenInitKey(key string) string {
	return key + seenInitSuffix
}
