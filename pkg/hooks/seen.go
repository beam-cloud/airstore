package hooks

import (
	"context"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const seenInitSuffix = ":init"

// CompareResult holds the diff between previous and current ID sets.
type CompareResult struct {
	Added   []string
	Removed []string
}

// SeenTracker detects new and removed query result IDs by diffing against the previous set.
// Usage: Compare (read-only) → act on added/removed IDs → Commit (update stored set).
// This two-phase approach ensures the stored set only advances after the caller
// has successfully processed the changes.
type SeenTracker struct {
	rdb *common.RedisClient
}

func NewSeenTracker(rdb *common.RedisClient) *SeenTracker {
	return &SeenTracker{rdb: rdb}
}

// Compare diffs current against the stored set at key, returning added and
// removed IDs. Does NOT modify the stored set — call Commit after successful
// processing. On first call, Added contains all current IDs and Removed is empty.
func (t *SeenTracker) Compare(ctx context.Context, key string, current []string) (*CompareResult, error) {
	initKey := seenInitKey(key)
	initialized, err := t.rdb.Exists(ctx, initKey).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	// Not yet initialized: nothing to compare against.
	if initialized == 0 {
		if len(current) == 0 {
			return nil, nil
		}
		log.Debug().Str("key", key).Int("current", len(current)).
			Msg("seen tracker: first call, bootstrap emit")
		return &CompareResult{Added: append([]string(nil), current...)}, nil
	}

	old, err := t.rdb.SMembers(ctx, key).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if len(current) == 0 && len(old) == 0 {
		return nil, nil
	}

	oldSet := make(map[string]struct{}, len(old))
	for _, id := range old {
		oldSet[id] = struct{}{}
	}
	curSet := make(map[string]struct{}, len(current))
	for _, id := range current {
		curSet[id] = struct{}{}
	}

	var added []string
	for _, id := range current {
		if _, seen := oldSet[id]; !seen {
			added = append(added, id)
		}
	}

	var removed []string
	for _, id := range old {
		if _, present := curSet[id]; !present {
			removed = append(removed, id)
		}
	}

	if len(added) == 0 && len(removed) == 0 {
		return nil, nil
	}

	log.Debug().Str("key", key).Int("previous", len(old)).Int("current", len(current)).
		Int("added", len(added)).Int("removed", len(removed)).
		Msg("seen tracker: compare complete")
	return &CompareResult{Added: added, Removed: removed}, nil
}

// Commit replaces the stored set with current and marks the key as initialized.
// Call only after the caller has successfully acted on the new IDs from Compare.
func (t *SeenTracker) Commit(ctx context.Context, key string, current []string) error {
	initKey := seenInitKey(key)

	// Zero results: clear the stored set so stale IDs don't suppress
	// future events when results reappear.
	// NOTE: callers should guard against transient empty results before
	// calling Commit with an empty slice — see emitSourceHookEvents.
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
