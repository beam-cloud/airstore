package hooks

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	sourcePollTick    = 30 * time.Second // how often the poller checks for stale queries
	sourcePollStale   = 2 * time.Minute  // query is stale if not executed in this window
	sourcePollBatch   = 50               // max queries per poll cycle
	sourcePollWorkers = 5                // concurrent refresh workers
)

// ViewSyncer re-executes a source view query and emits change events.
// Implemented by SourceService in the gateway layer.
type ViewSyncer interface {
	RefreshQuery(ctx context.Context, query *types.FilesystemQuery) error
}

// SourcePoller periodically syncs source views watched by active hooks.
// Each view is locked via Redis SETNX so only one replica refreshes it per interval.
type SourcePoller struct {
	store     repository.FilesystemStore
	refresher ViewSyncer
	rdb       *common.RedisClient
}

func NewSourcePoller(store repository.FilesystemStore, refresher ViewSyncer, rdb *common.RedisClient) *SourcePoller {
	return &SourcePoller{store: store, refresher: refresher, rdb: rdb}
}

// Start runs the poll loop. Call as a goroutine.
func (p *SourcePoller) Start(ctx context.Context) {
	log.Info().Msg("source poller started")

	t := time.NewTicker(sourcePollTick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.Poll(ctx)
		}
	}
}

// Poll fetches stale watched queries and refreshes them with distributed locking.
func (p *SourcePoller) Poll(ctx context.Context) {
	queries, err := p.store.GetWatchedSourceQueries(ctx, sourcePollStale, sourcePollBatch)
	if err != nil {
		log.Warn().Err(err).Msg("source poller: failed to get watched queries")
		return
	}

	if len(queries) == 0 {
		log.Debug().Msg("source poller: no stale watched queries")
		return
	}

	log.Info().Int("stale_queries", len(queries)).Msg("source poller: poll cycle starting")

	sem := make(chan struct{}, sourcePollWorkers)
	var wg sync.WaitGroup
	var refreshed, locked, failed atomic.Int32

	for _, q := range queries {
		// Distributed lock: only one replica refreshes this query per interval.
		lockKey := common.Keys.HookPollLock(q.ExternalId)
		acquired, err := p.rdb.SetNX(ctx, lockKey, "1", sourcePollStale).Result()
		if err != nil || !acquired {
			locked.Add(1)
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // acquire semaphore slot

		go func(query *types.FilesystemQuery) {
			defer wg.Done()
			defer func() { <-sem }() // release semaphore slot

			rctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			defer cancel()

			if err := p.refresher.RefreshQuery(rctx, query); err != nil {
				failed.Add(1)
				log.Warn().Err(err).
					Str("path", query.Path).
					Str("integration", query.Integration).
					Msg("source poller: refresh failed")
			} else {
				refreshed.Add(1)
			}
		}(q)
	}

	wg.Wait()

	log.Info().
		Int32("refreshed", refreshed.Load()).
		Int32("already_locked", locked.Load()).
		Int32("failed", failed.Load()).
		Msg("source poller: poll cycle complete")
}
