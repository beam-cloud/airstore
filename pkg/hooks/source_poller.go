package hooks

import (
	"context"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	sourcePollTick     = 60 * time.Second
	sourcePollStale    = 5 * time.Minute
	sourcePollBatch    = 50
	sourcePollWorkers  = 5
)

// QueryRefresher executes a source query and emits change events.
// Implemented by SourceService in the gateway layer.
type QueryRefresher interface {
	RefreshQuery(ctx context.Context, query *types.FilesystemQuery) error
}

// SourcePoller periodically refreshes source queries watched by active hooks.
// Each query is locked via Redis SETNX so only one replica refreshes it per interval.
type SourcePoller struct {
	store     repository.FilesystemStore
	refresher QueryRefresher
	rdb       *common.RedisClient
}

func NewSourcePoller(store repository.FilesystemStore, refresher QueryRefresher, rdb *common.RedisClient) *SourcePoller {
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

	log.Debug().Int("count", len(queries)).Msg("source poller: refreshing watched queries")

	sem := make(chan struct{}, sourcePollWorkers)
	var wg sync.WaitGroup

	for _, q := range queries {
		// Distributed lock: only one replica refreshes this query per interval.
		lockKey := common.Keys.HookPollLock(q.ExternalId)
		acquired, err := p.rdb.SetNX(ctx, lockKey, "1", sourcePollStale).Result()
		if err != nil || !acquired {
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
				log.Warn().Err(err).
					Str("path", query.Path).
					Str("integration", query.Integration).
					Msg("source poller: refresh failed")
			}
		}(q)
	}

	wg.Wait()
}
