package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	poolHealthCheckInterval time.Duration = 10 * time.Second
	maxConsecutiveFailures  int           = 5
)

// WorkerPoolController manages a pool of workers
// In the queue-based model, this is primarily for visibility/monitoring
// Actual scaling is handled by PoolScaler based on queue depth
type WorkerPoolController struct {
	name       string
	config     types.WorkerPoolConfig
	workerRepo repository.WorkerRepository
	appConfig  types.AppConfig
	ctx        context.Context
	cancel     context.CancelFunc

	failureCount int
}

// NewWorkerPoolController creates a new worker pool controller
func NewWorkerPoolController(
	ctx context.Context,
	name string,
	poolConfig types.WorkerPoolConfig,
	appConfig types.AppConfig,
	workerRepo repository.WorkerRepository,
) *WorkerPoolController {
	ctx, cancel := context.WithCancel(ctx)

	return &WorkerPoolController{
		name:       name,
		config:     poolConfig,
		workerRepo: workerRepo,
		appConfig:  appConfig,
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Name returns the pool name
func (p *WorkerPoolController) Name() string {
	return p.name
}

// Context returns the pool's context
func (p *WorkerPoolController) Context() context.Context {
	return p.ctx
}

// Config returns the pool configuration
func (p *WorkerPoolController) Config() types.WorkerPoolConfig {
	return p.config
}

// Stop stops the pool controller
func (p *WorkerPoolController) Stop() {
	p.cancel()
}

// State returns the current state of the pool
func (p *WorkerPoolController) State() (*types.WorkerPoolState, error) {
	workers, err := p.workerRepo.GetAllWorkers(p.ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get workers: %w", err)
	}

	state := &types.WorkerPoolState{
		Name:   p.name,
		Status: types.WorkerPoolStatusHealthy,
	}

	for _, worker := range workers {
		if worker.PoolName != p.name {
			continue
		}

		state.TotalWorkers++

		if worker.Status == types.WorkerStatusAvailable {
			state.AvailableWorkers++
		}
	}

	// Mark as degraded if we have too many failures
	if p.failureCount >= maxConsecutiveFailures {
		state.Status = types.WorkerPoolStatusDegraded
	}

	return state, nil
}

// GetWorkers returns all workers in this pool
func (p *WorkerPoolController) GetWorkers() ([]*types.Worker, error) {
	allWorkers, err := p.workerRepo.GetAllWorkers(p.ctx)
	if err != nil {
		return nil, err
	}

	var poolWorkers []*types.Worker
	for _, worker := range allWorkers {
		if worker.PoolName == p.name {
			poolWorkers = append(poolWorkers, worker)
		}
	}

	return poolWorkers, nil
}

// OnWorkerRegistered is called when a worker registers with the gateway
func (p *WorkerPoolController) OnWorkerRegistered(workerId string) {
	log.Debug().
		Str("pool_name", p.name).
		Str("worker_id", workerId).
		Msg("worker registered in pool")
}

// OnWorkerDeregistered is called when a worker deregisters
func (p *WorkerPoolController) OnWorkerDeregistered(workerId string) {
	log.Debug().
		Str("pool_name", p.name).
		Str("worker_id", workerId).
		Msg("worker deregistered from pool")
}
