package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// Scheduler manages worker registration, heartbeats, and pool scaling
type Scheduler struct {
	ctx         context.Context
	cancel      context.CancelFunc
	config      types.AppConfig
	redisClient *common.RedisClient
	workerRepo  repository.WorkerRepository
	backendRepo repository.BackendRepository
	taskQueue   repository.TaskQueue

	// Pool controllers (for monitoring)
	pools map[string]*WorkerPoolController

	// Pool scalers (for autoscaling)
	scalers map[string]*PoolScaler

	// Worker tokens per pool (raw token strings)
	workerTokens map[string]string

	mu      sync.RWMutex
	running bool
}

// NewScheduler creates a new Scheduler instance
func NewScheduler(ctx context.Context, config types.AppConfig, redisClient *common.RedisClient, backendRepo repository.BackendRepository) (*Scheduler, error) {
	ctx, cancel := context.WithCancel(ctx)

	workerRepo := repository.NewWorkerRedisRepository(redisClient)

	scheduler := &Scheduler{
		ctx:          ctx,
		cancel:       cancel,
		config:       config,
		redisClient:  redisClient,
		workerRepo:   workerRepo,
		backendRepo:  backendRepo,
		pools:        make(map[string]*WorkerPoolController),
		scalers:      make(map[string]*PoolScaler),
		workerTokens: make(map[string]string),
	}

	return scheduler, nil
}

// Start begins the scheduler's background processes
func (s *Scheduler) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("scheduler already running")
	}
	s.running = true
	s.mu.Unlock()

	log.Info().Msg("scheduler started")

	// Initialize worker pools from config
	if err := s.initializePools(); err != nil {
		log.Error().Err(err).Msg("failed to initialize worker pools")
	}

	// Start background goroutines
	go s.cleanupLoop()
	go s.monitorWorkersLoop()

	return nil
}

// initializePools creates pool controllers from config
func (s *Scheduler) initializePools() error {
	// Initialize configured pools
	for poolName, poolConfig := range s.config.Scheduler.Pools {
		s.initializePool(poolName, poolConfig)
	}

	// Create a default pool if no pools are configured
	if len(s.pools) == 0 {
		log.Info().Msg("no pools configured, creating default pool")
		s.initializePool("default", types.WorkerPoolConfig{
			Namespace:      "airstore",
			MinReplicas:    1,
			MaxReplicas:    10,
			ScaleDownDelay: 5 * time.Minute,
			WorkerImage:    s.config.Scheduler.WorkerImage,
		})
	}

	return nil
}

// initializePool creates a pool controller, task queue, and scaler for a single pool
func (s *Scheduler) initializePool(poolName string, poolConfig types.WorkerPoolConfig) {
	log.Info().
		Str("pool_name", poolName).
		Str("deployment", poolConfig.DeploymentName).
		Int32("min_replicas", poolConfig.MinReplicas).
		Int32("max_replicas", poolConfig.MaxReplicas).
		Msg("initializing worker pool")

	// Ensure worker token exists for this pool
	s.ensureWorkerToken(poolName)

	// Create pool controller for monitoring
	s.pools[poolName] = NewWorkerPoolController(
		s.ctx,
		poolName,
		poolConfig,
		s.config,
		s.workerRepo,
	)

	// Create task queue for this pool
	taskQueue := repository.NewRedisTaskQueue(s.redisClient, poolName)
	if s.taskQueue == nil {
		s.taskQueue = taskQueue
	}

	// Create and start pool scaler
	s.initializeScaler(poolName, poolConfig, taskQueue)
}

// ensureWorkerToken creates a worker token for the pool if one doesn't exist
func (s *Scheduler) ensureWorkerToken(poolName string) {
	if s.backendRepo == nil {
		log.Debug().Str("pool", poolName).Msg("no backend repo, skipping worker token provisioning")
		return
	}

	// Check if a token already exists for this pool
	tokens, err := s.backendRepo.ListWorkerTokens(s.ctx)
	if err != nil {
		log.Error().Err(err).Str("pool", poolName).Msg("failed to list worker tokens")
		return
	}

	for _, t := range tokens {
		if t.PoolName != nil && *t.PoolName == poolName {
			log.Debug().Str("pool", poolName).Msg("worker token already exists")
			// We don't have the raw token, can't use existing tokens
			// TODO: store raw tokens in k8s secret instead
			return
		}
	}

	// Create new worker token for this pool
	tokenName := fmt.Sprintf("pool-%s", poolName)
	_, rawToken, err := s.backendRepo.CreateWorkerToken(s.ctx, tokenName, &poolName, nil)
	if err != nil {
		log.Error().Err(err).Str("pool", poolName).Msg("failed to create worker token")
		return
	}

	s.workerTokens[poolName] = rawToken
	log.Info().Str("pool", poolName).Msg("created worker token for pool")
}

// initializeScaler creates and starts a pool scaler
func (s *Scheduler) initializeScaler(poolName string, poolConfig types.WorkerPoolConfig, taskQueue repository.TaskQueue) {
	scalerConfig := s.buildScalerConfig(poolName, poolConfig)

	scaler, err := NewPoolScaler(s.ctx, scalerConfig, taskQueue)
	if err != nil {
		log.Warn().Err(err).Str("pool_name", poolName).Msg("failed to create pool scaler (not in k8s?)")
		return
	}

	if err := scaler.EnsureDeployment(); err != nil {
		log.Error().Err(err).Str("pool_name", poolName).Msg("failed to ensure worker deployment")
	}

	s.scalers[poolName] = scaler
	go scaler.Start()
}

// buildScalerConfig creates a PoolScalerConfig from pool configuration
func (s *Scheduler) buildScalerConfig(poolName string, poolConfig types.WorkerPoolConfig) PoolScalerConfig {
	return PoolScalerConfig{
		PoolName:           poolName,
		DeploymentName:     poolConfig.DeploymentName,
		Namespace:          poolConfig.Namespace,
		MinReplicas:        poolConfig.MinReplicas,
		MaxReplicas:        poolConfig.MaxReplicas,
		ScaleDownDelay:     poolConfig.ScaleDownDelay,
		ScalingInterval:    defaultScalingInterval,
		WorkerImage:        poolConfig.WorkerImage,
		WorkerCpu:          poolConfig.Cpu,
		WorkerMemory:       poolConfig.Memory,
		GatewayServiceName: s.config.Scheduler.GatewayServiceName,
		GatewayPort:        s.config.Gateway.HTTP.Port,
		WorkerToken:        s.workerTokens[poolName],
		AppConfig:          s.config,
	}
}

// Stop gracefully stops the scheduler
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	// Stop all pool controllers
	for _, pool := range s.pools {
		pool.Stop()
	}

	// Stop all scalers
	for _, scaler := range s.scalers {
		scaler.Stop()
	}

	s.cancel()
	s.running = false
	log.Info().Msg("scheduler stopped")
	return nil
}

// RegisterWorker registers a new worker with the scheduler
func (s *Scheduler) RegisterWorker(ctx context.Context, worker *types.Worker) error {
	if worker.ID == "" {
		worker.ID = common.GenerateWorkerID()
	}
	worker.Status = types.WorkerStatusAvailable
	worker.RegisteredAt = time.Now()
	worker.LastSeenAt = time.Now()

	// Default to "default" pool if not specified
	if worker.PoolName == "" {
		worker.PoolName = "default"
	}

	log.Info().
		Str("worker_id", worker.ID).
		Str("pool_name", worker.PoolName).
		Str("hostname", worker.Hostname).
		Msg("registering worker")

	// Notify pool controller
	if pool, ok := s.pools[worker.PoolName]; ok {
		pool.OnWorkerRegistered(worker.ID)
	}

	return s.workerRepo.AddWorker(ctx, worker)
}

// DeregisterWorker removes a worker from the scheduler
func (s *Scheduler) DeregisterWorker(ctx context.Context, workerId string) error {
	log.Info().Str("worker_id", workerId).Msg("deregistering worker")
	return s.workerRepo.RemoveWorker(ctx, workerId)
}

// WorkerHeartbeat updates a worker's last seen timestamp
func (s *Scheduler) WorkerHeartbeat(ctx context.Context, workerId string) error {
	return s.workerRepo.SetWorkerKeepAlive(ctx, workerId)
}

// UpdateWorkerStatus updates a worker's status
func (s *Scheduler) UpdateWorkerStatus(ctx context.Context, workerId string, status types.WorkerStatus) error {
	return s.workerRepo.UpdateWorkerStatus(ctx, workerId, status)
}

// GetWorkers returns all registered workers
func (s *Scheduler) GetWorkers(ctx context.Context) ([]*types.Worker, error) {
	return s.workerRepo.GetAllWorkers(ctx)
}

// GetWorker returns a specific worker
func (s *Scheduler) GetWorker(ctx context.Context, workerId string) (*types.Worker, error) {
	return s.workerRepo.GetWorker(ctx, workerId)
}

// GetAvailableWorkers returns workers that are available
func (s *Scheduler) GetAvailableWorkers(ctx context.Context) ([]*types.Worker, error) {
	return s.workerRepo.GetAvailableWorkers(ctx)
}

// GetPool returns a pool controller by name
func (s *Scheduler) GetPool(poolName string) (*WorkerPoolController, error) {
	pool, ok := s.pools[poolName]
	if !ok {
		return nil, &types.ErrWorkerPoolNotFound{PoolName: poolName}
	}
	return pool, nil
}

// GetPools returns all pool controllers
func (s *Scheduler) GetPools() map[string]*WorkerPoolController {
	return s.pools
}

// TaskQueue returns the task queue (for submitting tasks)
func (s *Scheduler) TaskQueue() repository.TaskQueue {
	return s.taskQueue
}

// GetTaskQueue returns the task queue for a specific pool
func (s *Scheduler) GetTaskQueue(poolName string) repository.TaskQueue {
	if poolName == "" {
		poolName = "default"
	}
	return repository.NewRedisTaskQueue(s.redisClient, poolName)
}

// cleanupLoop periodically cleans up stale workers
func (s *Scheduler) cleanupLoop() {
	interval := s.config.Scheduler.CleanupInterval
	if interval == 0 {
		interval = 1 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

// cleanup removes stale workers
func (s *Scheduler) cleanup() {
	ctx := context.Background()

	workers, err := s.workerRepo.GetAllWorkers(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to get workers for cleanup")
		return
	}

	timeout := s.config.Scheduler.HeartbeatTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	for _, worker := range workers {
		if time.Since(worker.LastSeenAt) > timeout {
			log.Info().
				Str("worker_id", worker.ID).
				Time("last_seen", worker.LastSeenAt).
				Msg("removing stale worker")

			if err := s.workerRepo.RemoveWorker(ctx, worker.ID); err != nil {
				log.Warn().Err(err).Str("worker_id", worker.ID).Msg("failed to remove stale worker")
			}
		}
	}
}

// monitorWorkersLoop monitors worker health
func (s *Scheduler) monitorWorkersLoop() {
	interval := s.config.Scheduler.HeartbeatInterval
	if interval == 0 {
		interval = 10 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.monitorWorkers()
		}
	}
}

// monitorWorkers checks worker health and updates status
func (s *Scheduler) monitorWorkers() {
	ctx := context.Background()

	workers, err := s.workerRepo.GetAllWorkers(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to get workers for monitoring")
		return
	}

	timeout := s.config.Scheduler.HeartbeatTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	for _, worker := range workers {
		if worker.Status != types.WorkerStatusOffline && time.Since(worker.LastSeenAt) > timeout {
			log.Warn().
				Str("worker_id", worker.ID).
				Time("last_seen", worker.LastSeenAt).
				Msg("worker went offline")

			s.workerRepo.UpdateWorkerStatus(ctx, worker.ID, types.WorkerStatusOffline)
		}
	}
}

// WorkerRepo returns the worker repository (for service layer access)
func (s *Scheduler) WorkerRepo() repository.WorkerRepository {
	return s.workerRepo
}
