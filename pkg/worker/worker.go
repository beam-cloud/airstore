package worker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	defaultHeartbeatInterval time.Duration = 10 * time.Second
	defaultShutdownTimeout   time.Duration = 30 * time.Second
)

// Worker represents a airstore worker that:
// 1. Registers with the gateway
// 2. Pulls tasks from the queue
// 3. Runs tasks in gVisor sandboxes
type Worker struct {
	workerId        string
	poolName        string
	hostname        string
	cpuLimit        int64
	memoryLimit     int64
	gatewayGRPCAddr string
	gatewayClient   *gateway.GatewayClient
	config          types.AppConfig
	ctx             context.Context
	cancel          context.CancelFunc
	taskQueue       repository.TaskQueue
	sandboxManager  *SandboxManager
}

// NewWorker creates a new Worker instance
func NewWorker() (*Worker, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Load configuration
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to load config: %w", err)
	}
	config := configManager.GetConfig()

	// Read required environment variables
	workerId := os.Getenv("WORKER_ID")
	if workerId == "" {
		cancel()
		return nil, fmt.Errorf("WORKER_ID environment variable is required")
	}

	// gRPC address for gateway communication
	gatewayGRPCAddr := os.Getenv("GATEWAY_GRPC_ADDR")
	if gatewayGRPCAddr == "" {
		// Default to k8s service DNS name for gRPC
		gatewayGRPCAddr = fmt.Sprintf("airstore-gateway.airstore.svc.cluster.local:%d", config.Gateway.GRPC.Port)
	}

	authToken := os.Getenv("AIRSTORE_TOKEN")

	poolName := os.Getenv("WORKER_POOL")
	if poolName == "" {
		poolName = "default"
	}

	hostname, err := os.Hostname()
	if err != nil {
		hostname = workerId
	}

	cpuLimit, err := strconv.ParseInt(os.Getenv("CPU_LIMIT"), 10, 64)
	if err != nil {
		cpuLimit = config.Scheduler.DefaultWorkerCpu
	}

	memoryLimit, err := strconv.ParseInt(os.Getenv("MEMORY_LIMIT"), 10, 64)
	if err != nil {
		memoryLimit = config.Scheduler.DefaultWorkerMemory
	}

	gatewayClient, err := gateway.NewGatewayClient(gatewayGRPCAddr, authToken)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create gateway client: %w", err)
	}

	redisClient, err := common.NewRedisClient(config.Database.Redis)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create redis client: %w", err)
	}

	// Create task queue
	taskQueue := repository.NewRedisTaskQueue(redisClient, poolName)

	// Determine runtime type (default to gVisor for security)
	runtimeType := os.Getenv("RUNTIME_TYPE")
	if runtimeType == "" {
		runtimeType = types.ContainerRuntimeGvisor.String()
	}

	// Create sandbox manager with CLIP image manager
	sandboxManager, err := NewSandboxManager(ctx, SandboxManagerConfig{
		RuntimeType:      runtimeType,
		WorkerID:         workerId,
		GatewayGRPCAddr:  gatewayGRPCAddr,
		AuthToken:        authToken,
		EnableFilesystem: true,
		ImageConfig:      config.Image,
		RuntimeConfig: runtime.Config{
			Type: runtimeType,
		},
		TaskQueue: taskQueue, // For streaming task logs
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create sandbox manager: %w", err)
	}

	worker := &Worker{
		workerId:        workerId,
		poolName:        poolName,
		hostname:        hostname,
		cpuLimit:        cpuLimit,
		memoryLimit:     memoryLimit,
		gatewayGRPCAddr: gatewayGRPCAddr,
		gatewayClient:   gatewayClient,
		config:          config,
		ctx:             ctx,
		cancel:          cancel,
		taskQueue:       taskQueue,
		sandboxManager:  sandboxManager,
	}

	return worker, nil
}

// Run starts the worker and blocks until shutdown
func (w *Worker) Run() error {
	log.Info().
		Str("worker_id", w.workerId).
		Str("pool_name", w.poolName).
		Str("gateway_grpc_addr", w.gatewayGRPCAddr).
		Int64("cpu_limit", w.cpuLimit).
		Int64("memory_limit", w.memoryLimit).
		Msg("worker starting")

	// Register with gateway
	if err := w.register(); err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	// Start heartbeat loop
	go w.heartbeatLoop()

	// Start task loop
	go w.taskLoop()

	// Listen for shutdown signals
	w.listenForShutdown()

	return w.shutdown()
}

// taskLoop pulls tasks from the queue and executes them
func (w *Worker) taskLoop() {
	log.Info().Str("worker_id", w.workerId).Msg("starting task loop")

	for {
		select {
		case <-w.ctx.Done():
			log.Info().Str("worker_id", w.workerId).Msg("task loop stopped")
			return
		default:
		}

		// Pop task from queue (blocks up to 5 seconds)
		task, err := w.taskQueue.Pop(w.ctx, w.workerId)
		if err != nil {
			log.Warn().Err(err).Str("worker_id", w.workerId).Msg("failed to pop task")
			time.Sleep(1 * time.Second)
			continue
		}

		if task == nil {
			// No task available, continue waiting
			continue
		}

		log.Info().
			Str("worker_id", w.workerId).
			Str("task_id", task.ExternalId).
			Uint("workspace_id", task.WorkspaceId).
			Msg("received task")

		// Execute the task in a sandbox
		result, err := w.sandboxManager.RunTask(w.ctx, *task)
		if err != nil {
			log.Error().Err(err).
				Str("worker_id", w.workerId).
				Str("task_id", task.ExternalId).
				Msg("task execution failed")

			// Mark task as failed
			w.taskQueue.Fail(w.ctx, task.ExternalId, err)
			continue
		}

		// Mark task as complete in Redis queue
		if err := w.taskQueue.Complete(w.ctx, task.ExternalId, result); err != nil {
			log.Warn().Err(err).Str("task_id", task.ExternalId).Msg("failed to complete task in queue")
		}

		// Report result to gateway (persists to Postgres)
		if err := w.reportTaskResult(task.ExternalId, result); err != nil {
			log.Warn().Err(err).Str("task_id", task.ExternalId).Msg("failed to report task result to gateway")
		}

		log.Info().
			Str("worker_id", w.workerId).
			Str("task_id", task.ExternalId).
			Int("exit_code", result.ExitCode).
			Dur("duration", result.Duration).
			Msg("task completed")
	}
}

// reportTaskResult reports the task result to the gateway for persistence
func (w *Worker) reportTaskResult(taskID string, result *types.TaskResult) error {
	return w.gatewayClient.SetTaskResult(w.ctx, taskID, result.ExitCode, result.Error)
}

// register registers the worker with the gateway
func (w *Worker) register() error {
	resp, err := w.gatewayClient.RegisterWorker(w.ctx, &gateway.RegisterWorkerRequest{
		Hostname: w.hostname,
		PoolName: w.poolName,
		Cpu:      w.cpuLimit,
		Memory:   w.memoryLimit,
		Version:  "1.0.0",
	})
	if err != nil {
		return err
	}

	// Update worker ID if gateway assigned a different one
	if resp.WorkerID != "" && resp.WorkerID != w.workerId {
		log.Info().
			Str("old_worker_id", w.workerId).
			Str("new_worker_id", resp.WorkerID).
			Msg("gateway assigned new worker id")
		w.workerId = resp.WorkerID
	}

	log.Info().
		Str("worker_id", w.workerId).
		Msg("worker registered with gateway")

	return nil
}

// heartbeatLoop sends periodic heartbeats to the gateway
func (w *Worker) heartbeatLoop() {
	ticker := time.NewTicker(defaultHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.gatewayClient.Heartbeat(w.ctx, w.workerId); err != nil {
				log.Warn().Err(err).Str("worker_id", w.workerId).Msg("heartbeat failed")

				// Check if worker was removed
				notFoundErr := &types.ErrWorkerNotFound{}
				if notFoundErr.From(err) {
					log.Error().Str("worker_id", w.workerId).Msg("worker not found, shutting down")
					w.cancel()
					return
				}
			} else {
				log.Debug().Str("worker_id", w.workerId).Msg("heartbeat sent")
			}
		}
	}
}

// listenForShutdown waits for termination signals
func (w *Worker) listenForShutdown() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	select {
	case s := <-sig:
		log.Info().Str("signal", s.String()).Msg("received shutdown signal")
	case <-w.ctx.Done():
		log.Info().Msg("context cancelled")
	}
}

// shutdown gracefully shuts down the worker
func (w *Worker) shutdown() error {
	log.Info().Str("worker_id", w.workerId).Msg("worker shutting down")

	// Cancel context to stop loops
	w.cancel()

	// Close sandbox manager (stops all sandboxes)
	if w.sandboxManager != nil {
		if err := w.sandboxManager.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close sandbox manager")
		}
	}

	// Create a timeout context for deregistration
	ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
	defer cancel()

	// Update status to draining
	if err := w.gatewayClient.UpdateStatus(ctx, w.workerId, types.WorkerStatusDraining); err != nil {
		log.Warn().Err(err).Msg("failed to update status to draining")
	}

	// Deregister from gateway
	if err := w.gatewayClient.Deregister(ctx, w.workerId); err != nil {
		log.Warn().Err(err).Msg("failed to deregister from gateway")
	} else {
		log.Info().Str("worker_id", w.workerId).Msg("worker deregistered from gateway")
	}

	// Close gRPC connection
	if w.gatewayClient != nil {
		if err := w.gatewayClient.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close gateway client")
		}
	}

	log.Info().Str("worker_id", w.workerId).Msg("worker stopped")
	return nil
}

// WorkerId returns the worker's ID
func (w *Worker) WorkerId() string {
	return w.workerId
}

// PoolName returns the worker's pool name
func (w *Worker) PoolName() string {
	return w.poolName
}
