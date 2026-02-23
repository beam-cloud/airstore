package worker

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/runtime"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultHeartbeatInterval time.Duration = 10 * time.Second
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
	terminalIO      repository.TerminalIORepository
	sandboxManager  *SandboxManager

	// Concurrency & shutdown
	maxConcurrentTasks int            // Worker goroutine count (derived from CPU/memory capacity)
	draining           atomic.Bool    // True when shutdown initiated, stops accepting new tasks
	activeTasks        sync.WaitGroup // Tracks in-progress tasks for graceful drain
	shutdownTimeout    time.Duration  // Max time to wait for tasks to complete
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

	// Derive max concurrent tasks from worker capacity and default task resources.
	// cpuLimit is in millicores; memoryLimit is in MiB.
	maxConcurrentTasks := computeMaxTasks(cpuLimit, memoryLimit)

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

	// Create task queue (for pulling tasks, not for logs)
	taskQueue := repository.NewRedisTaskQueue(redisClient, poolName)
	terminalIO := repository.NewRedisTerminalIORepository(redisClient)

	// Determine runtime type (default to gVisor for security)
	runtimeType := os.Getenv("RUNTIME_TYPE")
	if runtimeType == "" {
		runtimeType = types.ContainerRuntimeGvisor.String()
	}

	// Create sandbox manager
	sandboxManager, err := NewSandboxManager(ctx, Config{
		BundleDir:        os.Getenv("AIRSTORE_WORKER_BUNDLE_DIR"),
		StateDir:         os.Getenv("AIRSTORE_WORKER_STATE_DIR"),
		MountDir:         os.Getenv("AIRSTORE_WORKER_MOUNT_DIR"),
		WorkerMount:      os.Getenv("AIRSTORE_WORKER_FS_MOUNT"),
		CLIBinary:        os.Getenv("AIRSTORE_WORKER_CLI_BINARY"),
		WorkerID:         workerId,
		GatewayAddr:      gatewayGRPCAddr,
		AuthToken:        authToken,
		GatewayClient:    gatewayClient,
		EnableFilesystem: true,
		EnableNetwork:    true,
		RuntimeType:      runtimeType,
		RuntimeConfig:    runtime.Config{Type: runtimeType},
		ImageConfig:      config.Image,
		S2Token:          config.Streams.Token,
		S2Basin:          config.Streams.Basin,
		AnthropicAPIKey:  config.AnthropicAPIKey(),
		KernelAPIKey:     config.KernelAPIKey(),
	})
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create sandbox manager: %w", err)
	}

	// Get shutdown timeout from config, default to 5 minutes
	shutdownTimeout := config.Scheduler.WorkerShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = 5 * time.Minute
	}

	worker := &Worker{
		workerId:           workerId,
		poolName:           poolName,
		hostname:           hostname,
		cpuLimit:           cpuLimit,
		memoryLimit:        memoryLimit,
		gatewayGRPCAddr:    gatewayGRPCAddr,
		gatewayClient:      gatewayClient,
		config:             config,
		ctx:                ctx,
		cancel:             cancel,
		taskQueue:          taskQueue,
		terminalIO:         terminalIO,
		sandboxManager:     sandboxManager,
		maxConcurrentTasks: maxConcurrentTasks,
		shutdownTimeout:    shutdownTimeout,
	}

	return worker, nil
}

// computeMaxTasks derives how many tasks this worker can run concurrently
// based on its CPU (millicores) and memory (MiB) capacity versus default
// task resource requirements. Always returns at least 1.
func computeMaxTasks(cpuMillis, memMiB int64) int {
	memBytes := memMiB << 20 // MiB → bytes (same unit as DefaultRunExecutionMemory)

	cpuSlots := cpuMillis / types.DefaultRunExecutionCPU
	memSlots := memBytes / types.DefaultRunExecutionMemory

	n := int(min(cpuSlots, memSlots))
	if n < 1 {
		return 1
	}
	return n
}

// Run starts the worker and blocks until shutdown
func (w *Worker) Run() error {
	log.Info().
		Str("worker_id", w.workerId).
		Str("pool_name", w.poolName).
		Str("gateway_grpc_addr", w.gatewayGRPCAddr).
		Int64("cpu_limit", w.cpuLimit).
		Int64("memory_limit", w.memoryLimit).
		Int("max_concurrent_tasks", w.maxConcurrentTasks).
		Msg("worker starting")

	if err := w.register(); err != nil {
		return fmt.Errorf("failed to register: %w", err)
	}

	go w.heartbeatLoop()

	// N goroutines = N concurrent tasks. Each one independently pulls from
	// the Redis queue via BRPOP. Redis handles the fan-out: when a task
	// arrives, exactly one of the N blocked BRPOPs wins. No semaphores,
	// no dispatcher, no hand-off. The goroutine IS the concurrency slot.
	for i := 0; i < w.maxConcurrentTasks; i++ {
		go w.workerLoop()
	}

	w.listenForShutdown()
	return w.shutdown()
}

// workerLoop is the main loop for a single worker goroutine. It pulls
// tasks from the queue and executes them sequentially. N of these run
// in parallel — the concurrency is structural, not coordinated.
func (w *Worker) workerLoop() {
	for {
		if w.draining.Load() {
			return
		}

		task, err := w.taskQueue.Pop(w.ctx, w.workerId)
		if err != nil {
			if w.ctx.Err() != nil || w.draining.Load() {
				return
			}
			log.Warn().Err(err).Str("worker_id", w.workerId).Msg("failed to pop task")
			time.Sleep(time.Second)
			continue
		}
		if task == nil {
			continue // BRPOP timeout, loop and check drain
		}

		w.runTask(*task)
	}
}

// runTask wraps executeTask with activeTasks tracking. The defer
// guarantees Done() is called even if executeTask panics (Go unwinds
// defers before crashing), so activeTasks.Wait() in shutdown never hangs.
func (w *Worker) runTask(task types.RunExecution) {
	w.activeTasks.Add(1)
	defer w.activeTasks.Done()
	w.executeTask(task)
}

// executeTask runs a single task to completion: mark started → execute → record result.
func (w *Worker) executeTask(task types.RunExecution) {
	addTaskExecutionContext(
		log.Info().
			Str("worker_id", w.workerId).
			Uint("workspace_id", task.WorkspaceId),
		task,
	).Msg("received task")

	if err := w.gatewayClient.SetTaskStarted(w.ctx, task.ExternalId); err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			addTaskExecutionContext(log.Info().Err(err), task).Msg("gateway rejected task start due to terminal run state")
			if qErr := w.taskQueue.Fail(w.ctx, task.ExternalId, fmt.Errorf("task start rejected: %w", err)); qErr != nil {
				addTaskExecutionContext(log.Warn().Err(qErr), task).Msg("failed to mark skipped task as failed in queue")
			}
			return
		}
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to mark task as started")
	}

	taskCtx := w.ctx
	cancel := func() {}
	if task.TimeoutMs != nil && *task.TimeoutMs > 0 {
		taskCtx, cancel = context.WithTimeout(w.ctx, time.Duration(*task.TimeoutMs)*time.Millisecond)
	}
	defer cancel()

	var result *types.RunExecutionResult
	var err error
	if task.IsInteractive() {
		result, err = w.runInteractiveTask(taskCtx, task)
	} else {
		result, err = w.sandboxManager.RunTask(taskCtx, task)
	}

	if err != nil {
		addTaskExecutionContext(log.Error().Err(err), task).Msg("task execution failed")
		result = &types.RunExecutionResult{ID: task.ExternalId, ExitCode: -1, Error: err.Error()}
	}

	addTaskExecutionContext(
		log.Info().
			Str("worker_id", w.workerId).
			Int("exit_code", result.ExitCode),
		task,
	).Msg("task finished, returning capacity")

	w.finishTask(task, result)
}

// finishTask records the result in Redis and Postgres. Single path for both success and failure.
func (w *Worker) finishTask(task types.RunExecution, result *types.RunExecutionResult) {
	taskID := task.ExternalId
	var qErr error
	if result.ExitCode == 0 && result.Error == "" {
		qErr = w.taskQueue.Complete(w.ctx, taskID, result)
	} else {
		qErr = w.taskQueue.Fail(w.ctx, taskID, fmt.Errorf("%s", result.Error))
	}
	if qErr != nil {
		addTaskExecutionContext(log.Warn().Err(qErr), task).Msg("failed to update task queue")
	}

	if err := w.gatewayClient.SetTaskResult(w.ctx, taskID, result.ExitCode, result.Error); err != nil {
		addTaskExecutionContext(log.Warn().Err(err), task).Msg("failed to report result to gateway")
	}

	addTaskExecutionContext(log.Info().Int("exit_code", result.ExitCode), task).Msg("task finished")
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
	log.Info().
		Str("worker_id", w.workerId).
		Dur("timeout", w.shutdownTimeout).
		Msg("worker shutting down, draining tasks")

	// Set draining flag - task loop will stop accepting new tasks
	w.draining.Store(true)

	// Update status to draining so gateway knows not to send more tasks
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := w.gatewayClient.UpdateStatus(ctx, w.workerId, types.WorkerStatusDraining); err != nil {
		log.Warn().Err(err).Msg("failed to update status to draining")
	}
	cancel()

	// Wait for running tasks to complete with timeout
	done := make(chan struct{})
	go func() {
		w.activeTasks.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info().Str("worker_id", w.workerId).Msg("all tasks completed, shutting down cleanly")
	case <-time.After(w.shutdownTimeout):
		log.Warn().
			Str("worker_id", w.workerId).
			Dur("timeout", w.shutdownTimeout).
			Msg("shutdown timeout reached, force closing remaining tasks")
	}

	// Cancel context to stop any remaining loops
	w.cancel()

	// Close sandbox manager (cleanup)
	if w.sandboxManager != nil {
		if err := w.sandboxManager.Close(); err != nil {
			log.Warn().Err(err).Msg("failed to close sandbox manager")
		}
	}

	// Deregister from gateway
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

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
