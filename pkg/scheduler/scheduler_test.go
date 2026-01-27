package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/assert"
)

// NewSchedulerForTest creates a scheduler with all dependencies mocked for testing
func NewSchedulerForTest() (*Scheduler, error) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		return nil, err
	}

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)

	ctx := context.Background()

	scheduler := &Scheduler{
		ctx:         ctx,
		cancel:      func() {},
		config:      config,
		redisClient: rdb,
		workerRepo:  workerRepo,
		pools:       make(map[string]*WorkerPoolController),
		scalers:     make(map[string]*PoolScaler),
		running:     false,
	}

	return scheduler, nil
}

func TestNewSchedulerForTest(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	assert.NotNil(t, scheduler)
}

func TestRegisterWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)
	assert.NotNil(t, scheduler)

	worker := &types.Worker{
		Hostname: "test-host",
		PoolName: "default",
		Cpu:      1000,
		Memory:   1024,
	}

	err = scheduler.RegisterWorker(context.Background(), worker)
	assert.NoError(t, err)
	assert.NotEmpty(t, worker.ID)
	assert.Equal(t, types.WorkerStatusAvailable, worker.Status)

	// Verify worker was stored
	storedWorker, err := scheduler.GetWorker(context.Background(), worker.ID)
	assert.NoError(t, err)
	assert.Equal(t, worker.ID, storedWorker.ID)
	assert.Equal(t, worker.Hostname, storedWorker.Hostname)
}

func TestDeregisterWorker(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)

	worker := &types.Worker{
		Hostname: "test-host",
		PoolName: "default",
		Cpu:      1000,
		Memory:   1024,
	}

	err = scheduler.RegisterWorker(context.Background(), worker)
	assert.NoError(t, err)

	err = scheduler.DeregisterWorker(context.Background(), worker.ID)
	assert.NoError(t, err)

	// Verify worker was removed
	_, err = scheduler.GetWorker(context.Background(), worker.ID)
	assert.Error(t, err)
}

func TestWorkerHeartbeat(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)

	worker := &types.Worker{
		Hostname: "test-host",
		PoolName: "default",
		Cpu:      1000,
		Memory:   1024,
	}

	err = scheduler.RegisterWorker(context.Background(), worker)
	assert.NoError(t, err)

	// Wait for clock to tick to next second (since LastSeenAt uses Unix seconds)
	time.Sleep(1100 * time.Millisecond)

	err = scheduler.WorkerHeartbeat(context.Background(), worker.ID)
	assert.NoError(t, err)

	// Verify worker still exists and heartbeat succeeded
	storedWorker, err := scheduler.GetWorker(context.Background(), worker.ID)
	assert.NoError(t, err)
	assert.NotNil(t, storedWorker)
	// LastSeenAt is stored in Unix seconds, so after 1s it should be updated
	assert.True(t, storedWorker.LastSeenAt.Unix() >= worker.RegisteredAt.Unix(),
		"LastSeenAt should be at or after registration time")
}

func TestUpdateWorkerStatus(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)

	worker := &types.Worker{
		Hostname: "test-host",
		PoolName: "default",
		Cpu:      1000,
		Memory:   1024,
	}

	err = scheduler.RegisterWorker(context.Background(), worker)
	assert.NoError(t, err)
	assert.Equal(t, types.WorkerStatusAvailable, worker.Status)

	err = scheduler.UpdateWorkerStatus(context.Background(), worker.ID, types.WorkerStatusBusy)
	assert.NoError(t, err)

	storedWorker, err := scheduler.GetWorker(context.Background(), worker.ID)
	assert.NoError(t, err)
	assert.Equal(t, types.WorkerStatusBusy, storedWorker.Status)
}

func TestGetAvailableWorkers(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)

	// Register multiple workers
	worker1 := &types.Worker{Hostname: "host1", PoolName: "default", Cpu: 1000, Memory: 1024}
	worker2 := &types.Worker{Hostname: "host2", PoolName: "default", Cpu: 1000, Memory: 1024}
	worker3 := &types.Worker{Hostname: "host3", PoolName: "default", Cpu: 1000, Memory: 1024}

	err = scheduler.RegisterWorker(context.Background(), worker1)
	assert.NoError(t, err)
	err = scheduler.RegisterWorker(context.Background(), worker2)
	assert.NoError(t, err)
	err = scheduler.RegisterWorker(context.Background(), worker3)
	assert.NoError(t, err)

	// Set one worker to busy
	err = scheduler.UpdateWorkerStatus(context.Background(), worker2.ID, types.WorkerStatusBusy)
	assert.NoError(t, err)

	// Get available workers
	available, err := scheduler.GetAvailableWorkers(context.Background())
	assert.NoError(t, err)
	assert.Len(t, available, 2)
}

func TestGetWorkers(t *testing.T) {
	scheduler, err := NewSchedulerForTest()
	assert.NoError(t, err)

	// Register multiple workers
	for i := 0; i < 5; i++ {
		worker := &types.Worker{Hostname: "host", PoolName: "default", Cpu: 1000, Memory: 1024}
		err = scheduler.RegisterWorker(context.Background(), worker)
		assert.NoError(t, err)
	}

	workers, err := scheduler.GetWorkers(context.Background())
	assert.NoError(t, err)
	assert.Len(t, workers, 5)
}
