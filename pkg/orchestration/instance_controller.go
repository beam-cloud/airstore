package orchestration

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

type ExecutionInstanceController struct {
	store               ExecutionInstanceStore
	locker              InstanceDispatchLocker
	instanceLockKeyFunc func(string) string
	attemptEvents       *AttemptEventManager
}

func NewExecutionInstanceController(
	ctx context.Context,
	store ExecutionInstanceStore,
	locker InstanceDispatchLocker,
	instanceLockKeyFunc func(string) string,
) *ExecutionInstanceController {
	if instanceLockKeyFunc == nil {
		instanceLockKeyFunc = func(instanceKey string) string {
			return instanceKey
		}
	}
	controller := &ExecutionInstanceController{
		store:               store,
		locker:              locker,
		instanceLockKeyFunc: instanceLockKeyFunc,
	}
	controller.attemptEvents = NewAttemptEventManager(ctx, func(factoryCtx context.Context, instanceKey string) (IExecutionInstance, error) {
		if store == nil {
			return nil, fmt.Errorf("instance store is required")
		}
		inst, err := store.GetExecutionInstanceByKey(factoryCtx, instanceKey)
		if err != nil {
			return nil, err
		}
		return NewExecutionInstance(factoryCtx, ExecutionInstanceConfig{
			InstanceKey:            inst.InstanceKey,
			WorkspaceID:            inst.WorkspaceID,
			AgentID:                inst.AgentID,
			Lane:                   inst.Lane,
			ExecutionClassKey:      inst.ExecutionClassKey,
			FailedAttemptThreshold: inst.FailedAttemptThreshold,
			InstanceLockKey:        instanceLockKeyFunc(inst.InstanceKey),
		}, store, locker)
	})
	return controller
}

func (c *ExecutionInstanceController) ensureReady() error {
	if c.store == nil {
		return fmt.Errorf("instance store is required")
	}
	if c.attemptEvents == nil {
		return fmt.Errorf("attempt event manager is not configured")
	}
	return nil
}

func (c *ExecutionInstanceController) EnsureInstance(ctx context.Context, cfg ExecutionInstanceConfig) (IExecutionInstance, error) {
	if err := c.ensureReady(); err != nil {
		return nil, err
	}
	cfg.InstanceKey = strings.TrimSpace(cfg.InstanceKey)
	if cfg.InstanceKey == "" {
		return nil, fmt.Errorf("instance_key is required")
	}
	if cfg.FailedAttemptThreshold <= 0 {
		cfg.FailedAttemptThreshold = defaultFailedAttemptThreshold
	}
	if strings.TrimSpace(cfg.ExecutionClassKey) == "" {
		cfg.ExecutionClassKey = cfg.InstanceKey
	}
	if strings.TrimSpace(cfg.InstanceLockKey) == "" {
		cfg.InstanceLockKey = c.instanceLockKeyFunc(cfg.InstanceKey)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, err := c.store.GetOrCreateExecutionInstance(ctx, &types.AgentExecutionInstance{
		InstanceKey:                cfg.InstanceKey,
		WorkspaceID:                cfg.WorkspaceID,
		AgentID:                    cfg.AgentID,
		Lane:                       cfg.Lane,
		ExecutionClassKey:          cfg.ExecutionClassKey,
		PoolName:                   "default",
		Active:                     true,
		Status:                     types.AgentExecutionInstanceStatusHealthy,
		FailedAttemptThreshold:     cfg.FailedAttemptThreshold,
		DesiredDispatchConcurrency: 0,
		RunningAttempts:            0,
		PendingAttempts:            0,
		StoppingAttempts:           0,
	}); err != nil {
		return nil, err
	}
	return c.attemptEvents.getOrCreate(ctx, cfg.InstanceKey)
}

func (c *ExecutionInstanceController) RouteAttemptEvent(ctx context.Context, instanceKey string, event AttemptEvent) error {
	if err := c.ensureReady(); err != nil {
		return err
	}
	return c.attemptEvents.RouteAttemptEvent(ctx, instanceKey, event)
}

func (c *ExecutionInstanceController) RouteDispatchTarget(ctx context.Context, instanceKey string, target int) error {
	if err := c.ensureReady(); err != nil {
		return err
	}
	return c.attemptEvents.RouteDispatchTarget(ctx, instanceKey, target)
}
