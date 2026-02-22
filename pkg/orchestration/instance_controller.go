package orchestration

import (
	"context"
	"fmt"
	"github.com/beam-cloud/airstore/pkg/types"
)

type ExecutionInstanceController struct {
	ctx                 context.Context
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
		ctx:                 ctx,
		store:               store,
		locker:              locker,
		instanceLockKeyFunc: instanceLockKeyFunc,
	}
	controller.attemptEvents = NewAttemptEventManager(ctx, func(factoryCtx context.Context, instanceKey string) (IExecutionInstance, error) {
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

func (c *ExecutionInstanceController) EnsureInstance(cfg ExecutionInstanceConfig) (IExecutionInstance, error) {
	if cfg.InstanceKey == "" {
		return nil, fmt.Errorf("instance_key is required")
	}
	if _, err := c.store.GetOrCreateExecutionInstance(c.ctx, &types.AgentExecutionInstance{
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
	return c.attemptEvents.getOrCreate(cfg.InstanceKey)
}

func (c *ExecutionInstanceController) RouteAttemptEvent(instanceKey string, event AttemptEvent) error {
	return c.attemptEvents.RouteAttemptEvent(instanceKey, event)
}

func (c *ExecutionInstanceController) RouteDispatchTarget(instanceKey string, target int) error {
	return c.attemptEvents.RouteDispatchTarget(instanceKey, target)
}
