package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

type ExecutionInstanceState struct {
	RunningAttempts  int
	PendingAttempts  int
	StoppingAttempts int
	FailedAttempts   []string
}

type ExecutionInstanceConfig struct {
	InstanceKey            string
	WorkspaceID            uint
	AgentID                *string
	Lane                   *string
	ExecutionClassKey      string
	FailedAttemptThreshold int
	InstanceLockKey        string
}

type ExecutionInstance struct {
	Ctx        context.Context
	CancelFunc context.CancelFunc
	IsActive   bool

	AttemptEventChan  chan AttemptEvent
	DispatchEventChan chan int

	InstanceLockKey        string
	FailedAttemptThreshold int

	mu                    sync.RWMutex
	instanceKey           string
	store                 ExecutionInstanceStore
	locker                InstanceDispatchLocker
	state                 ExecutionInstanceState
	status                types.AgentExecutionInstanceStatus
	desiredDispatchTarget int
}

type ExecutionInstanceStore interface {
	GetOrCreateExecutionInstance(ctx context.Context, instance *types.AgentExecutionInstance) (*types.AgentExecutionInstance, error)
	GetExecutionInstanceByKey(ctx context.Context, instanceKey string) (*types.AgentExecutionInstance, error)
	UpdateExecutionInstanceState(
		ctx context.Context,
		instanceKey string,
		runningAttempts int,
		pendingAttempts int,
		stoppingAttempts int,
		desiredDispatchConcurrency int,
		status types.AgentExecutionInstanceStatus,
		lastSyncAt *time.Time,
	) error
}

type InstanceDispatchLocker interface {
	WithInstanceLock(ctx context.Context, lockKey string, fn func() error) error
}

func NewExecutionInstance(
	ctx context.Context,
	cfg ExecutionInstanceConfig,
	store ExecutionInstanceStore,
	locker InstanceDispatchLocker,
) (*ExecutionInstance, error) {
	if store == nil {
		return nil, fmt.Errorf("instance store is required")
	}
	if cfg.InstanceKey == "" {
		return nil, fmt.Errorf("instance_key is required")
	}
	if cfg.FailedAttemptThreshold <= 0 {
		cfg.FailedAttemptThreshold = 5
	}
	if cfg.InstanceLockKey == "" {
		cfg.InstanceLockKey = cfg.InstanceKey
	}

	instCtx, cancel := context.WithCancel(ctx)
	inst := &ExecutionInstance{
		Ctx:                    instCtx,
		CancelFunc:             cancel,
		IsActive:               true,
		AttemptEventChan:       make(chan AttemptEvent, 256),
		DispatchEventChan:      make(chan int, 64),
		InstanceLockKey:        cfg.InstanceLockKey,
		FailedAttemptThreshold: cfg.FailedAttemptThreshold,
		instanceKey:            cfg.InstanceKey,
		store:                  store,
		locker:                 locker,
		status:                 types.AgentExecutionInstanceStatusHealthy,
	}

	_, err := store.GetOrCreateExecutionInstance(ctx, &types.AgentExecutionInstance{
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
	})
	if err != nil {
		return nil, err
	}

	go inst.loop()
	return inst, nil
}

func (e *ExecutionInstance) loop() {
	for {
		select {
		case <-e.Ctx.Done():
			e.IsActive = false
			return
		case ev := <-e.AttemptEventChan:
			e.applyAttemptEvent(ev)
		case target := <-e.DispatchEventChan:
			if err := e.HandleDispatchEvent(target); err != nil {
				log.Warn().
					Err(err).
					Str("instance_key", e.instanceKey).
					Int("target", target).
					Msg("failed to handle dispatch event")
			}
		}
	}
}

func (e *ExecutionInstance) applyAttemptEvent(ev AttemptEvent) {
	e.mu.Lock()
	defer e.mu.Unlock()

	switch {
	case ev.Change > 0:
		e.state.RunningAttempts += ev.Change
		if e.state.RunningAttempts < 0 {
			e.state.RunningAttempts = 0
		}
	case ev.Change < 0:
		e.state.RunningAttempts += ev.Change
		if e.state.RunningAttempts < 0 {
			e.state.RunningAttempts = 0
		}
	}

	switch ev.Status {
	case string(types.AgentAttemptStatusError), string(types.AgentAttemptStatusTimeout), string(types.AgentAttemptStatusCancelled):
		e.state.FailedAttempts = append(e.state.FailedAttempts, ev.AttemptID)
		if len(e.state.FailedAttempts) > 128 {
			e.state.FailedAttempts = e.state.FailedAttempts[len(e.state.FailedAttempts)-128:]
		}
	}

	if len(e.state.FailedAttempts) >= e.FailedAttemptThreshold {
		e.status = types.AgentExecutionInstanceStatusDegraded
		e.desiredDispatchTarget = 0
	}

	now := time.Now()
	_ = e.store.UpdateExecutionInstanceState(
		e.Ctx,
		e.instanceKey,
		e.state.RunningAttempts,
		e.state.PendingAttempts,
		e.state.StoppingAttempts,
		e.desiredDispatchTarget,
		e.status,
		&now,
	)
}

func (e *ExecutionInstance) ConsumeAttemptEvent(ev AttemptEvent) {
	select {
	case e.AttemptEventChan <- ev:
	default:
		log.Warn().
			Str("instance_key", e.instanceKey).
			Msg("dropping attempt event due to full channel")
	}
}

func (e *ExecutionInstance) ConsumeDispatchTarget(target int) {
	select {
	case e.DispatchEventChan <- target:
	default:
		log.Warn().
			Str("instance_key", e.instanceKey).
			Msg("dropping dispatch target update due to full channel")
	}
}

func (e *ExecutionInstance) HandleDispatchEvent(target int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	apply := func() error {
		if len(e.state.FailedAttempts) >= e.FailedAttemptThreshold {
			target = 0
			e.status = types.AgentExecutionInstanceStatusDegraded
		}
		if target < 0 {
			target = 0
		}
		e.desiredDispatchTarget = target

		now := time.Now()
		return e.store.UpdateExecutionInstanceState(
			e.Ctx,
			e.instanceKey,
			e.state.RunningAttempts,
			e.state.PendingAttempts,
			e.state.StoppingAttempts,
			e.desiredDispatchTarget,
			e.status,
			&now,
		)
	}
	if e.locker != nil {
		return e.locker.WithInstanceLock(e.Ctx, e.InstanceLockKey, apply)
	}
	return apply()
}

func (e *ExecutionInstance) Sync() error {
	inst, err := e.store.GetExecutionInstanceByKey(e.Ctx, e.instanceKey)
	if err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	e.FailedAttemptThreshold = inst.FailedAttemptThreshold
	e.desiredDispatchTarget = inst.DesiredDispatchConcurrency
	e.state.RunningAttempts = inst.RunningAttempts
	e.state.PendingAttempts = inst.PendingAttempts
	e.state.StoppingAttempts = inst.StoppingAttempts
	e.status = inst.Status
	return nil
}

func (e *ExecutionInstance) DesiredDispatchTarget() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.desiredDispatchTarget
}
