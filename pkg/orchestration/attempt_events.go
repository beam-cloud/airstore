package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

type AttemptEvent struct {
	AttemptID string
	RunID     string
	Change    int
	Status    string
	Ts        int64
}

type IExecutionInstance interface {
	ConsumeAttemptEvent(AttemptEvent)
	ConsumeDispatchTarget(int)
	HandleDispatchEvent(int) error
	Sync() error
}

type AttemptEventManager struct {
	ctx             context.Context
	mu              sync.RWMutex
	instances       map[string]IExecutionInstance
	initGroup       singleflight.Group
	instanceFactory func(ctx context.Context, instanceKey string) (IExecutionInstance, error)
}

func NewAttemptEventManager(
	ctx context.Context,
	instanceFactory func(ctx context.Context, instanceKey string) (IExecutionInstance, error),
) *AttemptEventManager {
	return &AttemptEventManager{
		ctx:             ctx,
		instances:       make(map[string]IExecutionInstance),
		instanceFactory: instanceFactory,
	}
}

func (m *AttemptEventManager) getOrCreate(instanceKey string) (IExecutionInstance, error) {
	m.mu.RLock()
	existing, ok := m.instances[instanceKey]
	m.mu.RUnlock()
	if ok {
		return existing, nil
	}
	v, err, _ := m.initGroup.Do(instanceKey, func() (any, error) {
		m.mu.RLock()
		if existing, ok := m.instances[instanceKey]; ok {
			m.mu.RUnlock()
			return existing, nil
		}
		m.mu.RUnlock()
		if m.instanceFactory == nil {
			return nil, fmt.Errorf("instance factory is not configured")
		}
		instance, err := m.instanceFactory(m.ctx, instanceKey)
		if err != nil {
			return nil, err
		}
		m.mu.Lock()
		m.instances[instanceKey] = instance
		m.mu.Unlock()
		return instance, nil
	})
	if err != nil {
		return nil, err
	}
	instance, ok := v.(IExecutionInstance)
	if !ok || instance == nil {
		return nil, fmt.Errorf("failed to initialize execution instance")
	}
	return instance, nil
}

func (m *AttemptEventManager) RouteAttemptEvent(instanceKey string, event AttemptEvent) error {
	instance, err := m.getOrCreate(instanceKey)
	if err != nil {
		return err
	}
	if event.Ts == 0 {
		event.Ts = time.Now().UnixMilli()
	}
	instance.ConsumeAttemptEvent(event)
	return nil
}

func (m *AttemptEventManager) RouteDispatchTarget(instanceKey string, target int) error {
	instance, err := m.getOrCreate(instanceKey)
	if err != nil {
		return err
	}
	instance.ConsumeDispatchTarget(target)
	return nil
}
