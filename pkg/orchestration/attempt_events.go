package orchestration

import (
	"context"
	"fmt"
	"strings"
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
	defaultCtx      context.Context
	mu              sync.RWMutex
	instances       map[string]IExecutionInstance
	initGroup       singleflight.Group
	instanceFactory func(ctx context.Context, instanceKey string) (IExecutionInstance, error)
}

func NewAttemptEventManager(
	defaultCtx context.Context,
	instanceFactory func(ctx context.Context, instanceKey string) (IExecutionInstance, error),
) *AttemptEventManager {
	if defaultCtx == nil {
		defaultCtx = context.Background()
	}
	return &AttemptEventManager{
		defaultCtx:      defaultCtx,
		instances:       make(map[string]IExecutionInstance),
		instanceFactory: instanceFactory,
	}
}

func (m *AttemptEventManager) getOrCreate(ctx context.Context, instanceKey string) (IExecutionInstance, error) {
	instanceKey = strings.TrimSpace(instanceKey)
	if instanceKey == "" {
		return nil, fmt.Errorf("instance_key is required")
	}
	if ctx == nil {
		ctx = m.defaultCtx
	}

	m.mu.RLock()
	existing, ok := m.instances[instanceKey]
	m.mu.RUnlock()
	if ok {
		return existing, nil
	}

	factoryCtx := context.WithoutCancel(ctx)
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
		instance, err := m.instanceFactory(factoryCtx, instanceKey)
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

func (m *AttemptEventManager) RouteAttemptEvent(ctx context.Context, instanceKey string, event AttemptEvent) error {
	instance, err := m.getOrCreate(ctx, instanceKey)
	if err != nil {
		return err
	}
	if event.Ts == 0 {
		event.Ts = time.Now().UnixMilli()
	}
	instance.ConsumeAttemptEvent(event)
	return nil
}

func (m *AttemptEventManager) RouteDispatchTarget(ctx context.Context, instanceKey string, target int) error {
	instance, err := m.getOrCreate(ctx, instanceKey)
	if err != nil {
		return err
	}
	instance.ConsumeDispatchTarget(target)
	return nil
}
