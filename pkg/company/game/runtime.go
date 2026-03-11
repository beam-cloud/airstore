package game

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/company"
)

type Manager struct {
	mu       sync.Mutex
	store    *Store
	runtimes map[string]*Runtime
}

type Runtime struct {
	mu          sync.RWMutex
	workspaceID string
	state       *WorldState
	store       *Store
	fanout      *Fanout
}

func NewManager(store *Store) *Manager {
	return &Manager{
		store:    store,
		runtimes: make(map[string]*Runtime),
	}
}

func (m *Manager) SyncCompanySnapshot(ctx context.Context, workspaceID string, snapshot *company.CompanySnapshot) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	if snapshot == nil {
		return nil, nil, fmt.Errorf("company snapshot is required")
	}
	runtime, err := m.ensureRuntime(ctx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	return runtime.SyncCompanySnapshot(ctx, snapshot)
}

func (m *Manager) Snapshot(ctx context.Context, workspaceID string) (*company.CompanyWorldSnapshot, error) {
	runtime, err := m.ensureRuntime(ctx, workspaceID)
	if err != nil {
		return nil, err
	}
	runtime.mu.RLock()
	defer runtime.mu.RUnlock()
	if runtime.state == nil {
		return nil, nil
	}
	return runtime.state.Snapshot, nil
}

func (m *Manager) Subscribe(ctx context.Context, workspaceID string) (<-chan StreamMessage, func(), error) {
	runtime, err := m.ensureRuntime(ctx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	ch, cancel := runtime.fanout.Subscribe(32)
	return ch, cancel, nil
}

func (m *Manager) RecordActivity(ctx context.Context, workspaceID, channel, message, entityID string) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	runtime, err := m.ensureRuntime(ctx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	return runtime.RecordActivity(ctx, channel, message, entityID)
}

func (m *Manager) RecordActionResults(ctx context.Context, workspaceID string, results []ActionResultEnvelope) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	runtime, err := m.ensureRuntime(ctx, workspaceID)
	if err != nil {
		return nil, nil, err
	}
	return runtime.RecordActionResults(ctx, results)
}

func (m *Manager) ensureRuntime(ctx context.Context, workspaceID string) (*Runtime, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if existing, ok := m.runtimes[workspaceID]; ok {
		return existing, nil
	}

	var checkpoint *company.CompanyWorldSnapshot
	if m.store != nil {
		var err error
		checkpoint, err = m.store.LoadCheckpoint(ctx, workspaceID)
		if err != nil {
			return nil, err
		}
	}

	runtime := &Runtime{
		workspaceID: workspaceID,
		store:       m.store,
		fanout:      NewFanout(),
		state: &WorldState{
			WorkspaceID: workspaceID,
			Snapshot:    checkpoint,
		},
	}
	if checkpoint != nil {
		runtime.state.Version = checkpoint.Version
		runtime.state.Sequence = checkpoint.Version
		runtime.state.Activity = checkpoint.Activity
	}
	m.runtimes[workspaceID] = runtime
	return runtime, nil
}

func (r *Runtime) SyncCompanySnapshot(ctx context.Context, business *company.CompanySnapshot) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	var previousBusiness *company.CompanySnapshot
	var previousSnapshot *company.CompanyWorldSnapshot
	if r.state != nil {
		previousBusiness = r.state.Business
		previousSnapshot = r.state.Snapshot
	}

	version := int64(1)
	sequence := int64(1)
	if r.state != nil {
		version = r.state.Version + 1
		sequence = r.state.Sequence + 1
	}

	activity, newEvents := BuildActivityFeed(previousBusiness, business, r.currentActivity())
	snapshot, _ := ProjectSnapshot(r.workspaceID, version, business, activity)
	delta := DiffWorld(previousSnapshot, snapshot, sequence, newEvents)

	r.state = &WorldState{
		WorkspaceID: r.workspaceID,
		Version:     version,
		Sequence:    sequence,
		Business:    business,
		Snapshot:    snapshot,
		Activity:    activity,
	}

	if r.store != nil {
		_ = r.store.SaveCheckpoint(ctx, r.workspaceID, snapshot)
		_ = r.store.AppendEvent(ctx, r.workspaceID, Event{
			ID:          fmt.Sprintf("world-sync:%s:%d", r.workspaceID, nowMs()),
			WorkspaceID: r.workspaceID,
			Type:        EventTypeWorldSynced,
			Message:     "world snapshot synced",
			Channel:     "system",
			Timestamp:   nowMs(),
			Metadata: map[string]any{
				"version": version,
				"agents":  len(business.Agents),
				"tasks":   len(business.RunningTasks),
			},
		})
	}

	r.fanout.Publish(StreamMessage{
		Event:     "world_delta",
		Delta:     delta,
		Timestamp: nowMs(),
	})
	return snapshot, delta, nil
}

func (r *Runtime) RecordActivity(ctx context.Context, channel, message, entityID string) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == nil || r.state.Snapshot == nil {
		return nil, nil, nil
	}

	event := company.ActivityFeedEvent{
		ID:        fmt.Sprintf("activity:%s:%d", r.workspaceID, nowMs()),
		Kind:      "activity",
		Channel:   channel,
		Message:   message,
		EntityID:  entityID,
		Timestamp: nowMs(),
	}

	activity := append(r.currentActivity(), event)
	activity = trimActivity(activity)

	snapshot := *r.state.Snapshot
	snapshot.Activity = activity
	hud := snapshot.Hud
	hud.Tick = nowMs()
	snapshot.Hud = hud

	r.state.Activity = activity
	r.state.Snapshot = &snapshot

	if r.store != nil {
		_ = r.store.SaveCheckpoint(ctx, r.workspaceID, &snapshot)
		_ = r.store.AppendEvent(ctx, r.workspaceID, Event{
			ID:          event.ID,
			WorkspaceID: r.workspaceID,
			Type:        EventTypeActivity,
			Message:     event.Message,
			Channel:     channel,
			EntityID:    entityID,
			Timestamp:   event.Timestamp,
		})
	}

	delta := &company.CompanyWorldDelta{
		Sequence:    r.nextSequence(),
		GeneratedAt: nowMs(),
		Activity:    []company.ActivityFeedEvent{event},
		Hud:         &hud,
	}
	r.fanout.Publish(StreamMessage{
		Event:     "world_delta",
		Delta:     delta,
		Timestamp: nowMs(),
	})

	return &snapshot, delta, nil
}

func (r *Runtime) RecordActionResults(ctx context.Context, results []ActionResultEnvelope) (*company.CompanyWorldSnapshot, *company.CompanyWorldDelta, error) {
	if len(results) == 0 {
		return nil, nil, nil
	}
	var lastSnapshot *company.CompanyWorldSnapshot
	var lastDelta *company.CompanyWorldDelta
	for _, result := range results {
		channel := "system"
		if result.Status == "error" {
			channel = "warning"
		}
		message := fmt.Sprintf("%s: %s", stringsTitle(result.Type), result.Status)
		if result.Description != "" {
			message = fmt.Sprintf("%s - %s", message, result.Description)
		}
		snapshot, delta, err := r.RecordActivity(ctx, channel, message, "")
		if err != nil {
			return nil, nil, err
		}
		lastSnapshot = snapshot
		lastDelta = delta
	}
	return lastSnapshot, lastDelta, nil
}

func (r *Runtime) currentActivity() []company.ActivityFeedEvent {
	if r.state == nil {
		return nil
	}
	return append([]company.ActivityFeedEvent(nil), r.state.Activity...)
}

func (r *Runtime) nextSequence() int64 {
	if r.state == nil {
		return 1
	}
	r.state.Sequence++
	return r.state.Sequence
}

func nowMs() int64 {
	return time.Now().UnixMilli()
}

func stringsTitle(value string) string {
	switch value {
	case "":
		return "Action"
	default:
		return value
	}
}
