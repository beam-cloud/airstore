package company

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/rs/zerolog/log"
)

const (
	streamHeartbeatInterval = 2 * time.Second
	streamSnapshotInterval  = 3 * time.Second
	activityPulseInterval   = 500 * time.Millisecond
)

// StreamCompanyState sends SSE events with the aggregated company state.
// Sends full snapshots on interval plus lightweight activity pulses for smooth animations.
func StreamCompanyState(
	ctx context.Context,
	w http.ResponseWriter,
	flusher http.Flusher,
	workspaceID uint,
	agentAPI *orchestration.AgentAPI,
	backend repository.BackendRepository,
	integrations repository.IntegrationRepository,
	store *repository.OrchestrationStore,
) {
	writeSSE := func(evt CompanyStreamEvent) {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	var lastSnapshot *CompanySnapshot

	snap, err := BuildSnapshot(ctx, workspaceID, agentAPI, backend, integrations)
	if err != nil {
		log.Error().Err(err).Msg("company stream: initial snapshot failed")
		writeSSE(CompanyStreamEvent{
			Event:     CompanyStreamEventHeartbeat,
			Timestamp: nowMs(),
		})
	} else {
		lastSnapshot = snap
		writeSSE(CompanyStreamEvent{
			Event:     CompanyStreamEventSnapshot,
			Snapshot:  snap,
			Timestamp: nowMs(),
		})
	}

	heartbeat := time.NewTicker(streamHeartbeatInterval)
	defer heartbeat.Stop()

	snapshotRefresh := time.NewTicker(streamSnapshotInterval)
	defer snapshotRefresh.Stop()

	activityPulse := time.NewTicker(activityPulseInterval)
	defer activityPulse.Stop()

	// Subscribe to workspace live events for push-based updates.
	var notifyCh <-chan struct{}
	var unsubscribe func()
	if store != nil {
		ch, unsub, err := store.SubscribeWorkspaceLive(ctx, workspaceID)
		if err == nil {
			notifyCh = ch
			unsubscribe = unsub
		}
	}
	if unsubscribe != nil {
		defer unsubscribe()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-heartbeat.C:
			writeSSE(CompanyStreamEvent{
				Event:     CompanyStreamEventHeartbeat,
				Timestamp: nowMs(),
			})

		case <-activityPulse.C:
			if lastSnapshot == nil {
				continue
			}
			pulse := buildActivityPulse(lastSnapshot)
			writeSSE(CompanyStreamEvent{
				Event:     "activity_pulse",
				Pulse:     pulse,
				Timestamp: nowMs(),
			})

		case <-snapshotRefresh.C:
			snap, err := BuildSnapshot(ctx, workspaceID, agentAPI, backend, integrations)
			if err != nil {
				log.Warn().Err(err).Msg("company stream: snapshot refresh failed")
				continue
			}
			lastSnapshot = snap
			writeSSE(CompanyStreamEvent{
				Event:     CompanyStreamEventSnapshot,
				Snapshot:  snap,
				Timestamp: nowMs(),
			})

		case _, ok := <-notifyCh:
			if !ok {
				notifyCh = nil
				continue
			}
			snap, err := BuildSnapshot(ctx, workspaceID, agentAPI, backend, integrations)
			if err != nil {
				continue
			}
			lastSnapshot = snap
			writeSSE(CompanyStreamEvent{
				Event:     CompanyStreamEventUpdate,
				Snapshot:  snap,
				Timestamp: nowMs(),
			})
		}
	}
}

// buildActivityPulse creates a lightweight signal summarizing what's active
func buildActivityPulse(snap *CompanySnapshot) *ActivityPulse {
	activeAgents := 0
	agentStates := make(map[string]string)
	for _, a := range snap.Agents {
		agentStates[a.ID] = string(a.State)
		if a.State == "working" {
			activeAgents++
		}
	}

	return &ActivityPulse{
		ActiveAgents: activeAgents,
		TotalTasks:   len(snap.RunningTasks),
		AgentStates:  agentStates,
		ServerTimeMs: nowMs(),
	}
}
