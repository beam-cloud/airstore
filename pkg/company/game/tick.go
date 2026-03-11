package game

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/company"
	"github.com/rs/zerolog/log"
)

// ---------------------------------------------------------------------------
// TickLoop — fixed-rate server tick that polls company state, diffs, broadcasts
// ---------------------------------------------------------------------------

const (
	TickRate     = 1 * time.Second
	SyncTimeout  = 5 * time.Second
)

type TickLoop struct {
	mu          sync.Mutex
	server      *GameServer
	workspaces  map[string]bool
	cancel      context.CancelFunc
}

func NewTickLoop(server *GameServer) *TickLoop {
	return &TickLoop{
		server:     server,
		workspaces: make(map[string]bool),
	}
}

func (t *TickLoop) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel
	go t.loop(ctx)
}

func (t *TickLoop) Stop() {
	if t.cancel != nil {
		t.cancel()
	}
}

func (t *TickLoop) RegisterWorkspace(workspaceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.workspaces[workspaceID] = true
}

func (t *TickLoop) UnregisterWorkspace(workspaceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.workspaces, workspaceID)
}

func (t *TickLoop) loop(ctx context.Context) {
	ticker := time.NewTicker(TickRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.tick(ctx)
		}
	}
}

func (t *TickLoop) tick(ctx context.Context) {
	t.mu.Lock()
	workspaces := make([]string, 0, len(t.workspaces))
	for wid := range t.workspaces {
		workspaces = append(workspaces, wid)
	}
	t.mu.Unlock()

	for _, wid := range workspaces {
		t.tickWorkspace(ctx, wid)
	}
}

func (t *TickLoop) tickWorkspace(ctx context.Context, workspaceID string) {
	tickCtx, cancel := context.WithTimeout(ctx, SyncTimeout)
	defer cancel()

	var widUint uint
	fmt.Sscanf(workspaceID, "%d", &widUint)

	snapshot, delta, err := t.server.worldRT.SyncWorkspace(tickCtx, widUint)
	if err != nil {
		log.Debug().Err(err).Str("workspace", workspaceID).Msg("tick: sync failed")
		return
	}

	if delta != nil && hasChanges(delta) {
		msg := MustGameMessage(OpcodeWorldDelta, WorldDeltaPayload{Delta: delta})
		t.server.Broadcast(workspaceID, msg)
	}

	_ = snapshot
}

func hasChanges(delta *company.CompanyWorldDelta) bool {
	if delta == nil {
		return false
	}
	return len(delta.UpdatedZones) > 0 ||
		len(delta.UpdatedEntities) > 0 ||
		len(delta.RemovedEntityIDs) > 0 ||
		len(delta.Activity) > 0 ||
		len(delta.TaskBeacons) > 0
}
