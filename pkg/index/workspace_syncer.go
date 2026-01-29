package index

import (
	"context"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

// SyncerConfig configures index syncers
type SyncerConfig struct {
	// DefaultInterval is the default sync interval
	DefaultInterval time.Duration

	// InitialDelay is the delay before starting the first sync
	InitialDelay time.Duration
}

// DefaultSyncerConfig returns sensible defaults
func DefaultSyncerConfig() SyncerConfig {
	return SyncerConfig{
		DefaultInterval: 30 * time.Second,
		InitialDelay:    5 * time.Second,
	}
}

// WorkspaceConnection represents a workspace's connected integration
type WorkspaceConnection struct {
	WorkspaceID   string
	WorkspaceName string
	Integration   string
	Credentials   *types.IntegrationCredentials
}

// ConnectionProvider provides workspace connections for syncing
type ConnectionProvider interface {
	// ListAllConnections returns all connected integrations across all workspaces
	ListAllConnections(ctx context.Context) ([]WorkspaceConnection, error)
}

// WorkspaceSyncer syncs index data per-workspace.
// It iterates over all workspaces and their connected integrations,
// syncing data with proper workspace namespacing.
type WorkspaceSyncer struct {
	store       IndexStore
	registry    *ProviderRegistry
	connProvider ConnectionProvider
	config      SyncerConfig

	mu       sync.Mutex
	running  bool
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// Metrics per workspace+integration
	lastSync   map[string]time.Time // key: "workspace_id:integration"
	syncErrors map[string]error
	syncCounts map[string]int
}

// NewWorkspaceSyncer creates a new workspace-aware index syncer
func NewWorkspaceSyncer(store IndexStore, registry *ProviderRegistry, connProvider ConnectionProvider) *WorkspaceSyncer {
	return &WorkspaceSyncer{
		store:        store,
		registry:     registry,
		connProvider: connProvider,
		config:       DefaultSyncerConfig(),
		lastSync:     make(map[string]time.Time),
		syncErrors:   make(map[string]error),
		syncCounts:   make(map[string]int),
	}
}

// SetConfig updates the syncer configuration
func (s *WorkspaceSyncer) SetConfig(cfg SyncerConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.config = cfg
}

// Start begins background syncing for all workspaces
func (s *WorkspaceSyncer) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}

	ctx, s.cancel = context.WithCancel(ctx)
	s.running = true
	s.mu.Unlock()

	log.Info().Msg("starting workspace index syncer")

	// Start the main sync loop
	s.wg.Add(1)
	go s.runSyncLoop(ctx)
}

// Stop gracefully stops all background syncing
func (s *WorkspaceSyncer) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}

	log.Info().Msg("stopping workspace index syncer")

	if s.cancel != nil {
		s.cancel()
	}

	s.running = false
	s.mu.Unlock()

	s.wg.Wait()
	log.Info().Msg("workspace index syncer stopped")
}

// runSyncLoop runs the main sync loop
func (s *WorkspaceSyncer) runSyncLoop(ctx context.Context) {
	defer s.wg.Done()

	// Initial delay
	select {
	case <-time.After(s.config.InitialDelay):
	case <-ctx.Done():
		return
	}

	// Do initial sync
	s.syncAll(ctx)

	// Periodic sync
	ticker := time.NewTicker(s.config.DefaultInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.syncAll(ctx)
		case <-ctx.Done():
			log.Info().Msg("workspace syncer loop stopping")
			return
		}
	}
}

// syncAll syncs all workspaces and their connected integrations
func (s *WorkspaceSyncer) syncAll(ctx context.Context) {
	log.Info().Msg("starting workspace sync cycle")
	start := time.Now()

	// Get all connections
	connections, err := s.connProvider.ListAllConnections(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to list workspace connections")
		return
	}

	log.Info().Int("connections", len(connections)).Msg("found workspace connections to sync")

	// Sync each connection
	var syncedCount, errorCount int
	for _, conn := range connections {
		if ctx.Err() != nil {
			break
		}

		if err := s.syncConnection(ctx, conn); err != nil {
			errorCount++
			log.Warn().
				Err(err).
				Str("workspace_id", conn.WorkspaceID).
				Str("workspace_name", conn.WorkspaceName).
				Str("integration", conn.Integration).
				Msg("sync failed")
		} else {
			syncedCount++
		}
	}

	log.Info().
		Int("synced", syncedCount).
		Int("errors", errorCount).
		Dur("duration", time.Since(start)).
		Msg("workspace sync cycle completed")
}

// syncConnection syncs a single workspace connection
func (s *WorkspaceSyncer) syncConnection(ctx context.Context, conn WorkspaceConnection) error {
	provider := s.registry.Get(conn.Integration)
	if provider == nil {
		log.Debug().
			Str("integration", conn.Integration).
			Msg("no index provider for integration")
		return nil
	}

	syncKey := conn.WorkspaceID + ":" + conn.Integration

	// Get last sync time for this workspace+integration
	lastSync, err := s.store.LastSync(ctx, syncKey)
	if err != nil {
		log.Warn().Err(err).Str("key", syncKey).Msg("failed to get last sync time")
		lastSync = time.Time{}
	}

	log.Info().
		Str("workspace_id", conn.WorkspaceID).
		Str("workspace_name", conn.WorkspaceName).
		Str("integration", conn.Integration).
		Time("last_sync", lastSync).
		Msg("syncing workspace integration")

	syncStart := time.Now()

	// Create a workspace-scoped store wrapper
	scopedStore := &workspaceScopedStore{
		store:       s.store,
		workspaceID: conn.WorkspaceID,
	}

	// Perform sync
	if err := provider.Sync(ctx, scopedStore, conn.Credentials, lastSync); err != nil {
		s.recordError(syncKey, err)
		return err
	}

	// Record successful sync
	now := time.Now()
	if err := s.store.SetLastSync(ctx, syncKey, now); err != nil {
		log.Warn().Err(err).Str("key", syncKey).Msg("failed to record last sync time")
	}

	s.recordSuccess(syncKey, now)

	log.Info().
		Str("workspace_id", conn.WorkspaceID).
		Str("integration", conn.Integration).
		Dur("duration", time.Since(syncStart)).
		Msg("workspace sync completed")

	return nil
}

// SyncWorkspace triggers an immediate sync for a specific workspace
func (s *WorkspaceSyncer) SyncWorkspace(ctx context.Context, workspaceID string) error {
	connections, err := s.connProvider.ListAllConnections(ctx)
	if err != nil {
		return err
	}

	for _, conn := range connections {
		if conn.WorkspaceID == workspaceID {
			if err := s.syncConnection(ctx, conn); err != nil {
				log.Warn().Err(err).
					Str("workspace_id", workspaceID).
					Str("integration", conn.Integration).
					Msg("sync failed")
			}
		}
	}
	return nil
}

func (s *WorkspaceSyncer) recordError(key string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.syncErrors[key] = err
}

func (s *WorkspaceSyncer) recordSuccess(key string, t time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSync[key] = t
	s.syncCounts[key]++
	delete(s.syncErrors, key)
}

// Status returns the current sync status
func (s *WorkspaceSyncer) Status() map[string]WorkspaceSyncStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	status := make(map[string]WorkspaceSyncStatus)
	for key := range s.lastSync {
		status[key] = WorkspaceSyncStatus{
			LastSync:  s.lastSync[key],
			LastError: s.syncErrors[key],
			SyncCount: s.syncCounts[key],
		}
	}
	return status
}

// WorkspaceSyncStatus represents sync status for a workspace+integration
type WorkspaceSyncStatus struct {
	LastSync  time.Time
	LastError error
	SyncCount int
}

// IsRunning returns whether the syncer is currently running
func (s *WorkspaceSyncer) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// workspaceScopedStore wraps an IndexStore to automatically add workspace_id
type workspaceScopedStore struct {
	store       IndexStore
	workspaceID string
}

func (w *workspaceScopedStore) Upsert(ctx context.Context, entry *IndexEntry) error {
	entry.WorkspaceID = w.workspaceID
	return w.store.Upsert(ctx, entry)
}

func (w *workspaceScopedStore) Delete(ctx context.Context, integration, entityID string) error {
	// For scoped deletes, we'd need to extend the interface
	// For now, delegate to the underlying store
	return w.store.Delete(ctx, integration, entityID)
}

func (w *workspaceScopedStore) DeleteByIntegration(ctx context.Context, integration string) error {
	return w.store.DeleteByIntegration(ctx, integration)
}

func (w *workspaceScopedStore) List(ctx context.Context, integration, pathPrefix string) ([]*IndexEntry, error) {
	return w.store.List(ctx, integration, pathPrefix)
}

func (w *workspaceScopedStore) ListAll(ctx context.Context, integration string) ([]*IndexEntry, error) {
	return w.store.ListAll(ctx, integration)
}

func (w *workspaceScopedStore) Get(ctx context.Context, integration, entityID string) (*IndexEntry, error) {
	return w.store.Get(ctx, integration, entityID)
}

func (w *workspaceScopedStore) GetByPath(ctx context.Context, path string) (*IndexEntry, error) {
	return w.store.GetByPath(ctx, path)
}

func (w *workspaceScopedStore) Search(ctx context.Context, integration, query string) ([]*IndexEntry, error) {
	return w.store.Search(ctx, integration, query)
}

func (w *workspaceScopedStore) LastSync(ctx context.Context, integration string) (time.Time, error) {
	return w.store.LastSync(ctx, integration)
}

func (w *workspaceScopedStore) SetLastSync(ctx context.Context, integration string, t time.Time) error {
	return w.store.SetLastSync(ctx, integration, t)
}

func (w *workspaceScopedStore) Close() error {
	return w.store.Close()
}
