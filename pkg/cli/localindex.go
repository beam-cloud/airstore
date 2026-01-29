// Package cli provides the CLI mount functionality with local index caching.
package cli

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/index"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// LazyIndex provides on-demand local index caching.
// When an integration is first accessed, it syncs that integration's data
// from the gateway to a local SQLite database for instant subsequent reads.
type LazyIndex struct {
	store  index.IndexStore
	client pb.SourceServiceClient
	token  string

	mu      sync.RWMutex
	synced  map[string]time.Time // Last sync time per integration
	syncing map[string]bool      // Currently syncing
}

// NewLazyIndex creates a local index at ~/.airstore/index.db
func NewLazyIndex(conn *grpc.ClientConn, token string) (*LazyIndex, error) {
	dbPath := filepath.Join(os.Getenv("HOME"), ".airstore", "index.db")
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, err
	}

	store, err := index.NewSQLiteIndexStore(dbPath)
	if err != nil {
		return nil, err
	}

	return &LazyIndex{
		store:   store,
		client:  pb.NewSourceServiceClient(conn),
		token:   token,
		synced:  make(map[string]time.Time),
		syncing: make(map[string]bool),
	}, nil
}

// Store returns the underlying index store
func (li *LazyIndex) Store() index.IndexStore { return li.store }

// Close closes the index store
func (li *LazyIndex) Close() { li.store.Close() }

// IsSynced returns true if the integration has been synced recently (within 5 min)
func (li *LazyIndex) IsSynced(integration string) bool {
	li.mu.RLock()
	defer li.mu.RUnlock()
	t, ok := li.synced[integration]
	return ok && time.Since(t) < 5*time.Minute
}

// EnsureSynced triggers a background sync if the integration hasn't been synced
func (li *LazyIndex) EnsureSynced(ctx context.Context, integration string) {
	if li.IsSynced(integration) {
		return
	}

	li.mu.Lock()
	if li.syncing[integration] {
		li.mu.Unlock()
		return
	}
	li.syncing[integration] = true
	li.mu.Unlock()

	go li.sync(ctx, integration)
}

// sync fetches data from the gateway and populates the local index
func (li *LazyIndex) sync(ctx context.Context, integration string) {
	defer func() {
		li.mu.Lock()
		delete(li.syncing, integration)
		li.mu.Unlock()
	}()

	start := time.Now()
	log.Info().Str("integration", integration).Msg("syncing to local index")

	syncCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	if li.token != "" {
		syncCtx = metadata.AppendToOutgoingContext(syncCtx, "authorization", "Bearer "+li.token)
	}

	count, err := li.walkDir(syncCtx, integration, "")
	if err != nil {
		log.Warn().Err(err).Str("integration", integration).Msg("sync failed")
		return
	}

	li.mu.Lock()
	li.synced[integration] = time.Now()
	li.mu.Unlock()

	log.Info().
		Str("integration", integration).
		Int("entries", count).
		Dur("took", time.Since(start)).
		Msg("local index sync complete")
}

// walkDir recursively walks a directory and indexes its contents
func (li *LazyIndex) walkDir(ctx context.Context, integration, dir string) (int, error) {
	path := integration
	if dir != "" {
		path += "/" + dir
	}

	resp, err := li.client.ReadDir(ctx, &pb.SourceReadDirRequest{Path: path})
	if err != nil || !resp.Ok {
		return 0, err
	}

	count := 0
	for _, e := range resp.Entries {
		entryPath := dir
		if entryPath != "" {
			entryPath += "/"
		}
		entryPath += e.Name

		// Fetch file content for small files
		var body string
		if !e.IsDir && e.Size > 0 && e.Size < 512*1024 {
			if r, err := li.client.Read(ctx, &pb.SourceReadRequest{Path: path + "/" + e.Name}); err == nil && r.Ok {
				body = string(r.Data)
			}
		}

		entryType := index.EntryTypeFile
		if e.IsDir {
			entryType = index.EntryTypeDir
		}

		li.store.Upsert(ctx, &index.IndexEntry{
			Integration: integration,
			EntityID:    entryPath,
			Path:        entryPath,
			ParentPath:  dir,
			Name:        e.Name,
			Type:        entryType,
			Size:        e.Size,
			ModTime:     time.Unix(e.Mtime, 0),
			Body:        body,
		})
		count++

		// Recurse into directories (limit depth)
		if e.IsDir && depth(entryPath) < 4 {
			n, _ := li.walkDir(ctx, integration, entryPath)
			count += n
		}
	}
	return count, nil
}

func depth(path string) int {
	if path == "" {
		return 0
	}
	n := 1
	for _, c := range path {
		if c == '/' {
			n++
		}
	}
	return n
}
