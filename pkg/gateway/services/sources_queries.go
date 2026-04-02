package services

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	baml "github.com/beam-cloud/airstore/pkg/sources/queries/baml_client"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

// Default pagination settings for source views.
const (
	defaultPageSize   = 50
	defaultMaxResults = 500
)

// SyncResult is the outcome of a view sync operation.
type SyncResult struct {
	Query        *types.FilesystemQuery
	ResultsCount int
	NewResults   int
}

// ---------------------------------------------------------------------------
// View sync
// ---------------------------------------------------------------------------

// InvalidateQueryCache invalidates cached results for a query path.
func (s *SourceService) InvalidateQueryCache(ctx context.Context, workspaceId uint, queryPath string) error {
	if s.fsStore == nil {
		return nil
	}
	log.Info().Uint("workspace_id", workspaceId).Str("path", queryPath).Msg("invalidating query cache")
	return s.fsStore.InvalidateQuery(ctx, workspaceId, queryPath)
}

// invalidateAndExecute clears cached results for a view and re-executes its
// provider query. The cache invalidation is best-effort (logged, not fatal).
func (s *SourceService) invalidateAndExecute(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery, op string) ([]repository.QueryResult, error) {
	if err := s.fsStore.InvalidateQuery(ctx, pctx.WorkspaceId, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Str("op", op).Msg("failed to invalidate query cache")
	}
	return s.executeAndCacheQuery(ctx, pctx, query)
}

// SyncViewByPath forces re-execution of a source view by path, bypassing all caches.
// Returns the results and emits hook events for newly seen items.
func (s *SourceService) SyncViewByPath(ctx context.Context, queryPath string) ([]repository.QueryResult, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get provider context: %w", err)
	}

	query, err := s.fsStore.GetQuery(ctx, pctx.WorkspaceId, queryPath)
	if err != nil {
		return nil, fmt.Errorf("query lookup failed: %w", err)
	}
	if query == nil {
		return nil, fmt.Errorf("query not found: %s", queryPath)
	}

	pctx, connected := s.loadQueryCredentials(ctx, pctx, query)
	if !connected {
		return nil, fmt.Errorf("not connected to %s", query.Integration)
	}

	log.Info().
		Str("path", queryPath).Str("integration", query.Integration).
		Str("query_spec", query.QuerySpec).
		Msg("syncing source view")

	results, err := s.invalidateAndExecute(ctx, pctx, query, "sync_by_path")
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Emit hook events for newly seen items (same path as poller).
	s.emitSourceHookEvents(ctx, pctx.WorkspaceId, query, results)

	log.Info().Str("path", queryPath).Int("results", len(results)).Msg("source view sync complete")
	return results, nil
}

// SyncViewByExternalId syncs a view by its external ID.
func (s *SourceService) SyncViewByExternalId(ctx context.Context, externalId string) (*SyncResult, error) {
	pctx, err := s.providerContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get provider context: %w", err)
	}

	query, err := s.fsStore.GetQueryByExternalId(ctx, externalId)
	if err != nil || query == nil {
		return nil, fmt.Errorf("view not found: %s", externalId)
	}
	if query.WorkspaceId != pctx.WorkspaceId {
		return nil, fmt.Errorf("unauthorized")
	}

	pctx, connected := s.loadQueryCredentials(ctx, pctx, query)
	if !connected {
		return nil, fmt.Errorf("not connected to %s", query.Integration)
	}

	results, err := s.invalidateAndExecute(ctx, pctx, query, "sync_by_external_id")
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	newCount := s.emitSourceHookEvents(ctx, pctx.WorkspaceId, query, results)
	log.Info().Str("path", query.Path).Int("results", len(results)).Int("new", newCount).Msg("source view sync complete")

	return &SyncResult{
		Query:        query,
		ResultsCount: len(results),
		NewResults:   newCount,
	}, nil
}

// RefreshQuery re-executes a query and processes new results.
// Called by the source poller.
//
// For task_followup queries with a TaskWaker configured, this bypasses the
// Redis SeenTracker + stream pipeline entirely: it compares results against
// a durable Postgres baseline and delivers input directly to the sleeping
// task. This is atomic, retry-safe, and works across replicas.
//
// For all other queries, the existing SeenTracker + hook stream path is used.
func (s *SourceService) RefreshQuery(ctx context.Context, query *types.FilesystemQuery) error {
	query, err := s.refreshQueryDefinition(ctx, query)
	if err != nil {
		return err
	}
	if query == nil {
		return fmt.Errorf("query not found")
	}

	pctx := &sources.ProviderContext{WorkspaceId: query.WorkspaceId}
	pctx, connected := s.loadQueryCredentials(ctx, pctx, query)
	if !connected {
		return fmt.Errorf("not connected to %s (workspace %d)", query.Integration, query.WorkspaceId)
	}

	results, err := s.executeAndCacheQuery(ctx, pctx, query)
	if err != nil {
		return err
	}

	if query.IsTaskFollowUp() && s.taskWaker != nil {
		s.handleFollowupDirectDelivery(ctx, pctx.WorkspaceId, query, results)
		return nil
	}

	s.emitSourceHookEvents(ctx, pctx.WorkspaceId, query, results)
	return nil
}

// handleFollowupDirectDelivery compares current results against the Postgres
// baseline and, if new items are found, delivers input directly to the target
// task. On success, baseline and hook are updated atomically in Postgres.
func (s *SourceService) handleFollowupDirectDelivery(
	ctx context.Context,
	workspaceID uint,
	query *types.FilesystemQuery,
	results []repository.QueryResult,
) {
	currentIDs := make([]string, 0, len(results))
	for _, r := range results {
		if id := strings.TrimSpace(r.ID); id != "" {
			currentIDs = append(currentIDs, id)
		}
	}

	if query.BaselineItemIDs == nil {
		log.Info().Str("path", query.Path).Int("items", len(currentIDs)).
			Msg("followup: no baseline set, establishing from current results")
		if err := s.fsStore.UpdateQueryBaseline(ctx, query.Id, currentIDs); err != nil {
			log.Warn().Err(err).Str("path", query.Path).Msg("followup: failed to set initial baseline")
		}
		return
	}

	baselineSet := make(map[string]struct{}, len(query.BaselineItemIDs))
	for _, id := range query.BaselineItemIDs {
		baselineSet[id] = struct{}{}
	}

	var newIDs []string
	for _, id := range currentIDs {
		if _, seen := baselineSet[id]; !seen {
			newIDs = append(newIDs, id)
		}
	}

	if len(newIDs) == 0 {
		return
	}

	queryPath := hooks.NormalizePath(query.Path)
	hook, err := s.fsStore.GetActiveFollowupHook(ctx, workspaceID, queryPath)
	if err != nil {
		log.Warn().Err(err).Str("path", queryPath).Msg("followup: failed to look up hook")
		return
	}
	if hook == nil || hook.TargetTaskID == nil {
		log.Warn().Str("path", queryPath).Msg("followup: no active hook found")
		return
	}

	message := fmt.Sprintf("%s\n\nNew items detected: %d", hook.Prompt, len(newIDs))

	if s.contextEnricher != nil {
		enrichData := map[string]any{
			"path":        queryPath,
			"integration": query.Integration,
		}
		newPaths := make([]string, 0, len(newIDs))
		for _, id := range newIDs {
			for _, r := range results {
				if r.ID == id && r.Filename != "" {
					newPaths = append(newPaths, queryPath+"/"+r.Filename)
					break
				}
			}
		}
		if len(newPaths) > 0 {
			enrichData["new_items"] = strings.Join(newPaths, ", ")
		}
		var threadIDs []string
		seen := make(map[string]struct{})
		for _, id := range newIDs {
			for _, r := range results {
				if r.ID == id {
					if ck := strings.TrimSpace(r.Metadata["thread_id"]); ck != "" {
						if _, dup := seen[ck]; !dup {
							threadIDs = append(threadIDs, ck)
							seen[ck] = struct{}{}
						}
					}
					break
				}
			}
		}
		if len(threadIDs) > 0 {
			enrichData["correlation_keys"] = strings.Join(threadIDs, ",")
		}

		if query.Integration == "gmail" && len(threadIDs) > 0 {
			message += "\n\n## Reply Instructions"
			for _, tid := range threadIDs {
				message += fmt.Sprintf("\nGmail Thread ID: %s\nIMPORTANT: When replying to this email, use --thread-id %s to keep the conversation in the same thread.", tid, tid)
			}
		}

		if content := s.contextEnricher.FetchSourceContent(ctx, workspaceID, query.Integration, enrichData); content != "" {
			message = message + "\n\n" + content
		}
		if viewRows := s.contextEnricher.FetchViewRows(ctx, workspaceID, *hook.TargetTaskID); viewRows != "" {
			message = message + "\n\n" + viewRows
		}
	}

	log.Info().
		Str("path", queryPath).Str("task_id", *hook.TargetTaskID).
		Int("new_items", len(newIDs)).
		Msg("followup: delivering wake directly to task")

	if err := s.taskWaker.WakeTask(ctx, workspaceID, *hook.TargetTaskID, message); err != nil {
		log.Warn().Err(err).
			Str("path", queryPath).Str("task_id", *hook.TargetTaskID).
			Msg("followup: wake delivery failed, will retry next poll")
		return
	}

	if hook.OneShot {
		if err := s.fsStore.DeactivateHookAndUpdateBaseline(ctx, hook.Id, query.Id, currentIDs); err != nil {
			log.Warn().Err(err).Str("path", queryPath).Msg("followup: failed to deactivate hook and update baseline")
		}
	} else {
		if err := s.fsStore.UpdateQueryBaseline(ctx, query.Id, currentIDs); err != nil {
			log.Warn().Err(err).Str("path", queryPath).Msg("followup: failed to update baseline")
		}
	}

	log.Info().
		Str("path", queryPath).Str("task_id", *hook.TargetTaskID).
		Int("new_items", len(newIDs)).Bool("one_shot", hook.OneShot).
		Msg("followup: wake delivered successfully")
}

func (s *SourceService) refreshQueryDefinition(
	ctx context.Context,
	query *types.FilesystemQuery,
) (*types.FilesystemQuery, error) {
	if s == nil || s.fsStore == nil || query == nil {
		return query, nil
	}
	if externalID := strings.TrimSpace(query.ExternalId); externalID != "" {
		refreshed, err := s.fsStore.GetQueryByExternalId(ctx, externalID)
		if err != nil {
			return nil, fmt.Errorf("refresh query definition %s: %w", externalID, err)
		}
		if refreshed != nil {
			return refreshed, nil
		}
	}
	if query.WorkspaceId == 0 || strings.TrimSpace(query.Path) == "" {
		return query, nil
	}
	refreshed, err := s.fsStore.GetQuery(ctx, query.WorkspaceId, query.Path)
	if err != nil {
		return nil, fmt.Errorf("refresh query definition %s: %w", query.Path, err)
	}
	if refreshed != nil {
		return refreshed, nil
	}
	return query, nil
}

// extractCorrelationKey returns the stable entity identifier from result
// metadata for a given integration. This is the only integration-aware
// mapping in the source watch system.
func extractCorrelationKey(integration string, metadata map[string]string) string {
	switch types.SourceType(strings.ToLower(integration)) {
	case types.SourceGmail:
		return strings.TrimSpace(metadata["thread_id"])
	default:
		return strings.TrimSpace(metadata["id"])
	}
}

// emitSourceHookEvents detects new and removed results via the seen tracker
// and emits fs.create / fs.delete events with source metadata attached.
// Returns the count of newly detected results.
func (s *SourceService) emitSourceHookEvents(ctx context.Context, workspaceId uint, query *types.FilesystemQuery, results []repository.QueryResult) int {
	if s.seenTracker == nil || s.hookStream == nil {
		return 0
	}

	queryPath := hooks.NormalizePath(query.Path)
	seenKey := common.Keys.HookSeen(workspaceId, types.GeneratePathID(queryPath))

	// Build ID list for seen-tracking, ID→filepath map, and collect
	// correlation keys for spawn-time routing.
	ids := make([]string, 0, len(results))
	idToPath := make(map[string]string, len(results))
	idToCorrelationKey := make(map[string]string, len(results))
	for _, r := range results {
		id := strings.TrimSpace(r.ID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
		if r.Filename != "" {
			idToPath[id] = queryPath + "/" + r.Filename
		}
		if ck := extractCorrelationKey(query.Integration, r.Metadata); ck != "" {
			idToCorrelationKey[id] = ck
		}
	}

	diff, compareErr := s.seenTracker.Compare(ctx, seenKey, ids)
	if compareErr != nil {
		log.Warn().Err(compareErr).Str("path", queryPath).Msg("seen tracker compare failed, skipping commit")
		return 0
	}

	// Guard against transient empty results: if the source returned zero
	// items but the tracker previously had data (all items "removed"),
	// skip both emit and commit. This prevents an oscillation pattern
	// where transient API failures return empty → baseline resets →
	// items reappear as "new" → spurious hook fires → task wakes for
	// nothing → re-registers watch → repeat.
	if len(ids) == 0 && diff != nil && len(diff.Removed) > 0 {
		log.Warn().Str("path", queryPath).Int("previously_tracked", len(diff.Removed)).
			Msg("source returned 0 results but tracker had items; skipping commit to prevent re-trigger oscillation")
		return 0
	}

	if diff != nil && len(diff.Added) > 0 {
		addedForEmit := diff.Added

		if len(addedForEmit) > 0 {
			newItemsHash := hashHookItemIDs(addedForEmit)
			newPaths := make([]string, 0, len(addedForEmit))
			for _, id := range addedForEmit {
				if p, ok := idToPath[id]; ok {
					newPaths = append(newPaths, p)
				} else {
					newPaths = append(newPaths, id)
				}
			}

			// Collect unique correlation keys for new items so
			// TaskFactory can route to sleeping tasks at spawn time.
			correlationKeysSet := make(map[string]struct{})
			for _, id := range addedForEmit {
				if ck, ok := idToCorrelationKey[id]; ok && ck != "" {
					correlationKeysSet[ck] = struct{}{}
				}
			}
			correlationKeys := make([]string, 0, len(correlationKeysSet))
			for ck := range correlationKeysSet {
				correlationKeys = append(correlationKeys, ck)
			}

			eventPayload := map[string]any{
				"event":          hooks.EventFsCreate,
				"workspace_id":   fmt.Sprintf("%d", workspaceId),
				"path":           queryPath,
				"integration":    query.Integration,
				"new_count":      fmt.Sprintf("%d", len(addedForEmit)),
				"new_items":      strings.Join(newPaths, ", "),
				"new_items_hash": newItemsHash,
			}
			if len(correlationKeys) > 0 {
				eventPayload["correlation_keys"] = strings.Join(correlationKeys, ",")
			}

			if emitErr := s.hookStream.Emit(ctx, eventPayload); emitErr != nil {
				log.Error().Err(emitErr).Str("path", queryPath).Int("new_results", len(addedForEmit)).
					Msg("failed to emit source fs.create event, will retry next poll")
				return 0
			}
			log.Info().
				Str("path", queryPath).Str("integration", query.Integration).
				Int("new_results", len(addedForEmit)).
				Int("correlation_keys", len(correlationKeys)).
				Msg("source items created, fs.create event emitted")
		}
	}

	if diff != nil && len(diff.Removed) > 0 {
		removedPaths := make([]string, 0, len(diff.Removed))
		for _, id := range diff.Removed {
			if p, ok := idToPath[id]; ok {
				removedPaths = append(removedPaths, p)
			} else {
				removedPaths = append(removedPaths, id)
			}
		}
		if emitErr := s.hookStream.Emit(ctx, map[string]any{
			"event":         hooks.EventFsDelete,
			"workspace_id":  fmt.Sprintf("%d", workspaceId),
			"path":          queryPath,
			"integration":   query.Integration,
			"removed_count": fmt.Sprintf("%d", len(diff.Removed)),
			"removed_items": strings.Join(removedPaths, ", "),
		}); emitErr != nil {
			log.Error().Err(emitErr).Str("path", queryPath).Int("removed_results", len(diff.Removed)).
				Msg("failed to emit source fs.delete event, will retry next poll")
			return 0
		}
	}

	if err := s.seenTracker.Commit(ctx, seenKey, ids); err != nil {
		log.Warn().Err(err).Str("path", queryPath).Msg("seen tracker commit failed, next poll may re-fire")
	}
	newCount := 0
	if diff != nil {
		newCount = len(diff.Added)
	}
	return newCount
}

// SeedSeenBaseline marks a query path as "initialized" in the SeenTracker
// with the cached results the agent last saw, so the first poller run diffs
// properly against the agent's perspective instead of a fresh query.
//
// Using cached results (not a fresh query) is critical: a fresh query could
// include items that arrived after the agent's last read but before watch
// registration, permanently baking them into the baseline so they never
// trigger a wake.
func (s *SourceService) SeedSeenBaseline(ctx context.Context, workspaceID uint, query *types.FilesystemQuery) {
	if s.seenTracker == nil {
		return
	}

	queryPath := hooks.NormalizePath(query.Path)
	seenKey := common.Keys.HookSeen(workspaceID, types.GeneratePathID(queryPath))

	results, err := s.fsStore.GetQueryResults(ctx, workspaceID, queryPath)
	if err != nil || len(results) == 0 {
		log.Info().Str("path", queryPath).Msg("seed baseline: no cached results, first poll will establish baseline")
		return
	}

	ids := make([]string, 0, len(results))
	for _, r := range results {
		if id := strings.TrimSpace(r.ID); id != "" {
			ids = append(ids, id)
		}
	}

	if err := s.seenTracker.Commit(ctx, seenKey, ids); err != nil {
		log.Warn().Err(err).Str("path", queryPath).Msg("seed baseline: commit failed")
		return
	}
	log.Info().Str("path", queryPath).Int("baseline_items", len(ids)).
		Msg("seeded seen baseline from cached query results")
}

func hashHookItemIDs(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	seen := make(map[string]struct{}, len(ids))
	normalized := make([]string, 0, len(ids))
	for _, raw := range ids {
		id := strings.TrimSpace(raw)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		normalized = append(normalized, id)
	}
	if len(normalized) == 0 {
		return ""
	}
	sort.Strings(normalized)
	sum := sha1.Sum([]byte(strings.Join(normalized, "\n")))
	return hex.EncodeToString(sum[:])
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

func (s *SourceService) executeAndCacheQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	if s.eventInjector != nil && query.IsTaskFollowUp() && query.OwnerTaskID != nil {
		if injected := s.eventInjector.DrainItems(*query.OwnerTaskID); len(injected) > 0 {
			results := make([]repository.QueryResult, len(injected))
			for i, item := range injected {
				results[i] = repository.QueryResult{ID: item.ID, Filename: item.Filename, Metadata: item.Metadata, Size: item.Size, Mtime: item.Mtime}
			}
			log.Info().Str("task_id", *query.OwnerTaskID).Int("items", len(results)).Msg("event injector: returning synthetic results")
			return results, nil
		}
	}

	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return nil, fmt.Errorf("provider not found: %s", query.Integration)
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("provider does not support queries: %s", query.Integration)
	}

	spec := parseQuerySpec(query.Integration, query.QuerySpec)
	if spec.Query == "" && !querySpecAllowsEmptyQuery(query.Integration, spec) {
		return nil, fmt.Errorf("empty query spec for %s", query.Integration)
	}

	log.Info().
		Str("integration", query.Integration).Str("path", query.Path).
		Str("query", spec.Query).Int("limit", spec.Limit).Int("max_results", spec.MaxResults).
		Msg("executing provider query")

	// Fetch all pages synchronously.
	var allResults []repository.QueryResult
	seenIDs := make(map[string]bool)
	pageNum := 0

	for {
		pageNum++
		queryResp, err := executor.ExecuteQuery(ctx, pctx, spec)
		if err != nil {
			return nil, fmt.Errorf("query execution failed (page %d): %w", pageNum, err)
		}

		for _, qr := range queryResp.Results {
			if qr.ID != "" && !seenIDs[qr.ID] {
				seenIDs[qr.ID] = true
				filename := qr.Filename
				if filename == "" {
					filename = executor.FormatFilename(spec.FilenameFormat, qr.Metadata)
				}
				allResults = append(allResults, repository.QueryResult{
					ID: qr.ID, Filename: filename, Metadata: qr.Metadata,
					Size: qr.Size, Mtime: qr.Mtime,
				})
			}
		}

		log.Debug().
			Str("path", query.Path).Int("page", pageNum).
			Int("page_results", len(queryResp.Results)).Int("total_unique", len(allResults)).
			Bool("has_more", queryResp.HasMore).
			Msg("fetched page")

		if !queryResp.HasMore || queryResp.NextPageToken == "" || len(allResults) >= spec.MaxResults {
			break
		}
		spec.PageToken = queryResp.NextPageToken
	}

	if len(allResults) > spec.MaxResults {
		allResults = allResults[:spec.MaxResults]
	}

	log.Info().
		Str("integration", query.Integration).Str("path", query.Path).
		Int("total_results", len(allResults)).Int("pages", pageNum).
		Msg("query complete")

	// Cache results.
	ttl := time.Duration(query.CacheTTL) * time.Second
	if ttl == 0 {
		ttl = 5 * time.Minute
	}
	if err := s.fsStore.StoreQueryResults(ctx, pctx.WorkspaceId, query.Path, allResults, ttl); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to cache query results")
	}

	now := time.Now()
	query.LastExecuted = &now
	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to update query timestamp")
	}

	return allResults, nil
}

// defaultStaleThreshold is the minimum stale window for access-triggered
// refresh. Even when a query has a shorter CacheTTL, we avoid re-executing on
// every access to keep provider/API load predictable.
const defaultStaleThreshold = 5 * time.Minute

func (s *SourceService) getOrExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	// Fast path: cache hit and still fresh.
	if results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, query.Path); err == nil && len(results) > 0 && !s.shouldRefreshQueryOnAccess(query) {
		return results, nil
	}

	// Cache miss or stale cache: refresh synchronously (singleflight-deduplicated).
	key := fmt.Sprintf("%d:%s", pctx.WorkspaceId, query.Path)
	value, err, _ := s.queryGroup.Do(key, func() (any, error) {
		// Double-check after acquiring the flight.
		if results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, query.Path); err == nil && len(results) > 0 && !s.shouldRefreshQueryOnAccess(query) {
			return results, nil
		}
		return s.invalidateAndExecute(ctx, pctx, query, "access_refresh")
	})
	if err != nil {
		return nil, err
	}
	results, ok := value.([]repository.QueryResult)
	if !ok {
		return nil, fmt.Errorf("unexpected query result type for %s", query.Path)
	}
	return results, nil
}

// shouldRefreshQueryOnAccess reports whether a query should be re-executed on
// this access based on LastExecuted and the access refresh interval.
func (s *SourceService) shouldRefreshQueryOnAccess(query *types.FilesystemQuery) bool {
	if query.LastExecuted == nil {
		return true // never executed
	}
	return time.Since(*query.LastExecuted) > s.accessRefreshInterval(query)
}

// accessRefreshInterval returns the age threshold for access-triggered refresh.
// We use max(CacheTTL, defaultStaleThreshold) for stable load behavior.
func (s *SourceService) accessRefreshInterval(query *types.FilesystemQuery) time.Duration {
	if query.CacheTTL <= 0 {
		return defaultStaleThreshold
	}
	cfg := time.Duration(query.CacheTTL) * time.Second
	if cfg < defaultStaleThreshold {
		return defaultStaleThreshold
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Source View CRUD (gRPC handlers)
// ---------------------------------------------------------------------------

func (s *SourceService) CreateView(ctx context.Context, req *pb.CreateViewRequest) (*pb.CreateViewResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.CreateViewResponse{Ok: false, Error: "unauthorized"}, nil
	}
	if !isValidQueryName(req.Name) {
		return &pb.CreateViewResponse{Ok: false, Error: "invalid query name: must not contain '/' or '..' sequences"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	path := types.PathSources + "/" + req.Integration + "/" + req.Name
	if req.FileExt != "" {
		path += req.FileExt
	}

	// Determine mode: if Filter is provided, use query mode; otherwise smart mode (LLM).
	mode := types.ViewModeSmart
	var querySpec, filenameFormat string
	var err error

	if req.Filter != "" {
		mode = types.ViewModeQuery
		querySpec, err = buildQuerySpecFromFilter(req.Integration, []byte(req.Filter), defaultPageSize)
		if err != nil {
			return &pb.CreateViewResponse{Ok: false, Error: "invalid filter: " + err.Error()}, nil
		}
		// Validate the filter produces a non-empty query for integrations that need it.
		spec := parseQuerySpec(req.Integration, querySpec)
		if spec.Query == "" && !querySpecAllowsEmptyQuery(req.Integration, spec) {
			return &pb.CreateViewResponse{Ok: false, Error: "filter produces no query criteria"}, nil
		}
		filenameFormat = sources.DefaultFilenameFormat(req.Integration)
	} else {
		querySpec, filenameFormat, err = s.resolveQuerySpec(ctx, req.Integration, req.Name, req.Guidance)
		if err != nil {
			return &pb.CreateViewResponse{Ok: false, Error: err.Error()}, nil
		}
		querySpec = s.refineQueryIfNeeded(ctx, req.Integration, req.Guidance, querySpec, filenameFormat)
	}

	var credentialMemberID *uint
	if memberID := auth.MemberId(ctx); memberID != 0 {
		credentialMemberID = &memberID
	}

	created, _, err := s.saveViewDefinition(ctx, &types.FilesystemQuery{
		WorkspaceId:        workspaceId,
		CredentialMemberID: credentialMemberID,
		Integration:        req.Integration,
		Path:               path,
		Name:               req.Name,
		QuerySpec:          querySpec,
		Guidance:           req.Guidance,
		OutputFormat:       types.ViewOutputFormat(req.OutputFormat),
		FileExt:            req.FileExt,
		FilenameFormat:     filenameFormat,
		CacheTTL:           0,
		Mode:               mode,
		Filter:             req.Filter,
	}, false)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("failed to create view")
		return &pb.CreateViewResponse{Ok: false, Error: err.Error()}, nil
	}

	log.Info().Str("path", path).Str("mode", string(mode)).Str("query", querySpec).Msg("created source view")
	return &pb.CreateViewResponse{Ok: true, View: viewToProto(created)}, nil
}

func (s *SourceService) saveViewDefinition(
	ctx context.Context,
	query *types.FilesystemQuery,
	allowUpdate bool,
) (*types.FilesystemQuery, bool, error) {
	if s == nil || s.fsStore == nil {
		return nil, false, fmt.Errorf("source service is unavailable")
	}
	if query == nil {
		return nil, false, fmt.Errorf("source view is required")
	}
	query.Path = strings.TrimSpace(query.Path)
	if query.Path == "" {
		return nil, false, fmt.Errorf("view path is required")
	}
	if strings.TrimSpace(query.Name) == "" {
		query.Name = path.Base(query.Path)
	}

	existing, err := s.fsStore.GetQuery(ctx, query.WorkspaceId, query.Path)
	if err != nil {
		return nil, false, fmt.Errorf("get source view: %w", err)
	}
	if existing == nil {
		created, err := s.fsStore.CreateQuery(ctx, query)
		if err != nil {
			return nil, false, err
		}
		return created, true, nil
	}
	if !allowUpdate {
		return nil, false, fmt.Errorf("query already exists at path: %s", query.Path)
	}

	existing.CredentialMemberID = query.CredentialMemberID
	existing.SystemManaged = query.SystemManaged
	existing.Lifecycle = query.Lifecycle
	existing.OwnerTaskID = query.OwnerTaskID
	existing.OwnerRunID = query.OwnerRunID
	existing.Integration = query.Integration
	existing.Path = query.Path
	existing.Name = query.Name
	existing.QuerySpec = query.QuerySpec
	existing.Guidance = query.Guidance
	existing.OutputFormat = query.OutputFormat
	existing.FileExt = query.FileExt
	existing.FilenameFormat = query.FilenameFormat
	existing.CacheTTL = query.CacheTTL
	existing.Mode = query.Mode
	existing.Filter = query.Filter
	if err := s.fsStore.UpdateQuery(ctx, existing); err != nil {
		return nil, false, fmt.Errorf("update source view: %w", err)
	}
	return existing, false, nil
}

func (s *SourceService) GetView(ctx context.Context, req *pb.GetViewRequest) (*pb.GetViewResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.GetViewResponse{Ok: false, Error: "unauthorized"}, nil
	}
	query, err := s.fsStore.GetQuery(ctx, auth.WorkspaceId(ctx), req.Path)
	if err != nil {
		return &pb.GetViewResponse{Ok: false, Error: err.Error()}, nil
	}
	if query == nil {
		return &pb.GetViewResponse{Ok: true, View: nil}, nil
	}
	return &pb.GetViewResponse{Ok: true, View: viewToProto(query)}, nil
}

func (s *SourceService) ListViews(ctx context.Context, req *pb.ListViewsRequest) (*pb.ListViewsResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ListViewsResponse{Ok: false, Error: "unauthorized"}, nil
	}
	queries, err := s.fsStore.ListQueries(ctx, auth.WorkspaceId(ctx), req.ParentPath)
	if err != nil {
		return &pb.ListViewsResponse{Ok: false, Error: err.Error()}, nil
	}
	out := make([]*pb.SourceView, len(queries))
	for i, q := range queries {
		out[i] = viewToProto(q)
	}
	return &pb.ListViewsResponse{Ok: true, Views: out}, nil
}

func (s *SourceService) DeleteView(ctx context.Context, req *pb.DeleteViewRequest) (*pb.DeleteViewResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.DeleteViewResponse{Ok: false, Error: "unauthorized"}, nil
	}
	query, err := s.fsStore.GetQueryByExternalId(ctx, req.ExternalId)
	if err != nil || query == nil {
		return &pb.DeleteViewResponse{Ok: false, Error: "view not found"}, nil
	}
	if query.WorkspaceId != auth.WorkspaceId(ctx) {
		return &pb.DeleteViewResponse{Ok: false, Error: "unauthorized"}, nil
	}

	if err := s.fsStore.InvalidateQuery(ctx, query.WorkspaceId, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate query cache")
	}
	if err := s.fsStore.DeleteQuery(ctx, req.ExternalId); err != nil {
		return &pb.DeleteViewResponse{Ok: false, Error: err.Error()}, nil
	}

	log.Info().Str("external_id", req.ExternalId).Str("path", query.Path).Msg("deleted source view")
	return &pb.DeleteViewResponse{Ok: true}, nil
}

func (s *SourceService) UpdateView(ctx context.Context, req *pb.UpdateViewRequest) (*pb.UpdateViewResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.UpdateViewResponse{Ok: false, Error: "unauthorized"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	query, err := s.fsStore.GetQueryByExternalId(ctx, req.ExternalId)
	if err != nil || query == nil {
		return &pb.UpdateViewResponse{Ok: false, Error: "view not found"}, nil
	}
	if query.WorkspaceId != workspaceId {
		return &pb.UpdateViewResponse{Ok: false, Error: "unauthorized"}, nil
	}

	oldPath := query.Path
	needsUpdate := false

	// Rename: update path.
	if req.Name != "" && !isValidQueryName(req.Name) {
		return &pb.UpdateViewResponse{Ok: false, Error: "invalid query name: must not contain '/' or '..' sequences"}, nil
	}
	if req.Name != "" && req.Name != query.Name {
		query.Name = req.Name
		query.Path = types.PathSources + "/" + query.Integration + "/" + req.Name
		if query.FileExt != "" {
			query.Path += query.FileExt
		}
		needsUpdate = true
	}

	// Update filter (query mode) or guidance (smart mode).
	if req.Filter != "" {
		query.Filter = req.Filter
		query.Mode = types.ViewModeQuery
		querySpec, err := buildQuerySpecFromFilter(query.Integration, []byte(req.Filter), defaultPageSize)
		if err != nil {
			return &pb.UpdateViewResponse{Ok: false, Error: "invalid filter: " + err.Error()}, nil
		}
		query.QuerySpec = querySpec
		query.FilenameFormat = sources.DefaultFilenameFormat(query.Integration)
		needsUpdate = true
	} else if req.Mode == string(types.ViewModeSmart) && query.Mode != types.ViewModeSmart {
		// Explicit switch from query → smart mode.
		query.Mode = types.ViewModeSmart
		query.Filter = ""
		if req.Guidance != "" {
			query.Guidance = req.Guidance
		}

		// Always regenerate query spec from name + guidance (may be empty).
		guidance := query.Guidance
		querySpec, filenameFormat, err := s.resolveQuerySpec(ctx, query.Integration, query.Name, guidance)
		if err != nil {
			return &pb.UpdateViewResponse{Ok: false, Error: "failed to regenerate query: " + err.Error()}, nil
		}
		query.QuerySpec = s.refineQueryIfNeeded(ctx, query.Integration, guidance, querySpec, filenameFormat)
		if filenameFormat != "" {
			query.FilenameFormat = filenameFormat
		}

		needsUpdate = true
	} else if req.Guidance != "" && req.Guidance != query.Guidance {
		query.Guidance = req.Guidance
		query.Mode = types.ViewModeSmart

		querySpec, filenameFormat, err := s.resolveQuerySpec(ctx, query.Integration, query.Name, req.Guidance)
		if err != nil {
			return &pb.UpdateViewResponse{Ok: false, Error: "failed to regenerate query: " + err.Error()}, nil
		}

		query.QuerySpec = s.refineQueryIfNeeded(ctx, query.Integration, req.Guidance, querySpec, filenameFormat)
		if filenameFormat != "" {
			query.FilenameFormat = filenameFormat
		}
		needsUpdate = true
	}

	if !needsUpdate {
		return &pb.UpdateViewResponse{Ok: true, View: viewToProto(query)}, nil
	}

	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		log.Error().Err(err).Str("external_id", req.ExternalId).Msg("failed to update view")
		return &pb.UpdateViewResponse{Ok: false, Error: err.Error()}, nil
	}

	// Invalidate caches for old and new paths.
	if oldPath != query.Path {
		if err := s.fsStore.InvalidateQuery(ctx, workspaceId, oldPath); err != nil {
			log.Warn().Err(err).Str("path", oldPath).Msg("failed to invalidate old query cache")
		}
	}
	if err := s.fsStore.InvalidateQuery(ctx, workspaceId, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate query cache")
	}

	log.Info().
		Str("external_id", req.ExternalId).Str("old_path", oldPath).
		Str("new_path", query.Path).Str("name", query.Name).
		Msg("updated source view")
	return &pb.UpdateViewResponse{Ok: true, View: viewToProto(query)}, nil
}

// ExecuteView runs a view's query and returns materialized results.
func (s *SourceService) ExecuteView(ctx context.Context, req *pb.ExecuteViewRequest) (*pb.ExecuteViewResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ExecuteViewResponse{Ok: false, Error: "unauthorized"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	query, err := s.fsStore.GetQuery(ctx, workspaceId, req.Path)
	if err != nil || query == nil {
		return &pb.ExecuteViewResponse{Ok: false, Error: "view not found"}, nil
	}

	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.ExecuteViewResponse{Ok: false, Error: err.Error()}, nil
	}
	pctx, connected := s.loadQueryCredentials(ctx, pctx, query)
	if !connected {
		return &pb.ExecuteViewResponse{Ok: false, Error: "not connected"}, nil
	}

	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return &pb.ExecuteViewResponse{Ok: false, Error: "integration not available"}, nil
	}

	// If requesting file content, resolve the result ID and read.
	if req.Filename != "" {
		executor, ok := provider.(sources.QueryExecutor)
		if !ok {
			return &pb.ExecuteViewResponse{Ok: false, Error: "provider does not support queries"}, nil
		}

		resultId := req.ResultId
		if resultId == "" {
			results, err := s.getOrExecuteQuery(ctx, pctx, query)
			if err != nil {
				return &pb.ExecuteViewResponse{Ok: false, Error: "failed to get query results: " + err.Error()}, nil
			}
			for _, r := range results {
				if r.Filename == req.Filename {
					resultId = r.ID
					break
				}
			}
			if resultId == "" {
				return &pb.ExecuteViewResponse{Ok: false, Error: "file not found in query results"}, nil
			}
		}

		// Compressed read path.
		strategyStr, session := s.compressionMeta(ctx)
		if strategyStr != "" && s.compressor != nil {
			log.Debug().
				Str("strategy", strategyStr).Str("file", req.Filename).Str("path", req.Path).
				Msg("compression: entering compressed read path (ExecuteView)")
			resp, err := s.readWithCompression(ctx, pctx, executor, query.Integration, req.Path, req.Filename, resultId, query.QuerySpec, 0, 0, strategyStr, session)
			if err != nil {
				return &pb.ExecuteViewResponse{Ok: false, Error: err.Error()}, nil
			}
			return &pb.ExecuteViewResponse{Ok: true, FileData: resp.Data}, nil
		} else if strategyStr != "" {
			log.Warn().Str("strategy", strategyStr).Str("file", req.Filename).
				Msg("compression: requested but compressor not initialized")
		}

		// Standard read.
		if content, err := s.fsStore.GetResultContent(ctx, workspaceId, req.Path, resultId); err == nil && len(content) > 0 {
			return &pb.ExecuteViewResponse{Ok: true, FileData: content}, nil
		}
		data, err := executor.ReadResult(ctx, pctx, resultId)
		if err != nil {
			return &pb.ExecuteViewResponse{Ok: false, Error: err.Error()}, nil
		}
		if err := s.fsStore.StoreResultContent(ctx, workspaceId, req.Path, resultId, data); err != nil {
			log.Warn().Err(err).Str("path", req.Path).Str("result", resultId).Msg("failed to cache result content")
		}
		return &pb.ExecuteViewResponse{Ok: true, FileData: data}, nil
	}

	// List mode: return directory entries.
	results, err := s.getOrExecuteQuery(ctx, pctx, query)
	if err != nil {
		return &pb.ExecuteViewResponse{Ok: false, Error: err.Error()}, nil
	}
	entries := make([]*pb.SourceDirEntry, 0, len(results))
	for _, r := range results {
		entries = append(entries, &pb.SourceDirEntry{
			Name: r.Filename, Mode: sources.ModeFile, Size: r.Size, Mtime: r.Mtime, ResultId: r.ID,
		})
	}
	return &pb.ExecuteViewResponse{Ok: true, Entries: entries}, nil
}

// SyncView handles the gRPC SyncView RPC.
func (s *SourceService) SyncView(ctx context.Context, req *pb.SyncViewRequest) (*pb.SyncViewResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.SyncViewResponse{Ok: false, Error: "unauthorized"}, nil
	}

	result, err := s.SyncViewByExternalId(ctx, req.ExternalId)
	if err != nil {
		return &pb.SyncViewResponse{Ok: false, Error: err.Error()}, nil
	}

	return &pb.SyncViewResponse{
		Ok:           true,
		View:         viewToProto(result.Query),
		ResultsCount: int32(result.ResultsCount),
		NewResults:   int32(result.NewResults),
	}, nil
}

// ---------------------------------------------------------------------------
// Resource listing
// ---------------------------------------------------------------------------

func (s *SourceService) ListResources(ctx context.Context, req *pb.ListResourcesRequest) (*pb.ListResourcesResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ListResourcesResponse{Ok: false, Error: "unauthorized"}, nil
	}

	provider := s.registry.Get(req.Integration)
	if provider == nil {
		return &pb.ListResourcesResponse{Ok: false, Error: fmt.Sprintf("unknown integration: %s", req.Integration)}, nil
	}

	lister, ok := provider.(sources.ResourceLister)
	if !ok {
		return &pb.ListResourcesResponse{Ok: false, Error: fmt.Sprintf("integration %s does not support resource listing", req.Integration)}, nil
	}

	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.ListResourcesResponse{Ok: false, Error: err.Error()}, nil
	}
	pctx, ok = s.loadCredentials(ctx, pctx, req.Integration)
	if !ok {
		return &pb.ListResourcesResponse{Ok: false, Error: "integration not connected"}, nil
	}

	resources, err := lister.ListResources(ctx, pctx, req.ResourceType)
	if err != nil {
		return &pb.ListResourcesResponse{Ok: false, Error: err.Error()}, nil
	}

	pbResources := make([]*pb.SourceResource, len(resources))
	for i, r := range resources {
		pbResources[i] = &pb.SourceResource{Id: r.ID, Name: r.Name}
	}
	return &pb.ListResourcesResponse{Ok: true, Resources: pbResources}, nil
}

// ---------------------------------------------------------------------------
// LLM inference
// ---------------------------------------------------------------------------

// inferQuerySpec uses BAML to convert a folder name + guidance into a query spec.
func (s *SourceService) inferQuerySpec(ctx context.Context, integration, name, guidance string) (string, string, error) {
	guidancePtr := buildGuidancePtr(integration, guidance)

	var result any
	var err error
	switch types.SourceType(integration) {
	case types.SourceGmail:
		result, err = baml.InferGmailQuery(ctx, name, guidancePtr)
	case types.SourceGDrive:
		result, err = baml.InferGDriveQuery(ctx, name, guidancePtr)
	case types.SourceNotion:
		result, err = baml.InferNotionQuery(ctx, name, guidancePtr)
	case types.SourceGitHub:
		result, err = baml.InferGitHubQuery(ctx, name, guidancePtr)
	case types.SourceSlack:
		result, err = baml.InferSlackQuery(ctx, name, guidancePtr)
	case types.SourceLinear:
		result, err = baml.InferLinearQuery(ctx, name, guidancePtr)
	case types.SourcePostHog:
		result, err = baml.InferPostHogQuery(ctx, name, guidancePtr)
	case types.SourceConfluence:
		result, err = baml.InferConfluenceQuery(ctx, name, guidancePtr)
	case types.SourceWeb:
		result, err = baml.InferWebQuery(ctx, name, guidancePtr)
	default:
		return "", "", fmt.Errorf("unsupported integration: %s", integration)
	}
	if err != nil {
		return "", "", err
	}

	data, err := json.Marshal(result)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal query spec: %w", err)
	}
	return string(data), extractFilenameFormat(data), nil
}

// resolveQuerySpec runs LLM inference, validates the result, and fills in
// filename format defaults. Returns the raw query spec and filename format.
func (s *SourceService) resolveQuerySpec(ctx context.Context, integration, name, guidance string) (querySpec, filenameFormat string, err error) {
	querySpec, filenameFormat, err = s.inferQuerySpec(ctx, integration, name, guidance)
	if err != nil {
		log.Warn().Err(err).Str("name", name).Str("integration", integration).Msg("BAML inference failed")
		return "", "", err
	}

	spec := parseQuerySpec(integration, querySpec)
	if spec.Query == "" && !querySpecAllowsEmptyQuery(integration, spec) {
		return "", "", fmt.Errorf("invalid query spec from inference")
	}
	if filenameFormat == "" {
		filenameFormat = spec.FilenameFormat
	}
	// Ensure the filename format is a valid template (contains placeholders).
	// LLMs sometimes return literal filenames (e.g., "test.md") instead of
	// templates with {identifier}/{title} placeholders.
	if filenameFormat == "" || !strings.Contains(filenameFormat, "{") {
		filenameFormat = sources.DefaultFilenameFormat(integration)
	}
	return querySpec, filenameFormat, nil
}

// refineQueryIfNeeded runs provider-specific iterative refinement on an
// already-resolved query spec. Currently only Gmail benefits from this
// (evaluating real API results against user guidance). For all other sources
// this is a no-op and returns the inputs unchanged.
func (s *SourceService) refineQueryIfNeeded(ctx context.Context, integration, guidance, querySpec, filenameFormat string) string {
	if guidance == "" {
		return querySpec
	}

	spec := parseQuerySpec(integration, querySpec)

	switch types.SourceType(integration) {
	case types.SourceGmail:
		pctx, err := s.providerContext(ctx)
		if err != nil {
			return querySpec
		}

		pctx, connected := s.loadCredentials(ctx, pctx, integration)
		if !connected {
			return querySpec
		}

		refined, err := s.refineGmailQueryWithResults(ctx, pctx, guidance, spec.Query)
		if err != nil {
			log.Warn().Err(err).Msg("query refinement failed, using initial query")
			return querySpec
		}

		if refined != spec.Query {
			log.Info().Str("original", spec.Query).Str("refined", refined).Msg("refined query")
			return buildGmailQuerySpec(refined, spec.Limit, filenameFormat)
		}
	}

	return querySpec
}

// buildGuidancePtr wraps guidance for BAML. For GDrive queries, appends a UTC
// timestamp hint so the LLM can interpret time-relative guidance.
func buildGuidancePtr(integration, guidance string) *string {
	if types.SourceType(integration) == types.SourceGDrive {
		now := time.Now().UTC()
		hint := fmt.Sprintf("Current time (UTC): %s\nCurrent date (UTC): %s",
			now.Format(time.RFC3339), now.Format("2006-01-02"))
		g := strings.TrimSpace(guidance)
		if g != "" {
			g += "\n"
		}
		g += hint
		return &g
	}
	if guidance != "" {
		return &guidance
	}
	return nil
}

// refineGmailQueryWithResults iteratively refines a Gmail query by evaluating
// actual API results against the user's guidance (up to 2 iterations).
func (s *SourceService) refineGmailQueryWithResults(ctx context.Context, pctx *sources.ProviderContext, guidance, query string) (string, error) {
	const maxIterations = 2
	currentQuery := query

	for i := 0; i < maxIterations; i++ {
		results, err := s.executeQueryForEvaluation(ctx, pctx, string(types.SourceGmail), currentQuery, 20)
		if err != nil {
			return currentQuery, nil
		}

		eval, err := baml.EvaluateGmailQueryResults(ctx, guidance, currentQuery, int64(len(results)), formatResultsForEvaluation(results))
		if err != nil {
			return currentQuery, nil
		}
		if eval.Is_satisfactory || eval.Refined_query == nil {
			break
		}

		log.Info().
			Str("reasoning", eval.Reasoning).Str("old_query", currentQuery).Str("new_query", *eval.Refined_query).
			Int("iteration", i).Msg("refining gmail query")
		currentQuery = *eval.Refined_query
	}
	return currentQuery, nil
}

func (s *SourceService) executeQueryForEvaluation(ctx context.Context, pctx *sources.ProviderContext, integration, query string, limit int) ([]sources.QueryResult, error) {
	provider := s.registry.Get(integration)
	if provider == nil {
		return nil, fmt.Errorf("provider not found: %s", integration)
	}
	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("provider does not support queries: %s", integration)
	}
	resp, err := executor.ExecuteQuery(ctx, pctx, sources.QuerySpec{Query: query, Limit: limit})
	if err != nil {
		return nil, err
	}
	return resp.Results, nil
}

func formatResultsForEvaluation(results []sources.QueryResult) string {
	type sample struct {
		From    string `json:"from"`
		Subject string `json:"subject"`
		Snippet string `json:"snippet"`
	}
	n := len(results)
	if n > 10 {
		n = 10
	}
	samples := make([]sample, n)
	for i := 0; i < n; i++ {
		r := results[i]
		samples[i] = sample{From: r.Metadata["from"], Subject: r.Metadata["subject"], Snippet: r.Metadata["snippet"]}
	}
	data, _ := json.Marshal(samples)
	return string(data)
}

// ---------------------------------------------------------------------------
// Query spec parsing & helpers
// ---------------------------------------------------------------------------

func parseQuerySpec(integration, querySpec string) sources.QuerySpec {
	var spec struct {
		GmailQuery         string   `json:"gmail_query"`
		OutlookQuery       string   `json:"outlook_query"`
		GDriveQuery        string   `json:"gdrive_query"`
		NotionQuery        string   `json:"notion_query"`
		GitHubQuery        string   `json:"github_query"`
		SlackQuery         string   `json:"slack_query"`
		LinearQuery        string   `json:"linear_query"`
		PostHogQuery       string   `json:"posthog_query"`
		ConfluenceQuery    string   `json:"cql_query"`
		WebQuery           string   `json:"web_query"`
		WebMode            string   `json:"web_mode"`
		IncludePaths       []string `json:"include_paths"`
		SearchType         string   `json:"search_type"`
		ContentType        string   `json:"content_type"`
		ProjectID          int      `json:"project_id"`
		IncludeAttachments *bool    `json:"include_attachments"`
		IncludeInline      *bool    `json:"include_inline"`
		IncludeMessageBody *bool    `json:"include_message_body"`
		ThreadID           string   `json:"thread_id"`
		MessageID          string   `json:"message_id"`
		CredentialMemberID string   `json:"credential_member_id"`
		Limit              int      `json:"limit"`
		MaxResults         int      `json:"max_results"`
		FilenameFormat     string   `json:"filename_format"`
	}

	limit := defaultPageSize
	maxResults := defaultMaxResults
	if json.Unmarshal([]byte(querySpec), &spec) == nil {
		if spec.Limit > 0 {
			limit = spec.Limit
		}
		if spec.MaxResults > 0 {
			maxResults = spec.MaxResults
		}
	}
	if maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	var query string
	switch types.SourceType(integration) {
	case types.SourceGmail:
		query = spec.GmailQuery
	case types.SourceOutlook:
		query = spec.OutlookQuery
	case types.SourceGDrive:
		query = spec.GDriveQuery
	case types.SourceNotion:
		query = spec.NotionQuery
	case types.SourceGitHub:
		query = spec.GitHubQuery
	case types.SourceSlack:
		query = spec.SlackQuery
	case types.SourceLinear:
		query = spec.LinearQuery
	case types.SourcePostHog:
		query = spec.PostHogQuery
	case types.SourceConfluence:
		query = spec.ConfluenceQuery
	case types.SourceWeb:
		query = spec.WebQuery
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" && spec.ContentType == "" {
		// Only apply the generic default when no content_type is set.
		// Providers with content_type-aware filename logic (e.g., GitHub)
		// will choose the right format themselves when filenameFormat is empty.
		filenameFormat = sources.DefaultFilenameFormat(integration)
	}

	metadata := make(map[string]string)
	if spec.SearchType != "" {
		metadata["search_type"] = spec.SearchType
	}
	if spec.ContentType != "" {
		metadata["content_type"] = spec.ContentType
	}
	if spec.ProjectID > 0 {
		metadata["project_id"] = strconv.Itoa(spec.ProjectID)
	}
	if len(spec.IncludePaths) > 0 {
		if pathsJSON, err := json.Marshal(spec.IncludePaths); err == nil {
			metadata["include_paths"] = string(pathsJSON)
		}
	}
	if spec.WebMode != "" {
		metadata["web_mode"] = spec.WebMode
	}
	if spec.IncludeAttachments != nil {
		metadata["include_attachments"] = strconv.FormatBool(*spec.IncludeAttachments)
	}
	if spec.IncludeInline != nil {
		metadata["include_inline"] = strconv.FormatBool(*spec.IncludeInline)
	}
	if spec.IncludeMessageBody != nil {
		metadata["include_message_body"] = strconv.FormatBool(*spec.IncludeMessageBody)
	}
	if spec.ThreadID != "" {
		metadata["thread_id"] = strings.TrimSpace(spec.ThreadID)
	}
	if spec.MessageID != "" {
		metadata["message_id"] = strings.TrimSpace(spec.MessageID)
	}
	if spec.CredentialMemberID != "" {
		metadata[legacyQueryCredentialMemberIDKey] = strings.TrimSpace(spec.CredentialMemberID)
	}

	return sources.QuerySpec{
		Query:          query,
		Limit:          limit,
		MaxResults:     maxResults,
		FilenameFormat: filenameFormat,
		Metadata:       metadata,
	}
}

func extractFilenameFormat(specJSON []byte) string {
	var spec struct {
		FilenameFormat string `json:"filename_format"`
	}
	if json.Unmarshal(specJSON, &spec) != nil {
		return ""
	}
	return spec.FilenameFormat
}

func buildGmailQuerySpec(query string, limit int, filenameFormat string) string {
	data, _ := json.Marshal(map[string]any{
		"gmail_query":     query,
		"limit":           limit,
		"filename_format": filenameFormat,
	})
	return string(data)
}

// ---------------------------------------------------------------------------
// Proto converters
// ---------------------------------------------------------------------------

func viewToProto(q *types.FilesystemQuery) *pb.SourceView {
	if q == nil {
		return nil
	}
	v := &pb.SourceView{
		ExternalId:   q.ExternalId,
		Integration:  q.Integration,
		Path:         q.Path,
		Name:         q.Name,
		QuerySpec:    q.QuerySpec,
		Guidance:     q.Guidance,
		OutputFormat: string(q.OutputFormat),
		FileExt:      q.FileExt,
		CacheTtl:     int32(q.CacheTTL),
		CreatedAt:    q.CreatedAt.Unix(),
		UpdatedAt:    q.UpdatedAt.Unix(),
		Mode:         string(q.Mode),
		Filter:       q.Filter,
	}
	if q.LastExecuted != nil {
		v.LastExecuted = q.LastExecuted.Unix()
	}
	return v
}

// emptyQueryAllowed returns true for integrations whose providers handle
// empty query strings gracefully (i.e., return all items with no filter).
func emptyQueryAllowed(integration string) bool {
	switch types.SourceType(integration) {
	case types.SourceLinear, types.SourcePostHog:
		return true
	default:
		return false
	}
}

func querySpecAllowsEmptyQuery(integration string, spec sources.QuerySpec) bool {
	if emptyQueryAllowed(integration) {
		return true
	}
	switch types.SourceType(integration) {
	case types.SourceGmail:
		return strings.TrimSpace(spec.Metadata["thread_id"]) != "" ||
			strings.TrimSpace(spec.Metadata["message_id"]) != ""
	default:
		return false
	}
}

// isValidQueryName rejects names that could cause path traversal.
func isValidQueryName(name string) bool {
	return name != "" &&
		!strings.Contains(name, "/") &&
		!strings.Contains(name, "\\") &&
		!strings.Contains(name, "..")
}
