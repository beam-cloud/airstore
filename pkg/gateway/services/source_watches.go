package services

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

type sourceWatchRegistration struct {
	Query   *types.FilesystemQuery
	Hook    *types.Hook
	Request *types.SourceWatchRequest
	Results []repository.QueryResult
}

type taskSourceWatchController struct {
	service *SourceService
	task    *types.AgentTask
}

func (s *SourceService) RegisterTaskSourceWatches(
	ctx context.Context,
	task *types.AgentTask,
	wakeSignal *types.RunExecutionWakeSignal,
	requests []*types.SourceWatchRequest,
) (*types.TaskBlockerSpec, error) {
	if task == nil || len(requests) == 0 {
		return nil, nil
	}
	controller, err := newTaskSourceWatchController(s, task)
	if err != nil {
		return nil, err
	}
	return controller.Register(ctx, wakeSignal, requests)
}

func (s *SourceService) HasTaskSourceWatches(ctx context.Context, task *types.AgentTask) bool {
	if s == nil || s.fsStore == nil || task == nil || task.WorkspaceID == 0 || strings.TrimSpace(task.ID) == "" {
		return false
	}
	queries, err := s.fsStore.ListTaskOwnedQueries(ctx, task.WorkspaceID, task.ID)
	return err == nil && len(queries) > 0
}

func (s *SourceService) CleanupTaskSourceWatches(ctx context.Context, task *types.AgentTask) error {
	if s == nil || s.fsStore == nil || task == nil || task.WorkspaceID == 0 || strings.TrimSpace(task.ID) == "" {
		return nil
	}
	controller, err := newTaskSourceWatchController(s, task)
	if err != nil {
		return err
	}
	return controller.Cleanup(ctx)
}

func newTaskSourceWatchController(service *SourceService, task *types.AgentTask) (*taskSourceWatchController, error) {
	if service == nil || service.fsStore == nil {
		return nil, fmt.Errorf("source service is unavailable")
	}
	if service.registry == nil {
		return nil, fmt.Errorf("source registry is unavailable")
	}
	if service.seenTracker == nil && service.taskWaker == nil {
		return nil, fmt.Errorf("seen tracker or task waker is required")
	}
	if task == nil || task.WorkspaceID == 0 || strings.TrimSpace(task.ID) == "" {
		return nil, fmt.Errorf("task is required")
	}
	return &taskSourceWatchController{service: service, task: task}, nil
}

func (c *taskSourceWatchController) Register(
	ctx context.Context,
	wakeSignal *types.RunExecutionWakeSignal,
	requests []*types.SourceWatchRequest,
) (*types.TaskBlockerSpec, error) {
	normalized := normalizeSourceWatchRequestsForRegistration(requests)
	if len(normalized) == 0 {
		return nil, fmt.Errorf("no valid source watch requests")
	}
	merged, err := c.mergeExistingWatchContext(ctx, normalized)
	if err != nil {
		return nil, err
	}
	normalized = merged

	// Compute paths that will be re-registered so Cleanup can preserve
	// their SeenTracker baselines. Without this, the poller can race
	// between the reset and re-seed, treating existing items as "new".
	preservePaths := make(map[string]struct{}, len(normalized))
	for _, req := range normalized {
		preservePaths[systemManagedSourceWatchPath(c.task.ID, req)] = struct{}{}
	}
	if err := c.cleanupPreservingPaths(ctx, preservePaths); err != nil {
		return nil, err
	}

	registrations := make([]*sourceWatchRegistration, 0, len(normalized))
	for _, req := range normalized {
		registration, err := c.register(ctx, req)
		if err != nil {
			return nil, err
		}
		if registration != nil {
			registrations = append(registrations, registration)
		}
	}
	if len(registrations) == 0 {
		return nil, fmt.Errorf("source watch registration produced no materialized views")
	}
	return buildSourceWatchBlockerSpec(wakeSignal, registrations), nil
}

func (c *taskSourceWatchController) mergeExistingWatchContext(
	ctx context.Context,
	requests []*types.SourceWatchRequest,
) ([]*types.SourceWatchRequest, error) {
	existing, err := c.existingWatchRequests(ctx)
	if err != nil {
		return nil, err
	}
	if len(existing) == 0 {
		return requests, nil
	}

	merged := make([]*types.SourceWatchRequest, 0, len(requests))
	for _, req := range requests {
		merged = append(merged, mergeSourceWatchRequestWithFallback(req, bestMatchingExistingSourceWatchRequest(req, existing)))
	}
	return normalizeSourceWatchRequestsForRegistration(merged), nil
}

func (c *taskSourceWatchController) existingWatchRequests(ctx context.Context) ([]*types.SourceWatchRequest, error) {
	taskQueries, err := c.service.fsStore.ListTaskOwnedQueries(ctx, c.task.WorkspaceID, c.task.ID)
	if err != nil {
		return nil, fmt.Errorf("list existing task source watches: %w", err)
	}

	requests := make([]*types.SourceWatchRequest, 0, len(taskQueries))
	for _, query := range taskQueries {
		if req := sourceWatchRequestFromQuery(query); req != nil {
			requests = append(requests, req)
		}
	}
	return requests, nil
}

func (c *taskSourceWatchController) Cleanup(ctx context.Context) error {
	return c.cleanupPreservingPaths(ctx, nil)
}

// cleanupPreservingPaths removes task-owned source watches. Paths in
// preservePaths skip the SeenTracker reset — their baseline data is
// kept intact so the poller cannot race and treat existing items as new.
func (c *taskSourceWatchController) cleanupPreservingPaths(ctx context.Context, preservePaths map[string]struct{}) error {
	var cleanupErrs []string

	taskQueries, err := c.service.fsStore.ListTaskOwnedQueries(ctx, c.task.WorkspaceID, c.task.ID)
	if err != nil {
		return fmt.Errorf("list task-owned source watches: %w", err)
	}
	for _, query := range taskQueries {
		if preservePaths != nil {
			if _, keep := preservePaths[query.Path]; keep {
				continue
			}
		}
		if err := c.cleanupOwnedQuery(ctx, query, true); err != nil {
			cleanupErrs = append(cleanupErrs, err.Error())
		}
	}

	hooksForWorkspace, err := c.service.fsStore.ListHooks(ctx, c.task.WorkspaceID)
	if err != nil {
		return fmt.Errorf("list hooks: %w", err)
	}
	for _, hook := range hooksForWorkspace {
		if hook == nil || !hook.SystemManaged {
			continue
		}
		if hook.TargetTaskID == nil || strings.TrimSpace(*hook.TargetTaskID) != strings.TrimSpace(c.task.ID) {
			continue
		}
		if preservePaths != nil {
			if _, keep := preservePaths[hook.Path]; keep {
				continue
			}
		}
		if err := c.service.cleanupSourceWatchResources(ctx, c.task.WorkspaceID, hook, true); err != nil {
			cleanupErrs = append(cleanupErrs, err.Error())
		}
	}
	if len(cleanupErrs) > 0 {
		return fmt.Errorf("cleanup source watches: %s", strings.Join(cleanupErrs, "; "))
	}
	return nil
}

func (c *taskSourceWatchController) register(
	ctx context.Context,
	req *types.SourceWatchRequest,
) (*sourceWatchRegistration, error) {
	queryPath := systemManagedSourceWatchPath(c.task.ID, req)
	query, createdQuery, err := c.upsertQuery(ctx, queryPath, req)
	if err != nil {
		return nil, err
	}

	hook, createdHook, err := c.upsertHook(ctx, query, req)
	if err != nil {
		if createdHook {
			_ = c.service.cleanupSourceWatchResources(ctx, c.task.WorkspaceID, hook, true)
		} else if createdQuery {
			_ = c.service.cleanupQueryOnly(ctx, c.task.WorkspaceID, query, true)
		}
		return nil, err
	}

	if createdQuery {
		c.seedBaseline(ctx, query)
	}

	return &sourceWatchRegistration{
		Query:   query,
		Hook:    hook,
		Request: req,
		Results: nil,
	}, nil
}

// seedBaseline sets the Postgres baseline_item_ids for a followup query from
// cached query results, and also seeds the Redis SeenTracker for backward
// compatibility with non-followup paths.
func (c *taskSourceWatchController) seedBaseline(ctx context.Context, query *types.FilesystemQuery) {
	queryPath := hooks.NormalizePath(query.Path)
	results, err := c.service.fsStore.GetQueryResults(ctx, c.task.WorkspaceID, queryPath)
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

	if query.IsTaskFollowUp() {
		if err := c.service.fsStore.UpdateQueryBaseline(ctx, query.Id, ids); err != nil {
			log.Warn().Err(err).Str("path", queryPath).Msg("seed baseline: postgres update failed")
		} else {
			log.Info().Str("path", queryPath).Int("baseline_items", len(ids)).
				Msg("seeded postgres baseline from cached query results")
		}
		return
	}

	c.service.SeedSeenBaseline(ctx, c.task.WorkspaceID, query)
}

func (c *taskSourceWatchController) upsertQuery(
	ctx context.Context,
	queryPath string,
	req *types.SourceWatchRequest,
) (*types.FilesystemQuery, bool, error) {
	credentialMemberID, err := c.service.sourceWatchCredentialMemberID(ctx, c.task, req.Integration)
	if err != nil {
		return nil, false, fmt.Errorf("resolve source watch credentials: %w", err)
	}
	querySpec, filenameFormat, err := buildSourceWatchQuerySpec(req, credentialMemberID)
	if err != nil {
		return nil, false, err
	}
	ownerTaskID := c.task.ID
	return c.service.saveViewDefinition(ctx, &types.FilesystemQuery{
		WorkspaceId:        c.task.WorkspaceID,
		CredentialMemberID: credentialMemberID,
		SystemManaged:      true,
		Lifecycle:          types.FilesystemQueryLifecycleTaskFollowUp,
		OwnerTaskID:        &ownerTaskID,
		OwnerRunID:         c.task.TargetRunID,
		Integration:        req.Integration,
		Path:               queryPath,
		Name:               path.Base(queryPath),
		QuerySpec:          querySpec,
		Guidance:           systemManagedSourceWatchGuidance(c.task.ID, req),
		OutputFormat:       types.ViewOutputFolder,
		FilenameFormat:     filenameFormat,
		CacheTTL:           0,
		Mode:               types.ViewModeQuery,
	}, true)
}

func (c *taskSourceWatchController) upsertHook(
	ctx context.Context,
	query *types.FilesystemQuery,
	req *types.SourceWatchRequest,
) (*types.Hook, bool, error) {
	existing, err := c.service.findHookByPath(ctx, c.task.WorkspaceID, query.Path)
	if err != nil {
		return nil, false, err
	}

	targetTaskID := c.task.ID
	prompt := sourceWatchHookPrompt(req)
	agentID := c.task.AgentID
	eventTypes := types.NormalizeSourceWatchEventTypes(req.EventTypes)

	if existing != nil {
		existing.Prompt = prompt
		existing.AgentId = agentID
		existing.Active = true
		existing.EventTypes = eventTypes
		existing.DeliveryMode = types.HookDeliveryModeTaskInput
		existing.TargetTaskID = &targetTaskID
		existing.SystemManaged = true
		existing.OneShot = true
		existing.CreatedByMemberId = query.CredentialMemberID
		if err := c.service.fsStore.UpdateHook(ctx, existing); err != nil {
			return nil, false, fmt.Errorf("update source watch hook: %w", err)
		}
		c.service.invalidateHookCache(c.task.WorkspaceID)
		return existing, false, nil
	}

	created, err := c.service.fsStore.CreateHook(ctx, &types.Hook{
		WorkspaceId:       c.task.WorkspaceID,
		Path:              query.Path,
		Prompt:            prompt,
		AgentId:           agentID,
		Active:            true,
		EventTypes:        eventTypes,
		DeliveryMode:      types.HookDeliveryModeTaskInput,
		TargetTaskID:      &targetTaskID,
		SystemManaged:     true,
		OneShot:           true,
		CreatedByMemberId: query.CredentialMemberID,
	})
	if err != nil {
		return nil, false, fmt.Errorf("create source watch hook: %w", err)
	}
	c.service.invalidateHookCache(c.task.WorkspaceID)
	return created, true, nil
}

func (s *SourceService) invalidateHookCache(workspaceID uint) {
	if s.eventBus == nil {
		return
	}
	s.eventBus.Emit(common.Event{
		Type: common.EventCacheInvalidate,
		Data: map[string]any{
			"scope":        "hooks",
			"workspace_id": workspaceID,
		},
	})
}

func (s *SourceService) findHookByPath(ctx context.Context, workspaceID uint, queryPath string) (*types.Hook, error) {
	hooksForWorkspace, err := s.fsStore.ListHooks(ctx, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("list hooks: %w", err)
	}
	normalizedPath := hooks.NormalizePath(queryPath)
	for _, hook := range hooksForWorkspace {
		if hook == nil {
			continue
		}
		if hooks.NormalizePath(hook.Path) == normalizedPath {
			return hook, nil
		}
	}
	return nil, nil
}

func (s *SourceService) cleanupSourceWatchResources(ctx context.Context, workspaceID uint, hook *types.Hook, resetSeen bool) error {
	if hook == nil {
		return nil
	}
	query, err := s.fsStore.GetQuery(ctx, workspaceID, hook.Path)
	if err != nil {
		return fmt.Errorf("lookup source watch query for %s: %w", hook.Path, err)
	}
	if err := s.fsStore.DeleteHook(ctx, hook.ExternalId); err != nil {
		return fmt.Errorf("delete source watch hook %s: %w", hook.ExternalId, err)
	}
	if query != nil {
		if err := s.cleanupQueryOnly(ctx, workspaceID, query, resetSeen); err != nil {
			return err
		}
	} else if resetSeen && s.seenTracker != nil {
		if err := s.seenTracker.ResetPath(ctx, workspaceID, hook.Path); err != nil {
			return fmt.Errorf("reset seen tracker %s: %w", hook.Path, err)
		}
	}
	return nil
}

func (c *taskSourceWatchController) cleanupOwnedQuery(ctx context.Context, query *types.FilesystemQuery, resetSeen bool) error {
	if query == nil {
		return nil
	}
	hook, err := c.service.findHookByPath(ctx, c.task.WorkspaceID, query.Path)
	if err != nil {
		return fmt.Errorf("lookup source watch hook for %s: %w", query.Path, err)
	}
	if hook != nil {
		if err := c.service.fsStore.DeleteHook(ctx, hook.ExternalId); err != nil {
			return fmt.Errorf("delete source watch hook %s: %w", hook.ExternalId, err)
		}
	}
	return c.service.cleanupQueryOnly(ctx, c.task.WorkspaceID, query, resetSeen)
}

func (s *SourceService) cleanupQueryOnly(ctx context.Context, workspaceID uint, query *types.FilesystemQuery, resetSeen bool) error {
	if query == nil {
		return nil
	}
	if err := s.fsStore.InvalidateQuery(ctx, workspaceID, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate source watch query cache during cleanup")
	}
	if err := s.fsStore.DeleteQuery(ctx, query.ExternalId); err != nil {
		return fmt.Errorf("delete source watch query %s: %w", query.ExternalId, err)
	}
	if resetSeen && s.seenTracker != nil {
		if err := s.seenTracker.ResetPath(ctx, workspaceID, query.Path); err != nil {
			return fmt.Errorf("reset seen tracker %s: %w", query.Path, err)
		}
	}
	return nil
}

func normalizeSourceWatchRequestsForRegistration(requests []*types.SourceWatchRequest) []*types.SourceWatchRequest {
	out := make([]*types.SourceWatchRequest, 0, len(requests))
	seen := make(map[string]struct{}, len(requests))
	for _, req := range requests {
		normalized := types.NormalizeSourceWatchRequest(req)
		if normalized == nil {
			continue
		}
		signature := types.SourceWatchRequestSignature(normalized)
		if _, exists := seen[signature]; exists {
			continue
		}
		seen[signature] = struct{}{}
		out = append(out, normalized)
	}
	sort.SliceStable(out, func(i, j int) bool {
		return types.SourceWatchRequestSignature(out[i]) < types.SourceWatchRequestSignature(out[j])
	})
	return out
}

func sourceWatchRequestFromQuery(query *types.FilesystemQuery) *types.SourceWatchRequest {
	if query == nil {
		return nil
	}
	spec := parseQuerySpec(query.Integration, query.QuerySpec)
	return types.NormalizeSourceWatchRequest(&types.SourceWatchRequest{
		Integration:        query.Integration,
		Query:              spec.Query,
		FilenameFormat:     spec.FilenameFormat,
		EntityLabel:        strings.TrimSpace(query.Name),
		ThreadID:           strings.TrimSpace(spec.Metadata["thread_id"]),
		MessageID:          strings.TrimSpace(spec.Metadata["message_id"]),
		IncludeAttachments: strings.EqualFold(spec.Metadata["include_attachments"], "true"),
		IncludeInline:      strings.EqualFold(spec.Metadata["include_inline"], "true"),
		IncludeMessageBody: strings.EqualFold(spec.Metadata["include_message_body"], "true"),
	})
}

func bestMatchingExistingSourceWatchRequest(
	req *types.SourceWatchRequest,
	existing []*types.SourceWatchRequest,
) *types.SourceWatchRequest {
	req = types.CanonicalizeSourceWatchRequest(req)
	if req == nil || len(existing) == 0 {
		return nil
	}

	bestScore := 0
	var best *types.SourceWatchRequest
	sameIntegrationCount := 0
	var sole *types.SourceWatchRequest
	for _, candidate := range existing {
		candidate = types.CanonicalizeSourceWatchRequest(candidate)
		if candidate == nil || !strings.EqualFold(req.Integration, candidate.Integration) {
			continue
		}
		sameIntegrationCount++
		sole = candidate

		score := 0
		if req.ThreadID != "" && req.ThreadID == candidate.ThreadID {
			score += 100
		}
		if req.MessageID != "" && req.MessageID == candidate.MessageID {
			score += 90
		}
		if req.SourceOutputID != "" && req.SourceOutputID == candidate.SourceOutputID {
			score += 80
		}
		if req.Query != "" && req.Query == candidate.Query {
			score += 70
		}
		if req.EntityKey != "" && req.EntityKey == candidate.EntityKey {
			score += 60
		}
		if req.EntityLabel != "" && req.EntityLabel == candidate.EntityLabel {
			score += 50
		}
		if score > bestScore {
			bestScore = score
			best = candidate
		}
	}
	if best != nil {
		return best
	}
	if sameIntegrationCount == 1 {
		return sole
	}
	return nil
}

func mergeSourceWatchRequestWithFallback(req, fallback *types.SourceWatchRequest) *types.SourceWatchRequest {
	req = types.CanonicalizeSourceWatchRequest(req)
	fallback = types.CanonicalizeSourceWatchRequest(fallback)
	if req == nil {
		return types.NormalizeSourceWatchRequest(fallback)
	}
	if fallback == nil {
		return types.NormalizeSourceWatchRequest(req)
	}

	merged := *req
	merged.Reason = firstNonEmptyWatchValue(req.Reason, fallback.Reason)
	merged.Query = firstNonEmptyWatchValue(req.Query, fallback.Query)
	merged.FilenameFormat = firstNonEmptyWatchValue(req.FilenameFormat, fallback.FilenameFormat)
	merged.EntityKey = firstNonEmptyWatchValue(req.EntityKey, fallback.EntityKey)
	merged.EntityLabel = firstNonEmptyWatchValue(req.EntityLabel, fallback.EntityLabel)
	merged.SourceOutputID = firstNonEmptyWatchValue(req.SourceOutputID, fallback.SourceOutputID)
	merged.ThreadID = firstNonEmptyWatchValue(req.ThreadID, fallback.ThreadID)
	merged.MessageID = firstNonEmptyWatchValue(req.MessageID, fallback.MessageID)
	merged.IncludeAttachments = req.IncludeAttachments || fallback.IncludeAttachments
	merged.IncludeInline = req.IncludeInline || fallback.IncludeInline
	merged.IncludeMessageBody = req.IncludeMessageBody || fallback.IncludeMessageBody
	if len(req.EventTypes) == 0 {
		merged.EventTypes = fallback.EventTypes
	}
	return types.NormalizeSourceWatchRequest(&merged)
}

func buildSourceWatchQuerySpec(
	req *types.SourceWatchRequest,
	credentialMemberID *uint,
) (string, string, error) {
	req = types.NormalizeSourceWatchRequest(req)
	if req == nil {
		return "", "", fmt.Errorf("source watch request is required")
	}
	filenameFormat := req.FilenameFormat
	if filenameFormat == "" {
		filenameFormat = sources.DefaultFilenameFormat(req.Integration)
	}

	payload := map[string]any{
		"limit":           defaultPageSize,
		"max_results":     defaultMaxResults,
		"filename_format": filenameFormat,
	}
	if credentialMemberID != nil {
		payload[legacyQueryCredentialMemberIDKey] = fmt.Sprintf("%d", *credentialMemberID)
	}

	switch strings.ToLower(req.Integration) {
	case string(types.SourceGmail):
		if req.ThreadID != "" {
			payload["thread_id"] = req.ThreadID
		} else {
			log.Warn().
				Str("query", req.Query).
				Str("entity_key", req.EntityKey).
				Str("entity_label", req.EntityLabel).
				Msg("gmail source watch has no thread_id — reply detection will use unreliable text query fallback")
		}
		payload["gmail_query"] = req.Query
		if req.MessageID != "" {
			payload["message_id"] = req.MessageID
		}
		payload["include_attachments"] = req.IncludeAttachments
		payload["include_inline"] = req.IncludeInline
		payload["include_message_body"] = req.IncludeMessageBody || (!req.IncludeAttachments && !req.IncludeInline)
	case string(types.SourceOutlook):
		if req.ThreadID != "" {
			payload["thread_id"] = req.ThreadID
		}
		if req.MessageID != "" {
			payload["message_id"] = req.MessageID
		}
		payload["outlook_query"] = req.Query
		payload["include_attachments"] = req.IncludeAttachments
		payload["include_inline"] = req.IncludeInline
		payload["include_message_body"] = req.IncludeMessageBody || (!req.IncludeAttachments && !req.IncludeInline)
	case string(types.SourceGDrive):
		payload["gdrive_query"] = req.Query
	case string(types.SourceNotion):
		payload["notion_query"] = req.Query
	case string(types.SourceGitHub):
		payload["github_query"] = req.Query
	case string(types.SourceSlack):
		payload["slack_query"] = req.Query
	case string(types.SourceLinear):
		payload["linear_query"] = req.Query
	case string(types.SourcePostHog):
		payload["posthog_query"] = req.Query
	case string(types.SourceConfluence):
		payload["cql_query"] = req.Query
	case string(types.SourceWeb):
		payload["web_query"] = req.Query
	default:
		return "", "", fmt.Errorf("unsupported source watch integration %q", req.Integration)
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", "", fmt.Errorf("marshal source watch query spec: %w", err)
	}
	return string(data), filenameFormat, nil
}

func buildSourceWatchBlockerSpec(wakeSignal *types.RunExecutionWakeSignal, registrations []*sourceWatchRegistration) *types.TaskBlockerSpec {
	if len(registrations) == 0 {
		return nil
	}

	summary := "Waiting for source updates."
	if len(registrations) == 1 {
		entry := registrations[0]
		label := firstNonEmptyWatchValue(entry.Request.EntityLabel, entry.Request.EntityKey, entry.Query.Path)
		summary = fmt.Sprintf("Waiting for %s updates on %s.", entry.Query.Integration, label)
	} else {
		summary = fmt.Sprintf("Waiting for %d source watches to change.", len(registrations))
	}
	if wakeSignal != nil && strings.TrimSpace(wakeSignal.Reason) != "" {
		summary = strings.TrimSpace(wakeSignal.Reason)
	}

	detailsLines := []string{summary}
	watchLines := make([]string, 0, len(registrations))
	for _, registration := range registrations {
		if registration == nil || registration.Query == nil || registration.Request == nil {
			continue
		}
		label := firstNonEmptyWatchValue(registration.Request.EntityLabel, registration.Request.EntityKey, registration.Query.Path)
		watchLines = append(watchLines, fmt.Sprintf("- Watching %s at `%s`", label, registration.Query.Path))
	}
	if len(watchLines) > 0 {
		detailsLines = append(detailsLines, "", "Registered source watches:")
		detailsLines = append(detailsLines, watchLines...)
	}

	entries := make([]types.SourceWatchBlockerEntry, 0, len(registrations))
	for _, registration := range registrations {
		if registration == nil || registration.Query == nil || registration.Request == nil {
			continue
		}
		entries = append(entries, types.SourceWatchBlockerEntry{
			Integration:    registration.Query.Integration,
			Path:           registration.Query.Path,
			EntityLabel:    registration.Request.EntityLabel,
			EntityKey:      registration.Request.EntityKey,
			SourceOutputID: registration.Request.SourceOutputID,
			ThreadID:       registration.Request.ThreadID,
			MessageID:      registration.Request.MessageID,
		})
	}
	return types.NewSourceWatchBlockerSpec(summary, strings.Join(detailsLines, "\n"), entries)
}

func sourceWatchHookPrompt(req *types.SourceWatchRequest) string {
	prompt := strings.TrimSpace(req.Reason)
	if prompt == "" {
		prompt = "Resume the originating task using this source update."
	}
	return prompt
}

func systemManagedSourceWatchGuidance(taskID string, req *types.SourceWatchRequest) string {
	label := firstNonEmptyWatchValue(req.EntityLabel, req.EntityKey, req.Query)
	return fmt.Sprintf("System-managed follow-up watch for task %s (%s).", strings.TrimSpace(taskID), label)
}

func systemManagedSourceWatchPath(taskID string, req *types.SourceWatchRequest) string {
	integration := strings.ToLower(strings.TrimSpace(req.Integration))
	identity := sourceWatchPathIdentity(integration, req)
	sum := sha1.Sum([]byte(identity))
	hash := hex.EncodeToString(sum[:])[:12]
	taskPrefix := strings.TrimSpace(taskID)
	if len(taskPrefix) > 12 {
		taskPrefix = taskPrefix[:12]
	}
	name := fmt.Sprintf("__followup__%s__%s", taskPrefix, hash)
	return path.Join(types.PathSources, integration, name)
}

// sourceWatchPathIdentity returns a stable identity string for path generation.
// Uses only the fields that form a true stable identity for the watch:
//   - ThreadID anchors Gmail watches (query text may be reformulated)
//   - EntityKey anchors entity-scoped watches
//   - Query is used only as last resort when no structural identity exists
func sourceWatchPathIdentity(integration string, req *types.SourceWatchRequest) string {
	threadID := strings.TrimSpace(req.ThreadID)
	entityKey := strings.TrimSpace(req.EntityKey)
	if threadID != "" {
		return strings.Join([]string{integration, threadID}, "\x00")
	}
	if entityKey != "" {
		return strings.Join([]string{integration, entityKey}, "\x00")
	}
	query := strings.TrimSpace(req.Query)
	return strings.Join([]string{integration, query}, "\x00")
}

func firstNonEmptyWatchValue(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

var _ orchestration.SourceWatchRegistrar = (*SourceService)(nil)
