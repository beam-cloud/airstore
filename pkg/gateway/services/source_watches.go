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

const sourceWatchBaselinePendingKey = "source_watch_baseline_pending"

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
	if service.seenTracker == nil {
		return nil, fmt.Errorf("seen tracker is unavailable")
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

func (c *taskSourceWatchController) Cleanup(ctx context.Context) error {
	var cleanupErrs []string

	taskQueries, err := c.service.fsStore.ListTaskOwnedQueries(ctx, c.task.WorkspaceID, c.task.ID)
	if err != nil {
		return fmt.Errorf("list task-owned source watches: %w", err)
	}
	for _, query := range taskQueries {
		if err := c.cleanupOwnedQuery(ctx, query); err != nil {
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
		if err := c.service.cleanupSourceWatchResources(ctx, c.task.WorkspaceID, hook); err != nil {
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
			_ = c.service.cleanupSourceWatchResources(ctx, c.task.WorkspaceID, hook)
		} else if createdQuery {
			_ = c.service.cleanupQueryOnly(ctx, c.task.WorkspaceID, query)
		}
		return nil, err
	}

	if err := c.service.resetSourceWatchBaseline(ctx, c.task.WorkspaceID, query.Path); err != nil {
		return nil, fmt.Errorf("reset source watch baseline %s: %w", query.Path, err)
	}

	results, err := c.service.bootstrapSourceWatchBaseline(ctx, c.task.WorkspaceID, query)
	if err != nil {
		log.Warn().
			Err(err).
			Str("task_id", c.task.ID).
			Str("path", query.Path).
			Str("integration", query.Integration).
			Msg("source watch bootstrap failed; watch remains armed with pending baseline")
	}

	return &sourceWatchRegistration{
		Query:   query,
		Hook:    hook,
		Request: req,
		Results: results,
	}, nil
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
	querySpec, filenameFormat, err := buildSourceWatchQuerySpec(req, credentialMemberID, true)
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
	return created, true, nil
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

func (s *SourceService) seedSourceWatchBaseline(ctx context.Context, workspaceID uint, queryPath string, results []repository.QueryResult) error {
	ids := make([]string, 0, len(results))
	for _, result := range results {
		id := strings.TrimSpace(result.ID)
		if id == "" {
			continue
		}
		ids = append(ids, id)
	}
	seenKey := common.Keys.HookSeen(workspaceID, types.GeneratePathID(hooks.NormalizePath(queryPath)))
	return s.seenTracker.Commit(ctx, seenKey, ids)
}

func (s *SourceService) resetSourceWatchBaseline(ctx context.Context, workspaceID uint, queryPath string) error {
	if s == nil || s.seenTracker == nil {
		return nil
	}
	return s.seenTracker.ResetPath(ctx, workspaceID, queryPath)
}

func (s *SourceService) bootstrapSourceWatchBaseline(
	ctx context.Context,
	workspaceID uint,
	query *types.FilesystemQuery,
) ([]repository.QueryResult, error) {
	if query == nil {
		return nil, fmt.Errorf("source watch query is required")
	}
	pctx, connected := s.loadQueryCredentials(ctx, &sources.ProviderContext{WorkspaceId: workspaceID}, query)
	if !connected {
		return nil, fmt.Errorf("not connected to %s", query.Integration)
	}
	results, err := s.invalidateAndExecute(ctx, pctx, query, "source_watch_register")
	if err != nil {
		return nil, fmt.Errorf("initial sync %s: %w", query.Path, err)
	}
	if err := s.completePendingSourceWatchBaseline(ctx, query, results); err != nil {
		return nil, err
	}
	return results, nil
}

func (s *SourceService) completePendingSourceWatchBaseline(
	ctx context.Context,
	query *types.FilesystemQuery,
	results []repository.QueryResult,
) error {
	if query == nil {
		return fmt.Errorf("source watch query is required")
	}
	if err := s.seedSourceWatchBaseline(ctx, query.WorkspaceId, query.Path, results); err != nil {
		return fmt.Errorf("seed source watch baseline %s: %w", query.Path, err)
	}
	updated, err := setSourceWatchQueryState(query, query.CredentialMemberID, false)
	if err != nil {
		return fmt.Errorf("update source watch query state %s: %w", query.Path, err)
	}
	if !updated || s == nil || s.fsStore == nil {
		return nil
	}
	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		return fmt.Errorf("persist source watch query state %s: %w", query.Path, err)
	}
	return nil
}

func (s *SourceService) cleanupSourceWatchResources(ctx context.Context, workspaceID uint, hook *types.Hook) error {
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
		if err := s.cleanupQueryOnly(ctx, workspaceID, query); err != nil {
			return err
		}
	} else if s.seenTracker != nil {
		if err := s.seenTracker.ResetPath(ctx, workspaceID, hook.Path); err != nil {
			return fmt.Errorf("reset seen tracker %s: %w", hook.Path, err)
		}
	}
	return nil
}

func (c *taskSourceWatchController) cleanupOwnedQuery(ctx context.Context, query *types.FilesystemQuery) error {
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
	return c.service.cleanupQueryOnly(ctx, c.task.WorkspaceID, query)
}

func (s *SourceService) cleanupQueryOnly(ctx context.Context, workspaceID uint, query *types.FilesystemQuery) error {
	if query == nil {
		return nil
	}
	if err := s.fsStore.InvalidateQuery(ctx, workspaceID, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate source watch query cache during cleanup")
	}
	if err := s.fsStore.DeleteQuery(ctx, query.ExternalId); err != nil {
		return fmt.Errorf("delete source watch query %s: %w", query.ExternalId, err)
	}
	if s.seenTracker != nil {
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

func buildSourceWatchQuerySpec(
	req *types.SourceWatchRequest,
	credentialMemberID *uint,
	baselinePending bool,
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
	if baselinePending {
		payload[sourceWatchBaselinePendingKey] = true
	}

	switch strings.ToLower(req.Integration) {
	case string(types.SourceGmail):
		payload["gmail_query"] = req.Query
		if req.ThreadID != "" {
			payload["thread_id"] = req.ThreadID
		}
		if req.MessageID != "" {
			payload["message_id"] = req.MessageID
		}
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

func sourceWatchBaselinePending(query *types.FilesystemQuery) bool {
	if query == nil || strings.TrimSpace(query.QuerySpec) == "" {
		return false
	}
	var spec struct {
		BaselinePending bool `json:"source_watch_baseline_pending"`
	}
	if err := json.Unmarshal([]byte(query.QuerySpec), &spec); err != nil {
		return false
	}
	return spec.BaselinePending
}

func setSourceWatchQueryState(
	query *types.FilesystemQuery,
	credentialMemberID *uint,
	baselinePending bool,
) (bool, error) {
	if query == nil {
		return false, nil
	}

	payload := make(map[string]any)
	if raw := strings.TrimSpace(query.QuerySpec); raw != "" {
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			return false, fmt.Errorf("unmarshal source watch query spec: %w", err)
		}
	}

	if credentialMemberID != nil {
		payload[legacyQueryCredentialMemberIDKey] = fmt.Sprintf("%d", *credentialMemberID)
	} else {
		delete(payload, legacyQueryCredentialMemberIDKey)
	}
	if baselinePending {
		payload[sourceWatchBaselinePendingKey] = true
	} else {
		delete(payload, sourceWatchBaselinePendingKey)
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return false, fmt.Errorf("marshal source watch query spec: %w", err)
	}
	nextSpec := string(encoded)
	if query.QuerySpec == nextSpec {
		return false, nil
	}
	query.QuerySpec = nextSpec
	return true, nil
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
	sum := sha1.Sum([]byte(types.SourceWatchRequestSignature(req)))
	hash := hex.EncodeToString(sum[:])[:12]
	taskPrefix := strings.TrimSpace(taskID)
	if len(taskPrefix) > 12 {
		taskPrefix = taskPrefix[:12]
	}
	name := fmt.Sprintf("__followup__%s__%s", taskPrefix, hash)
	return path.Join(types.PathSources, integration, name)
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
