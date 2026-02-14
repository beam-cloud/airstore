package services

import (
	"context"
	"encoding/json"
	"fmt"
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

// Default pagination settings for smart queries.
const (
	defaultPageSize   = 50
	defaultMaxResults = 500
)

// ---------------------------------------------------------------------------
// Query refresh
// ---------------------------------------------------------------------------

// InvalidateQueryCache invalidates cached results for a query path.
func (s *SourceService) InvalidateQueryCache(ctx context.Context, workspaceId uint, queryPath string) error {
	if s.fsStore == nil {
		return nil
	}
	log.Info().Uint("workspace_id", workspaceId).Str("path", queryPath).Msg("invalidating query cache")
	return s.fsStore.InvalidateQuery(ctx, workspaceId, queryPath)
}

// RefreshSmartQuery forces re-execution of a smart query, bypassing all caches.
func (s *SourceService) RefreshSmartQuery(ctx context.Context, queryPath string) ([]repository.QueryResult, error) {
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

	pctx, connected := s.loadCredentials(ctx, pctx, query.Integration)
	if !connected {
		return nil, fmt.Errorf("not connected to %s", query.Integration)
	}

	log.Info().
		Str("path", queryPath).Str("integration", query.Integration).
		Str("query_spec", query.QuerySpec).
		Msg("refreshing smart query")

	results, err := s.executeAndCacheQuery(ctx, pctx, query)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	log.Info().Str("path", queryPath).Int("results", len(results)).Msg("smart query refresh complete")
	return results, nil
}

// RefreshQuery re-executes a query and emits hook events for new results.
// Called ONLY by the source poller — never by user browsing or task reads.
// This prevents a feedback loop where hook-triggered tasks re-fire hooks.
func (s *SourceService) RefreshQuery(ctx context.Context, query *types.FilesystemQuery) error {
	pctx := &sources.ProviderContext{WorkspaceId: query.WorkspaceId}
	pctx, connected := s.loadCredentials(ctx, pctx, query.Integration)
	if !connected {
		return fmt.Errorf("not connected to %s (workspace %d)", query.Integration, query.WorkspaceId)
	}

	results, err := s.executeAndCacheQuery(ctx, pctx, query)
	if err != nil {
		return err
	}

	if s.seenTracker == nil || s.hookStream == nil || len(results) == 0 {
		return nil
	}

	seenKey := common.Keys.HookSeen(pctx.WorkspaceId, types.GeneratePathID(query.Path))
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.ID
	}

	newIDs, compareErr := s.seenTracker.Compare(ctx, seenKey, ids)
	if compareErr != nil {
		log.Warn().Err(compareErr).Str("path", query.Path).Msg("seen tracker compare failed, skipping commit")
		return nil
	}

	if len(newIDs) > 0 {
		if emitErr := s.hookStream.Emit(ctx, map[string]any{
			"event":        hooks.EventSourceChange,
			"workspace_id": fmt.Sprintf("%d", pctx.WorkspaceId),
			"path":         query.Path,
			"integration":  query.Integration,
			"new_count":    fmt.Sprintf("%d", len(newIDs)),
			"new_items":    strings.Join(newIDs, ", "),
		}); emitErr != nil {
			log.Error().Err(emitErr).Str("path", query.Path).Int("new_results", len(newIDs)).
				Msg("failed to emit source change event, will retry next poll")
			return nil // don't commit — retry on next poll
		}
		log.Info().
			Str("path", query.Path).Str("integration", query.Integration).
			Int("new_results", len(newIDs)).
			Msg("source change detected, hook event emitted")
	}

	if err := s.seenTracker.Commit(ctx, seenKey, ids); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("seen tracker commit failed, next poll may re-fire")
	}
	return nil
}

// ---------------------------------------------------------------------------
// Query execution
// ---------------------------------------------------------------------------

func (s *SourceService) executeAndCacheQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return nil, fmt.Errorf("provider not found: %s", query.Integration)
	}

	executor, ok := provider.(sources.QueryExecutor)
	if !ok {
		return nil, fmt.Errorf("provider does not support queries: %s", query.Integration)
	}

	spec := parseQuerySpec(query.Integration, query.QuerySpec)
	if spec.Query == "" && query.Integration != string(types.SourcePostHog) {
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

func (s *SourceService) getOrExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, query *types.FilesystemQuery) ([]repository.QueryResult, error) {
	if results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, query.Path); err == nil && len(results) > 0 {
		return results, nil
	}

	key := fmt.Sprintf("%d:%s", pctx.WorkspaceId, query.Path)
	value, err, _ := s.queryGroup.Do(key, func() (any, error) {
		if results, err := s.fsStore.GetQueryResults(ctx, pctx.WorkspaceId, query.Path); err == nil && len(results) > 0 {
			return results, nil
		}
		return s.executeAndCacheQuery(ctx, pctx, query)
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

// ---------------------------------------------------------------------------
// Smart Query CRUD (gRPC handlers)
// ---------------------------------------------------------------------------

func (s *SourceService) CreateSmartQuery(ctx context.Context, req *pb.CreateSmartQueryRequest) (*pb.CreateSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.CreateSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	if !isValidQueryName(req.Name) {
		return &pb.CreateSmartQueryResponse{Ok: false, Error: "invalid query name: must not contain '/' or '..' sequences"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	path := types.PathSources + "/" + req.Integration + "/" + req.Name
	if req.FileExt != "" {
		path += req.FileExt
	}

	querySpec, filenameFormat, err := s.resolveQuerySpec(ctx, req.Integration, req.Name, req.Guidance)
	if err != nil {
		return &pb.CreateSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}
	querySpec = s.refineQueryIfNeeded(ctx, req.Integration, req.Guidance, querySpec, filenameFormat)

	query := &types.FilesystemQuery{
		WorkspaceId:    workspaceId,
		Integration:    req.Integration,
		Path:           path,
		Name:           req.Name,
		QuerySpec:      querySpec,
		Guidance:       req.Guidance,
		OutputFormat:   types.QueryOutputFormat(req.OutputFormat),
		FileExt:        req.FileExt,
		FilenameFormat: filenameFormat,
		CacheTTL:       0,
	}

	created, err := s.fsStore.CreateQuery(ctx, query)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("failed to create query")
		return &pb.CreateSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	log.Info().Str("path", path).Str("query", querySpec).Msg("created filesystem query")
	return &pb.CreateSmartQueryResponse{Ok: true, Query: smartQueryToProto(created)}, nil
}

func (s *SourceService) GetSmartQuery(ctx context.Context, req *pb.GetSmartQueryRequest) (*pb.GetSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.GetSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	query, err := s.fsStore.GetQuery(ctx, auth.WorkspaceId(ctx), req.Path)
	if err != nil {
		return &pb.GetSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}
	if query == nil {
		return &pb.GetSmartQueryResponse{Ok: true, Query: nil}, nil
	}
	return &pb.GetSmartQueryResponse{Ok: true, Query: smartQueryToProto(query)}, nil
}

func (s *SourceService) ListSmartQueries(ctx context.Context, req *pb.ListSmartQueriesRequest) (*pb.ListSmartQueriesResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ListSmartQueriesResponse{Ok: false, Error: "unauthorized"}, nil
	}
	queries, err := s.fsStore.ListQueries(ctx, auth.WorkspaceId(ctx), req.ParentPath)
	if err != nil {
		return &pb.ListSmartQueriesResponse{Ok: false, Error: err.Error()}, nil
	}
	out := make([]*pb.SmartQuery, len(queries))
	for i, q := range queries {
		out[i] = smartQueryToProto(q)
	}
	return &pb.ListSmartQueriesResponse{Ok: true, Queries: out}, nil
}

func (s *SourceService) DeleteSmartQuery(ctx context.Context, req *pb.DeleteSmartQueryRequest) (*pb.DeleteSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	query, err := s.fsStore.GetQueryByExternalId(ctx, req.ExternalId)
	if err != nil || query == nil {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}
	if query.WorkspaceId != auth.WorkspaceId(ctx) {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	if err := s.fsStore.InvalidateQuery(ctx, query.WorkspaceId, query.Path); err != nil {
		log.Warn().Err(err).Str("path", query.Path).Msg("failed to invalidate query cache")
	}
	if err := s.fsStore.DeleteQuery(ctx, req.ExternalId); err != nil {
		return &pb.DeleteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}

	log.Info().Str("external_id", req.ExternalId).Str("path", query.Path).Msg("deleted filesystem query")
	return &pb.DeleteSmartQueryResponse{Ok: true}, nil
}

func (s *SourceService) UpdateSmartQuery(ctx context.Context, req *pb.UpdateSmartQueryRequest) (*pb.UpdateSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	query, err := s.fsStore.GetQueryByExternalId(ctx, req.ExternalId)
	if err != nil || query == nil {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}
	if query.WorkspaceId != workspaceId {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}

	oldPath := query.Path
	needsUpdate := false

	// Rename: update path.
	if req.Name != "" && !isValidQueryName(req.Name) {
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: "invalid query name: must not contain '/' or '..' sequences"}, nil
	}
	if req.Name != "" && req.Name != query.Name {
		query.Name = req.Name
		query.Path = types.PathSources + "/" + query.Integration + "/" + req.Name
		if query.FileExt != "" {
			query.Path += query.FileExt
		}
		needsUpdate = true
	}

	// Re-run LLM inference if guidance changed.
	if req.Guidance != query.Guidance {
		query.Guidance = req.Guidance
		querySpec, filenameFormat, err := s.resolveQuerySpec(ctx, query.Integration, query.Name, req.Guidance)
		if err != nil {
			return &pb.UpdateSmartQueryResponse{Ok: false, Error: "failed to regenerate query: " + err.Error()}, nil
		}
		query.QuerySpec = s.refineQueryIfNeeded(ctx, query.Integration, req.Guidance, querySpec, filenameFormat)
		if filenameFormat != "" {
			query.FilenameFormat = filenameFormat
		}
		needsUpdate = true
	}

	if !needsUpdate {
		return &pb.UpdateSmartQueryResponse{Ok: true, Query: filesystemQueryToProto(query)}, nil
	}

	if err := s.fsStore.UpdateQuery(ctx, query); err != nil {
		log.Error().Err(err).Str("external_id", req.ExternalId).Msg("failed to update query")
		return &pb.UpdateSmartQueryResponse{Ok: false, Error: err.Error()}, nil
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
		Msg("updated filesystem query")
	return &pb.UpdateSmartQueryResponse{Ok: true, Query: filesystemQueryToProto(query)}, nil
}

// ExecuteSmartQuery runs a query and returns materialized results.
func (s *SourceService) ExecuteSmartQuery(ctx context.Context, req *pb.ExecuteSmartQueryRequest) (*pb.ExecuteSmartQueryResponse, error) {
	if !auth.IsAuthenticated(ctx) {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "unauthorized"}, nil
	}
	workspaceId := auth.WorkspaceId(ctx)

	query, err := s.fsStore.GetQuery(ctx, workspaceId, req.Path)
	if err != nil || query == nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "query not found"}, nil
	}

	pctx, err := s.providerContext(ctx)
	if err != nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}
	pctx, connected := s.loadCredentials(ctx, pctx, query.Integration)
	if !connected {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "not connected"}, nil
	}

	provider := s.registry.Get(query.Integration)
	if provider == nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "integration not available"}, nil
	}

	// If requesting file content, resolve the result ID and read.
	if req.Filename != "" {
		executor, ok := provider.(sources.QueryExecutor)
		if !ok {
			return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "provider does not support queries"}, nil
		}

		resultId := req.ResultId
		if resultId == "" {
			results, err := s.getOrExecuteQuery(ctx, pctx, query)
			if err != nil {
				return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "failed to get query results: " + err.Error()}, nil
			}
			for _, r := range results {
				if r.Filename == req.Filename {
					resultId = r.ID
					break
				}
			}
			if resultId == "" {
				return &pb.ExecuteSmartQueryResponse{Ok: false, Error: "file not found in query results"}, nil
			}
		}

		// Compressed read path.
		strategyStr, session := s.compressionMeta(ctx)
		if strategyStr != "" && s.compressor != nil {
			log.Debug().
				Str("strategy", strategyStr).Str("file", req.Filename).Str("path", req.Path).
				Msg("compression: entering compressed read path (ExecuteSmartQuery)")
			resp, err := s.readWithCompression(ctx, pctx, executor, query.Integration, req.Path, req.Filename, resultId, query.QuerySpec, 0, 0, strategyStr, session)
			if err != nil {
				return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
			}
			return &pb.ExecuteSmartQueryResponse{Ok: true, FileData: resp.Data}, nil
		} else if strategyStr != "" {
			log.Warn().Str("strategy", strategyStr).Str("file", req.Filename).
				Msg("compression: requested but compressor not initialized")
		}

		// Standard read.
		if content, err := s.fsStore.GetResultContent(ctx, workspaceId, req.Path, resultId); err == nil && len(content) > 0 {
			return &pb.ExecuteSmartQueryResponse{Ok: true, FileData: content}, nil
		}
		data, err := executor.ReadResult(ctx, pctx, resultId)
		if err != nil {
			return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
		}
		if err := s.fsStore.StoreResultContent(ctx, workspaceId, req.Path, resultId, data); err != nil {
			log.Warn().Err(err).Str("path", req.Path).Str("result", resultId).Msg("failed to cache query result content")
		}
		return &pb.ExecuteSmartQueryResponse{Ok: true, FileData: data}, nil
	}

	// List mode: return directory entries.
	results, err := s.getOrExecuteQuery(ctx, pctx, query)
	if err != nil {
		return &pb.ExecuteSmartQueryResponse{Ok: false, Error: err.Error()}, nil
	}
	entries := make([]*pb.SourceDirEntry, 0, len(results))
	for _, r := range results {
		entries = append(entries, &pb.SourceDirEntry{
			Name: r.Filename, Mode: sources.ModeFile, Size: r.Size, Mtime: r.Mtime, ResultId: r.ID,
		})
	}
	return &pb.ExecuteSmartQueryResponse{Ok: true, Entries: entries}, nil
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
	if spec.Query == "" && integration != string(types.SourcePostHog) {
		return "", "", fmt.Errorf("invalid query spec from inference")
	}
	if filenameFormat == "" {
		filenameFormat = spec.FilenameFormat
	}
	if filenameFormat == "" {
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
		GmailQuery     string   `json:"gmail_query"`
		GDriveQuery    string   `json:"gdrive_query"`
		NotionQuery    string   `json:"notion_query"`
		GitHubQuery    string   `json:"github_query"`
		SlackQuery     string   `json:"slack_query"`
		LinearQuery    string   `json:"linear_query"`
		PostHogQuery   string   `json:"posthog_query"`
		WebQuery       string   `json:"web_query"`
		IncludePaths   []string `json:"include_paths"`
		SearchType     string   `json:"search_type"`
		ContentType    string   `json:"content_type"`
		ProjectID      int      `json:"project_id"`
		Limit          int      `json:"limit"`
		MaxResults     int      `json:"max_results"`
		FilenameFormat string   `json:"filename_format"`
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
	case types.SourceWeb:
		query = spec.WebQuery
	}

	filenameFormat := spec.FilenameFormat
	if filenameFormat == "" {
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

func smartQueryToProto(q *types.SmartQuery) *pb.SmartQuery {
	if q == nil {
		return nil
	}
	return &pb.SmartQuery{
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
	}
}

func filesystemQueryToProto(q *types.FilesystemQuery) *pb.SmartQuery {
	return smartQueryToProto(q)
}

// isValidQueryName rejects names that could cause path traversal.
func isValidQueryName(name string) bool {
	return name != "" &&
		!strings.Contains(name, "/") &&
		!strings.Contains(name, "\\") &&
		!strings.Contains(name, "..")
}
