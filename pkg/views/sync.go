package views

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	viewbaml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	viewbamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
	"github.com/rs/zerolog/log"
)

type ViewSyncOpts struct {
	Store   *ViewStore
	Backend repository.BackendRepository
	Config  types.ViewSyncConfig
}

type ViewSync struct {
	store   *ViewStore
	backend repository.BackendRepository
	config  types.ViewSyncConfig
	locks   sync.Map // "taskID:sheetID" -> *sync.Mutex
}

func NewViewSync(opts ViewSyncOpts) *ViewSync {
	return &ViewSync{
		store:   opts.Store,
		backend: opts.Backend,
		config:  opts.Config.WithDefaults(),
	}
}

func (vs *ViewSync) HighMatchThreshold() float64 {
	if vs == nil {
		return 0.87
	}
	return vs.config.HighMatchThreshold
}

func (vs *ViewSync) lockFor(taskID, sheetID string) *sync.Mutex {
	key := taskID + ":" + sheetID
	actual, _ := vs.locks.LoadOrStore(key, &sync.Mutex{})
	return actual.(*sync.Mutex)
}

// SyncResult captures what happened across all sheets.
type SyncResult struct {
	Updated []string
	Created []string
	Skipped bool
}

func (r *SyncResult) merge(other *SyncResult) {
	if other == nil {
		return
	}
	r.Updated = append(r.Updated, other.Updated...)
	r.Created = append(r.Created, other.Created...)
}

func (r *SyncResult) changed() bool {
	return len(r.Updated) > 0 || len(r.Created) > 0
}

// Sync is the top-level entry point. It loads all schema contexts for the
// view and runs syncSchema for every sheet. Vector search + BAML determine
// relevance per sheet — there is no hardcoded output-type gating.
func (vs *ViewSync) Sync(ctx context.Context, output *types.TaskOutput) *SyncResult {
	if vs.store == nil || !vs.store.Available() || vs.backend == nil || output == nil {
		return nil
	}
	if skipOutput(output) {
		return &SyncResult{Skipped: true}
	}

	agentID := ""
	if output.AgentID != nil {
		agentID = strings.TrimSpace(*output.AgentID)
	}
	if agentID == "" {
		log.Debug().
			Str("task_id", output.TaskID).
			Str("output_type", output.OutputType).
			Str("title", output.Title).
			Msg("viewsync: skipped — no agent_id on output")
		return nil
	}

	schemas, err := types.LoadViewOutputSchemaContexts(ctx, vs.backend, output.WorkspaceID, agentID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: load schema contexts failed")
		return nil
	}

	// Scope to the output's source view if known. Check metadata first,
	// then fall back to the task's payload for source_view_id.
	targetViewID := outputSourceViewID(output)
	if targetViewID == "" {
		targetViewID = vs.taskSourceViewID(ctx, output)
	}

	// Group schemas by view so each view gets its own timeout budget.
	viewSchemas := make(map[string][]types.ViewOutputSchemaContext)
	var viewOrder []string
	for _, sc := range schemas {
		if sc.ViewID == "" || sc.SheetID == "" || len(sc.Columns) == 0 {
			continue
		}
		if targetViewID != "" && sc.ViewID != targetViewID {
			continue
		}
		if _, seen := viewSchemas[sc.ViewID]; !seen {
			viewOrder = append(viewOrder, sc.ViewID)
		}
		viewSchemas[sc.ViewID] = append(viewSchemas[sc.ViewID], sc)
	}

	log.Info().
		Str("task_id", output.TaskID).
		Str("output_type", output.OutputType).
		Str("title", output.Title).
		Str("agent_id", agentID).
		Str("target_view", targetViewID).
		Int("schemas", len(schemas)).
		Int("views", len(viewOrder)).
		Msg("viewsync: Sync invoked")

	ch := make(chan *SyncResult, len(viewOrder))
	for _, viewID := range viewOrder {
		go func(vid string) {
			vr := &SyncResult{}
			viewCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
			defer cancel()
			for _, sc := range viewSchemas[vid] {
				if viewCtx.Err() != nil {
					break
				}
				mu := vs.lockFor(output.TaskID, sc.SheetID)
				mu.Lock()
				r := vs.syncSchema(viewCtx, output, sc, viewSchemas[vid])
				mu.Unlock()
				vr.merge(r)
			}
			ch <- vr
		}(viewID)
	}

	result := &SyncResult{}
	for range viewOrder {
		result.merge(<-ch)
	}
	return result
}

// ToolWriteInput describes a write made by the view tool that should be
// propagated to other sheets in the same view via the full BAML pipeline.
type ToolWriteInput struct {
	ViewID            string
	WorkspaceID       uint
	SourceSheetID     string
	SourceComponentID string
	Cells             map[string]string
	RowID             string
	ForceInsert       bool // skip candidate matching, always insert a new row
}

// SyncToolWrite propagates a view-tool write to all other sheets in the view
// using the same vector search + BAML pipeline as Sync. The source sheet
// (already written by the tool) is excluded.
func (vs *ViewSync) SyncToolWrite(ctx context.Context, input ToolWriteInput) *SyncResult {
	if vs == nil || vs.store == nil || !vs.store.Available() || vs.backend == nil {
		return nil
	}
	if len(input.Cells) == 0 || input.ViewID == "" {
		return nil
	}

	view, err := vs.backend.GetView(ctx, input.WorkspaceID, input.ViewID)
	if err != nil || view == nil {
		log.Warn().Err(err).Str("view_id", input.ViewID).Msg("viewsync-tool: load view failed")
		return nil
	}

	var allSchemas []types.ViewOutputSchemaContext
	var targetSchemas []types.ViewOutputSchemaContext
	for _, sheet := range view.Definition.Sheets {
		for _, comp := range sheet.Components {
			if !comp.IsTable() {
				continue
			}
			sc := types.BuildViewOutputSchemaContext(view, sheet, comp)
			if sc == nil {
				continue
			}
			allSchemas = append(allSchemas, *sc)
			if sheet.ID != input.SourceSheetID || comp.ID != input.SourceComponentID {
				targetSchemas = append(targetSchemas, *sc)
			}
		}
	}
	if len(targetSchemas) == 0 {
		log.Debug().Str("view_id", input.ViewID).Int("all_schemas", len(allSchemas)).Msg("viewsync-tool: no target sheets")
		return &SyncResult{Skipped: true}
	}

	cellData := make(map[string]any, len(input.Cells))
	for k, v := range input.Cells {
		cellData[k] = v
	}
	summaryBytes, _ := json.Marshal(input.Cells)
	summary := string(summaryBytes)

	output := &types.TaskOutput{
		ID:          "tool-write-" + input.RowID,
		WorkspaceID: input.WorkspaceID,
		TaskID:      "tool:" + input.RowID,
		OutputType:  "view_update",
		Title:       "View row update",
		Summary:     &summary,
		Data:        cellData,
		Status:      types.TaskOutputStatusActive,
	}

	log.Debug().
		Str("view_id", input.ViewID).
		Str("source_sheet", input.SourceSheetID).
		Str("row_id", input.RowID).
		Bool("force_insert", input.ForceInsert).
		Int("targets", len(targetSchemas)).
		Msg("viewsync-tool: propagating")

	viewCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
	defer cancel()

	result := &SyncResult{}
	for _, sc := range targetSchemas {
		if viewCtx.Err() != nil {
			break
		}
		mu := vs.lockFor(output.TaskID, sc.SheetID)
		mu.Lock()
		var r *SyncResult
		if input.ForceInsert {
			r = vs.syncSchemaInsertOnly(viewCtx, output, sc, allSchemas)
		} else {
			r = vs.syncSchema(viewCtx, output, sc, allSchemas)
		}
		mu.Unlock()
		result.merge(r)
	}

	return result
}

// syncSchemaInsertOnly always inserts a new row, skipping candidate matching.
// Used for import propagation where each source row must create a distinct
// target row.
func (vs *ViewSync) syncSchemaInsertOnly(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	allSchemas []types.ViewOutputSchemaContext,
) *SyncResult {
	cols := bamlColumns(sc)
	data := serializeOutput(output)
	summary := safeDeref(output.Summary)
	viewCtxStr := buildViewContext(allSchemas)

	ec := vs.store.Embedder()

	var queries []string
	plan, planErr := viewbaml.PlanRowSearch(ctx, cols, output.OutputType, output.Title, summary, data, sc.SheetName, viewCtxStr)
	if planErr == nil {
		criteria := buildSearchCriteria(&plan)
		hints := entityHints(&plan, nil)
		queries = vectorQueryTexts(criteria, hints, output.OutputType, output.Title, summary, data)
	}

	var crossCtx string
	if ec != nil && ec.Available() && len(queries) > 0 {
		crossCtx = vs.crossSheetContext(ctx, sc, ec, queries, allSchemas)
	}

	created, err := vs.insertRows(ctx, output, sc, cols, data, summary, &plan, entityHints(&plan, nil), crossCtx, viewCtxStr)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: force-insert failed")
	}
	return &SyncResult{Created: created}
}

// syncSchema handles resolution and upsert/insert for a single sheet.
// allSchemas is the full set of schema contexts across the view, used to
// gather cross-sheet context when inserting into a sheet that has no data yet.
func (vs *ViewSync) syncSchema(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	allSchemas []types.ViewOutputSchemaContext,
) *SyncResult {
	cols := bamlColumns(sc)
	keys := sc.ColumnKeys()
	data := serializeOutput(output)
	summary := safeDeref(output.Summary)
	viewCtxStr := buildViewContext(allSchemas)

	ec := vs.store.Embedder()

	if ec != nil && ec.Available() {
		return vs.syncVectorPath(ctx, output, sc, cols, keys, data, summary, ec, allSchemas, viewCtxStr)
	}
	return vs.syncFallbackPath(ctx, output, sc, cols, keys, data, summary, viewCtxStr)
}

// syncVectorPath uses embedding-based resolution: vector search partitioned
// by score into high-confidence matches and moderate candidates for BAML
// classification. Falls through to insert if no matches. When inserting,
// cross-sheet vector search provides context from related rows in other
// sheets so BAML can produce richer cell values.
func (vs *ViewSync) syncVectorPath(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	keys []string,
	data, summary string,
	ec *EmbeddingClient,
	allSchemas []types.ViewOutputSchemaContext,
	viewCtxStr string,
) *SyncResult {
	result := &SyncResult{}

	if err := vs.store.EnsureVectorIndex(ctx, sc.ViewID, ec.Dims()); err != nil {
		log.Warn().Err(err).Str("view_id", sc.ViewID).Msg("viewsync: vector index failed")
	}

	plan, planErr := viewbaml.PlanRowSearch(ctx, cols, output.OutputType, output.Title, summary, data, sc.SheetName, viewCtxStr)
	if planErr != nil {
		log.Warn().Err(planErr).Str("task_id", output.TaskID).Msg("viewsync: PlanRowSearch failed")
	}

	criteria := buildSearchCriteria(&plan)
	hints := entityHints(&plan, nil)
	queries := vectorQueryTexts(criteria, hints, output.OutputType, output.Title, summary, data)
	results, err := vs.vectorCandidates(ctx, sc, ec, queries)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: search failed, falling back")
		return vs.syncFallbackPath(ctx, output, sc, cols, keys, data, summary, viewCtxStr)
	}

	var candidates []ViewRow
	var highMatches int
	var moderateCandidates int
	for _, r := range results {
		if r.Score >= vs.config.HighMatchThreshold {
			highMatches++
			candidates = append(candidates, r.ViewRow)
		} else if r.Score >= vs.config.ClassifyFloor {
			moderateCandidates++
			candidates = append(candidates, r.ViewRow)
		}
	}

	log.Debug().
		Str("task_id", output.TaskID).
		Str("sheet_id", sc.SheetID).
		Int("total", len(results)).
		Int("high", highMatches).
		Int("moderate", moderateCandidates).
		Msg("viewsync: scored")

	if len(candidates) > 0 {
		updated, unmatched := vs.classifyCandidateRows(ctx, output, sc, cols, keys, data, summary, candidates, viewCtxStr)
		result.Updated = append(result.Updated, updated...)
		if result.changed() {
			return result
		}
		hints = entityHints(&plan, unmatched)
	}

	// Path C: no existing rows matched in this sheet -> insert new row(s).
	// Gather cross-sheet context via vector search so BAML can enrich the
	// new row with data from related rows in other sheets (e.g. seed data).
	crossCtx := vs.crossSheetContext(ctx, sc, ec, queries, allSchemas)

	created, err := vs.insertRows(ctx, output, sc, cols, data, summary, &plan, hints, crossCtx, viewCtxStr)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: insert failed")
	}
	result.Created = append(result.Created, created...)
	return result
}

// crossSheetContext searches for related rows in OTHER sheets of the same
// view via vector search. Returns a serialized string of those rows' cells
// so BAML can use them as enrichment context when creating new rows.
func (vs *ViewSync) crossSheetContext(
	ctx context.Context,
	targetSC types.ViewOutputSchemaContext,
	ec *EmbeddingClient,
	queries []string,
	allSchemas []types.ViewOutputSchemaContext,
) string {
	if ec == nil || !ec.Available() || len(queries) == 0 {
		return ""
	}

	// Collect rows from other sheets via unscoped vector search.
	// Use only the first few queries to limit API calls.
	capped := queries
	if len(capped) > 3 {
		capped = capped[:3]
	}
	byID := make(map[string]VectorSearchResult)
	for _, query := range capped {
		if ctx.Err() != nil {
			break
		}
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		queryVec, err := ec.EmbedOne(ctx, query)
		if err != nil {
			continue
		}
		results, err := vs.store.VectorSearch(ctx, targetSC.ViewID, "", queryVec, vs.config.VectorLimit)
		if err != nil {
			continue
		}
		for _, r := range results {
			if r.ViewRow.SheetID == targetSC.SheetID {
				continue
			}
			if r.Score < vs.config.ClassifyFloor {
				continue
			}
			if cur, ok := byID[r.ID]; !ok || r.Score > cur.Score {
				byID[r.ID] = r
			}
		}
	}

	if len(byID) == 0 {
		return ""
	}

	// Build schema metadata lookup for readable output.
	schemaKeys := make(map[string][]string)
	sheetNames := make(map[string]string)
	for _, sc := range allSchemas {
		if sc.SheetID == targetSC.SheetID {
			continue
		}
		schemaKeys[sc.SheetID] = sc.ColumnKeys()
		sheetNames[sc.SheetID] = sc.SheetName
	}

	var rows []ViewRow
	for _, r := range byID {
		rows = append(rows, r.ViewRow)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID })

	return formatCrossSheetContext(rows, schemaKeys, sheetNames)
}

func (vs *ViewSync) vectorCandidates(
	ctx context.Context,
	sc types.ViewOutputSchemaContext,
	ec *EmbeddingClient,
	queries []string,
) ([]VectorSearchResult, error) {
	if ec == nil || !ec.Available() || len(queries) == 0 {
		return nil, nil
	}

	byID := make(map[string]VectorSearchResult)
	for _, query := range queries {
		if ctx.Err() != nil {
			break
		}
		query = strings.TrimSpace(query)
		if query == "" {
			continue
		}
		queryVec, err := ec.EmbedOne(ctx, query)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return nil, fmt.Errorf("embed query %q: %w", query, err)
		}
		results, err := vs.store.VectorSearch(ctx, sc.ViewID, sc.SheetID, queryVec, vs.config.VectorLimit)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return nil, err
		}
		for _, result := range results {
			current, ok := byID[result.ID]
			if !ok || result.Score > current.Score {
				byID[result.ID] = result
			}
		}
	}

	out := make([]VectorSearchResult, 0, len(byID))
	for _, result := range byID {
		out = append(out, result)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score == out[j].Score {
			return out[i].ID < out[j].ID
		}
		return out[i].Score > out[j].Score
	})
	return out, nil
}

func (vs *ViewSync) classifyCandidateRows(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	keys []string,
	data, summary string,
	candidates []ViewRow,
	viewCtxStr string,
) ([]string, []string) {
	if len(candidates) == 0 {
		return nil, nil
	}

	cls, err := viewbaml.ClassifyAffectedRows(
		ctx, cols,
		output.OutputType, output.Title, summary, data,
		serializeRows(candidates, keys),
	)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: classify failed")
		return nil, nil
	}

	rowIdx := make(map[string]*ViewRow, len(candidates))
	for i := range candidates {
		rowIdx[candidates[i].ID] = &candidates[i]
	}

	var updated []string
	for _, rid := range cls.Affected_row_ids {
		if row, ok := rowIdx[rid]; ok {
			if vs.upsertRow(ctx, output, sc, cols, keys, data, summary, row) {
				updated = append(updated, row.ID)
			}
		}
	}

	log.Debug().
		Str("task_id", output.TaskID).
		Int("affected", len(cls.Affected_row_ids)).
		Int("unmatched", len(cls.Unmatched_entities)).
		Msg("viewsync: classified")

	return updated, cls.Unmatched_entities
}

// syncFallbackPath uses LLM-based search + classification when embeddings
// are not available.
func (vs *ViewSync) syncFallbackPath(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	keys []string,
	data, summary string,
	viewCtxStr string,
) *SyncResult {
	result := &SyncResult{}

	plan, err := viewbaml.PlanRowSearch(ctx, cols, output.OutputType, output.Title, summary, data, sc.SheetName, viewCtxStr)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: PlanRowSearch failed")
		return result
	}

	var candidates []ViewRow
	criteria := buildSearchCriteria(&plan)
	if len(criteria) > 0 {
		candidates, err = vs.store.SearchRows(ctx, sc.ViewID, sc.SheetID, sc.ComponentID, criteria, 50)
		if err != nil {
			log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: SearchRows failed")
		}
	}

	if len(candidates) == 0 {
		created, err := vs.insertRows(ctx, output, sc, cols, data, summary, &plan, nil, "", viewCtxStr)
		if err != nil {
			log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: insert failed")
		}
		result.Created = append(result.Created, created...)
		return result
	}

	updated, unmatched := vs.classifyCandidateRows(ctx, output, sc, cols, keys, data, summary, candidates, viewCtxStr)
	result.Updated = append(result.Updated, updated...)

	if !result.changed() {
		created, err := vs.insertRows(ctx, output, sc, cols, data, summary, &plan, unmatched, "", viewCtxStr)
		if err != nil {
			log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: insert failed")
		}
		result.Created = append(result.Created, created...)
	}
	return result
}

// upsertRow populates cells via BAML and merges into an existing row using
// the shared ViewStore.UpdateRow primitive.
func (vs *ViewSync) upsertRow(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	keys []string,
	data, summary string,
	row *ViewRow,
) bool {
	res, err := viewbaml.PopulateRowCells(
		ctx, cols,
		output.OutputType, output.Title, summary, data,
		row.ID, cellsJSON(row, keys), "",
		sc.SheetName,
	)
	if err != nil {
		log.Warn().Err(err).Str("row_id", row.ID).Msg("viewsync: PopulateRowCells failed")
		return false
	}
	cells := extractCells(res.Cells)
	if len(cells) == 0 {
		return false
	}
	if err := vs.store.UpdateRow(ctx, sc.ViewID, row.ID, cells, output.ID); err != nil {
		log.Warn().Err(err).Str("row_id", row.ID).Msg("viewsync: UpdateRow failed")
		return false
	}

	log.Debug().
		Str("task_id", output.TaskID).
		Str("row_id", row.ID).
		Int("cells", len(cells)).
		Msg("viewsync: upserted row")
	return true
}

// insertRows creates new row(s) from an output using row-level entity hints
// from BAML. If BAML identifies multiple entities, we insert one row per hint
// and never synthesize a generic summary row in Go. crossCtx provides
// serialized rows from other sheets for enrichment context.
func (vs *ViewSync) insertRows(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	data, summary string,
	plan *viewbamltypes.RowSearchPlan,
	preferredHints []string,
	crossCtx string,
	viewCtxStr string,
) ([]string, error) {
	schemaHash := vs.resolveSchemaHash(ctx, output.WorkspaceID, sc)
	opts := UpsertOpts{
		TaskID:     output.TaskID,
		GroupID:    output.TaskID,
		OutputID:   output.ID,
		SchemaHash: schemaHash,
	}

	minCells := vs.config.MinInsertCells
	if nCols := len(cols); nCols > 0 && nCols < minCells*2 {
		proportional := (nCols + 2) / 3
		if proportional < 2 {
			proportional = 2
		}
		if proportional < minCells {
			minCells = proportional
		}
	}

	hints := entityHints(plan, preferredHints)
	if len(hints) > 0 {
		log.Debug().
			Str("task_id", output.TaskID).
			Str("sheet_id", sc.SheetID).
			Int("entities", len(hints)).
			Msg("viewsync: inserting entity-scoped rows")

		var created []string
		for _, entity := range hints {
			rowID, ok, err := vs.tryInsertEntity(ctx, output, sc, cols, data, summary, entity, opts, crossCtx, minCells)
			if err != nil {
				return created, err
			}
			if ok {
				created = append(created, rowID)
			}
		}
		return created, nil
	}

	rowID, ok, err := vs.tryInsertEntity(ctx, output, sc, cols, data, summary, "", opts, crossCtx, minCells)
	if err != nil {
		return nil, err
	}
	if ok {
		return []string{rowID}, nil
	}
	return nil, nil
}

// tryInsertEntity populates cells for a single entity and inserts it via
// the shared ViewStore.UpsertRow primitive. crossCtx provides data from
// related rows in other sheets so BAML can enrich the new row.
func (vs *ViewSync) tryInsertEntity(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	data, summary, entityHint string,
	opts UpsertOpts,
	crossCtx string,
	minCells int,
) (string, bool, error) {
	enrichedData := data
	if crossCtx != "" {
		enrichedData = data + "\n\n" + crossCtx
	}

	res, err := viewbaml.PopulateRowCells(
		ctx, cols,
		output.OutputType, output.Title, summary, enrichedData,
		"", "", entityHint,
		sc.SheetName,
	)
	if err != nil {
		return "", false, fmt.Errorf("PopulateRowCells: %w", err)
	}

	cells := extractCells(res.Cells)
	if len(cells) < minCells {
		log.Debug().
			Str("task_id", output.TaskID).
			Str("sheet_id", sc.SheetID).
			Str("entity", entityHint).
			Int("cells", len(cells)).
			Int("min", minCells).
			Msg("viewsync: skipped insert (too few cells)")
		return "", false, nil
	}

	insertOpts := opts
	if rowKey := strings.TrimSpace(res.Row_key); rowKey != "" {
		insertOpts.RowKey = rowKey
	} else if entityHint != "" {
		insertOpts.RowKey = entityHint
	}

	rowID, created, err := vs.store.UpsertRow(ctx, sc.ViewID, sc.SheetID, sc.ComponentID, cells, insertOpts)
	if err != nil {
		return "", false, fmt.Errorf("UpsertRow: %w", err)
	}

	log.Debug().
		Str("task_id", output.TaskID).
		Str("row_id", rowID).
		Bool("created", created).
		Msg("viewsync: insert entity")

	return rowID, true, nil
}

func (vs *ViewSync) resolveSchemaHash(
	ctx context.Context,
	workspaceID uint,
	sc types.ViewOutputSchemaContext,
) string {
	if vs.backend == nil {
		return ""
	}
	v, err := vs.backend.GetView(ctx, workspaceID, sc.ViewID)
	if err != nil || v == nil {
		return ""
	}
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sc.SheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID == sc.ComponentID && comp.IsTable() {
				return MappingSchemaHash(sheet, comp)
			}
		}
		for _, comp := range sheet.Components {
			if comp.IsTable() {
				return MappingSchemaHash(sheet, comp)
			}
		}
	}
	return ""
}

func outputSourceViewID(output *types.TaskOutput) string {
	if output.Metadata == nil {
		return ""
	}
	if v, ok := output.Metadata[types.TaskOutputMetadataViewSchemaViewID]; ok {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func (vs *ViewSync) taskSourceViewID(ctx context.Context, output *types.TaskOutput) string {
	if vs.backend == nil || output.TaskID == "" {
		return ""
	}
	task, err := vs.backend.GetTask(ctx, output.WorkspaceID, output.TaskID)
	if err != nil || task == nil {
		return ""
	}
	vid, _ := task.PayloadJSON["source_view_id"].(string)
	return strings.TrimSpace(vid)
}
