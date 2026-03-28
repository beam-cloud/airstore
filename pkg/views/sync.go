package views

import (
	"context"
	"fmt"
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

// ResolvedRow pairs a ViewRow with its match score.
type ResolvedRow struct {
	Row   ViewRow
	Score float64
}

// Sync is the top-level entry point. It loads all matching schema contexts
// for the output, acquires per-task+sheet locks, and runs syncSchema for each.
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
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	schemas, err := types.LoadViewOutputSchemaContexts(ctx, vs.backend, output.WorkspaceID, agentID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: load schema contexts failed")
		return nil
	}

	result := &SyncResult{}
	for _, sc := range schemas {
		if sc.ViewID == "" || sc.SheetID == "" || len(sc.Columns) == 0 {
			continue
		}
		if !matchesSchema(output, sc) {
			continue
		}

		mu := vs.lockFor(output.TaskID, sc.SheetID)
		mu.Lock()
		r := vs.syncSchema(ctx, output, sc)
		mu.Unlock()
		result.merge(r)
	}
	return result
}

// syncSchema handles resolution and upsert/insert for a single sheet.
func (vs *ViewSync) syncSchema(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
) *SyncResult {
	cols := bamlColumns(sc)
	keys := sc.ColumnKeys()
	data := serializeOutput(output)
	summary := safeDeref(output.Summary)

	ec := vs.store.Embedder()

	if ec != nil && ec.Available() {
		return vs.syncVectorPath(ctx, output, sc, cols, keys, data, summary, ec)
	}
	return vs.syncFallbackPath(ctx, output, sc, cols, keys, data, summary)
}

// syncVectorPath uses embedding-based resolution: vector search partitioned
// by score into high-confidence matches and moderate candidates for BAML
// classification. Falls through to insert if no matches.
func (vs *ViewSync) syncVectorPath(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	keys []string,
	data, summary string,
	ec *EmbeddingClient,
) *SyncResult {
	result := &SyncResult{}

	if err := vs.store.EnsureVectorIndex(ctx, sc.ViewID, ec.Dims()); err != nil {
		log.Warn().Err(err).Str("view_id", sc.ViewID).Msg("viewsync: vector index failed")
	}

	// Try StableRef lookup first for deterministic resolution.
	refKey := NormalizeRowKey(output.Title)
	if refKey != "" {
		if row, _ := vs.store.FindByStableRef(ctx, sc.ViewID, sc.SheetID, refKey); row != nil {
			if vs.upsertRow(ctx, output, sc, cols, keys, data, summary, row) {
				result.Updated = append(result.Updated, row.ID)
			}
			return result
		}
	}

	queryText := OutputSearchText(output.OutputType, output.Title, summary, data)
	queryVec, err := ec.EmbedOne(ctx, queryText)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: embed failed, falling back")
		return vs.syncFallbackPath(ctx, output, sc, cols, keys, data, summary)
	}

	results, err := vs.store.VectorSearch(ctx, sc.ViewID, sc.SheetID, queryVec, vs.config.VectorLimit)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: search failed, falling back")
		return vs.syncFallbackPath(ctx, output, sc, cols, keys, data, summary)
	}

	var highMatches []VectorSearchResult
	var moderateCandidates []VectorSearchResult
	for _, r := range results {
		if r.Score >= vs.config.HighMatchThreshold {
			highMatches = append(highMatches, r)
		} else if r.Score >= vs.config.ClassifyFloor {
			moderateCandidates = append(moderateCandidates, r)
		}
	}

	log.Info().
		Str("task_id", output.TaskID).
		Str("view_id", sc.ViewID).
		Str("sheet_id", sc.SheetID).
		Int("total", len(results)).
		Int("high", len(highMatches)).
		Int("moderate", len(moderateCandidates)).
		Msg("viewsync: scored")

	// Path A: direct 1:1 update for high-confidence matches.
	if len(highMatches) > 0 {
		for _, m := range highMatches {
			if vs.upsertRow(ctx, output, sc, cols, keys, data, summary, &m.ViewRow) {
				result.Updated = append(result.Updated, m.ViewRow.ID)
			}
		}
		if result.changed() {
			return result
		}
	}

	// Path B: 1:many classification for moderate candidates.
	if len(moderateCandidates) > 0 {
		rows := make([]ViewRow, len(moderateCandidates))
		for i := range moderateCandidates {
			rows[i] = moderateCandidates[i].ViewRow
		}

		cls, err := viewbaml.ClassifyAffectedRows(
			ctx, cols,
			output.OutputType, output.Title, summary, data,
			serializeRows(rows, keys),
		)
		if err != nil {
			log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: classify failed")
		} else {
			rowIdx := make(map[string]*ViewRow, len(rows))
			for i := range rows {
				rowIdx[rows[i].ID] = &rows[i]
			}
			for _, rid := range cls.Affected_row_ids {
				if row, ok := rowIdx[rid]; ok {
					if vs.upsertRow(ctx, output, sc, cols, keys, data, summary, row) {
						result.Updated = append(result.Updated, row.ID)
					}
				}
			}

			log.Info().
				Str("task_id", output.TaskID).
				Int("affected", len(cls.Affected_row_ids)).
				Int("unmatched", len(cls.Unmatched_entities)).
				Msg("viewsync: classified")

			if result.changed() {
				return result
			}
		}
	}

	// Path C: no existing rows matched -> insert new row(s).
	created, err := vs.insertRows(ctx, output, sc, cols, data, summary)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: insert failed")
	}
	result.Created = append(result.Created, created...)
	return result
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
) *SyncResult {
	result := &SyncResult{}

	plan, err := viewbaml.PlanRowSearch(ctx, cols, output.OutputType, output.Title, summary, data)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: PlanRowSearch failed")
		return result
	}

	var candidates []ViewRow
	if len(plan.Criteria) > 0 {
		criteria := make([]SearchCriterion, len(plan.Criteria))
		for i, c := range plan.Criteria {
			criteria[i] = SearchCriterion{Column: c.Column, Value: c.Value}
		}
		candidates, err = vs.store.SearchRows(ctx, sc.ViewID, sc.SheetID, sc.ComponentID, criteria, 50)
		if err != nil {
			log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: SearchRows failed")
		}
	}

	if len(candidates) == 0 {
		created, err := vs.insertRows(ctx, output, sc, cols, data, summary)
		if err != nil {
			log.Warn().Err(err).Str("task_id", output.TaskID).Msg("viewsync: insert failed")
		}
		result.Created = append(result.Created, created...)
		return result
	}

	cls, err := viewbaml.ClassifyAffectedRows(ctx, cols, output.OutputType, output.Title, summary, data, serializeRows(candidates, keys))
	if err != nil {
		return result
	}

	rowIdx := make(map[string]*ViewRow, len(candidates))
	for i := range candidates {
		rowIdx[candidates[i].ID] = &candidates[i]
	}

	for _, rid := range cls.Affected_row_ids {
		if row, ok := rowIdx[rid]; ok {
			if vs.upsertRow(ctx, output, sc, cols, keys, data, summary, row) {
				result.Updated = append(result.Updated, row.ID)
			}
		}
	}

	if !result.changed() {
		created, err := vs.insertRows(ctx, output, sc, cols, data, summary)
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

	log.Info().
		Str("task_id", output.TaskID).
		Str("row_id", row.ID).
		Int("cells", len(cells)).
		Msg("viewsync: upserted row")
	return true
}

// insertRows creates new row(s) from an output. Attempts single-entity
// insert first; if the result is concatenated, decomposes via PlanRowSearch
// and inserts one row per entity.
func (vs *ViewSync) insertRows(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	data, summary string,
) ([]string, error) {
	schemaHash := vs.resolveSchemaHash(ctx, output.WorkspaceID, sc)
	opts := UpsertOpts{
		TaskID:     output.TaskID,
		GroupID:    output.TaskID,
		OutputID:   output.ID,
		SchemaHash: schemaHash,
	}

	// Try single-entity insert.
	rowID, ok, err := vs.tryInsertEntity(ctx, output, sc, cols, data, summary, "", opts)
	if err != nil {
		return nil, err
	}
	if ok {
		return []string{rowID}, nil
	}

	// Single insert failed (concatenated or too sparse). Decompose into
	// individual entities.
	plan, planErr := viewbaml.PlanRowSearch(ctx, cols, output.OutputType, output.Title, summary, data)
	if planErr != nil || len(plan.Entity_labels) < 2 {
		return nil, nil
	}

	log.Info().
		Str("task_id", output.TaskID).
		Int("entities", len(plan.Entity_labels)).
		Strs("labels", plan.Entity_labels).
		Msg("viewsync: decomposing multi-entity output")

	var created []string
	for _, entity := range plan.Entity_labels {
		rowID, ok, err := vs.tryInsertEntity(ctx, output, sc, cols, data, summary, entity, opts)
		if err != nil {
			return created, err
		}
		if ok {
			created = append(created, rowID)
		}
	}
	return created, nil
}

// tryInsertEntity populates cells for a single entity and inserts it via
// the shared ViewStore.UpsertRow primitive.
func (vs *ViewSync) tryInsertEntity(
	ctx context.Context,
	output *types.TaskOutput,
	sc types.ViewOutputSchemaContext,
	cols []viewbamltypes.ViewColumn,
	data, summary, entityHint string,
	opts UpsertOpts,
) (string, bool, error) {
	res, err := viewbaml.PopulateRowCells(
		ctx, cols,
		output.OutputType, output.Title, summary, data,
		"", "", entityHint,
	)
	if err != nil {
		return "", false, fmt.Errorf("PopulateRowCells: %w", err)
	}

	cells := extractCells(res.Cells)
	if len(cells) < vs.config.MinInsertCells || isConcatenated(cells) {
		return "", false, nil
	}

	rowKey := res.Row_key
	if rowKey == "" {
		rowKey = "task"
	}
	insertOpts := opts
	insertOpts.RowKey = rowKey

	rowID, created, err := vs.store.UpsertRow(ctx, sc.ViewID, sc.SheetID, sc.ComponentID, cells, insertOpts)
	if err != nil {
		return "", false, fmt.Errorf("UpsertRow: %w", err)
	}

	log.Info().
		Str("task_id", output.TaskID).
		Str("row_id", rowID).
		Str("entity", entityHint).
		Bool("created", created).
		Int("cells", len(cells)).
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
