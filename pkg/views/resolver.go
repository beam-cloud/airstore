package views

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"

	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

// ---------------------------------------------------------------------------
// DataResolver
// ---------------------------------------------------------------------------

var viewMappingFlight singleflight.Group

type viewMappingResult struct {
	Rows     []resolvedSheetRow
	TaskMeta map[string]*types.AgentTask
}

type resolvedSheetRow struct {
	SheetID  string
	TaskID   string
	RowID    string
	RowKey   string
	OutputID string
	Cells    map[string]string
}

const (
	taskMetadataColumnNextWakeAt      = "next_wake_at"
	taskMetadataColumnNextWakeSummary = "next_wake_summary"
	taskMetadataColumnWakeAt          = "wake_at"
	taskMetadataColumnWakeReason      = "wake_reason"
)

type DataResolver struct {
	backend dataResolverBackend
	store   *ViewStore
}

type ResolveOptions struct {
	ForceRefresh  bool
	ViewAgentRefs []string
}

func NewDataResolver(backend repository.BackendRepository, store *ViewStore) *DataResolver {
	return &DataResolver{backend: backend, store: store}
}

type dataResolverBackend interface {
	GetAgentProfileByKey(ctx context.Context, workspaceId uint, agentKey string) (*types.AgentProfile, error)
	ListAgentProfiles(ctx context.Context, workspaceId uint) ([]*types.AgentProfile, error)
	ListWorkspaceTaskOutputs(ctx context.Context, workspaceId uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error)
	GetTaskByID(ctx context.Context, taskId string) (*types.AgentTask, error)
}

// Resolve maps task outputs to a sheet table's schema using BAML.
func (r *DataResolver) Resolve(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, opts ResolveOptions) (*types.ResolvedData, error) {
	if !comp.IsTable() {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
	result, err := r.ensureSheetMapped(ctx, workspaceID, viewID, sheet, comp, opts)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
	return assembleTable(sheet.ID, comp, result.Rows, result.TaskMeta), nil
}

// RegenerateRow re-maps a single task's outputs through BAML for one sheet,
// replacing the cached rows for that task. Returns the full re-assembled table.
func (r *DataResolver) RegenerateRow(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, taskID string, opts ResolveOptions) (*types.ResolvedData, error) {
	if !comp.IsTable() {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
	tableCols := buildColumnSchemas(comp)
	if len(tableCols) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
	rowStrategy := effectiveRowStrategy(comp.DataSource)
	mappingCols := filterBamlColumns(tableCols)
	schemaH := hashColumns(mappingCols, rowStrategy, sheet.Name)

	allOutputs, err := r.fetchComponentOutputs(ctx, workspaceID, comp.DataSource, opts.ViewAgentRefs)
	if err != nil {
		return nil, fmt.Errorf("fetch component outputs: %w", err)
	}

	taskGroups := groupOutputsByTask(allOutputs)
	outputs, ok := taskGroups[taskID]
	if !ok || len(outputs) == 0 {
		return nil, fmt.Errorf("no outputs found for task %s", taskID)
	}

	taskPrompts := r.fetchTaskPrompts(ctx, []string{taskID})
	singleGroup := map[string][]*types.TaskOutput{taskID: outputs}
	outputsJSON, err := serializeOutputsForMapping(outputs, taskPrompts)
	if err != nil {
		return nil, fmt.Errorf("serialize outputs: %w", err)
	}

	result, err := baml.MapOutputsToSchema(
		ctx,
		sheet.Name,
		comp.Title,
		comp.Type,
		rowStrategy.Mode,
		rowStrategy.Description,
		mappingCols,
		outputsJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("BAML mapping: %w", err)
	}

	now := time.Now()
	var persisted []ViewRow
	for _, row := range result.Rows {
		if row.Task_id != taskID {
			continue
		}
		persisted = append(persisted, mappedRowToViewRow(sheet.ID, taskID, schemaH, singleGroup[taskID], row, now))
	}
	if len(persisted) == 0 {
		persisted = []ViewRow{fallbackViewRow(sheet.ID, taskID, schemaH, singleGroup[taskID], now)}
	}

	var keepRowIDs []string
	for _, row := range persisted {
		keepRowIDs = append(keepRowIDs, row.ID)
	}
	if err := r.store.UpsertRows(ctx, viewID, persisted); err != nil {
		return nil, fmt.Errorf("persist regenerated rows: %w", err)
	}
	if err := r.store.DeleteRowsNotInGroups(ctx, viewID, sheet.ID, []string{taskID}, keepRowIDs); err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Str("task_id", taskID).Msg("failed to delete stale rows after regeneration")
	}

	fullResult, err := r.Resolve(ctx, workspaceID, viewID, sheet, comp, ResolveOptions{ViewAgentRefs: opts.ViewAgentRefs})
	if err != nil {
		return nil, fmt.Errorf("re-resolve after regeneration: %w", err)
	}
	return fullResult, nil
}

// ensureSheetMapped uses singleflight to guarantee that concurrent Resolve
// calls for the same sheet table share a single mapping operation.
func (r *DataResolver) ensureSheetMapped(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, opts ResolveOptions) (*viewMappingResult, error) {
	flightKey := viewMappingFlightKey(workspaceID, viewID, sheet.ID, comp.ID, opts)

	val, err, _ := viewMappingFlight.Do(flightKey, func() (any, error) {
		return r.mapSheet(ctx, workspaceID, viewID, sheet, comp, opts)
	})
	if err != nil {
		return nil, err
	}

	return val.(*viewMappingResult), nil
}

// mapSheet does the actual work: fetch outputs for the sheet's table binding,
// check stored rows in MongoDB, call BAML for uncached/stale task groups, and
// persist results back.
func (r *DataResolver) mapSheet(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, opts ResolveOptions) (*viewMappingResult, error) {
	tableCols := buildColumnSchemas(comp)
	if len(tableCols) == 0 {
		return &viewMappingResult{Rows: nil, TaskMeta: map[string]*types.AgentTask{}}, nil
	}
	rowStrategy := effectiveRowStrategy(comp.DataSource)
	mappingCols := filterBamlColumns(tableCols)
	schemaH := hashColumns(mappingCols, rowStrategy, sheet.Name)

	allOutputs, err := r.fetchComponentOutputs(ctx, workspaceID, comp.DataSource, opts.ViewAgentRefs)
	if err != nil {
		return nil, fmt.Errorf("fetch component outputs: %w", err)
	}
	if len(allOutputs) == 0 {
		return &viewMappingResult{Rows: nil, TaskMeta: map[string]*types.AgentTask{}}, nil
	}

	taskGroups := groupOutputsByTask(allOutputs)
	taskMeta := r.fetchTaskMetadata(ctx, taskIDsFromGroups(taskGroups))

	existingRows, err := r.store.GetRows(ctx, viewID, sheet.ID)
	if err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Msg("failed to load stored rows, treating all as uncached")
		existingRows = nil
	}
	rowsByGroup := make(map[string][]ViewRow)
	for i := range existingRows {
		row := existingRows[i]
		rowsByGroup[row.GroupID] = append(rowsByGroup[row.GroupID], row)
	}

	uncachedIDs := make(map[string]bool)
	var resolvedRows []resolvedSheetRow
	applyManualEdits := !opts.ForceRefresh

	if opts.ForceRefresh {
		log.Info().
			Str("view_id", viewID).
			Str("sheet_id", sheet.ID).
			Str("schema_hash", schemaH).
			Int("tasks", len(taskGroups)).
			Msg("mongo: force refresh requested")
	}

	for taskID, outputs := range taskGroups {
		if opts.ForceRefresh {
			uncachedIDs[taskID] = true
			continue
		}

		taskOIDs := sortedOutputIDs(outputs)
		storedRows := rowsByGroup[taskID]
		if groupRowsFresh(storedRows, schemaH, taskOIDs) {
			resolvedRows = append(resolvedRows, resolvedRowsFromStored(storedRows, applyManualEdits)...)
			continue
		}
		uncachedIDs[taskID] = true
	}

	if len(uncachedIDs) == 0 {
		sortResolvedRows(resolvedRows, taskMeta)
		return &viewMappingResult{Rows: resolvedRows, TaskMeta: taskMeta}, nil
	}

	uncachedTIDs := make([]string, 0, len(uncachedIDs))
	for tid := range uncachedIDs {
		uncachedTIDs = append(uncachedTIDs, tid)
	}
	sort.Strings(uncachedTIDs)

	colKeys := make([]string, 0, len(mappingCols))
	for _, c := range mappingCols {
		colKeys = append(colKeys, c.Key)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheet.ID).
		Str("sheet_name", sheet.Name).
		Str("schema_hash", schemaH).
		Bool("force_refresh", opts.ForceRefresh).
		Int("tasks", len(taskGroups)).
		Int("cached", len(taskGroups)-len(uncachedIDs)).
		Int("uncached", len(uncachedIDs)).
		Int("total_columns", len(tableCols)).
		Int("mapping_columns", len(mappingCols)).
		Strs("column_keys", colKeys).
		Str("row_mode", rowStrategy.Mode).
		Msg("BAML mapping required")

	persistedByGroup := make(map[string][]ViewRow)
	mappedByGroup := make(map[string][]resolvedSheetRow)
	now := time.Now()

	if len(mappingCols) == 0 {
		for _, taskID := range uncachedTIDs {
			row := fallbackViewRow(sheet.ID, taskID, schemaH, taskGroups[taskID], now)
			persistedByGroup[taskID] = []ViewRow{row}
			mappedByGroup[taskID] = resolvedRowsFromStored([]ViewRow{row}, applyManualEdits)
		}
	} else {
		taskPrompts := r.fetchTaskPrompts(ctx, uncachedTIDs)
		uncachedOutputs := outputsForTasks(allOutputs, uncachedIDs)
		outputsJSON, err := serializeOutputsForMapping(uncachedOutputs, taskPrompts)
		if err != nil {
			return nil, fmt.Errorf("serialize outputs: %w", err)
		}

		result, err := baml.MapOutputsToSchema(
			ctx,
			sheet.Name,
			comp.Title,
			comp.Type,
			rowStrategy.Mode,
			rowStrategy.Description,
			mappingCols,
			outputsJSON,
		)
		if err != nil {
			if opts.ForceRefresh {
				return nil, fmt.Errorf("force refresh BAML mapping: %w", err)
			}
			log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Int("tasks", len(uncachedIDs)).Msg("BAML mapping failed")
		} else {
			for _, row := range result.Rows {
				taskID := row.Task_id
				if _, ok := taskGroups[taskID]; !ok {
					continue
				}
				persisted := mappedRowToViewRow(sheet.ID, taskID, schemaH, taskGroups[taskID], row, now)
				persistedByGroup[taskID] = append(persistedByGroup[taskID], persisted)
			}
		}

		for _, taskID := range uncachedTIDs {
			if len(persistedByGroup[taskID]) == 0 {
				if !opts.ForceRefresh && len(rowsByGroup[taskID]) > 0 {
					mappedByGroup[taskID] = resolvedRowsFromStored(rowsByGroup[taskID], true)
				}
				// BAML intentionally omitted this task — it has no relevant
				// data for this sheet. Don't create a fallback empty row.
				continue
			}
			mappedByGroup[taskID] = resolvedRowsFromStored(persistedByGroup[taskID], applyManualEdits)
		}
	}

	{
		var toUpsert []ViewRow
		var keepRowIDs []string
		var cleanupGroupIDs []string
		for _, taskID := range uncachedTIDs {
			rows := persistedByGroup[taskID]
			if len(rows) > 0 {
				for _, row := range rows {
					toUpsert = append(toUpsert, row)
					keepRowIDs = append(keepRowIDs, row.ID)
				}
				cleanupGroupIDs = append(cleanupGroupIDs, taskID)
			} else if opts.ForceRefresh {
				// BAML intentionally omitted this task on force refresh —
				// delete any old rows for it (no keepRowIDs → all deleted).
				cleanupGroupIDs = append(cleanupGroupIDs, taskID)
			}
		}
		persistedOK := true
		if len(toUpsert) > 0 {
			if err := r.store.UpsertRows(ctx, viewID, toUpsert); err != nil {
				persistedOK = false
				log.Error().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Int("rows", len(toUpsert)).Msg("failed to persist mapped rows to MongoDB")
				if opts.ForceRefresh {
					return nil, fmt.Errorf("persist force refresh rows: %w", err)
				}
			}
		}
		if persistedOK && len(cleanupGroupIDs) > 0 {
			if err := r.store.DeleteRowsNotInGroups(ctx, viewID, sheet.ID, cleanupGroupIDs, keepRowIDs); err != nil {
				log.Error().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Int("groups", len(cleanupGroupIDs)).Msg("failed to delete stale rows from MongoDB")
				if opts.ForceRefresh {
					return nil, fmt.Errorf("delete stale force refresh rows: %w", err)
				}
			}
			if opts.ForceRefresh {
				if err := r.store.ClearManualCells(ctx, viewID, sheet.ID, keepRowIDs, schemaKeyList(mappingCols)); err != nil {
					return nil, fmt.Errorf("clear manual cells after force refresh: %w", err)
				}
			}
		}
	}

	for _, taskID := range uncachedTIDs {
		if rows := mappedByGroup[taskID]; len(rows) > 0 {
			resolvedRows = append(resolvedRows, rows...)
		}
	}

	sortResolvedRows(resolvedRows, taskMeta)
	return &viewMappingResult{Rows: resolvedRows, TaskMeta: taskMeta}, nil
}

// ---------------------------------------------------------------------------
// View-level helpers
// ---------------------------------------------------------------------------

func viewMappingFlightKey(workspaceID uint, viewID, sheetID, componentID string, opts ResolveOptions) string {
	return fmt.Sprintf("%d:%s:%s:%s:%t", workspaceID, viewID, sheetID, componentID, opts.ForceRefresh)
}

func schemaKeySet(cols []bamltypes.ColumnSchema) map[string]bool {
	keys := make(map[string]bool, len(cols))
	for _, col := range cols {
		keys[col.Key] = true
	}
	return keys
}

func schemaKeyList(cols []bamltypes.ColumnSchema) []string {
	keys := make([]string, 0, len(cols))
	for _, col := range cols {
		keys = append(keys, col.Key)
	}
	return keys
}

func canCarryStoredRow(stored *ViewRow, schemaKeys map[string]bool) bool {
	for key := range schemaKeys {
		if _, ok := stored.Cells[key]; ok {
			continue
		}
		if _, ok := stored.Manual[key]; ok {
			continue
		}
		return false
	}
	return true
}

func filterStoredCells(cells map[string]string, schemaKeys map[string]bool) map[string]string {
	filtered := make(map[string]string, len(cells))
	for key, value := range cells {
		if schemaKeys[key] {
			filtered[key] = value
		}
	}
	return filtered
}

func composeResolvedCells(cells, manual map[string]string, schemaKeys map[string]bool, applyManual bool) map[string]string {
	filtered := filterStoredCells(cells, schemaKeys)
	if !applyManual {
		return filtered
	}
	return mergeManualCells(filtered, manual, schemaKeys)
}

func mergeManualCells(cells, manual map[string]string, schemaKeys map[string]bool) map[string]string {
	merged := make(map[string]string, len(cells)+len(manual))
	for key, value := range cells {
		if schemaKeys[key] {
			merged[key] = value
		}
	}
	for key, value := range manual {
		if schemaKeys[key] {
			merged[key] = value
		}
	}
	return merged
}

func buildUnifiedSchema(allComponents []types.ComponentSpec) []bamltypes.ColumnSchema {
	seen := make(map[string]bool)
	var cols []bamltypes.ColumnSchema
	for _, comp := range allComponents {
		if !comp.IsTable() {
			continue
		}
		for _, col := range buildColumnSchemas(comp) {
			if !seen[col.Key] {
				seen[col.Key] = true
				cols = append(cols, col)
			}
		}
	}
	return cols
}

func groupOutputsByTask(outputs []*types.TaskOutput) map[string][]*types.TaskOutput {
	groups := make(map[string][]*types.TaskOutput)
	for _, o := range outputs {
		if o != nil {
			groups[o.TaskID] = append(groups[o.TaskID], o)
		}
	}
	return groups
}

func outputsForTasks(outputs []*types.TaskOutput, taskIDs map[string]bool) []*types.TaskOutput {
	var result []*types.TaskOutput
	for _, o := range outputs {
		if o != nil && taskIDs[o.TaskID] {
			result = append(result, o)
		}
	}
	return result
}

func groupRowsFresh(rows []ViewRow, schemaH string, outputIDs []string) bool {
	if len(rows) == 0 {
		return false
	}
	for _, row := range rows {
		if row.SchemaHash != schemaH || !slicesMatch(row.OutputIDs, outputIDs) {
			return false
		}
	}
	return true
}

func (r *DataResolver) resolveAgentIDsForDS(ctx context.Context, workspaceID uint, ds *types.DataSource) []string {
	if ds == nil {
		return nil
	}
	refs := append([]string{}, ds.AgentIDs...)
	if ds.AgentID != "" {
		refs = append(refs, ds.AgentID)
	}
	refs = uniqueTrimmedStrings(refs)
	var ids []string
	for _, ref := range refs {
		if aid, ok := r.resolveAgentRef(ctx, workspaceID, ref); ok {
			ids = append(ids, aid)
		}
	}
	return ids
}

func (r *DataResolver) resolveAgentIDsFromRefs(ctx context.Context, workspaceID uint, refs []string) []string {
	trimmed := uniqueTrimmedStrings(refs)
	var ids []string
	for _, ref := range trimmed {
		if aid, ok := r.resolveAgentRef(ctx, workspaceID, ref); ok {
			ids = append(ids, aid)
		}
	}
	return ids
}

func (r *DataResolver) fetchComponentOutputs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string) ([]*types.TaskOutput, error) {
	filter := types.TaskOutputListFilter{
		ExcludeArchived: false,
		Limit:           200,
	}
	if ds != nil && strings.TrimSpace(ds.OutputType) != "" {
		outputType := strings.TrimSpace(ds.OutputType)
		filter.OutputType = &outputType
	}

	resolvedAgentIDs := r.resolveAgentIDsForDS(ctx, workspaceID, ds)
	if ds != nil && (strings.TrimSpace(ds.AgentID) != "" || len(ds.AgentIDs) > 0) && len(resolvedAgentIDs) == 0 {
		return nil, nil
	}

	// Fall back to view-level agents when the component has no explicit agent filter.
	if len(resolvedAgentIDs) == 0 && len(viewAgentRefs) > 0 {
		resolvedAgentIDs = r.resolveAgentIDsFromRefs(ctx, workspaceID, viewAgentRefs)
	}

	if len(resolvedAgentIDs) == 0 {
		outputs, err := r.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, filter)
		if err != nil {
			return nil, err
		}
		return filterOutputsForDataSource(dedupeOutputs(outputs), ds, nil), nil
	}

	var all []*types.TaskOutput
	for _, agentID := range resolvedAgentIDs {
		localFilter := filter
		localFilter.AgentID = &agentID
		outputs, err := r.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, localFilter)
		if err != nil {
			return nil, err
		}
		all = append(all, outputs...)
	}
	return filterOutputsForDataSource(dedupeOutputs(all), ds, resolvedAgentIDs), nil
}

func filterOutputsForDataSource(outputs []*types.TaskOutput, ds *types.DataSource, resolvedAgentIDs []string) []*types.TaskOutput {
	if len(outputs) == 0 || ds == nil {
		return outputs
	}
	agentSet := make(map[string]bool, len(resolvedAgentIDs))
	for _, id := range resolvedAgentIDs {
		agentSet[id] = true
	}

	filtered := make([]*types.TaskOutput, 0, len(outputs))
	for _, output := range outputs {
		if output == nil {
			continue
		}
		if len(agentSet) > 0 {
			if output.AgentID == nil || !agentSet[strings.TrimSpace(*output.AgentID)] {
				continue
			}
		}
		if ds.OutputType != "" && !strings.EqualFold(strings.TrimSpace(ds.OutputType), strings.TrimSpace(output.OutputType)) {
			continue
		}
		if ds.ArtifactKey != "" && !ArtifactOf(output).MatchesKey(ds.ArtifactKey) {
			continue
		}
		filtered = append(filtered, output)
	}
	if ds.TimeRange != "" {
		filtered = filterOutputsByTimeRange(filtered, ds.TimeRange)
	}
	return filtered
}

func effectiveRowStrategy(ds *types.DataSource) types.RowStrategy {
	if ds == nil || ds.RowStrategy == nil {
		return types.RowStrategy{Mode: types.RowStrategyModeTask}
	}
	mode := strings.ToLower(strings.TrimSpace(ds.RowStrategy.Mode))
	if mode != types.RowStrategyModeSplit {
		mode = types.RowStrategyModeTask
	}
	return types.RowStrategy{
		Mode:        mode,
		Description: strings.TrimSpace(ds.RowStrategy.Description),
	}
}

// ---------------------------------------------------------------------------
// Component assembly
// ---------------------------------------------------------------------------

func assembleTable(sheetID string, comp types.ComponentSpec, mappedRows []resolvedSheetRow, taskMeta map[string]*types.AgentTask) *types.ResolvedData {
	tableCols := buildColumnSchemas(comp)
	if len(tableCols) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}
	}

	hiddenStart := len(tableCols)
	colNames := make([]string, hiddenStart+4)
	for i, col := range tableCols {
		colNames[i] = col.Key
	}
	colNames[hiddenStart] = "task_id"
	colNames[hiddenStart+1] = "row_id"
	colNames[hiddenStart+2] = "sheet_id"
	colNames[hiddenStart+3] = "output_id"

	meta := make([]types.ColumnMeta, len(colNames))
	for i, col := range tableCols {
		meta[i] = types.ColumnMeta{
			Key:   col.Key,
			Label: stripHint(col.Description),
			Type:  normalizeColumnType(col.Type),
		}
	}
	meta[hiddenStart] = types.ColumnMeta{Key: "task_id", Type: "text", Hidden: true}
	meta[hiddenStart+1] = types.ColumnMeta{Key: "row_id", Type: "text", Hidden: true}
	meta[hiddenStart+2] = types.ColumnMeta{Key: "sheet_id", Type: "text", Hidden: true}
	meta[hiddenStart+3] = types.ColumnMeta{Key: "output_id", Type: "text", Hidden: true}

	var rows [][]any
	for _, mapped := range mappedRows {
		row := make([]any, len(colNames))
		hasValue := false
		for i, col := range tableCols {
			if v, ok := mapped.Cells[col.Key]; ok && v != "" {
				row[i] = v
				hasValue = true
				continue
			}
			if task, ok := taskMeta[mapped.TaskID]; ok {
				if v, ok := taskMetadataValue(task, col.Key); ok && v != "" {
					row[i] = v
					hasValue = true
				}
			}
		}
		row[hiddenStart] = mapped.TaskID
		row[hiddenStart+1] = mapped.RowID
		row[hiddenStart+2] = sheetID
		row[hiddenStart+3] = mapped.OutputID
		if hasValue {
			rows = append(rows, row)
		}
	}

	if len(rows) == 0 {
		return &types.ResolvedData{Columns: colNames, ColumnMeta: meta, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}
	}
	return &types.ResolvedData{
		Columns:    colNames,
		ColumnMeta: meta,
		Rows:       rows,
		Total:      len(rows),
		Status:     types.ResolvedDataStatusOK,
	}
}

// ---------------------------------------------------------------------------
// Output fetching
// ---------------------------------------------------------------------------

func (r *DataResolver) resolveAgentRef(ctx context.Context, workspaceID uint, ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", false
	}
	if _, err := uuid.Parse(ref); err == nil {
		return ref, true
	}
	if profile, err := r.backend.GetAgentProfileByKey(ctx, workspaceID, ref); err == nil && profile != nil && strings.TrimSpace(profile.ID) != "" {
		return profile.ID, true
	}
	if profiles, err := r.backend.ListAgentProfiles(ctx, workspaceID); err == nil {
		if p := findUniqueAgentProfileByName(profiles, ref); p != nil && strings.TrimSpace(p.ID) != "" {
			return p.ID, true
		}
	}
	log.Debug().Str("ref", ref).Uint("workspace_id", workspaceID).Msg("agent ref could not be resolved, skipping")
	return "", false
}

func dedupeOutputs(all []*types.TaskOutput) []*types.TaskOutput {
	seen := make(map[string]struct{}, len(all))
	deduped := make([]*types.TaskOutput, 0, len(all))
	for _, o := range all {
		if o == nil {
			continue
		}
		if _, ok := seen[o.ID]; ok {
			continue
		}
		seen[o.ID] = struct{}{}
		deduped = append(deduped, o)
	}
	return deduped
}

func resolvedRowsFromStored(rows []ViewRow, applyManual bool) []resolvedSheetRow {
	if len(rows) == 0 {
		return nil
	}
	cloned := append([]ViewRow(nil), rows...)
	sort.SliceStable(cloned, func(i, j int) bool {
		if cloned[i].TaskID != cloned[j].TaskID {
			return cloned[i].TaskID < cloned[j].TaskID
		}
		if cloned[i].RowKey != cloned[j].RowKey {
			return cloned[i].RowKey < cloned[j].RowKey
		}
		return cloned[i].ID < cloned[j].ID
	})

	result := make([]resolvedSheetRow, 0, len(cloned))
	for _, row := range cloned {
		cells := copyStringMap(row.Cells)
		if applyManual {
			cells = copyStringMap(row.MergedCells())
		}
		result = append(result, resolvedSheetRow{
			SheetID:  row.SheetID,
			TaskID:   row.TaskID,
			RowID:    row.ID,
			RowKey:   row.RowKey,
			OutputID: firstSourceOutputID(row.SourceOutputIDs),
			Cells:    cells,
		})
	}
	return result
}

func mappedRowToViewRow(sheetID, taskID, schemaH string, groupOutputs []*types.TaskOutput, row bamltypes.MappedRow, now time.Time) ViewRow {
	rowKey := normalizeToken(strings.TrimSpace(row.Row_key))
	if rowKey == "" {
		rowKey = "task"
	}
	sourceOutputIDs := uniqueTrimmedStrings(row.Source_output_ids)
	if len(sourceOutputIDs) == 0 {
		sourceOutputIDs = sortedOutputIDs(groupOutputs)
	}
	cells := make(map[string]string, len(row.Cells))
	for _, cell := range row.Cells {
		if cell.Value != "" {
			cells[cell.Column] = cell.Value
		}
	}
	return ViewRow{
		ID:              stableRowID(sheetID, taskID, rowKey),
		SheetID:         sheetID,
		GroupID:         taskID,
		TaskID:          taskID,
		RowKey:          rowKey,
		SchemaHash:      schemaH,
		OutputIDs:       sortedOutputIDs(groupOutputs),
		SourceOutputIDs: sourceOutputIDs,
		Cells:           cells,
		UpdatedAt:       now,
	}
}

func fallbackViewRow(sheetID, taskID, schemaH string, groupOutputs []*types.TaskOutput, now time.Time) ViewRow {
	return ViewRow{
		ID:              stableRowID(sheetID, taskID, "task"),
		SheetID:         sheetID,
		GroupID:         taskID,
		TaskID:          taskID,
		RowKey:          "task",
		SchemaHash:      schemaH,
		OutputIDs:       sortedOutputIDs(groupOutputs),
		SourceOutputIDs: sortedOutputIDs(groupOutputs),
		Cells:           map[string]string{},
		UpdatedAt:       now,
	}
}

func stableRowID(sheetID, taskID, rowKey string) string {
	key := normalizeToken(strings.TrimSpace(rowKey))
	if key == "" {
		key = "task"
	}
	return fmt.Sprintf("%s:%s:%s", sheetID, taskID, key)
}

func firstSourceOutputID(outputIDs []string) string {
	if len(outputIDs) == 1 {
		return outputIDs[0]
	}
	return ""
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func sortResolvedRows(rows []resolvedSheetRow, taskMeta map[string]*types.AgentTask) {
	sort.SliceStable(rows, func(i, j int) bool {
		leftTask := taskMeta[rows[i].TaskID]
		rightTask := taskMeta[rows[j].TaskID]
		leftCreated := time.Time{}
		rightCreated := time.Time{}
		if leftTask != nil {
			leftCreated = leftTask.CreatedAt
		}
		if rightTask != nil {
			rightCreated = rightTask.CreatedAt
		}
		if !leftCreated.Equal(rightCreated) {
			return leftCreated.After(rightCreated)
		}
		if rows[i].TaskID != rows[j].TaskID {
			return rows[i].TaskID < rows[j].TaskID
		}
		if rows[i].RowKey != rows[j].RowKey {
			return rows[i].RowKey < rows[j].RowKey
		}
		return rows[i].RowID < rows[j].RowID
	})
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

func filterOutputsByTimeRange(outputs []*types.TaskOutput, raw string) []*types.TaskOutput {
	if len(outputs) == 0 {
		return outputs
	}
	duration, ok := parseTimeRange(raw)
	if !ok {
		return outputs
	}
	cutoff := time.Now().Add(-duration)
	filtered := make([]*types.TaskOutput, 0, len(outputs))
	for _, o := range outputs {
		if o != nil && !o.CreatedAt.Before(cutoff) {
			filtered = append(filtered, o)
		}
	}
	return filtered
}

func parseTimeRange(raw string) (time.Duration, bool) {
	value := strings.TrimSpace(strings.ToLower(raw))
	if value == "" {
		return 0, false
	}
	if strings.HasSuffix(value, "h") {
		dur, err := time.ParseDuration(value)
		return dur, err == nil
	}
	if len(value) < 2 {
		return 0, false
	}
	amount, err := strconv.Atoi(value[:len(value)-1])
	if err != nil || amount <= 0 {
		return 0, false
	}
	switch value[len(value)-1] {
	case 'd':
		return time.Duration(amount) * 24 * time.Hour, true
	case 'w':
		return time.Duration(amount) * 7 * 24 * time.Hour, true
	default:
		return 0, false
	}
}

// ---------------------------------------------------------------------------
// BAML mapping
// ---------------------------------------------------------------------------

// fetchTaskPrompts loads the initial user prompt for each task ID.
// Errors are silently ignored — prompts are supplemental context.
func (r *DataResolver) fetchTaskPrompts(ctx context.Context, taskIDs []string) map[string]string {
	prompts := make(map[string]string, len(taskIDs))
	for _, tid := range taskIDs {
		task, err := r.backend.GetTaskByID(ctx, tid)
		if err != nil || task == nil {
			continue
		}
		if m, _ := task.PayloadJSON["message"].(string); m != "" {
			prompts[tid] = m
		}
	}
	return prompts
}

func (r *DataResolver) fetchTaskMetadata(ctx context.Context, taskIDs []string) map[string]*types.AgentTask {
	meta := make(map[string]*types.AgentTask, len(taskIDs))
	for _, tid := range taskIDs {
		task, err := r.backend.GetTaskByID(ctx, tid)
		if err != nil || task == nil {
			continue
		}
		meta[tid] = task
	}
	return meta
}

func buildColumnSchemas(comp types.ComponentSpec) []bamltypes.ColumnSchema {
	if comp.DataSource == nil || len(comp.DataSource.Transform) == 0 {
		configCols := parseConfigColumns(comp.Config)
		if len(configCols) == 0 {
			return nil
		}
		schemas := make([]bamltypes.ColumnSchema, 0, len(configCols))
		for _, col := range configCols {
			schemas = append(schemas, bamltypes.ColumnSchema{
				Key:         col.Key,
				Type:        columnTypeForKey(col.Key, col.Type),
				Description: col.Label,
			})
		}
		return schemas
	}

	schemas := make([]bamltypes.ColumnSchema, 0, len(comp.DataSource.Transform))
	configCols := parseConfigColumns(comp.Config)
	configByKey := make(map[string]configColumn, len(configCols))
	for _, cc := range configCols {
		configByKey[cc.Key] = cc
	}
	seen := make(map[string]bool, len(comp.DataSource.Transform))

	for _, rule := range comp.DataSource.Transform {
		desc := humanizeColumn(rule.Column)
		if cc, ok := configByKey[rule.Column]; ok && cc.Label != "" {
			desc = cc.Label
		}
		if rule.Source != "" {
			desc += " (hint: " + rule.Source + ")"
		}
		schemas = append(schemas, bamltypes.ColumnSchema{
			Key:         rule.Column,
			Type:        columnTypeForKey(rule.Column, rule.Type),
			Description: desc,
		})
		seen[rule.Column] = true
	}
	for _, cc := range configCols {
		if cc.Key == "" || seen[cc.Key] {
			continue
		}
		schemas = append(schemas, bamltypes.ColumnSchema{
			Key:         cc.Key,
			Type:        columnTypeForKey(cc.Key, cc.Type),
			Description: cc.Label,
		})
	}
	return schemas
}

// serializeOutputsForMapping groups outputs by task_id and serializes as a
// JSON object keyed by task_id. Each entry contains the initial user prompt
// (when available) and an array of compact outputs produced by that task.
func serializeOutputsForMapping(outputs []*types.TaskOutput, taskPrompts map[string]string) (string, error) {
	type compactOutput struct {
		ID         string         `json:"id"`
		Title      string         `json:"title"`
		OutputType string         `json:"output_type"`
		AgentName  string         `json:"agent_name,omitempty"`
		Summary    *string        `json:"summary,omitempty"`
		URI        *string        `json:"uri,omitempty"`
		Data       map[string]any `json:"data,omitempty"`
		Metadata   map[string]any `json:"metadata,omitempty"`
		CreatedAt  string         `json:"created_at"`
	}

	type taskGroup struct {
		Prompt  string          `json:"prompt,omitempty"`
		Outputs []compactOutput `json:"outputs"`
	}

	grouped := make(map[string]*taskGroup)
	for _, o := range outputs {
		if o == nil {
			continue
		}
		g, ok := grouped[o.TaskID]
		if !ok {
			g = &taskGroup{Prompt: taskPrompts[o.TaskID]}
			grouped[o.TaskID] = g
		}
		g.Outputs = append(g.Outputs, compactOutput{
			ID:         o.ID,
			Title:      o.Title,
			OutputType: o.OutputType,
			AgentName:  o.AgentName,
			Summary:    o.Summary,
			URI:        o.URI,
			Data:       truncateLargeValues(o.Data),
			Metadata:   o.Metadata,
			CreatedAt:  o.CreatedAt.Format(time.RFC3339),
		})
	}

	raw, err := json.Marshal(grouped)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

const maxFieldValueLen = 2000

func truncateLargeValues(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case string:
			if len(val) > maxFieldValueLen {
				out[k] = val[:maxFieldValueLen] + "…"
			} else {
				out[k] = val
			}
		default:
			out[k] = v
		}
	}
	return out
}

func filterBamlColumns(cols []bamltypes.ColumnSchema) []bamltypes.ColumnSchema {
	filtered := make([]bamltypes.ColumnSchema, 0, len(cols))
	for _, col := range cols {
		if isTaskMetadataColumn(col.Key) {
			continue
		}
		filtered = append(filtered, col)
	}
	return filtered
}

func taskIDsFromGroups(taskGroups map[string][]*types.TaskOutput) []string {
	ids := make([]string, 0, len(taskGroups))
	for taskID := range taskGroups {
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	return ids
}

func isTaskMetadataColumn(key string) bool {
	switch key {
	case taskMetadataColumnNextWakeAt, taskMetadataColumnNextWakeSummary, taskMetadataColumnWakeAt, taskMetadataColumnWakeReason:
		return true
	default:
		return false
	}
}

func taskMetadataValue(task *types.AgentTask, key string) (string, bool) {
	if task == nil {
		return "", false
	}
	switch key {
	case taskMetadataColumnNextWakeAt, taskMetadataColumnWakeAt:
		if task.WakeAt == nil {
			return "", true
		}
		return task.WakeAt.Format(time.RFC3339), true
	case taskMetadataColumnNextWakeSummary, taskMetadataColumnWakeReason:
		if task.WakeReason != nil && strings.TrimSpace(*task.WakeReason) != "" {
			return strings.TrimSpace(*task.WakeReason), true
		}
		return wakeAgendaSummary(task.WakeAgenda), true
	default:
		return "", false
	}
}

func wakeAgendaSummary(items []*types.TaskWakeAgendaItem) string {
	titles := make([]string, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		if title := strings.TrimSpace(item.Title); title != "" {
			titles = append(titles, title)
		} else if reason := strings.TrimSpace(item.Reason); reason != "" {
			titles = append(titles, reason)
		}
		if len(titles) == 3 {
			break
		}
	}
	return strings.Join(titles, " | ")
}

// Store returns the ViewStore for direct access (used by the views API for cell edits).
func (r *DataResolver) Store() *ViewStore {
	return r.store
}

func hashColumns(columns []bamltypes.ColumnSchema, rowStrategy types.RowStrategy, sheetName string) string {
	type hashEntry struct {
		Key         string `json:"k"`
		Type        string `json:"t"`
		Description string `json:"d"`
	}
	payload := struct {
		Sheet       string      `json:"s"`
		Mode        string      `json:"m"`
		Description string      `json:"d"`
		Columns     []hashEntry `json:"c"`
	}{
		Sheet:       sheetName,
		Mode:        rowStrategy.Mode,
		Description: rowStrategy.Description,
		Columns:     make([]hashEntry, len(columns)),
	}
	for i, c := range columns {
		payload.Columns[i] = hashEntry{Key: c.Key, Type: c.Type, Description: c.Description}
	}
	raw, _ := json.Marshal(payload)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])[:16]
}

func sortedOutputIDs(outputs []*types.TaskOutput) []string {
	ids := make([]string, 0, len(outputs))
	for _, o := range outputs {
		if o != nil {
			ids = append(ids, o.ID)
		}
	}
	sort.Strings(ids)
	return ids
}

func slicesMatch(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Dot-path navigation (used by Artifact)
// ---------------------------------------------------------------------------

var indexBracketRe = regexp.MustCompile(`\[(\d+|\*)\]`)

func dotGet(m map[string]any, path string) any {
	if m == nil {
		return nil
	}
	return pathGet(m, splitPath(path))
}

func splitPath(path string) []string {
	normalized := strings.ReplaceAll(path, "[]", ".[].")
	normalized = indexBracketRe.ReplaceAllString(normalized, `.$1`)
	rawParts := strings.Split(normalized, ".")
	parts := make([]string, 0, len(rawParts))
	for _, part := range rawParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if part == "*" {
			part = "[]"
		}
		parts = append(parts, part)
	}
	return parts
}

func pathGet(current any, parts []string) any {
	if len(parts) == 0 {
		return current
	}
	part := parts[0]
	rest := parts[1:]

	switch typed := current.(type) {
	case map[string]any:
		return pathGet(typed[part], rest)
	case []any:
		return pathGetFromArray(typed, part, rest)
	default:
		return nil
	}
}

func pathGetFromArray(items []any, part string, rest []string) any {
	if len(items) == 0 {
		return nil
	}
	if part == "[]" {
		values := collectArrayValues(items, rest)
		if len(values) == 1 {
			return values[0]
		}
		if len(values) == 0 {
			return nil
		}
		return values
	}
	if idx, err := strconv.Atoi(part); err == nil {
		if idx >= 0 && idx < len(items) {
			return pathGet(items[idx], rest)
		}
		return nil
	}
	return firstNonEmpty(collectArrayValues(items, append([]string{part}, rest...)))
}

func collectArrayValues(items []any, parts []string) []any {
	var values []any
	for _, item := range items {
		switch typed := pathGet(item, parts).(type) {
		case nil:
			continue
		case []any:
			values = append(values, collectArrayValues(typed, nil)...)
		default:
			if !isEmpty(typed) {
				values = append(values, typed)
			}
		}
	}
	return values
}

func firstNonEmpty(values []any) any {
	for _, v := range values {
		if !isEmpty(v) {
			return v
		}
	}
	return nil
}

func isEmpty(value any) bool {
	switch typed := value.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(typed) == ""
	case []any:
		return len(typed) == 0
	case map[string]any:
		return len(typed) == 0
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

type configColumn struct {
	Key     string               `json:"key"`
	Label   string               `json:"label"`
	Type    string               `json:"type"`
	Format  string               `json:"format"`
	Frozen  bool                 `json:"frozen"`
	Options []types.StatusOption `json:"options"`
}

func parseConfigColumns(config map[string]any) []configColumn {
	raw, ok := config["columns"]
	if !ok {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var cols []configColumn
	if err := json.Unmarshal(b, &cols); err != nil {
		return nil
	}
	return cols
}

func normalizeColumnType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "datetime":
		return "date"
	case "text", "number", "currency", "date", "link", "email", "status", "tags", "boolean":
		return strings.ToLower(strings.TrimSpace(t))
	default:
		return "text"
	}
}

func columnTypeForKey(key, rawType string) string {
	if strings.TrimSpace(rawType) != "" {
		return normalizeColumnType(rawType)
	}
	switch {
	case strings.HasSuffix(key, "_at"), key == "date", key == "created", key == "updated":
		return "date"
	case key == "email", strings.HasSuffix(key, "_email"):
		return "email"
	case key == "url", key == "link", strings.HasSuffix(key, "_url"), strings.HasSuffix(key, "_link"):
		return "link"
	default:
		return "text"
	}
}

func humanizeColumn(col string) string {
	parts := strings.Split(col, "_")
	for i, p := range parts {
		if len(p) > 0 {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}
	return strings.Join(parts, " ")
}

func stripHint(desc string) string {
	if idx := strings.Index(desc, " (hint:"); idx > 0 {
		return strings.TrimSpace(desc[:idx])
	}
	return desc
}
