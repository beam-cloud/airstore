package views

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
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

var ErrNoOutputsForTask = errors.New("no outputs found for task")

// ---------------------------------------------------------------------------
// DataResolver
// ---------------------------------------------------------------------------

var viewMappingFlight singleflight.Group

type viewMappingResult struct {
	Rows        []resolvedSheetRow
	TaskMeta    map[string]*types.AgentTask
	Diagnostics map[string]any
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

type mappingSpec struct {
	tableCols   []bamltypes.ColumnSchema
	mappingCols []bamltypes.ColumnSchema
	rowStrategy types.RowStrategy
	schemaHash  string
}

func buildMappingSpec(sheetName string, comp types.ComponentSpec) mappingSpec {
	tableCols := buildColumnSchemas(comp)
	mappingCols := filterBamlColumns(tableCols)
	rowStrategy := effectiveRowStrategy(comp.DataSource)

	return mappingSpec{
		tableCols:   tableCols,
		mappingCols: mappingCols,
		rowStrategy: rowStrategy,
		schemaHash:  hashColumns(mappingCols, rowStrategy, sheetName, comp.Title, comp.Type),
	}
}

func MappingSchemaHash(sheet types.SheetSpec, comp types.ComponentSpec) string {
	if !comp.IsTable() {
		return ""
	}
	return buildMappingSpec(sheet.Name, comp).schemaHash
}

func NewDataResolver(backend repository.BackendRepository, store *ViewStore) *DataResolver {
	return &DataResolver{backend: backend, store: store}
}

type dataResolverBackend interface {
	GetAgentProfileByKey(ctx context.Context, workspaceId uint, agentKey string) (*types.AgentProfile, error)
	ListAgentProfiles(ctx context.Context, workspaceId uint) ([]*types.AgentProfile, error)
	ListTaskOutputs(ctx context.Context, workspaceId uint, taskID string) ([]*types.TaskOutput, error)
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

	rows := result.Rows
	if r.store != nil {
		excluded, err := r.store.GetExcludedRowIDs(ctx, viewID, sheet.ID)
		if err != nil {
			log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Msg("failed to load row exclusions")
		} else if len(excluded) > 0 {
			filtered := make([]resolvedSheetRow, 0, len(rows))
			for _, row := range rows {
				if !excluded[row.RowID] {
					filtered = append(filtered, row)
				}
			}
			rows = filtered
		}
	}

	data := assembleTable(sheet.ID, comp, rows, result.TaskMeta)
	if data != nil && len(result.Diagnostics) > 0 {
		data.Diagnostics = result.Diagnostics
	}
	return data, nil
}

// RegenerateRow re-maps a single task's outputs through BAML for one sheet,
// replacing the cached rows for that task. Returns the full re-assembled table.
func (r *DataResolver) RegenerateRow(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, taskID string, opts ResolveOptions) (*types.ResolvedData, error) {
	if !comp.IsTable() {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
	spec := buildMappingSpec(sheet.Name, comp)
	if len(spec.tableCols) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}

	agentIDs, ok := r.resolveScopedAgentIDs(ctx, workspaceID, comp.DataSource, opts.ViewAgentRefs)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoOutputsForTask, taskID)
	}
	allOutputs, err := r.fetchTaskOutputs(ctx, workspaceID, []string{taskID}, agentIDs)
	if err != nil {
		return nil, fmt.Errorf("fetch mapping outputs: %w", err)
	}
	if len(allOutputs) == 0 {
		return nil, fmt.Errorf("%w: %s", ErrNoOutputsForTask, taskID)
	}
	outputs := allOutputs

	taskPrompts := r.fetchTaskPrompts(ctx, []string{taskID})
	outputsJSON, err := serializeOutputsForMapping(outputs, taskPrompts)
	if err != nil {
		return nil, fmt.Errorf("serialize outputs: %w", err)
	}

	result, err := baml.MapOutputsToSchema(
		ctx,
		sheet.Name,
		comp.Title,
		comp.Type,
		spec.rowStrategy.Mode,
		spec.rowStrategy.Description,
		spec.mappingCols,
		outputsJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("BAML mapping: %w", err)
	}

	canonicalRows := canonicalizeMappedRows(spec.mappingCols, result.Rows)
	now := time.Now()
	var persisted []ViewRow
	for _, row := range canonicalRows {
		if row.Task_id != taskID {
			continue
		}
		persisted = append(persisted, mappedRowToViewRow(sheet.ID, comp.ID, taskID, spec.schemaHash, outputs, row, now))
	}
	if len(persisted) == 0 {
		persisted = []ViewRow{fallbackViewRow(sheet.ID, comp.ID, taskID, spec.schemaHash, outputs, now)}
	}

	var keepRowIDs []string
	for _, row := range persisted {
		keepRowIDs = append(keepRowIDs, row.ID)
	}
	if err := r.store.UpsertRows(ctx, viewID, persisted); err != nil {
		return nil, fmt.Errorf("persist regenerated rows: %w", err)
	}
	if err := r.store.DeleteRowsNotInGroups(ctx, viewID, sheet.ID, comp.ID, []string{taskID}, keepRowIDs); err != nil {
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

// mapSheet does the actual work: identify the task set selected by the sheet's
// table binding, expand those tasks back to their full output context for BAML,
// check stored rows in MongoDB, call BAML for uncached/stale task groups, and
// persist results back.
func (r *DataResolver) mapSheet(ctx context.Context, workspaceID uint, viewID string, sheet types.SheetSpec, comp types.ComponentSpec, opts ResolveOptions) (*viewMappingResult, error) {
	spec := buildMappingSpec(sheet.Name, comp)
	if len(spec.tableCols) == 0 {
		return &viewMappingResult{Rows: nil, TaskMeta: map[string]*types.AgentTask{}}, nil
	}

	allOutputs, err := r.fetchMappingOutputs(ctx, workspaceID, comp.DataSource, opts.ViewAgentRefs)
	if err != nil {
		return nil, fmt.Errorf("fetch mapping outputs: %w", err)
	}
	if len(allOutputs) == 0 {
		log.Info().
			Str("view_id", viewID).
			Str("sheet_id", sheet.ID).
			Str("component_id", comp.ID).
			Strs("view_agent_refs", opts.ViewAgentRefs).
			Bool("has_data_source", comp.DataSource != nil).
			Msg("view: no outputs resolved for component")
		return &viewMappingResult{Rows: nil, TaskMeta: map[string]*types.AgentTask{}}, nil
	}

	taskGroups := groupOutputsByTask(allOutputs)
	taskMeta := r.fetchTaskMetadata(ctx, taskIDsFromGroups(taskGroups))

	var existingRows []ViewRow
	if r.store != nil {
		existingRows, err = r.store.GetRows(ctx, viewID, sheet.ID, comp.ID)
		if err != nil {
			log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Msg("failed to load stored rows, treating all as uncached")
			existingRows = nil
		}
	}
	rowsByGroup := make(map[string][]ViewRow)
	for i := range existingRows {
		row := existingRows[i]
		rowsByGroup[row.GroupID] = append(rowsByGroup[row.GroupID], row)
	}

	uncachedIDs := make(map[string]bool)
	uncachedReasonCounts := map[string]int{}
	var resolvedRows []resolvedSheetRow
	applyManualEdits := !opts.ForceRefresh

	if opts.ForceRefresh {
		log.Info().
			Str("view_id", viewID).
			Str("sheet_id", sheet.ID).
			Str("component_id", comp.ID).
			Str("schema_hash", spec.schemaHash).
			Int("tasks", len(taskGroups)).
			Msg("view: force refresh requested")
	}

	for taskID, outputs := range taskGroups {
		if opts.ForceRefresh {
			uncachedIDs[taskID] = true
			uncachedReasonCounts["force_refresh"]++
			continue
		}

		taskOIDs := sortedOutputIDs(outputs)
		storedRows := rowsByGroup[taskID]
		if ok, reason := groupRowsFresh(storedRows, comp.ID, spec.schemaHash, taskOIDs); ok {
			resolvedRows = append(resolvedRows, resolvedRowsFromStored(storedRows, applyManualEdits)...)
			continue
		} else {
			uncachedReasonCounts[reason]++
		}
		uncachedIDs[taskID] = true
	}

	diagnostics := map[string]any{
		"cache": map[string]any{
			"component_id":           comp.ID,
			"loaded_rows":            len(existingRows),
			"tasks":                  len(taskGroups),
			"cached":                 len(taskGroups) - len(uncachedIDs),
			"uncached":               len(uncachedIDs),
			"uncached_reason_counts": uncachedReasonCounts,
		},
	}

	if len(uncachedIDs) == 0 {
		sortResolvedRows(resolvedRows, taskMeta)
		return &viewMappingResult{Rows: resolvedRows, TaskMeta: taskMeta, Diagnostics: diagnostics}, nil
	}

	uncachedTIDs := make([]string, 0, len(uncachedIDs))
	for tid := range uncachedIDs {
		uncachedTIDs = append(uncachedTIDs, tid)
	}
	sort.Strings(uncachedTIDs)

	colKeys := make([]string, 0, len(spec.mappingCols))
	for _, c := range spec.mappingCols {
		colKeys = append(colKeys, c.Key)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheet.ID).
		Str("component_id", comp.ID).
		Str("sheet_name", sheet.Name).
		Str("schema_hash", spec.schemaHash).
		Bool("force_refresh", opts.ForceRefresh).
		Int("tasks", len(taskGroups)).
		Int("cached", len(taskGroups)-len(uncachedIDs)).
		Int("uncached", len(uncachedIDs)).
		Int("total_columns", len(spec.tableCols)).
		Int("mapping_columns", len(spec.mappingCols)).
		Strs("column_keys", colKeys).
		Str("row_mode", spec.rowStrategy.Mode).
		Interface("uncached_reason_counts", uncachedReasonCounts).
		Msg("view: mapping required")

	persistedByGroup := make(map[string][]ViewRow)
	mappedByGroup := make(map[string][]resolvedSheetRow)
	now := time.Now()

	if len(spec.mappingCols) == 0 {
		for _, taskID := range uncachedTIDs {
			row := fallbackViewRow(sheet.ID, comp.ID, taskID, spec.schemaHash, taskGroups[taskID], now)
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
			spec.rowStrategy.Mode,
			spec.rowStrategy.Description,
			spec.mappingCols,
			outputsJSON,
		)
		if err != nil {
			if opts.ForceRefresh {
				return nil, fmt.Errorf("force refresh BAML mapping: %w", err)
			}
			log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Int("tasks", len(uncachedIDs)).Msg("BAML mapping failed")
		} else {
			for _, row := range canonicalizeMappedRows(spec.mappingCols, result.Rows) {
				taskID := row.Task_id
				if _, ok := taskGroups[taskID]; !ok {
					log.Warn().
						Str("view_id", viewID).
						Str("sheet_id", sheet.ID).
						Str("task_id", taskID).
						Msg("BAML returned row for unknown task_id, skipping")
					continue
				}
				persisted := mappedRowToViewRow(sheet.ID, comp.ID, taskID, spec.schemaHash, taskGroups[taskID], row, now)
				persistedByGroup[taskID] = append(persistedByGroup[taskID], persisted)
			}
		}

		for _, taskID := range uncachedTIDs {
			if len(persistedByGroup[taskID]) == 0 {
				// Persist an empty marker row so repeated GETs do not remap
				// forever when this task currently contributes no rows.
				row := fallbackViewRow(sheet.ID, comp.ID, taskID, spec.schemaHash, taskGroups[taskID], now)
				persistedByGroup[taskID] = []ViewRow{row}
				mappedByGroup[taskID] = nil
				continue
			}
			mappedByGroup[taskID] = resolvedRowsFromStored(persistedByGroup[taskID], applyManualEdits)
		}
	}

	if r.store != nil {
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
				// delete any old rows for it (no keepRowIDs -> all deleted).
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
			if err := r.store.DeleteRowsNotInGroups(ctx, viewID, sheet.ID, comp.ID, cleanupGroupIDs, keepRowIDs); err != nil {
				log.Error().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Int("groups", len(cleanupGroupIDs)).Msg("failed to delete stale rows from MongoDB")
				if opts.ForceRefresh {
					return nil, fmt.Errorf("delete stale force refresh rows: %w", err)
				}
			}
			if opts.ForceRefresh {
				if err := r.store.ClearManualCells(ctx, viewID, sheet.ID, comp.ID, keepRowIDs, schemaKeyList(spec.mappingCols)); err != nil {
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
	return &viewMappingResult{Rows: resolvedRows, TaskMeta: taskMeta, Diagnostics: diagnostics}, nil
}

// ---------------------------------------------------------------------------
// View-level helpers
// ---------------------------------------------------------------------------

func viewMappingFlightKey(workspaceID uint, viewID, sheetID, componentID string, opts ResolveOptions) string {
	return fmt.Sprintf(
		"%d:%s:%s:%s:%t:%s",
		workspaceID,
		viewID,
		sheetID,
		componentID,
		opts.ForceRefresh,
		normalizedViewAgentRefsKey(opts.ViewAgentRefs),
	)
}

func normalizedViewAgentRefsKey(refs []string) string {
	normalized := uniqueTrimmedStrings(refs)
	if len(normalized) == 0 {
		return "-"
	}
	sort.Strings(normalized)
	return strings.Join(normalized, ",")
}

func schemaKeyList(cols []bamltypes.ColumnSchema) []string {
	keys := make([]string, 0, len(cols))
	for _, col := range cols {
		keys = append(keys, col.Key)
	}
	return uniqueTrimmedStrings(keys)
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

func taskSetFromOutputs(outputs []*types.TaskOutput) map[string]bool {
	taskIDs := make(map[string]bool, len(outputs))
	for _, output := range outputs {
		if output == nil || strings.TrimSpace(output.TaskID) == "" {
			continue
		}
		taskIDs[output.TaskID] = true
	}
	return taskIDs
}

func groupRowsFresh(rows []ViewRow, componentID, schemaH string, outputIDs []string) (bool, string) {
	if len(rows) == 0 {
		return false, "missing_rows"
	}
	for _, row := range rows {
		if strings.TrimSpace(componentID) != "" && row.ComponentID != componentID {
			return false, "component_scope_mismatch"
		}
		if row.SchemaHash != schemaH {
			return false, "schema_hash_mismatch"
		}
		if !slicesMatch(row.OutputIDs, outputIDs) {
			return false, "output_ids_mismatch"
		}
	}
	return true, ""
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

func baseTaskOutputFilter() types.TaskOutputListFilter {
	return types.TaskOutputListFilter{
		ExcludeArchived: false,
		Limit:           200,
	}
}

func dataSourceHasExplicitAgentScope(ds *types.DataSource) bool {
	if ds == nil {
		return false
	}
	return strings.TrimSpace(ds.AgentID) != "" || len(ds.AgentIDs) > 0
}

func dataSourceNarrowsTaskSelection(ds *types.DataSource) bool {
	if ds == nil {
		return false
	}
	return strings.TrimSpace(ds.OutputType) != "" ||
		strings.TrimSpace(ds.ArtifactKey) != "" ||
		strings.TrimSpace(ds.TimeRange) != ""
}

func (r *DataResolver) resolveScopedAgentIDs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string) ([]string, bool) {
	resolvedAgentIDs := r.resolveAgentIDsForDS(ctx, workspaceID, ds)
	if dataSourceHasExplicitAgentScope(ds) && len(resolvedAgentIDs) == 0 {
		return nil, false
	}
	if len(resolvedAgentIDs) == 0 && len(viewAgentRefs) > 0 {
		resolvedAgentIDs = r.resolveAgentIDsFromRefs(ctx, workspaceID, viewAgentRefs)
	}
	return resolvedAgentIDs, true
}

func (r *DataResolver) listScopedOutputs(ctx context.Context, workspaceID uint, filter types.TaskOutputListFilter, agentIDs []string) ([]*types.TaskOutput, error) {
	if len(agentIDs) == 0 {
		outputs, err := r.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, filter)
		if err != nil {
			return nil, err
		}
		return dedupeOutputs(outputs), nil
	}

	var all []*types.TaskOutput
	for _, agentID := range agentIDs {
		localFilter := filter
		localFilter.AgentID = &agentID
		outputs, err := r.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, localFilter)
		if err != nil {
			return nil, err
		}
		all = append(all, outputs...)
	}
	return dedupeOutputs(all), nil
}

func (r *DataResolver) fetchOutputsForScope(ctx context.Context, workspaceID uint, ds *types.DataSource, agentIDs []string) ([]*types.TaskOutput, error) {
	filter := baseTaskOutputFilter()
	if ds != nil && strings.TrimSpace(ds.OutputType) != "" {
		outputType := strings.TrimSpace(ds.OutputType)
		filter.OutputType = &outputType
	}

	outputs, err := r.listScopedOutputs(ctx, workspaceID, filter, agentIDs)
	if err != nil {
		return nil, err
	}
	return filterOutputsForDataSource(outputs, ds, agentIDs), nil
}

func (r *DataResolver) fetchComponentOutputs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string) ([]*types.TaskOutput, error) {
	agentIDs, ok := r.resolveScopedAgentIDs(ctx, workspaceID, ds, viewAgentRefs)
	if !ok {
		return nil, nil
	}
	return r.fetchOutputsForScope(ctx, workspaceID, ds, agentIDs)
}

// fetchMappingOutputs lets the data source select which tasks belong to the
// sheet, then expands each selected task back to its full output set so BAML
// sees complete task context instead of a filtered artifact slice.
func (r *DataResolver) fetchMappingOutputs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string) ([]*types.TaskOutput, error) {
	agentIDs, ok := r.resolveScopedAgentIDs(ctx, workspaceID, ds, viewAgentRefs)
	if !ok {
		return nil, nil
	}

	selectedOutputs, err := r.fetchOutputsForScope(ctx, workspaceID, ds, agentIDs)
	if err != nil || len(selectedOutputs) == 0 || !dataSourceNarrowsTaskSelection(ds) {
		return selectedOutputs, err
	}

	taskIDs := taskSetFromOutputs(selectedOutputs)
	if len(taskIDs) == 0 {
		return nil, nil
	}

	allTaskOutputs, err := r.fetchTaskOutputs(ctx, workspaceID, sortedTaskIDSet(taskIDs), agentIDs)
	if err != nil {
		return nil, err
	}
	return allTaskOutputs, nil
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
	sortTaskOutputs(deduped)
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

func mappedRowToViewRow(sheetID, componentID, taskID, schemaH string, groupOutputs []*types.TaskOutput, row bamltypes.MappedRow, now time.Time) ViewRow {
	rowKey := normalizeToken(strings.TrimSpace(row.Row_key))
	if rowKey == "" {
		rowKey = "task"
	}
	outputIDs := sortedOutputIDs(groupOutputs)
	sourceOutputIDs := sanitizeSourceOutputIDs(row.Source_output_ids, outputIDs)
	cells := make(map[string]string, len(row.Cells))
	for _, cell := range row.Cells {
		if cell.Value != "" {
			cells[cell.Column] = cell.Value
		}
	}
	return ViewRow{
		ID:              stableRowID(sheetID, componentID, taskID, rowKey),
		SheetID:         sheetID,
		ComponentID:     componentID,
		GroupID:         taskID,
		TaskID:          taskID,
		RowKey:          rowKey,
		SchemaHash:      schemaH,
		OutputIDs:       outputIDs,
		SourceOutputIDs: sourceOutputIDs,
		Cells:           cells,
		UpdatedAt:       now,
	}
}

func sanitizeSourceOutputIDs(sourceOutputIDs, outputIDs []string) []string {
	if len(outputIDs) == 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(outputIDs))
	for _, id := range outputIDs {
		allowed[id] = struct{}{}
	}
	var filtered []string
	for _, id := range uniqueTrimmedStrings(sourceOutputIDs) {
		if _, ok := allowed[id]; ok {
			filtered = append(filtered, id)
		}
	}
	sort.Strings(filtered)
	if len(filtered) == 0 {
		return append([]string(nil), outputIDs...)
	}
	return filtered
}

func fallbackViewRow(sheetID, componentID, taskID, schemaH string, groupOutputs []*types.TaskOutput, now time.Time) ViewRow {
	return ViewRow{
		ID:              stableRowID(sheetID, componentID, taskID, "task"),
		SheetID:         sheetID,
		ComponentID:     componentID,
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

func stableRowID(sheetID, componentID, taskID, rowKey string) string {
	key := normalizeToken(strings.TrimSpace(rowKey))
	if key == "" {
		key = "task"
	}
	componentKey := normalizeToken(strings.TrimSpace(componentID))
	if componentKey == "" {
		componentKey = "component"
	}
	return fmt.Sprintf("%s:%s:%s:%s", sheetID, componentKey, taskID, key)
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
	configCols := parseConfigColumns(comp.Config)
	configByKey, configOrder := normalizeConfigColumnsForSchema(configCols)

	if comp.DataSource == nil || len(comp.DataSource.Transform) == 0 {
		if len(configByKey) == 0 {
			return nil
		}
		schemas := make([]bamltypes.ColumnSchema, 0, len(configByKey))
		for _, key := range configOrder {
			col := configByKey[key]
			name := schemaColumnName(key, col.Label)
			schemas = append(schemas, bamltypes.ColumnSchema{
				Name:        name,
				Key:         key,
				Type:        columnTypeForKey(key, col.Type),
				Description: name,
			})
		}
		return schemas
	}

	schemas := make([]bamltypes.ColumnSchema, 0, len(comp.DataSource.Transform))
	seen := make(map[string]bool, len(comp.DataSource.Transform))

	for _, rule := range comp.DataSource.Transform {
		key := canonicalSchemaColumnKey(rule.Column)
		if key == "" {
			key = canonicalSchemaColumnKey(sourceColumnHint(rule.Source))
		}
		if key == "" || seen[key] {
			continue
		}
		desc := humanizeColumn(key)
		if cc, ok := configByKey[key]; ok && cc.Label != "" {
			desc = cc.Label
		}
		name := schemaColumnName(key, desc)
		if rule.Source != "" {
			desc += " (hint: " + rule.Source + ")"
		}
		schemas = append(schemas, bamltypes.ColumnSchema{
			Name:        name,
			Key:         key,
			Type:        columnTypeForKey(key, rule.Type),
			Description: desc,
		})
		seen[key] = true
	}
	for _, key := range configOrder {
		if key == "" || seen[key] {
			continue
		}
		cc := configByKey[key]
		name := schemaColumnName(key, cc.Label)
		schemas = append(schemas, bamltypes.ColumnSchema{
			Name:        name,
			Key:         key,
			Type:        columnTypeForKey(key, cc.Type),
			Description: name,
		})
		seen[key] = true
	}
	return schemas
}

// serializeOutputsForMapping groups outputs by task_id and formats them as a
// deterministic text payload with explicit task/output boundaries. The mapper
// gets cleaned, high-signal evidence rather than a raw transport dump.
func serializeOutputsForMapping(outputs []*types.TaskOutput, taskPrompts map[string]string) (string, error) {
	grouped := make(map[string][]*types.TaskOutput)
	for _, o := range outputs {
		if o == nil {
			continue
		}
		grouped[o.TaskID] = append(grouped[o.TaskID], o)
	}

	taskIDs := make([]string, 0, len(grouped))
	for taskID := range grouped {
		taskIDs = append(taskIDs, taskID)
	}
	sort.Strings(taskIDs)

	var b strings.Builder
	for i, taskID := range taskIDs {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString("<<<BEGIN_TASK id=")
		b.WriteString(taskID)
		b.WriteString(">>>\n")
		writeMappingLine(&b, "PROMPT", taskPrompts[taskID])

		group := grouped[taskID]
		sort.SliceStable(group, func(i, j int) bool {
			if !group[i].CreatedAt.Equal(group[j].CreatedAt) {
				return group[i].CreatedAt.Before(group[j].CreatedAt)
			}
			return group[i].ID < group[j].ID
		})
		for _, output := range group {
			writeTaskOutputForMapping(&b, output)
		}
		b.WriteString("<<<END_TASK>>>\n")
	}
	return b.String(), nil
}

type mappingField struct {
	Path  string
	Value string
}

const (
	maxMappingFieldValueLen = 600
	maxMappingNestedItems   = 6
)

func writeTaskOutputForMapping(b *strings.Builder, output *types.TaskOutput) {
	if output == nil {
		return
	}
	b.WriteString("<<<BEGIN_OUTPUT id=")
	b.WriteString(output.ID)
	b.WriteString(">>>\n")
	writeMappingLine(b, "TITLE", output.Title)
	writeMappingLine(b, "OUTPUT_TYPE", output.OutputType)
	writeMappingLine(b, "AGENT_NAME", output.AgentName)
	writeMappingLine(b, "CREATED_AT", output.CreatedAt.Format(time.RFC3339))
	if output.Summary != nil {
		writeMappingLine(b, "SUMMARY", *output.Summary)
	}
	if output.URI != nil {
		writeMappingLine(b, "URI", *output.URI)
	}
	writeMappingSection(b, "DATA_FIELDS", collectMappingFields(output.Data))
	writeMappingSection(b, "METADATA_FIELDS", collectMappingFields(output.Metadata))
	b.WriteString("<<<END_OUTPUT>>>\n")
}

func writeMappingLine(b *strings.Builder, key, value string) {
	if b == nil {
		return
	}
	value = sanitizeMappingScalar(value)
	if value == "" {
		return
	}
	b.WriteString(key)
	b.WriteString(": ")
	b.WriteString(value)
	b.WriteByte('\n')
}

func writeMappingSection(b *strings.Builder, title string, fields []mappingField) {
	if b == nil || len(fields) == 0 {
		return
	}
	b.WriteString(title)
	b.WriteString(":\n")
	for _, field := range fields {
		if strings.TrimSpace(field.Path) == "" || strings.TrimSpace(field.Value) == "" {
			continue
		}
		b.WriteString("- ")
		b.WriteString(field.Path)
		b.WriteString(": ")
		b.WriteString(field.Value)
		b.WriteByte('\n')
	}
}

func collectMappingFields(values map[string]any) []mappingField {
	if len(values) == 0 {
		return nil
	}
	var out []mappingField
	collectMappingFieldsFromValue(&out, "", values)
	return out
}

func collectMappingFieldsFromValue(out *[]mappingField, path string, value any) {
	switch val := value.(type) {
	case nil:
		return
	case map[string]any:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		keys := make([]string, 0, len(val))
		for key := range val {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			childPath := key
			if path != "" {
				childPath = path + "." + key
			}
			collectMappingFieldsFromValue(out, childPath, val[key])
		}
	case []string:
		if summary := summarizeScalarSlice(val); summary != "" {
			*out = append(*out, mappingField{Path: path, Value: summary})
		}
	case []map[string]any:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		if strings.HasSuffix(path, "data_fields") {
			if summary := summarizeDataFields(val); summary != "" {
				*out = append(*out, mappingField{Path: path, Value: summary})
			}
			return
		}
		for i, item := range val {
			if i >= maxMappingNestedItems {
				break
			}
			childPath := fmt.Sprintf("%s[%d]", path, i)
			collectMappingFieldsFromValue(out, childPath, item)
		}
	case []any:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		if strings.HasSuffix(path, "data_fields") {
			if summary := summarizeDataFields(val); summary != "" {
				*out = append(*out, mappingField{Path: path, Value: summary})
			}
			return
		}
		if summary := summarizeScalarSlice(val); summary != "" {
			*out = append(*out, mappingField{Path: path, Value: summary})
			return
		}
		for i, item := range val {
			if i >= maxMappingNestedItems {
				break
			}
			childPath := fmt.Sprintf("%s[%d]", path, i)
			collectMappingFieldsFromValue(out, childPath, item)
		}
	default:
		if path != "" && shouldCondenseMappingPath(path) {
			if excerpt := summarizeMappingExcerpt(val); excerpt != "" {
				*out = append(*out, mappingField{Path: path + "_excerpt", Value: excerpt})
			}
			return
		}
		if text := sanitizeMappingScalar(fmt.Sprint(val)); text != "" && text != "<nil>" {
			*out = append(*out, mappingField{Path: path, Value: text})
		}
	}
}

func shouldCondenseMappingPath(path string) bool {
	return path == "source_input" ||
		path == "source_input_text" ||
		path == "source_excerpt" ||
		strings.HasSuffix(path, ".source_input") ||
		strings.HasSuffix(path, ".source_input_text") ||
		strings.HasSuffix(path, ".source_excerpt")
}

func summarizeDataFields(value any) string {
	var items []any
	switch fields := value.(type) {
	case []any:
		items = fields
	case []map[string]any:
		items = make([]any, 0, len(fields))
		for _, field := range fields {
			items = append(items, field)
		}
	default:
		return ""
	}
	parts := make([]string, 0, len(items))
	for _, item := range items {
		field, ok := item.(map[string]any)
		if !ok {
			continue
		}
		key := sanitizeMappingScalar(fmt.Sprint(field["key"]))
		if key == "" {
			continue
		}
		label := sanitizeMappingScalar(fmt.Sprint(field["label"]))
		typ := sanitizeMappingScalar(fmt.Sprint(field["type"]))
		part := key
		if label != "" || typ != "" {
			part += " ["
			if label != "" {
				part += label
			}
			if typ != "" {
				if label != "" {
					part += ": "
				}
				part += typ
			}
			part += "]"
		}
		parts = append(parts, part)
	}
	return strings.Join(parts, "; ")
}

func summarizeScalarSlice(values any) string {
	var items []string
	switch vals := values.(type) {
	case []string:
		items = vals
	case []any:
		items = make([]string, 0, len(vals))
		for _, raw := range vals {
			switch v := raw.(type) {
			case nil:
				continue
			case map[string]any, []any:
				return ""
			default:
				if text := sanitizeMappingScalar(fmt.Sprint(v)); text != "" {
					items = append(items, text)
				}
			}
		}
	default:
		return ""
	}
	if len(items) == 0 {
		return ""
	}
	if len(items) > maxMappingNestedItems {
		items = items[:maxMappingNestedItems]
	}
	return strings.Join(items, ", ")
}

func summarizeMappingExcerpt(value any) string {
	switch val := value.(type) {
	case string:
		return sanitizeMappingScalar(val)
	case map[string]any:
		for _, key := range []string{"content", "command", "description", "path", "file_path"} {
			if child, ok := val[key]; ok {
				if excerpt := summarizeMappingExcerpt(child); excerpt != "" {
					return excerpt
				}
			}
		}
		keys := make([]string, 0, len(val))
		for key := range val {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			if len(parts) == 4 {
				break
			}
			if excerpt := summarizeMappingExcerpt(val[key]); excerpt != "" {
				parts = append(parts, key+": "+excerpt)
			}
		}
		return strings.Join(parts, " | ")
	case []string:
		return summarizeScalarSlice(val)
	case []map[string]any:
		if summary := summarizeDataFields(val); summary != "" {
			return summary
		}
		return ""
	case []any:
		return summarizeScalarSlice(val)
	default:
		if value == nil {
			return ""
		}
		return sanitizeMappingScalar(fmt.Sprint(value))
	}
}

func sanitizeMappingScalar(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = html.UnescapeString(value)
	value = ansiEscapeRe.ReplaceAllString(value, "")
	value = strings.ReplaceAll(value, "\u00a0", " ")
	if markupTagRe.MatchString(value) {
		value = markupTagRe.ReplaceAllString(value, " ")
	}
	value = whitespaceRe.ReplaceAllString(value, " ")
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	runes := []rune(value)
	if len(runes) > maxMappingFieldValueLen {
		return string(runes[:maxMappingFieldValueLen]) + "…"
	}
	return value
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

// mappingVersion should be bumped whenever the BAML prompt, serialization
// logic, mapping scope, or output processing changes. This invalidates all
// cached rows.
const mappingVersion = "v6"

func hashColumns(columns []bamltypes.ColumnSchema, rowStrategy types.RowStrategy, sheetName, tableTitle, tableType string) string {
	type hashEntry struct {
		Name        string `json:"n"`
		Key         string `json:"k"`
		Type        string `json:"t"`
		Description string `json:"d"`
	}
	payload := struct {
		Version     string      `json:"v"`
		Sheet       string      `json:"s"`
		Title       string      `json:"t"`
		TableType   string      `json:"tt"`
		Mode        string      `json:"m"`
		Description string      `json:"d"`
		Columns     []hashEntry `json:"c"`
	}{
		Version:     mappingVersion,
		Sheet:       sheetName,
		Title:       strings.TrimSpace(tableTitle),
		TableType:   strings.TrimSpace(tableType),
		Mode:        rowStrategy.Mode,
		Description: rowStrategy.Description,
		Columns:     make([]hashEntry, len(columns)),
	}
	for i, c := range columns {
		payload.Columns[i] = hashEntry{Name: c.Name, Key: c.Key, Type: c.Type, Description: c.Description}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Errorf("hashColumns marshal payload: %w", err))
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])[:16]
}

func schemaColumnName(key, label string) string {
	if trimmed := strings.TrimSpace(label); trimmed != "" {
		return trimmed
	}
	return humanizeColumn(key)
}

func canonicalSchemaColumnKey(raw string) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		return ""
	}
	if normalized := normalizeColumnKey(key); normalized != "" {
		if isReservedViewColumnKey(normalized) {
			return normalized + "_value"
		}
		return normalized
	}
	return key
}

func mergeSchemaConfigColumn(existing, next configColumn) configColumn {
	if strings.TrimSpace(existing.Label) == "" && strings.TrimSpace(next.Label) != "" {
		existing.Label = strings.TrimSpace(next.Label)
	}
	if strings.TrimSpace(existing.Type) == "" && strings.TrimSpace(next.Type) != "" {
		existing.Type = normalizeColumnType(next.Type)
	}
	if strings.TrimSpace(existing.Format) == "" && strings.TrimSpace(next.Format) != "" {
		existing.Format = strings.TrimSpace(next.Format)
	}
	if !existing.Frozen && next.Frozen {
		existing.Frozen = true
	}
	if len(existing.Options) == 0 && len(next.Options) > 0 {
		existing.Options = next.Options
	}
	return existing
}

func normalizeConfigColumnsForSchema(configCols []configColumn) (map[string]configColumn, []string) {
	configByKey := make(map[string]configColumn, len(configCols))
	order := make([]string, 0, len(configCols))
	for _, cc := range configCols {
		key := canonicalSchemaColumnKey(cc.Key)
		if key == "" {
			continue
		}
		cc.Key = key
		cc.Label = strings.TrimSpace(cc.Label)
		if strings.TrimSpace(cc.Type) != "" {
			cc.Type = normalizeColumnType(cc.Type)
		}
		cc.Format = strings.TrimSpace(cc.Format)
		if existing, ok := configByKey[key]; ok {
			configByKey[key] = mergeSchemaConfigColumn(existing, cc)
			continue
		}
		configByKey[key] = cc
		order = append(order, key)
	}
	return configByKey, order
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

func sortedTaskIDSet(taskIDs map[string]bool) []string {
	ids := make([]string, 0, len(taskIDs))
	for taskID := range taskIDs {
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	return ids
}

func sortTaskOutputs(outputs []*types.TaskOutput) {
	sort.SliceStable(outputs, func(i, j int) bool {
		left := outputs[i]
		right := outputs[j]
		if left == nil || right == nil {
			return left != nil
		}
		if left.TaskID != right.TaskID {
			return left.TaskID < right.TaskID
		}
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		if left.ID != right.ID {
			return left.ID < right.ID
		}
		if left.OutputType != right.OutputType {
			return left.OutputType < right.OutputType
		}
		return left.Title < right.Title
	})
}

func filterOutputsByAgentIDs(outputs []*types.TaskOutput, agentIDs []string) []*types.TaskOutput {
	if len(agentIDs) == 0 {
		return outputs
	}
	agentSet := make(map[string]struct{}, len(agentIDs))
	for _, agentID := range agentIDs {
		if trimmed := strings.TrimSpace(agentID); trimmed != "" {
			agentSet[trimmed] = struct{}{}
		}
	}
	filtered := make([]*types.TaskOutput, 0, len(outputs))
	for _, output := range outputs {
		if output == nil || output.AgentID == nil {
			continue
		}
		if _, ok := agentSet[strings.TrimSpace(*output.AgentID)]; ok {
			filtered = append(filtered, output)
		}
	}
	return filtered
}

func (r *DataResolver) fetchTaskOutputs(ctx context.Context, workspaceID uint, taskIDs []string, agentIDs []string) ([]*types.TaskOutput, error) {
	var outputs []*types.TaskOutput
	for _, taskID := range taskIDs {
		if strings.TrimSpace(taskID) == "" {
			continue
		}
		taskOutputs, err := r.backend.ListTaskOutputs(ctx, workspaceID, taskID)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, filterOutputsByAgentIDs(taskOutputs, agentIDs)...)
	}
	return dedupeOutputs(outputs), nil
}

func canonicalizeMappedRows(columns []bamltypes.ColumnSchema, rows []bamltypes.MappedRow) []bamltypes.MappedRow {
	if len(rows) == 0 {
		return nil
	}
	columnKeys := make([]string, 0, len(columns))
	columnSet := make(map[string]struct{}, len(columns))
	columnAliases := make(map[string]string, len(columns)*3)
	ambiguousAliases := make(map[string]bool)
	for _, column := range columns {
		key := strings.TrimSpace(column.Key)
		if key == "" {
			continue
		}
		if _, exists := columnSet[key]; exists {
			continue
		}
		columnSet[key] = struct{}{}
		columnKeys = append(columnKeys, key)
		registerColumnAlias(columnAliases, ambiguousAliases, key, key)
		registerColumnAlias(columnAliases, ambiguousAliases, humanizeColumn(key), key)
		registerColumnAlias(columnAliases, ambiguousAliases, column.Name, key)
	}

	type candidateRow struct {
		TaskID          string
		RowKey          string
		SourceOutputIDs []string
		Cells           map[string]string
		FilledCount     int
	}
	candidates := make([]candidateRow, 0, len(rows))
	for _, row := range rows {
		taskID := strings.TrimSpace(row.Task_id)
		if taskID == "" {
			continue
		}
		rowKey := normalizeToken(strings.TrimSpace(row.Row_key))
		if rowKey == "" {
			rowKey = "task"
		}
		cells := make(map[string]string, len(columnKeys))
		filledCount := 0
		for _, cell := range row.Cells {
			columnKey := resolveMappedColumnKey(cell.Column, columnSet, columnAliases, ambiguousAliases)
			if columnKey == "" {
				continue
			}
			if _, exists := cells[columnKey]; exists {
				continue
			}
			cells[columnKey] = cell.Value
			if strings.TrimSpace(cell.Value) != "" {
				filledCount++
			}
		}
		if filledCount == 0 {
			continue
		}
		sourceOutputIDs := uniqueTrimmedStrings(row.Source_output_ids)
		sort.Strings(sourceOutputIDs)
		candidates = append(candidates, candidateRow{
			TaskID:          taskID,
			RowKey:          rowKey,
			SourceOutputIDs: sourceOutputIDs,
			Cells:           cells,
			FilledCount:     filledCount,
		})
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		left := candidates[i]
		right := candidates[j]
		if left.TaskID != right.TaskID {
			return left.TaskID < right.TaskID
		}
		if left.RowKey != right.RowKey {
			return left.RowKey < right.RowKey
		}
		if left.FilledCount != right.FilledCount {
			return left.FilledCount > right.FilledCount
		}
		leftSources := strings.Join(left.SourceOutputIDs, ",")
		rightSources := strings.Join(right.SourceOutputIDs, ",")
		if leftSources != rightSources {
			return leftSources < rightSources
		}
		for _, key := range columnKeys {
			if left.Cells[key] != right.Cells[key] {
				return left.Cells[key] < right.Cells[key]
			}
		}
		return false
	})

	type mergedRow struct {
		TaskID          string
		RowKey          string
		SourceOutputIDs []string
		Cells           map[string]string
	}
	mergedByKey := make(map[string]*mergedRow, len(candidates))
	order := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		mergeKey := candidate.TaskID + "\x00" + candidate.RowKey
		current, ok := mergedByKey[mergeKey]
		if !ok {
			current = &mergedRow{
				TaskID:          candidate.TaskID,
				RowKey:          candidate.RowKey,
				SourceOutputIDs: append([]string(nil), candidate.SourceOutputIDs...),
				Cells:           make(map[string]string, len(columnKeys)),
			}
			mergedByKey[mergeKey] = current
			order = append(order, mergeKey)
		} else {
			current.SourceOutputIDs = mergeSortedStrings(current.SourceOutputIDs, candidate.SourceOutputIDs)
		}
		for _, key := range columnKeys {
			if strings.TrimSpace(current.Cells[key]) == "" && strings.TrimSpace(candidate.Cells[key]) != "" {
				current.Cells[key] = candidate.Cells[key]
			}
		}
	}

	result := make([]bamltypes.MappedRow, 0, len(order))
	for _, mergeKey := range order {
		row := mergedByKey[mergeKey]
		cells := make([]bamltypes.MappedCell, 0, len(columnKeys))
		for _, key := range columnKeys {
			cells = append(cells, bamltypes.MappedCell{
				Column: key,
				Value:  row.Cells[key],
			})
		}
		result = append(result, bamltypes.MappedRow{
			Task_id:           row.TaskID,
			Row_key:           row.RowKey,
			Source_output_ids: row.SourceOutputIDs,
			Cells:             cells,
		})
	}
	return result
}

func registerColumnAlias(aliasToKey map[string]string, ambiguous map[string]bool, alias, key string) {
	normalized := normalizeColumnKey(alias)
	if normalized == "" {
		return
	}
	if existing, ok := aliasToKey[normalized]; ok && existing != key {
		delete(aliasToKey, normalized)
		ambiguous[normalized] = true
		return
	}
	if !ambiguous[normalized] {
		aliasToKey[normalized] = key
	}
}

func resolveMappedColumnKey(raw string, columnSet map[string]struct{}, aliasToKey map[string]string, ambiguous map[string]bool) string {
	key := strings.TrimSpace(raw)
	if key == "" {
		return ""
	}
	if _, ok := columnSet[key]; ok {
		return key
	}
	normalized := normalizeColumnKey(key)
	if normalized == "" {
		return ""
	}
	if _, ok := columnSet[normalized]; ok {
		return normalized
	}
	if ambiguous[normalized] {
		return ""
	}
	return aliasToKey[normalized]
}

func mergeSortedStrings(left, right []string) []string {
	if len(left) == 0 {
		return append([]string(nil), right...)
	}
	if len(right) == 0 {
		return append([]string(nil), left...)
	}
	merged := append(append([]string(nil), left...), right...)
	merged = uniqueTrimmedStrings(merged)
	sort.Strings(merged)
	return merged
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

var ansiEscapeRe = regexp.MustCompile(`\x1b\[[0-9;?]*[ -/]*[@-~]`)
var markupTagRe = regexp.MustCompile(`(?s)<[^>]+>`)
var whitespaceRe = regexp.MustCompile(`\s+`)
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
