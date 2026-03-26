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
	viewprojection "github.com/beam-cloud/airstore/pkg/views/projection"
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
	TaskID             string
	DetailTaskID       string
	RowID              string
	StableRef          string
	RowKey             string
	OutputID           string
	OutputStatus       string
	BlockerOutputIDs   string
	BlockerKind        string
	BlockerInputKind   string
	BlockerWaitGroupID string
	SourceOutputIDs    string
	Source             string
	Cells              map[string]string
}

type hiddenResolvedColumn struct {
	Key   string
	Value func(sheetID string, row resolvedSheetRow) any
}

const (
	taskMetadataColumnNextWakeAt      = "next_wake_at"
	taskMetadataColumnNextWakeSummary = "next_wake_summary"
	taskMetadataColumnWakeAt          = "wake_at"
	taskMetadataColumnWakeReason      = "wake_reason"
)

var hiddenResolvedColumns = []hiddenResolvedColumn{
	{Key: "task_id", Value: func(_ string, row resolvedSheetRow) any { return row.TaskID }},
	{Key: "detail_task_id", Value: func(_ string, row resolvedSheetRow) any { return row.DetailTaskID }},
	{Key: "row_id", Value: func(_ string, row resolvedSheetRow) any { return row.RowID }},
	{Key: "stable_ref", Value: func(_ string, row resolvedSheetRow) any { return row.StableRef }},
	{Key: "sheet_id", Value: func(sheetID string, _ resolvedSheetRow) any { return sheetID }},
	{Key: "output_id", Value: func(_ string, row resolvedSheetRow) any { return row.OutputID }},
	{Key: "output_status", Value: func(_ string, row resolvedSheetRow) any { return row.OutputStatus }},
	{Key: "blocker_output_ids", Value: func(_ string, row resolvedSheetRow) any { return row.BlockerOutputIDs }},
	{Key: "blocker_kind", Value: func(_ string, row resolvedSheetRow) any { return row.BlockerKind }},
	{Key: "blocker_input_kind", Value: func(_ string, row resolvedSheetRow) any { return row.BlockerInputKind }},
	{Key: "blocker_wait_group_id", Value: func(_ string, row resolvedSheetRow) any { return row.BlockerWaitGroupID }},
	{Key: "source_output_ids", Value: func(_ string, row resolvedSheetRow) any { return row.SourceOutputIDs }},
	{Key: "source", Value: func(_ string, row resolvedSheetRow) any { return row.Source }},
}

type DataResolver struct {
	backend dataResolverBackend
	store   *ViewStore
}

type ResolveOptions struct {
	ForceRefresh  bool
	ViewAgentRefs []string
	SourceViewID  string
}

type mappingSpec struct {
	tableCols   []bamltypes.ColumnSchema
	mappingCols []bamltypes.ColumnSchema
	schemaHash  string
}

func buildMappingSpec(sheetName string, comp types.ComponentSpec) mappingSpec {
	tableCols := buildColumnSchemas(comp)
	mappingCols := filterBamlColumns(tableCols)

	return mappingSpec{
		tableCols:   tableCols,
		mappingCols: mappingCols,
		schemaHash:  hashColumns(mappingCols, sheetName, comp.Title, comp.Type),
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
	ListTasksFiltered(ctx context.Context, workspaceId uint, filter types.AgentTaskListFilter) ([]*types.AgentTask, error)
	ListTaskOutputs(ctx context.Context, workspaceId uint, taskID string) ([]*types.TaskOutput, error)
	ListWorkspaceTaskOutputs(ctx context.Context, workspaceId uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error)
	GetTaskByID(ctx context.Context, taskId string) (*types.AgentTask, error)
	ListSpawnBindingsForOutputs(ctx context.Context, outputIDs []string) ([]repository.SpawnBinding, error)
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

	agentIDs, ok := r.resolveScopedAgentIDs(ctx, workspaceID, comp.DataSource, opts.ViewAgentRefs)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoOutputsForTask, taskID)
	}
	allOutputs, err := r.fetchTaskOutputs(ctx, workspaceID, []string{taskID}, agentIDs)
	if err != nil {
		return nil, fmt.Errorf("fetch mapping outputs: %w", err)
	}
	if len(allOutputs) == 0 {
		task, taskErr := r.backend.GetTaskByID(ctx, taskID)
		if taskErr != nil || task == nil {
			return nil, fmt.Errorf("%w: %s", ErrNoOutputsForTask, taskID)
		}
		if synthetic := blockerMappingOutput(task); synthetic != nil {
			allOutputs = []*types.TaskOutput{synthetic}
		} else {
			return nil, fmt.Errorf("%w: %s", ErrNoOutputsForTask, taskID)
		}
	}
	oldRows, excludedSnapshots := r.loadStoredRowsAndExclusions(ctx, viewID, sheet.ID, comp.ID)
	rowsByGroup := groupViewRowsByGroup(oldRows)
	taskGroups := map[string][]*types.TaskOutput{taskID: allOutputs}
	mappedRows, err := r.mapTaskGroupsWithBAML(ctx, viewID, sheet, comp, spec, []string{taskID}, taskGroups, rowsByGroup, excludedSnapshots)
	if err != nil {
		return nil, err
	}
	persisted := materializeTaskGroups(sheet.ID, comp.ID, spec, []string{taskID}, taskGroups, rowsByGroup, mappedRows, time.Now())[taskID]

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

	allOutputs, err := r.fetchMappingOutputs(ctx, workspaceID, comp.DataSource, opts.ViewAgentRefs, opts.SourceViewID)
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
			Msg("view: no task outputs resolved for component — checking for import rows")

		importRows := r.loadAndMergeImportRows(ctx, viewID, sheet.ID, comp.ID, nil)
		if len(importRows) > 0 {
			log.Info().
				Str("view_id", viewID).
				Str("sheet_id", sheet.ID).
				Int("import_rows", len(importRows)).
				Msg("view: returning import rows (no task outputs)")
			return &viewMappingResult{Rows: importRows, TaskMeta: map[string]*types.AgentTask{}}, nil
		}
		return &viewMappingResult{Rows: nil, TaskMeta: map[string]*types.AgentTask{}}, nil
	}

	if len(spec.tableCols) == 0 {
		log.Info().
			Str("view_id", viewID).
			Str("sheet_id", sheet.ID).
			Msg("view: no column schema defined — skipping BAML mapping, returning import rows only")

		importRows := r.loadAndMergeImportRows(ctx, viewID, sheet.ID, comp.ID, nil)
		return &viewMappingResult{Rows: importRows, TaskMeta: map[string]*types.AgentTask{}}, nil
	}

	taskGroups := groupOutputsByTask(allOutputs)
	taskMeta := r.fetchTaskMetadata(ctx, taskIDsFromGroups(taskGroups))

	existingRows, excludedSnapshots := r.loadStoredRowsAndExclusions(ctx, viewID, sheet.ID, comp.ID)
	rowsByGroup := groupViewRowsByGroup(existingRows)

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

	outputSignaturesByTask := make(map[string]string, len(taskGroups))
	for taskID, outputs := range taskGroups {
		outputSignaturesByTask[taskID] = outputGroupSignature(outputs)
		if opts.ForceRefresh {
			uncachedIDs[taskID] = true
			uncachedReasonCounts["force_refresh"]++
			continue
		}

		taskOIDs := sortedOutputIDs(outputs)
		storedRows := rowsByGroup[taskID]
		if ok, reason := groupRowsFresh(storedRows, comp.ID, spec.schemaHash, taskOIDs, outputSignaturesByTask[taskID]); ok {
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
	diagnosticsCache, _ := diagnostics["cache"].(map[string]any)

	boundContext := r.fetchBoundTaskContext(ctx, workspaceID, resolvedRows)

	if len(uncachedIDs) == 0 {
		importRows := r.loadAndMergeImportRows(ctx, viewID, sheet.ID, comp.ID, resolvedRows)
		resolvedRows = append(resolvedRows, importRows...)
		enrichRowsWithOutputState(resolvedRows, allOutputs, boundContext, taskMeta)
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
		Interface("uncached_reason_counts", uncachedReasonCounts).
		Msg("view: mapping required")

	persistedByGroup := make(map[string][]ViewRow)
	mappedByGroup := make(map[string][]resolvedSheetRow)
	mappedRows, err := r.mapTaskGroupsWithBAML(ctx, viewID, sheet, comp, spec, uncachedTIDs, taskGroups, rowsByGroup, excludedSnapshots)
	mappingFailed := false
	if err != nil {
		if opts.ForceRefresh {
			return nil, fmt.Errorf("force refresh BAML mapping: %w", err)
		}
		log.Warn().Err(err).Str("view_id", viewID).Str("sheet_id", sheet.ID).Int("tasks", len(uncachedIDs)).Msg("BAML mapping failed")
		mappingFailed = true
		if diagnosticsCache != nil {
			diagnosticsCache["mapping_failed"] = true
		}
	}
	if !mappingFailed {
		persistedByGroup = materializeTaskGroups(sheet.ID, comp.ID, spec, uncachedTIDs, taskGroups, rowsByGroup, mappedRows, time.Now())
		for _, taskID := range uncachedTIDs {
			mappedByGroup[taskID] = resolvedRowsFromStored(persistedByGroup[taskID], applyManualEdits)
		}
	} else {
		for _, taskID := range uncachedTIDs {
			if stored := rowsByGroup[taskID]; len(stored) > 0 {
				mappedByGroup[taskID] = resolvedRowsFromStored(stored, applyManualEdits)
			}
		}
	}

	if !mappingFailed && r.store != nil {
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

	resolvedRows = deduplicateResolvedRows(resolvedRows, taskMeta)

	importRows := r.loadAndMergeImportRows(ctx, viewID, sheet.ID, comp.ID, resolvedRows)
	resolvedRows = append(resolvedRows, importRows...)

	boundContext = r.fetchBoundTaskContext(ctx, workspaceID, resolvedRows)
	enrichRowsWithOutputState(resolvedRows, allOutputs, boundContext, taskMeta)
	sortResolvedRows(resolvedRows, taskMeta)
	return &viewMappingResult{Rows: resolvedRows, TaskMeta: taskMeta, Diagnostics: diagnostics}, nil
}

// loadAndMergeImportRows loads import-sourced rows from the store, applies any
// enrichment from task-mapped rows that matched import row keys, and converts
// them to resolvedSheetRows. Task-mapped rows that enriched an import row are
// removed from the resolved set to prevent duplicates.
func (r *DataResolver) loadAndMergeImportRows(ctx context.Context, viewID, sheetID, componentID string, taskRows []resolvedSheetRow) []resolvedSheetRow {
	if r.store == nil || !r.store.Available() {
		return nil
	}

	importRows, err := r.store.GetRowsBySource(ctx, viewID, sheetID, componentID, "import")
	if err != nil {
		log.Warn().Err(err).
			Str("view_id", viewID).Str("sheet_id", sheetID).Str("component_id", componentID).
			Msg("view resolver: failed to load import rows")
		return nil
	}
	if len(importRows) == 0 {
		log.Debug().
			Str("view_id", viewID).Str("sheet_id", sheetID).Str("component_id", componentID).
			Msg("view resolver: no import rows found")
		return nil
	}
	log.Info().
		Str("view_id", viewID).Str("sheet_id", sheetID).Str("component_id", componentID).
		Int("import_rows", len(importRows)).Int("task_rows", len(taskRows)).
		Msg("view resolver: loading import rows")

	importByRowKey := make(map[string]*ViewRow, len(importRows))
	for i := range importRows {
		importByRowKey[strings.TrimSpace(importRows[i].RowKey)] = &importRows[i]
	}

	for i, taskRow := range taskRows {
		rowKey := strings.TrimSpace(taskRow.RowKey)
		if importRow, ok := importByRowKey[rowKey]; ok {
			for colKey, val := range taskRow.Cells {
				if val != "" {
					if importRow.Cells == nil {
						importRow.Cells = make(map[string]string)
					}
					importRow.Cells[colKey] = val
				}
			}
			if taskRow.TaskID != "" {
				importRow.TaskID = taskRow.TaskID
			}
			taskRows[i].Cells = nil
		}
	}

	var resolved []resolvedSheetRow
	for _, row := range importRows {
		resolved = append(resolved, resolvedSheetRow{
			TaskID:    row.TaskID,
			RowID:     row.ID,
			StableRef: row.StableRef,
			RowKey:    row.RowKey,
			Source:    "import",
			Cells:     row.MergedCells(),
		})
	}
	return resolved
}

// ---------------------------------------------------------------------------
// View-level helpers
// ---------------------------------------------------------------------------

func viewMappingFlightKey(workspaceID uint, viewID, sheetID, componentID string, opts ResolveOptions) string {
	sourceView := strings.TrimSpace(opts.SourceViewID)
	if sourceView == "" {
		sourceView = "-"
	}
	return fmt.Sprintf(
		"%d:%s:%s:%s:%t:%s:%s",
		workspaceID,
		viewID,
		sheetID,
		componentID,
		opts.ForceRefresh,
		normalizedViewAgentRefsKey(opts.ViewAgentRefs),
		sourceView,
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

func groupViewRowsByGroup(rows []ViewRow) map[string][]ViewRow {
	grouped := make(map[string][]ViewRow, len(rows))
	for i := range rows {
		row := rows[i]
		groupID := strings.TrimSpace(row.GroupID)
		if groupID == "" {
			groupID = strings.TrimSpace(row.TaskID)
		}
		if groupID == "" {
			continue
		}
		grouped[groupID] = append(grouped[groupID], row)
	}
	return grouped
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

func outputsForTaskIDs(taskGroups map[string][]*types.TaskOutput, taskIDs []string) []*types.TaskOutput {
	if len(taskGroups) == 0 || len(taskIDs) == 0 {
		return nil
	}
	var outputs []*types.TaskOutput
	for _, taskID := range taskIDs {
		outputs = append(outputs, taskGroups[taskID]...)
	}
	return outputs
}

func storedRowsForTaskIDs(rowsByGroup map[string][]ViewRow, taskIDs []string) []ViewRow {
	if len(rowsByGroup) == 0 || len(taskIDs) == 0 {
		return nil
	}
	rows := make([]ViewRow, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		rows = append(rows, rowsByGroup[taskID]...)
	}
	return rows
}

func filterExcludedSnapshotsByTasks(snapshots []ExcludedRowSnapshot, taskIDs []string) []ExcludedRowSnapshot {
	if len(snapshots) == 0 || len(taskIDs) == 0 {
		return nil
	}
	taskSet := make(map[string]bool, len(taskIDs))
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if taskID != "" {
			taskSet[taskID] = true
		}
	}
	if len(taskSet) == 0 {
		return nil
	}
	filtered := make([]ExcludedRowSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if taskSet[strings.TrimSpace(snapshot.TaskID)] {
			filtered = append(filtered, snapshot)
		}
	}
	return filtered
}

const maxTasksPerMappingBatch = 5

func (r *DataResolver) mapTaskGroupsWithBAML(
	ctx context.Context,
	viewID string,
	sheet types.SheetSpec,
	comp types.ComponentSpec,
	spec mappingSpec,
	taskIDs []string,
	taskGroups map[string][]*types.TaskOutput,
	rowsByGroup map[string][]ViewRow,
	excludedSnapshots []ExcludedRowSnapshot,
) ([]bamltypes.MappedRow, error) {
	if len(taskIDs) == 0 || len(spec.mappingCols) == 0 {
		return nil, nil
	}
	requestedTasks := make(map[string]bool, len(taskIDs))
	for _, taskID := range taskIDs {
		taskID = strings.TrimSpace(taskID)
		if taskID != "" {
			requestedTasks[taskID] = true
		}
	}
	if len(requestedTasks) == 0 {
		return nil, nil
	}

	batches := splitTaskIDBatches(taskIDs, maxTasksPerMappingBatch)
	taskPrompts := r.fetchTaskPrompts(ctx, taskIDs)

	var importRows []ViewRow
	if r.store != nil && r.store.Available() {
		importRows, _ = r.store.GetRowsBySource(ctx, viewID, sheet.ID, comp.ID, "import")
	}

	var allRows []bamltypes.MappedRow
	var lastErr error
	successBatches := 0

	for batchIdx, batch := range batches {
		rows, err := r.mapTaskBatchWithBAML(ctx, viewID, sheet, comp, spec, batch, taskGroups, rowsByGroup, excludedSnapshots, taskPrompts, requestedTasks, importRows)
		if err != nil {
			log.Warn().Err(err).
				Str("view_id", viewID).
				Str("sheet_id", sheet.ID).
				Int("batch", batchIdx+1).
				Int("total_batches", len(batches)).
				Int("batch_tasks", len(batch)).
				Msg("BAML mapping batch failed")
			lastErr = err
			continue
		}
		successBatches++
		allRows = append(allRows, rows...)
	}

	if successBatches == 0 && lastErr != nil {
		return nil, fmt.Errorf("BAML mapping: all %d batches failed: %w", len(batches), lastErr)
	}
	if lastErr != nil {
		log.Warn().
			Str("view_id", viewID).
			Str("sheet_id", sheet.ID).
			Int("succeeded", successBatches).
			Int("failed", len(batches)-successBatches).
			Msg("BAML mapping partially succeeded")
	}

	return allRows, nil
}

func (r *DataResolver) mapTaskBatchWithBAML(
	ctx context.Context,
	viewID string,
	sheet types.SheetSpec,
	comp types.ComponentSpec,
	spec mappingSpec,
	batchTaskIDs []string,
	taskGroups map[string][]*types.TaskOutput,
	rowsByGroup map[string][]ViewRow,
	excludedSnapshots []ExcludedRowSnapshot,
	taskPrompts map[string]string,
	requestedTasks map[string]bool,
	importRows []ViewRow,
) ([]bamltypes.MappedRow, error) {
	outputsJSON, err := serializeOutputsForMapping(outputsForTaskIDs(taskGroups, batchTaskIDs), taskPrompts)
	if err != nil {
		return nil, fmt.Errorf("serialize outputs: %w", err)
	}

	taskExistingRows := storedRowsForTaskIDs(rowsByGroup, batchTaskIDs)
	allExistingRows := append(taskExistingRows, importRows...)
	existingData := serializeExistingRows(allExistingRows, spec.mappingCols)
	excludedForTasks := filterExcludedSnapshotsByTasks(excludedSnapshots, batchTaskIDs)
	excludedData := serializeExcludedRows(excludedForTasks)

	result, err := baml.MapOutputsToSchema(
		ctx,
		sheet.Name,
		comp.Title,
		comp.Type,
		spec.mappingCols,
		outputsJSON,
		existingData,
		excludedData,
	)
	if err != nil {
		return nil, fmt.Errorf("BAML mapping: %w", err)
	}

	rows := filterMappedRowsByExclusions(
		result.Rows,
		excludedForTasks,
		comp.ID,
	)
	filtered := make([]bamltypes.MappedRow, 0, len(rows))
	for _, row := range rows {
		taskID := strings.TrimSpace(row.Task_id)
		if requestedTasks[taskID] {
			filtered = append(filtered, row)
			continue
		}
		log.Warn().
			Str("view_id", viewID).
			Str("sheet_id", sheet.ID).
			Str("task_id", taskID).
			Msg("BAML returned row for unknown task_id, skipping")
	}
	return filtered, nil
}

func splitTaskIDBatches(taskIDs []string, batchSize int) [][]string {
	if batchSize <= 0 {
		batchSize = maxTasksPerMappingBatch
	}
	if len(taskIDs) <= batchSize {
		return [][]string{taskIDs}
	}
	var batches [][]string
	for i := 0; i < len(taskIDs); i += batchSize {
		end := i + batchSize
		if end > len(taskIDs) {
			end = len(taskIDs)
		}
		batches = append(batches, taskIDs[i:end])
	}
	return batches
}

func materializeTaskGroups(
	sheetID string,
	componentID string,
	spec mappingSpec,
	taskIDs []string,
	taskGroups map[string][]*types.TaskOutput,
	rowsByGroup map[string][]ViewRow,
	mappedRows []bamltypes.MappedRow,
	now time.Time,
) map[string][]ViewRow {
	persistedByGroup := make(map[string][]ViewRow, len(taskIDs))
	if len(taskIDs) == 0 {
		return persistedByGroup
	}
	mappedByTask := make(map[string][]bamltypes.MappedRow, len(taskIDs))
	for _, row := range mappedRows {
		taskID := strings.TrimSpace(row.Task_id)
		if taskID == "" {
			continue
		}
		mappedByTask[taskID] = append(mappedByTask[taskID], row)
	}
	for _, taskID := range taskIDs {
		persistedByGroup[taskID] = materializeTaskGroup(
			sheetID,
			componentID,
			taskID,
			spec.schemaHash,
			taskGroups[taskID],
			rowsByGroup[taskID],
			mappedByTask[taskID],
			now,
		)
	}
	return persistedByGroup
}

func materializeTaskGroup(
	sheetID string,
	componentID string,
	taskID string,
	schemaHash string,
	outputs []*types.TaskOutput,
	existingRows []ViewRow,
	mappedRows []bamltypes.MappedRow,
	now time.Time,
) []ViewRow {
	outputSignature := outputGroupSignature(outputs)
	persisted := make([]ViewRow, 0, len(mappedRows))
	for _, row := range mappedRows {
		persisted = append(persisted, mappedRowToViewRow(sheetID, componentID, taskID, schemaHash, outputSignature, outputs, row, now))
	}
	if len(persisted) == 0 {
		retained := retainExistingRows(existingRows, componentID, outputSignature, schemaHash)
		if len(retained) > 0 {
			return retained
		}
		persisted = []ViewRow{fallbackViewRow(sheetID, componentID, taskID, schemaHash, outputSignature, outputs, now)}
	}
	return persisted
}

// retainExistingRows returns stored rows that should be kept when BAML
// produced zero mapped rows for a task group. This prevents non-deterministic
// LLM drops from deleting rows that were successfully mapped in a prior call.
// Only rows whose output signature matches the current outputs are retained —
// genuinely stale rows (from old output sets) are not carried forward.
func retainExistingRows(existingRows []ViewRow, componentID, outputSignature, schemaHash string) []ViewRow {
	var retained []ViewRow
	for _, existing := range existingRows {
		if existing.Marker || existing.ComponentID != componentID {
			continue
		}
		if existing.OutputSignature != outputSignature {
			continue
		}
		existing.SchemaHash = schemaHash
		retained = append(retained, existing)
	}
	return retained
}

func viewRowSemanticKey(row ViewRow) string {
	sourceOutputIDs := append([]string(nil), row.SourceOutputIDs...)
	sort.Strings(sourceOutputIDs)
	cellFingerprint := excludedRowCellsFingerprint(row.Cells)
	if row.Marker {
		return "marker\x00" + strings.Join(sourceOutputIDs, ",") + "\x00" + strings.TrimSpace(row.OutputSignature)
	}
	if cellFingerprint == "" {
		cellFingerprint = strings.TrimSpace(row.RowKey)
	}
	return "row\x00" + strings.Join(sourceOutputIDs, ",") + "\x00" + cellFingerprint
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

func groupRowsFresh(rows []ViewRow, componentID, schemaH string, outputIDs []string, outputSignature string) (bool, string) {
	if len(rows) == 0 {
		return false, "missing_rows"
	}
	markerCount := 0
	seenRowIDs := make(map[string]struct{}, len(rows))
	seenSemantics := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		rowID := strings.TrimSpace(row.ID)
		if rowID == "" {
			return false, "missing_row_id"
		}
		if _, ok := seenRowIDs[rowID]; ok {
			return false, "duplicate_row_id"
		}
		seenRowIDs[rowID] = struct{}{}
		if row.Marker {
			markerCount++
		}
		if strings.TrimSpace(componentID) != "" && row.ComponentID != componentID {
			return false, "component_scope_mismatch"
		}
		if !slicesMatch(row.OutputIDs, outputIDs) {
			return false, "output_ids_mismatch"
		}
		if strings.TrimSpace(row.OutputSignature) != strings.TrimSpace(outputSignature) {
			return false, "output_signature_mismatch"
		}
		semanticKey := viewRowSemanticKey(row)
		if _, ok := seenSemantics[semanticKey]; ok {
			return false, "duplicate_row_signature"
		}
		seenSemantics[semanticKey] = struct{}{}
	}
	if markerCount > 1 {
		return false, "duplicate_markers"
	}
	if markerCount > 0 && markerCount != len(rows) {
		return false, "mixed_marker_state"
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
	if dataSourceOutputTypeFallback(ds) != "" {
		return true
	}
	return strings.TrimSpace(ds.ArtifactKey) != "" ||
		strings.TrimSpace(ds.TimeRange) != "" ||
		len(ds.Statuses) > 0
}

func dataSourceOutputTypeFallback(ds *types.DataSource) string {
	if ds == nil || strings.TrimSpace(ds.ArtifactKey) != "" {
		return ""
	}
	return strings.TrimSpace(ds.OutputType)
}

func (r *DataResolver) resolveScopedAgentIDs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string) ([]string, bool) {
	resolvedAgentIDs := r.resolveAgentIDsForDS(ctx, workspaceID, ds)
	if dataSourceHasExplicitAgentScope(ds) && len(resolvedAgentIDs) == 0 {
		log.Warn().
			Uint("workspace_id", workspaceID).
			Bool("has_explicit_scope", dataSourceHasExplicitAgentScope(ds)).
			Strs("view_agent_refs", viewAgentRefs).
			Msg("view resolver: explicit agent scope resolved 0 agents")
		return nil, false
	}
	if len(resolvedAgentIDs) == 0 && len(viewAgentRefs) > 0 {
		resolvedAgentIDs = r.resolveAgentIDsFromRefs(ctx, workspaceID, viewAgentRefs)
		if len(resolvedAgentIDs) == 0 {
			log.Warn().
				Uint("workspace_id", workspaceID).
				Strs("view_agent_refs", viewAgentRefs).
				Msg("view resolver: view agent refs resolved 0 agents")
			return nil, false
		}
	}
	log.Debug().
		Uint("workspace_id", workspaceID).
		Strs("resolved_agent_ids", resolvedAgentIDs).
		Strs("view_agent_refs", viewAgentRefs).
		Msg("view resolver: resolved agent IDs")
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

// deduplicateResolvedRows removes cross-task duplicates by RowKey.
// When two rows from different tasks share the same non-empty RowKey,
// the row with more populated cells wins; ties broken by most recent task.
func deduplicateResolvedRows(rows []resolvedSheetRow, taskMeta map[string]*types.AgentTask) []resolvedSheetRow {
	if len(rows) == 0 {
		return rows
	}
	type entry struct {
		index     int
		cellCount int
	}
	seen := make(map[string]entry, len(rows))
	for i, row := range rows {
		key := strings.TrimSpace(row.RowKey)
		if key == "" || row.Source == "import" {
			continue
		}
		cc := 0
		for _, v := range row.Cells {
			if strings.TrimSpace(v) != "" {
				cc++
			}
		}
		prev, exists := seen[key]
		if !exists {
			seen[key] = entry{index: i, cellCount: cc}
			continue
		}
		keepNew := false
		if cc > prev.cellCount {
			keepNew = true
		} else if cc == prev.cellCount {
			prevTask := taskMeta[rows[prev.index].TaskID]
			curTask := taskMeta[row.TaskID]
			if prevTask != nil && curTask != nil && curTask.CreatedAt.After(prevTask.CreatedAt) {
				keepNew = true
			}
		}
		if keepNew {
			rows[prev.index].Cells = nil
			seen[key] = entry{index: i, cellCount: cc}
		} else {
			rows[i].Cells = nil
		}
	}
	deduped := make([]resolvedSheetRow, 0, len(rows))
	for _, row := range rows {
		if row.Cells != nil || row.Source == "import" {
			deduped = append(deduped, row)
		}
	}
	return deduped
}

func dedupeTasks(tasks []*types.AgentTask) []*types.AgentTask {
	if len(tasks) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(tasks))
	deduped := make([]*types.AgentTask, 0, len(tasks))
	for _, task := range tasks {
		if task == nil || strings.TrimSpace(task.ID) == "" {
			continue
		}
		if _, ok := seen[task.ID]; ok {
			continue
		}
		seen[task.ID] = struct{}{}
		deduped = append(deduped, task)
	}
	sort.SliceStable(deduped, func(i, j int) bool {
		if deduped[i].CreatedAt.Equal(deduped[j].CreatedAt) {
			return strings.TrimSpace(deduped[i].ID) < strings.TrimSpace(deduped[j].ID)
		}
		return deduped[i].CreatedAt.Before(deduped[j].CreatedAt)
	})
	return deduped
}

func (r *DataResolver) listScopedTasks(ctx context.Context, workspaceID uint, filter types.AgentTaskListFilter, agentIDs []string) ([]*types.AgentTask, error) {
	if len(agentIDs) == 0 {
		tasks, err := r.backend.ListTasksFiltered(ctx, workspaceID, filter)
		if err != nil {
			return nil, err
		}
		return dedupeTasks(tasks), nil
	}

	var all []*types.AgentTask
	for _, agentID := range agentIDs {
		localFilter := filter
		localFilter.AgentID = &agentID
		tasks, err := r.backend.ListTasksFiltered(ctx, workspaceID, localFilter)
		if err != nil {
			return nil, err
		}
		all = append(all, tasks...)
	}
	return dedupeTasks(all), nil
}

func (r *DataResolver) fetchOutputsForScope(ctx context.Context, workspaceID uint, ds *types.DataSource, agentIDs []string, sourceViewID string) ([]*types.TaskOutput, error) {
	filter := baseTaskOutputFilter()
	if outputType := dataSourceOutputTypeFallback(ds); outputType != "" {
		filter.OutputType = &outputType
	}
	if sourceViewID != "" {
		filter.SourceViewID = &sourceViewID
	}
	outputs, err := r.listScopedOutputs(ctx, workspaceID, filter, agentIDs)
	if err != nil {
		return nil, err
	}

	filtered := filterOutputsForDataSource(outputs, ds, agentIDs)
	if len(outputs) > 0 && len(filtered) == 0 {
		log.Warn().
			Uint("workspace_id", workspaceID).
			Strs("agent_ids", agentIDs).
			Str("source_view_id", sourceViewID).
			Int("pre_filter", len(outputs)).
			Int("post_filter", len(filtered)).
			Msg("view resolver: all outputs filtered out by data source criteria")
	}
	return filtered, nil
}

func taskBelongsToView(task *types.AgentTask, viewID string) bool {
	if task == nil || task.PayloadJSON == nil {
		return false
	}
	v, _ := task.PayloadJSON["source_view_id"].(string)
	return strings.TrimSpace(v) == viewID
}

func (r *DataResolver) fetchComponentOutputs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string) ([]*types.TaskOutput, error) {
	agentIDs, ok := r.resolveScopedAgentIDs(ctx, workspaceID, ds, viewAgentRefs)
	if !ok {
		return nil, nil
	}
	return r.fetchOutputsForScope(ctx, workspaceID, ds, agentIDs, "")
}

// fetchMappingOutputs lets the data source select which tasks belong to the
// sheet, then expands each selected task back to its full output set so BAML
// sees complete task context instead of a filtered artifact slice.
//
// When the data source targets specific outputs (via statuses + artifact_key),
// expansion is skipped so BAML maps exactly the outputs the user selected.
// Expansion is only done when the filter selects tasks broadly (output_type
// or time_range alone) so BAML can synthesize a full-context row per task.
func (r *DataResolver) fetchMappingOutputs(ctx context.Context, workspaceID uint, ds *types.DataSource, viewAgentRefs []string, sourceViewID string) ([]*types.TaskOutput, error) {
	agentIDs, ok := r.resolveScopedAgentIDs(ctx, workspaceID, ds, viewAgentRefs)
	if !ok {
		log.Info().
			Uint("workspace_id", workspaceID).
			Strs("view_agent_refs", viewAgentRefs).
			Str("source_view_id", sourceViewID).
			Msg("view resolver: agent scope resolution failed — no outputs")
		return nil, nil
	}

	realSelectedOutputs, err := r.fetchOutputsForScope(ctx, workspaceID, ds, agentIDs, sourceViewID)
	if err != nil {
		return nil, err
	}

	log.Info().
		Uint("workspace_id", workspaceID).
		Strs("agent_ids", agentIDs).
		Str("source_view_id", sourceViewID).
		Int("real_outputs", len(realSelectedOutputs)).
		Msg("view resolver: fetched mapping outputs")

	if len(realSelectedOutputs) == 0 || !dataSourceNarrowsTaskSelection(ds) {
		return realSelectedOutputs, nil
	}

	if dataSourceTargetsSpecificOutputs(ds) {
		return realSelectedOutputs, nil
	}

	taskIDs := taskSetFromOutputs(realSelectedOutputs)
	if len(taskIDs) == 0 {
		return realSelectedOutputs, nil
	}

	allTaskOutputs, err := r.fetchTaskOutputs(ctx, workspaceID, sortedTaskIDSet(taskIDs), agentIDs)
	if err != nil {
		return nil, err
	}
	return dedupeOutputs(allTaskOutputs), nil
}

// dataSourceTargetsSpecificOutputs returns true when the filter selects a
// narrow slice of outputs rather than broadly selecting tasks. When statuses
// are specified alongside a type or artifact key filter, the user wants BAML
// to map exactly those outputs — not the full task context.
func dataSourceTargetsSpecificOutputs(ds *types.DataSource) bool {
	if ds == nil || len(ds.Statuses) == 0 {
		return false
	}
	return strings.TrimSpace(ds.ArtifactKey) != "" || strings.TrimSpace(ds.OutputType) != ""
}

func blockerMappingOutput(task *types.AgentTask) *types.TaskOutput {
	if task == nil || strings.TrimSpace(task.ID) == "" {
		return nil
	}
	blocker := viewprojection.ProjectBlocker(task, nil)
	if blocker == nil {
		return nil
	}
	if status := strings.TrimSpace(blocker.Status); status != "" && !strings.EqualFold(status, string(types.TaskBlockerStatusOpen)) {
		return nil
	}

	title := blockerMappingTitle(task, blocker)
	summary := strings.TrimSpace(blocker.Summary)
	details := strings.TrimSpace(blocker.Details)
	if title == "" && summary == "" && details == "" {
		return nil
	}

	channel := blockerMappingChannel(task)
	recipient, subject := blockerMappingEmailFields(blocker)
	outputType := blockerMappingOutputType(channel, details, recipient, subject)
	createdAt := task.CreatedAt
	if task.CurrentBlocker != nil && !task.CurrentBlocker.CreatedAt.IsZero() {
		createdAt = task.CurrentBlocker.CreatedAt
	}

	data := map[string]any{
		"blocker_kind": blocker.Kind,
		"input_kind":   blocker.InputKind,
		"task_state":   string(task.State),
	}
	if summary != "" {
		data["summary"] = summary
	}
	if details != "" {
		data["details"] = details
		data["content"] = details
	}
	if label := blockerMappingTaskLabel(task); label != "" {
		data["task_label"] = label
	}
	if channel != "" {
		data["channel"] = channel
	}
	if recipient != "" {
		data["recipient"] = recipient
		data["to"] = recipient
	}
	if subject != "" {
		data["subject"] = subject
	}

	output := &types.TaskOutput{
		ID:          blockerMappingOutputID(task, blocker),
		WorkspaceID: task.WorkspaceID,
		TaskID:      task.ID,
		AgentID:     task.AgentID,
		AgentName:   task.AgentName,
		OutputType:  outputType,
		Title:       title,
		Summary:     stringPtr(summary),
		Data:        data,
		Metadata: map[string]any{
			types.TaskOutputMetadataArtifactKey:  blockerMappingArtifactKey(outputType, blocker),
			types.TaskOutputMetadataArtifactKind: normalizeToken(outputType),
			types.TaskOutputMetadataArtifactRole: types.TaskOutputArtifactRolePrimary,
		},
		Status:    types.TaskOutputStatusPending,
		CreatedAt: createdAt,
	}
	output.SetBlocking(types.TaskOutputBlockingMetadata{
		Kind:            blocker.Kind,
		InputKind:       types.InputKind(blocker.InputKind),
		WaitGroupID:     blocker.WaitGroupID,
		ApprovalSurface: blocker.ApprovalSurface,
	})
	return output
}

func firstNonEmptyMappingString(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

func blockerMappingOutputID(task *types.AgentTask, blocker *types.ResolvedBlocker) string {
	seed := firstNonEmptyMappingString(
		strings.TrimSpace(blocker.ID),
		strings.TrimSpace(blocker.WaitGroupID),
		strings.TrimSpace(task.ID),
	)
	return "blocker:" + normalizeToken(seed)
}

func blockerMappingTaskLabel(task *types.AgentTask) string {
	if task == nil {
		return ""
	}
	return strings.TrimSpace(firstNonEmptyMappingString(
		toString(dotGet(task.PayloadJSON, "label")),
		toString(dotGet(task.PayloadJSON, "original_message")),
		toString(dotGet(task.PayloadJSON, "message")),
	))
}

func blockerMappingTitle(task *types.AgentTask, blocker *types.ResolvedBlocker) string {
	title := strings.TrimSpace(firstNonEmptyMappingString(
		blocker.Summary,
		blockerMappingTaskLabel(task),
	))
	if title != "" {
		return title
	}
	if blocker != nil && blocker.ApprovalSurface {
		return "Needs approval"
	}
	return "Needs input"
}

func blockerMappingChannel(task *types.AgentTask) string {
	if task == nil {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(firstNonEmptyMappingString(
		toString(dotGet(task.RoutingJSON, "channel")),
		toString(dotGet(task.PayloadJSON, "channel")),
	)))
}

func blockerMappingEmailFields(blocker *types.ResolvedBlocker) (string, string) {
	if blocker == nil {
		return "", ""
	}
	recipient := strings.TrimSpace(firstNonEmptyMappingString(
		toString(dotGet(blocker.PayloadJSON, "recipient")),
		toString(dotGet(blocker.PayloadJSON, "to")),
	))
	subject := strings.TrimSpace(toString(dotGet(blocker.PayloadJSON, "subject")))
	if recipient != "" && subject != "" {
		return recipient, subject
	}
	if details := strings.TrimSpace(blocker.Details); details != "" {
		if recipient == "" {
			recipient = blockerDetailsField(details, "To")
		}
		if subject == "" {
			subject = blockerDetailsField(details, "Subject")
		}
	}
	return recipient, subject
}

func blockerDetailsField(details, field string) string {
	prefix := strings.ToLower(strings.TrimSpace(field)) + ":"
	for _, line := range strings.Split(details, "\n") {
		cleaned := strings.TrimSpace(strings.ReplaceAll(line, "*", ""))
		lower := strings.ToLower(cleaned)
		if !strings.HasPrefix(lower, prefix) {
			continue
		}
		return strings.TrimSpace(cleaned[len(prefix):])
	}
	return ""
}

func blockerMappingOutputType(channel, details, recipient, subject string) string {
	if strings.EqualFold(strings.TrimSpace(channel), "email") {
		return types.TaskOutputTypeEmail
	}
	details = strings.ToLower(strings.TrimSpace(details))
	if recipient != "" || subject != "" || strings.Contains(details, "to:") || strings.Contains(details, "subject:") {
		return types.TaskOutputTypeEmail
	}
	return "text"
}

func blockerMappingArtifactKey(outputType string, blocker *types.ResolvedBlocker) string {
	outputType = normalizeToken(outputType)
	switch {
	case blocker != nil && blocker.ApprovalSurface && outputType != "":
		return "approval-" + outputType
	case blocker != nil && blocker.ApprovalSurface:
		return "approval-request"
	case outputType != "":
		return "blocked-" + outputType
	case blocker != nil && strings.TrimSpace(blocker.InputKind) != "":
		return normalizeToken(blocker.InputKind) + "-request"
	default:
		return "blocked-task"
	}
}

func stringPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func filterOutputsForDataSource(outputs []*types.TaskOutput, ds *types.DataSource, resolvedAgentIDs []string) []*types.TaskOutput {
	if len(outputs) == 0 || ds == nil {
		return outputs
	}
	agentSet := make(map[string]bool, len(resolvedAgentIDs))
	for _, id := range resolvedAgentIDs {
		agentSet[id] = true
	}

	outputTypeFallback := dataSourceOutputTypeFallback(ds)
	filtered := make([]*types.TaskOutput, 0, len(outputs))
	for _, output := range outputs {
		if output == nil {
			continue
		}
		if len(agentSet) > 0 && (output.AgentID == nil || !agentSet[strings.TrimSpace(*output.AgentID)]) {
			continue
		}
		if outputTypeFallback != "" && !strings.EqualFold(strings.TrimSpace(output.OutputType), outputTypeFallback) {
			continue
		}
		if len(ds.Statuses) > 0 {
			match := false
			for _, s := range ds.Statuses {
				if strings.EqualFold(strings.TrimSpace(s), strings.TrimSpace(output.Status)) {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		filtered = append(filtered, output)
	}
	if ds.TimeRange != "" {
		filtered = filterOutputsByTimeRange(filtered, ds.TimeRange)
	}
	return filtered
}

// ---------------------------------------------------------------------------
// Component assembly
// ---------------------------------------------------------------------------

func assembleTable(sheetID string, comp types.ComponentSpec, mappedRows []resolvedSheetRow, taskMeta map[string]*types.AgentTask) *types.ResolvedData {
	tableCols := buildColumnSchemas(comp)
	if len(tableCols) == 0 {
		tableCols = discoverColumnsFromRows(mappedRows)
	}
	if len(tableCols) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}
	}

	hiddenStart := len(tableCols)
	colNames := make([]string, hiddenStart+len(hiddenResolvedColumns))
	meta := make([]types.ColumnMeta, len(colNames))
	for i, col := range tableCols {
		colNames[i] = col.Key
		meta[i] = types.ColumnMeta{Key: col.Key, Label: stripHint(col.Description), Type: normalizeColumnType(col.Type)}
	}
	for i, hidden := range hiddenResolvedColumns {
		colNames[hiddenStart+i] = hidden.Key
		meta[hiddenStart+i] = types.ColumnMeta{Key: hidden.Key, Type: "text", Hidden: true}
	}

	var rows [][]any
	for _, mapped := range mappedRows {
		row := make([]any, len(colNames))
		hasValue := false
		for i, col := range tableCols {
			if v, ok := mapped.Cells[col.Key]; ok && v != "" {
				row[i] = normalizeStatusCellValue(col.Type, v)
				hasValue = true
				continue
			}
			if task, ok := taskMeta[mapped.TaskID]; ok {
				if v, ok := taskMetadataValue(task, col.Key); ok && v != "" {
					row[i] = normalizeStatusCellValue(col.Type, v)
					hasValue = true
				}
			}
		}
		for i, hidden := range hiddenResolvedColumns {
			row[hiddenStart+i] = hidden.Value(sheetID, mapped)
		}
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

// discoverColumnsFromRows derives column schemas from the cell keys present
// in the resolved rows. Used as a fallback when no schema is pre-defined,
// so import data is always visible regardless of LLM state.
func discoverColumnsFromRows(rows []resolvedSheetRow) []bamltypes.ColumnSchema {
	if len(rows) == 0 {
		return nil
	}

	seen := make(map[string]bool)
	var keys []string
	for _, row := range rows {
		for key := range row.Cells {
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	schemas := make([]bamltypes.ColumnSchema, 0, len(keys))
	for _, key := range keys {
		label := humanizeColumn(key)
		schemas = append(schemas, bamltypes.ColumnSchema{
			Name:        label,
			Key:         key,
			Type:        columnTypeForKey(key, ""),
			Description: label,
		})
	}
	return schemas
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

func (r *DataResolver) fetchBoundTaskContext(ctx context.Context, workspaceID uint, rows []resolvedSheetRow) boundDetailContext {
	if r == nil || r.backend == nil || len(rows) == 0 {
		return boundDetailContext{}
	}
	sourceOutputIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		sourceOutputIDs = append(sourceOutputIDs, uniqueTrimmedStrings(strings.Split(row.SourceOutputIDs, ","))...)
	}
	sourceOutputIDs = uniqueTrimmedStrings(sourceOutputIDs)
	if len(sourceOutputIDs) == 0 {
		return boundDetailContext{}
	}
	bound, err := fetchBoundDetailContext(ctx, r.backend, workspaceID, sourceOutputIDs)
	if err != nil {
		return boundDetailContext{}
	}
	return bound
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
		if row.Marker {
			continue
		}
		cells := copyStringMap(row.Cells)
		if applyManual {
			cells = copyStringMap(row.MergedCells())
		}
		result = append(result, resolvedSheetRow{
			TaskID:          row.TaskID,
			DetailTaskID:    row.TaskID,
			RowID:           row.ID,
			StableRef:       row.StableRef,
			RowKey:          row.RowKey,
			OutputID:        firstSourceOutputID(row.SourceOutputIDs),
			SourceOutputIDs: strings.Join(row.SourceOutputIDs, ","),
			Source:          row.Source,
			Cells:           cells,
		})
	}
	return result
}

func mappedRowToViewRow(sheetID, componentID, taskID, schemaH, outputSignature string, groupOutputs []*types.TaskOutput, row bamltypes.MappedRow, now time.Time) ViewRow {
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
		Marker:          false,
		GroupID:         taskID,
		TaskID:          taskID,
		RowKey:          rowKey,
		SchemaHash:      schemaH,
		OutputIDs:       outputIDs,
		OutputSignature: outputSignature,
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

func fallbackViewRow(sheetID, componentID, taskID, schemaH, outputSignature string, groupOutputs []*types.TaskOutput, now time.Time) ViewRow {
	return ViewRow{
		ID:              stableRowID(sheetID, componentID, taskID, "task"),
		SheetID:         sheetID,
		ComponentID:     componentID,
		Marker:          true,
		GroupID:         taskID,
		TaskID:          taskID,
		RowKey:          "task",
		SchemaHash:      schemaH,
		OutputIDs:       sortedOutputIDs(groupOutputs),
		OutputSignature: outputSignature,
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

func enrichRowsWithOutputState(
	rows []resolvedSheetRow,
	outputs []*types.TaskOutput,
	boundContext boundDetailContext,
	taskMeta map[string]*types.AgentTask,
) {
	outputsByTask := groupOutputsByTask(outputs)
	for i := range rows {
		sourceOutputIDs := uniqueTrimmedStrings(strings.Split(rows[i].SourceOutputIDs, ","))
		context := buildRowDetailContext(rowDetailContextInput{
			ParentTaskID:    rows[i].TaskID,
			ParentTask:      taskMeta[rows[i].TaskID],
			ParentOutputs:   outputsByTask[rows[i].TaskID],
			SourceOutputIDs: sourceOutputIDs,
			Bound:           boundContext,
		})
		if context.DetailTaskID != "" {
			rows[i].DetailTaskID = context.DetailTaskID
		} else if rows[i].DetailTaskID == "" {
			rows[i].DetailTaskID = rows[i].TaskID
		}
		task := context.Task
		if task != nil && strings.TrimSpace(task.ID) != "" {
			taskMeta[task.ID] = task
		}
		if task == nil {
			task = taskMeta[rows[i].DetailTaskID]
		}
		if task == nil {
			task = taskMeta[rows[i].TaskID]
		}
		blocker := viewprojection.ProjectBlocker(task, context.Outputs)
		if blocker != nil && blocker.OutputID != "" {
			rows[i].OutputID = blocker.OutputID
			rows[i].OutputStatus = blocker.OutputStatus
		} else {
			rows[i].OutputID = ""
			rows[i].OutputStatus = ""
		}
		if blocker != nil && len(blocker.OutputIDs) > 0 {
			rows[i].BlockerOutputIDs = strings.Join(blocker.OutputIDs, ",")
		} else {
			rows[i].BlockerOutputIDs = ""
		}
		if blocker != nil {
			rows[i].BlockerKind = blocker.Kind
			rows[i].BlockerInputKind = blocker.InputKind
			rows[i].BlockerWaitGroupID = blocker.WaitGroupID
		} else {
			rows[i].BlockerKind = ""
			rows[i].BlockerInputKind = ""
			rows[i].BlockerWaitGroupID = ""
		}
	}
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

func (r *DataResolver) loadStoredRowsAndExclusions(
	ctx context.Context,
	viewID string,
	sheetID string,
	componentID string,
) ([]ViewRow, []ExcludedRowSnapshot) {
	if r == nil || r.store == nil {
		return nil, nil
	}

	rows, err := r.store.GetRows(ctx, viewID, sheetID, componentID)
	if err != nil {
		log.Warn().
			Err(err).
			Str("view_id", viewID).
			Str("sheet_id", sheetID).
			Str("component_id", componentID).
			Msg("failed to load stored rows, treating all as uncached")
		rows = nil
	}

	excludedSnapshots, err := r.store.GetExcludedRows(ctx, viewID, sheetID)
	if err != nil {
		log.Warn().
			Err(err).
			Str("view_id", viewID).
			Str("sheet_id", sheetID).
			Str("component_id", componentID).
			Msg("failed to load excluded rows")
		excludedSnapshots = nil
	}
	excludedSnapshots = filterExcludedSnapshots(excludedSnapshots, componentID)
	rows = filterStoredRowsByExclusions(rows, excludedSnapshots, componentID)
	return rows, excludedSnapshots
}

func filterExcludedSnapshots(snapshots []ExcludedRowSnapshot, componentID string) []ExcludedRowSnapshot {
	if len(snapshots) == 0 {
		return nil
	}
	componentID = strings.TrimSpace(componentID)
	filtered := make([]ExcludedRowSnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		snapshotComponentID := strings.TrimSpace(snapshot.ComponentID)
		if snapshotComponentID != "" && componentID != "" && snapshotComponentID != componentID {
			continue
		}
		filtered = append(filtered, snapshot)
	}
	return filtered
}

func filterStoredRowsByExclusions(rows []ViewRow, snapshots []ExcludedRowSnapshot, componentID string) []ViewRow {
	if len(rows) == 0 || len(snapshots) == 0 {
		return rows
	}
	filtered := make([]ViewRow, 0, len(rows))
	for _, row := range rows {
		if row.Marker {
			// Marker rows are the persisted cache entry for "this task currently
			// has no visible rows". Keep them even when the corresponding visible
			// row is excluded, otherwise the task remaps on every page load.
			filtered = append(filtered, row)
			continue
		}
		if rowMatchesExcludedSnapshot(snapshots, componentID, row.TaskID, row.RowKey, row.SourceOutputIDs, row.MergedCells()) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func filterMappedRowsByExclusions(rows []bamltypes.MappedRow, snapshots []ExcludedRowSnapshot, componentID string) []bamltypes.MappedRow {
	if len(rows) == 0 || len(snapshots) == 0 {
		return rows
	}
	filtered := make([]bamltypes.MappedRow, 0, len(rows))
	for _, row := range rows {
		cells := make(map[string]string, len(row.Cells))
		for _, cell := range row.Cells {
			if strings.TrimSpace(cell.Value) != "" {
				cells[cell.Column] = cell.Value
			}
		}
		if rowMatchesExcludedSnapshot(snapshots, componentID, row.Task_id, row.Row_key, row.Source_output_ids, cells) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func rowMatchesExcludedSnapshot(
	snapshots []ExcludedRowSnapshot,
	componentID,
	taskID,
	rowKey string,
	sourceOutputIDs []string,
	cells map[string]string,
) bool {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" || len(snapshots) == 0 {
		return false
	}
	componentID = strings.TrimSpace(componentID)
	rowKey = normalizeToken(strings.TrimSpace(rowKey))
	normalizedSources := uniqueTrimmedStrings(sourceOutputIDs)
	sort.Strings(normalizedSources)
	cellFingerprint := excludedRowCellsFingerprint(cells)
	for _, snapshot := range snapshots {
		if strings.TrimSpace(snapshot.TaskID) != taskID {
			continue
		}
		snapshotComponentID := strings.TrimSpace(snapshot.ComponentID)
		if snapshotComponentID != "" && componentID != "" && snapshotComponentID != componentID {
			continue
		}
		snapshotSources := uniqueTrimmedStrings(snapshot.SourceOutputIDs)
		sort.Strings(snapshotSources)
		if sourceOutputIDsMatch(normalizedSources, snapshotSources) {
			return true
		}
		snapshotFingerprint := excludedRowCellsFingerprint(snapshot.Cells)
		if len(normalizedSources) > 0 && len(snapshotSources) > 0 {
			// If both sides have concrete source identity and they do not overlap,
			// treat this as a new row instance even if the mapper reused the same row_key.
			continue
		}
		if cellFingerprint != "" && snapshotFingerprint != "" {
			if cellFingerprint == snapshotFingerprint {
				return true
			}
			// When both sides have meaningful cell identity and it changed, do not
			// let a recycled row_key suppress the fresh row.
			continue
		}
		snapshotRowKey := normalizeToken(strings.TrimSpace(snapshot.RowKey))
		if rowKey != "" && snapshotRowKey != "" && snapshotRowKey == rowKey {
			return true
		}
	}
	return false
}

func sourceOutputIDsMatch(left, right []string) bool {
	left = uniqueTrimmedStrings(left)
	right = uniqueTrimmedStrings(right)
	if len(left) == 0 || len(right) == 0 {
		return false
	}
	if slicesMatch(left, right) {
		return true
	}
	rightSet := make(map[string]struct{}, len(right))
	for _, id := range right {
		rightSet[id] = struct{}{}
	}
	for _, id := range left {
		if _, ok := rightSet[id]; ok {
			return true
		}
	}
	return false
}

func excludedRowCellsFingerprint(cells map[string]string) string {
	if len(cells) == 0 {
		return ""
	}
	keys := make([]string, 0, len(cells))
	normalizedValues := make(map[string]string, len(cells))
	for key, value := range cells {
		normalized := normalizeExcludedCellValue(value)
		if normalized == "" {
			continue
		}
		keys = append(keys, key)
		normalizedValues[key] = normalized
	}
	if len(keys) == 0 {
		return ""
	}
	sort.Strings(keys)
	var sb strings.Builder
	for _, key := range keys {
		sb.WriteString(key)
		sb.WriteByte('=')
		sb.WriteString(normalizedValues[key])
		sb.WriteByte('\n')
	}
	return sb.String()
}

func normalizeExcludedCellValue(value string) string {
	value = html.UnescapeString(value)
	value = whitespaceRe.ReplaceAllString(strings.TrimSpace(strings.ToLower(value)), " ")
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
const mappingVersion = "v7"

func hashColumns(columns []bamltypes.ColumnSchema, sheetName, tableTitle, tableType string) string {
	type hashEntry struct {
		Name        string `json:"n"`
		Key         string `json:"k"`
		Type        string `json:"t"`
		Description string `json:"d"`
	}
	payload := struct {
		Version   string      `json:"v"`
		Sheet     string      `json:"s"`
		Title     string      `json:"t"`
		TableType string      `json:"tt"`
		Columns   []hashEntry `json:"c"`
	}{
		Version:   mappingVersion,
		Sheet:     sheetName,
		Title:     strings.TrimSpace(tableTitle),
		TableType: strings.TrimSpace(tableType),
		Columns:   make([]hashEntry, len(columns)),
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

func normalizeStatusCellValue(colType string, v any) any {
	if colType != "status" {
		return v
	}
	if s, ok := v.(string); ok {
		return strings.ToLower(strings.TrimSpace(s))
	}
	return v
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
