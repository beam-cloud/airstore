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

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
)

// ---------------------------------------------------------------------------
// DataResolver
// ---------------------------------------------------------------------------

type DataResolver struct {
	backend dataResolverBackend
	cache   *mappingCache
}

func NewDataResolver(backend repository.BackendRepository, rdb *common.RedisClient) *DataResolver {
	return &DataResolver{backend: backend, cache: newMappingCache(rdb)}
}

type dataResolverBackend interface {
	GetAgentProfileByKey(ctx context.Context, workspaceId uint, agentKey string) (*types.AgentProfile, error)
	ListAgentProfiles(ctx context.Context, workspaceId uint) ([]*types.AgentProfile, error)
	ListWorkspaceTaskOutputs(ctx context.Context, workspaceId uint, filter types.TaskOutputListFilter) ([]*types.TaskOutput, error)
	GetTaskByID(ctx context.Context, taskId string) (*types.AgentTask, error)
}

// Resolve maps task outputs to a view component's schema using BAML.
//
// All outputs across every component in the view are fetched and mapped in a
// single BAML call. Results are cached per-task — only tasks whose output set
// has changed (or whose schema hash is stale) are re-mapped. Both tables and
// metrics derive their data from this shared mapped result.
func (r *DataResolver) Resolve(ctx context.Context, workspaceID uint, viewID string, comp types.ComponentSpec, allComponents []types.ComponentSpec) (*types.ResolvedData, error) {
	unifiedCols := buildUnifiedSchema(allComponents)
	if len(unifiedCols) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
	schemaH := hashColumns(unifiedCols)

	allOutputs, err := r.fetchViewOutputs(ctx, workspaceID, allComponents)
	if err != nil {
		return nil, fmt.Errorf("fetch view outputs: %w", err)
	}
	if len(allOutputs) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}

	taskGroups := groupOutputsByTask(allOutputs)

	uncachedIDs := make(map[string]bool)
	mappedTasks := make(map[string]map[string]string, len(taskGroups))
	for taskID, outputs := range taskGroups {
		taskOIDs := sortedOutputIDs(outputs)
		if cached, ok := r.cache.getTask(ctx, viewID, taskID); ok &&
			cached.SchemaHash == schemaH &&
			slicesMatch(cached.OutputIDs, taskOIDs) {
			mappedTasks[taskID] = cached.Cells
		} else {
			uncachedIDs[taskID] = true
		}
	}

	if len(uncachedIDs) > 0 {
		uncachedTIDs := make([]string, 0, len(uncachedIDs))
		for tid := range uncachedIDs {
			uncachedTIDs = append(uncachedTIDs, tid)
		}
		sort.Strings(uncachedTIDs)

		taskPrompts := r.fetchTaskPrompts(ctx, uncachedTIDs)
		uncachedOutputs := outputsForTasks(allOutputs, uncachedIDs)
		outputsJSON, err := serializeOutputsForMapping(uncachedOutputs, taskPrompts)
		if err != nil {
			return nil, fmt.Errorf("serialize outputs: %w", err)
		}

		result, err := baml.MapOutputsToSchema(ctx, "View Mapping", "data-table", unifiedCols, outputsJSON)
		if err != nil {
			log.Warn().Err(err).Str("view_id", viewID).Int("tasks", len(uncachedIDs)).Msg("BAML mapping failed")
			if len(mappedTasks) == 0 {
				return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty, Error: "Failed to map outputs"}, nil
			}
		} else {
			for _, row := range result.Rows {
				cells := make(map[string]string, len(row.Cells))
				for _, cell := range row.Cells {
					if cell.Value != "" {
						cells[cell.Column] = cell.Value
					}
				}
				taskID := row.Task_id
				if _, ok := taskGroups[taskID]; !ok {
					continue
				}
				mappedTasks[taskID] = cells
				r.cache.setTask(ctx, viewID, taskID, &cachedTaskMapping{
					SchemaHash: schemaH,
					OutputIDs:  sortedOutputIDs(taskGroups[taskID]),
					Cells:      cells,
					CachedAt:   time.Now(),
				})
			}
		}
	}

	resolvedAgents := r.resolveAgentIDsForDS(ctx, workspaceID, comp.DataSource)

	switch {
	case comp.IsTable():
		return assembleTable(comp, mappedTasks, taskGroups, resolvedAgents), nil
	case comp.Type == types.ComponentTypeMetric:
		return assembleMetric(comp, mappedTasks, taskGroups, resolvedAgents), nil
	default:
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}
}

// ---------------------------------------------------------------------------
// View-level helpers
// ---------------------------------------------------------------------------

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

func (r *DataResolver) fetchViewOutputs(ctx context.Context, workspaceID uint, allComponents []types.ComponentSpec) ([]*types.TaskOutput, error) {
	var all []*types.TaskOutput
	seen := make(map[string]bool)
	for _, comp := range allComponents {
		if comp.DataSource == nil {
			continue
		}
		dsKey := fmt.Sprintf("%s:%s", comp.DataSource.AgentID, strings.Join(comp.DataSource.AgentIDs, ","))
		if seen[dsKey] {
			continue
		}
		seen[dsKey] = true
		outputs, err := r.fetchOutputs(ctx, workspaceID, comp.DataSource)
		if err != nil {
			return nil, err
		}
		all = append(all, outputs...)
	}
	return dedupeOutputs(all), nil
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

func (r *DataResolver) resolveAgentIDsForDS(ctx context.Context, workspaceID uint, ds *types.DataSource) []string {
	if ds == nil {
		return nil
	}
	refs := ds.AgentIDs
	if ds.AgentID != "" && len(refs) == 0 {
		refs = []string{ds.AgentID}
	}
	var ids []string
	for _, ref := range refs {
		if aid, ok := r.resolveAgentRef(ctx, workspaceID, ref); ok {
			ids = append(ids, aid)
		}
	}
	return ids
}

// ---------------------------------------------------------------------------
// Component assembly
// ---------------------------------------------------------------------------

func assembleTable(comp types.ComponentSpec, mappedTasks map[string]map[string]string, taskGroups map[string][]*types.TaskOutput, resolvedAgentIDs []string) *types.ResolvedData {
	tableCols := buildColumnSchemas(comp)
	if len(tableCols) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}
	}

	colNames := make([]string, len(tableCols)+1)
	for i, col := range tableCols {
		colNames[i] = col.Key
	}
	colNames[len(tableCols)] = "task_id"

	meta := make([]types.ColumnMeta, len(colNames))
	for i, col := range tableCols {
		meta[i] = types.ColumnMeta{
			Key:   col.Key,
			Label: stripHint(col.Description),
			Type:  normalizeColumnType(col.Type),
		}
	}
	meta[len(tableCols)] = types.ColumnMeta{Key: "task_id", Type: "text", Hidden: true}

	var rows [][]any
	for taskID, cells := range mappedTasks {
		outputs := taskGroups[taskID]
		if !taskMatchesDataSource(outputs, comp.DataSource, resolvedAgentIDs) {
			continue
		}
		row := make([]any, len(colNames))
		hasValue := false
		for i, col := range tableCols {
			if v, ok := cells[col.Key]; ok && v != "" {
				row[i] = v
				hasValue = true
			}
		}
		row[len(tableCols)] = taskID
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

func assembleMetric(comp types.ComponentSpec, mappedTasks map[string]map[string]string, taskGroups map[string][]*types.TaskOutput, resolvedAgentIDs []string) *types.ResolvedData {
	count := 0
	for taskID := range mappedTasks {
		outputs := taskGroups[taskID]
		if taskMatchesDataSource(outputs, comp.DataSource, resolvedAgentIDs) {
			count++
		}
	}

	if count == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Total: 0, Status: types.ResolvedDataStatusEmpty}
	}

	aggregate := "count"
	if comp.Config != nil {
		if a, ok := comp.Config["aggregate"].(string); ok && a != "" {
			aggregate = a
		}
	}

	switch aggregate {
	case "latest":
		var latest *types.TaskOutput
		for taskID := range mappedTasks {
			for _, o := range taskGroups[taskID] {
				if latest == nil || o.CreatedAt.After(latest.CreatedAt) {
					latest = o
				}
			}
		}
		val := ""
		if latest != nil {
			val = latest.Title
			if latest.Summary != nil && *latest.Summary != "" {
				val = *latest.Summary
			}
		}
		return &types.ResolvedData{
			Columns: []string{"value"},
			Rows:    [][]any{{val}},
			Total:   count,
			Status:  types.ResolvedDataStatusOK,
		}
	default:
		return &types.ResolvedData{
			Columns: []string{},
			Rows:    [][]any{},
			Total:   count,
			Status:  types.ResolvedDataStatusOK,
		}
	}
}

func taskMatchesDataSource(outputs []*types.TaskOutput, ds *types.DataSource, resolvedAgentIDs []string) bool {
	if ds == nil || len(outputs) == 0 {
		return true
	}
	if len(resolvedAgentIDs) > 0 {
		agentSet := make(map[string]bool, len(resolvedAgentIDs))
		for _, id := range resolvedAgentIDs {
			agentSet[id] = true
		}
		match := false
		for _, o := range outputs {
			if o.AgentID != nil && agentSet[*o.AgentID] {
				match = true
				break
			}
		}
		if !match {
			return false
		}
	}
	if ds.TimeRange != "" {
		if len(filterOutputsByTimeRange(outputs, ds.TimeRange)) == 0 {
			return false
		}
	}
	return true
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

func (r *DataResolver) fetchOutputs(ctx context.Context, workspaceID uint, ds *types.DataSource) ([]*types.TaskOutput, error) {
	agentIDs := ds.AgentIDs
	if ds.AgentID != "" && len(agentIDs) == 0 {
		agentIDs = []string{ds.AgentID}
	}

	if len(agentIDs) == 0 {
		filter := types.TaskOutputListFilter{ExcludeArchived: false, Limit: 200}
		if ds.OutputType != "" {
			filter.OutputType = &ds.OutputType
		}
		return r.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, filter)
	}

	var all []*types.TaskOutput
	for _, ref := range agentIDs {
		aid, ok := r.resolveAgentRef(ctx, workspaceID, ref)
		if !ok {
			continue
		}
		filter := types.TaskOutputListFilter{AgentID: &aid, ExcludeArchived: false, Limit: 200}
		if ds.OutputType != "" {
			filter.OutputType = &ds.OutputType
		}
		outputs, err := r.backend.ListWorkspaceTaskOutputs(ctx, workspaceID, filter)
		if err != nil {
			return nil, err
		}
		all = append(all, outputs...)
	}

	return dedupeOutputs(all), nil
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
				Type:        col.Type,
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
			Type:        rule.Type,
			Description: desc,
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
			Summary:    o.Summary,
			URI:        o.URI,
			Data:       filterDataForMapping(o.Data),
			Metadata:   filterMetadataForMapping(o.Metadata),
			CreatedAt:  o.CreatedAt.Format(time.RFC3339),
		})
	}

	raw, err := json.Marshal(grouped)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func filterDataForMapping(data map[string]any) map[string]any {
	if len(data) == 0 {
		return nil
	}
	filtered := make(map[string]any, len(data))
	for k, v := range data {
		if isExcludedDataKey(k) {
			continue
		}
		filtered[k] = v
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

var mappingMetadataKeys = map[string]bool{
	"artifact_key": true, "artifact_label": true, "artifact_kind": true,
	"artifact_role": true, "tags": true, "deeplink": true,
}

func filterMetadataForMapping(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	filtered := make(map[string]any)
	for k, v := range metadata {
		if mappingMetadataKeys[k] {
			filtered[k] = v
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// ---------------------------------------------------------------------------
// Task-level mapping cache
// ---------------------------------------------------------------------------

const (
	taskCacheTTL    = 10 * time.Minute
	taskCachePrefix = "view:task:"
)

type mappingCache struct {
	rdb *common.RedisClient
}

func newMappingCache(rdb *common.RedisClient) *mappingCache {
	return &mappingCache{rdb: rdb}
}

func (c *mappingCache) taskKey(viewID, taskID string) string {
	return fmt.Sprintf("%s%s:%s", taskCachePrefix, viewID, taskID)
}

func (c *mappingCache) getTask(ctx context.Context, viewID, taskID string) (*cachedTaskMapping, bool) {
	if c.rdb == nil {
		return nil, false
	}
	raw, err := c.rdb.Get(ctx, c.taskKey(viewID, taskID)).Bytes()
	if err != nil {
		return nil, false
	}
	var cached cachedTaskMapping
	if err := json.Unmarshal(raw, &cached); err != nil {
		return nil, false
	}
	return &cached, true
}

func (c *mappingCache) setTask(ctx context.Context, viewID, taskID string, value *cachedTaskMapping) {
	if c.rdb == nil {
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return
	}
	c.rdb.Set(ctx, c.taskKey(viewID, taskID), raw, taskCacheTTL)
}

type cachedTaskMapping struct {
	SchemaHash string            `json:"sh"`
	OutputIDs  []string          `json:"oids"`
	Cells      map[string]string `json:"cells"`
	CachedAt   time.Time         `json:"ca"`
}

func hashColumns(columns []bamltypes.ColumnSchema) string {
	raw, _ := json.Marshal(columns)
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
