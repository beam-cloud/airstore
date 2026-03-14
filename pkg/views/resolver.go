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

// Resolve fetches task outputs and maps them to the component's column schema
// using BAML. Results are cached in Redis keyed by view_id:component_id.
func (r *DataResolver) Resolve(ctx context.Context, workspaceID uint, viewID string, comp types.ComponentSpec) (*types.ResolvedData, error) {
	ds := comp.DataSource
	if ds == nil {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusOK}, nil
	}

	outputs, err := r.fetchOutputs(ctx, workspaceID, ds)
	if err != nil {
		return nil, fmt.Errorf("fetch outputs: %w", err)
	}

	outputs = filterOutputsByTimeRange(outputs, ds.TimeRange)

	if len(outputs) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}

	// Metrics are strict counts for a specific agent-produced artifact.
	// No untagged outputs, artifact_key required for meaningful scoping.
	if comp.Type == "metric" {
		if ds.ArtifactKey != "" {
			outputs = filterByArtifactKey(outputs, ds.ArtifactKey)
		}
		return resolveMetric(comp, outputs), nil
	}

	sh := schemaHash(comp)
	key := r.cache.componentKey(viewID, comp.ID)
	oids := sortedOutputIDs(outputs)

	if cached, ok := r.cache.get(ctx, key); ok && cached.SchemaHash == sh && slicesMatch(cached.OutputIDs, oids) {
		cachedAt := cached.CachedAt
		return &types.ResolvedData{
			Columns:    cached.Columns,
			ColumnMeta: cached.ColumnMeta,
			Rows:       cached.Rows,
			Total:      len(cached.Rows),
			CachedAt:   &cachedAt,
			Status:     types.ResolvedDataStatusOK,
		}, nil
	}

	tids := sortedTaskIDs(outputs)
	taskPrompts := r.fetchTaskPrompts(ctx, tids)

	result, err := mapOutputsToSchema(ctx, comp, outputs, taskPrompts)
	if err != nil {
		return nil, fmt.Errorf("map outputs to schema: %w", err)
	}

	r.cache.set(ctx, key, &cachedMapping{
		SchemaHash: sh,
		OutputIDs:  oids,
		Columns:    result.Columns,
		ColumnMeta: result.ColumnMeta,
		Rows:       result.Rows,
		CachedAt:   time.Now(),
	})
	return result, nil
}

// resolveMetric produces a simple result for metric components without BAML.
// The frontend uses data.total for "count", data.rows[0][0] for "latest",
// and sums over rows for "sum".
func resolveMetric(comp types.ComponentSpec, outputs []*types.TaskOutput) *types.ResolvedData {
	if len(outputs) == 0 {
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
		val := outputs[0].Title
		if outputs[0].Summary != nil && *outputs[0].Summary != "" {
			val = *outputs[0].Summary
		}
		return &types.ResolvedData{
			Columns: []string{"value"},
			Rows:    [][]any{{val}},
			Total:   len(outputs),
			Status:  types.ResolvedDataStatusOK,
		}
	default:
		return &types.ResolvedData{
			Columns: []string{},
			Rows:    [][]any{},
			Total:   len(outputs),
			Status:  types.ResolvedDataStatusOK,
		}
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

func filterByArtifactKey(outputs []*types.TaskOutput, key string) []*types.TaskOutput {
	filtered := make([]*types.TaskOutput, 0, len(outputs))
	for _, o := range outputs {
		if o == nil || o.Metadata == nil {
			continue
		}
		if k, _ := o.Metadata[types.TaskOutputMetadataArtifactKey].(string); k == key {
			filtered = append(filtered, o)
		}
	}
	return filtered
}

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

func mapOutputsToSchema(
	ctx context.Context,
	comp types.ComponentSpec,
	outputs []*types.TaskOutput,
	taskPrompts map[string]string,
) (*types.ResolvedData, error) {
	columns := buildColumnSchemas(comp)
	if len(columns) == 0 {
		return &types.ResolvedData{
			Columns: []string{}, Rows: [][]any{},
			Status: types.ResolvedDataStatusEmpty,
			Error:  "No column schema defined for this component",
		}, nil
	}

	outputsJSON, err := serializeOutputsForMapping(outputs, taskPrompts)
	if err != nil {
		return nil, fmt.Errorf("serialize outputs: %w", err)
	}

	artifactKeyFilter := ""
	if comp.DataSource != nil {
		artifactKeyFilter = comp.DataSource.ArtifactKey
	}

	result, err := baml.MapOutputsToSchema(
		ctx,
		comp.Title,
		comp.Type,
		columns,
		outputsJSON,
		artifactKeyFilter,
	)
	if err != nil {
		log.Warn().Err(err).Str("component", comp.ID).Msg("BAML MapOutputsToSchema failed")
		return &types.ResolvedData{
			Columns: []string{}, Rows: [][]any{},
			Status: types.ResolvedDataStatusEmpty,
			Error:  "Failed to map outputs to schema",
		}, nil
	}

	return convertMappedResult(result, columns)
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

func convertMappedResult(result bamltypes.MappedResult, columns []bamltypes.ColumnSchema) (*types.ResolvedData, error) {
	colNames := make([]string, len(columns)+1)
	for i, col := range columns {
		colNames[i] = col.Key
	}
	colNames[len(columns)] = "task_id"

	colIndex := make(map[string]int, len(columns))
	for i, col := range columns {
		colIndex[col.Key] = i
	}

	meta := make([]types.ColumnMeta, len(colNames))
	for i, col := range columns {
		meta[i] = types.ColumnMeta{
			Key:   col.Key,
			Label: stripHint(col.Description),
			Type:  normalizeColumnType(col.Type),
		}
	}
	meta[len(columns)] = types.ColumnMeta{Key: "task_id", Type: "text", Hidden: true}

	rows := make([][]any, 0, len(result.Rows))
	for _, mappedRow := range result.Rows {
		row := make([]any, len(colNames))
		for _, cell := range mappedRow.Cells {
			if idx, ok := colIndex[cell.Column]; ok {
				if cell.Value != "" {
					row[idx] = cell.Value
				}
			}
		}
		row[len(columns)] = mappedRow.Task_id
		rows = append(rows, row)
	}

	return &types.ResolvedData{
		Columns:    colNames,
		ColumnMeta: meta,
		Rows:       rows,
		Total:      len(rows),
		Status:     types.ResolvedDataStatusOK,
	}, nil
}

// ---------------------------------------------------------------------------
// Mapping cache
// ---------------------------------------------------------------------------

const (
	mappingCacheTTL    = 5 * time.Minute
	mappingCachePrefix = "view:mapping:"
)

type mappingCache struct {
	rdb *common.RedisClient
}

func newMappingCache(rdb *common.RedisClient) *mappingCache {
	return &mappingCache{rdb: rdb}
}

func (c *mappingCache) componentKey(viewID, componentID string) string {
	return fmt.Sprintf("%s%s:%s", mappingCachePrefix, viewID, componentID)
}

func (c *mappingCache) get(ctx context.Context, key string) (*cachedMapping, bool) {
	if c.rdb == nil {
		return nil, false
	}
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return nil, false
	}
	var cached cachedMapping
	if err := json.Unmarshal(raw, &cached); err != nil {
		log.Warn().Err(err).Str("key", key).Int("bytes", len(raw)).Msg("view cache: unmarshal failed")
		return nil, false
	}
	return &cached, true
}

func (c *mappingCache) set(ctx context.Context, key string, value *cachedMapping) {
	if c.rdb == nil {
		return
	}
	raw, err := json.Marshal(value)
	if err != nil {
		log.Warn().Err(err).Str("key", key).Msg("view cache: marshal failed")
		return
	}
	if err := c.rdb.Set(ctx, key, raw, mappingCacheTTL).Err(); err != nil {
		log.Warn().Err(err).Str("key", key).Msg("view cache: redis set failed")
	}
}

type cachedMapping struct {
	SchemaHash string             `json:"sh"`
	OutputIDs  []string           `json:"output_ids"`
	Columns    []string           `json:"columns"`
	ColumnMeta []types.ColumnMeta `json:"column_meta"`
	Rows       [][]any            `json:"rows"`
	CachedAt   time.Time          `json:"cached_at"`
}

func schemaHash(comp types.ComponentSpec) string {
	payload := struct {
		Title      string            `json:"n,omitempty"`
		Type       string            `json:"w,omitempty"`
		DataSource *types.DataSource `json:"d,omitempty"`
		Config     map[string]any    `json:"c,omitempty"`
	}{
		Title:      comp.Title,
		Type:       comp.Type,
		DataSource: comp.DataSource,
		Config:     comp.Config,
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

func sortedTaskIDs(outputs []*types.TaskOutput) []string {
	seen := make(map[string]struct{}, len(outputs))
	ids := make([]string, 0, len(outputs))
	for _, o := range outputs {
		if o == nil {
			continue
		}
		if _, ok := seen[o.TaskID]; !ok {
			seen[o.TaskID] = struct{}{}
			ids = append(ids, o.TaskID)
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
