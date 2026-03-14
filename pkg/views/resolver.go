package views

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

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
}

// Resolve fetches task outputs and maps them to the component's column schema
// using BAML. Results are cached in Redis keyed by (output IDs + schema hash).
func (r *DataResolver) Resolve(ctx context.Context, workspaceID uint, comp types.ComponentSpec) (*types.ResolvedData, error) {
	ds := comp.DataSource
	if ds == nil {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusOK}, nil
	}

	outputs, err := r.fetchOutputs(ctx, workspaceID, ds)
	if err != nil {
		return nil, fmt.Errorf("fetch outputs: %w", err)
	}
	if len(outputs) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}

	if ds.ArtifactKey != "" {
		filtered := filterOutputsByArtifactKey(outputs, ds.ArtifactKey)
		if len(filtered) == 0 {
			return &types.ResolvedData{
				Columns: []string{}, Rows: [][]any{},
				Status:      types.ResolvedDataStatusBindingError,
				Error:       "No outputs match this component's artifact_key",
				Diagnostics: map[string]any{"artifact_key": ds.ArtifactKey},
			}, nil
		}
		outputs = filtered
	}

	outputs = filterOutputsByTimeRange(outputs, ds.TimeRange)
	if len(outputs) == 0 {
		return &types.ResolvedData{Columns: []string{}, Rows: [][]any{}, Status: types.ResolvedDataStatusEmpty}, nil
	}

	outputIDs := make([]string, len(outputs))
	for i, o := range outputs {
		outputIDs[i] = o.ID
	}
	sh := schemaHash(comp)
	cacheKey := r.cache.cacheKey(outputIDs, sh)

	if cached, ok := r.cache.get(ctx, cacheKey); ok {
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

	result, err := mapOutputsToSchema(ctx, comp, outputs)
	if err != nil {
		return nil, fmt.Errorf("map outputs to schema: %w", err)
	}

	r.cache.set(ctx, cacheKey, &cachedMapping{
		Columns:    result.Columns,
		ColumnMeta: result.ColumnMeta,
		Rows:       result.Rows,
		CachedAt:   time.Now(),
	})

	return result, nil
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
	return deduped, nil
}

// ---------------------------------------------------------------------------
// Rule resolution
// ---------------------------------------------------------------------------

func resolveRules(comp types.ComponentSpec, outputs []*types.TaskOutput) []types.TransformRule {
	if comp.DataSource != nil && len(comp.DataSource.Transform) > 0 {
		return comp.DataSource.Transform
	}
	return inferRules(outputs)
}

func inferRules(outputs []*types.TaskOutput) []types.TransformRule {
	rules := []types.TransformRule{{Column: "title", Source: "title", Type: "text"}}
	seen := map[string]struct{}{"title": {}, "created_at": {}}

	for _, o := range outputs {
		if o == nil {
			continue
		}
		for _, key := range sortedMapKeys(o.Data) {
			if _, skip := seen[key]; skip || isExcludedDataKey(key) {
				continue
			}
			seen[key] = struct{}{}
			rules = append(rules, types.TransformRule{
				Column: key,
				Source:  "data." + key,
				Type:    inferTypeFromKey(key),
			})
			if len(rules) >= 6 {
				rules = append(rules, types.TransformRule{Column: "created_at", Source: "created_at", Type: "date"})
				return rules
			}
		}
	}

	rules = append(rules, types.TransformRule{Column: "created_at", Source: "created_at", Type: "date"})
	return rules
}

// ---------------------------------------------------------------------------
// Transform application
// ---------------------------------------------------------------------------

func applyTransform(outputs []*types.TaskOutput, rules []types.TransformRule) *types.ResolvedData {
	columns := make([]string, len(rules)+2)
	for i, rule := range rules {
		columns[i] = rule.Column
	}
	columns[len(rules)] = "task_id"
	columns[len(rules)+1] = "output_id"

	rows := make([][]any, 0, len(outputs))
	for _, o := range outputs {
		row := make([]any, len(rules)+2)
		for i, rule := range rules {
			row[i] = extractField(o, rule)
		}
		row[len(rules)] = o.TaskID
		row[len(rules)+1] = o.ID
		rows = append(rows, row)
	}
	return &types.ResolvedData{Columns: columns, Rows: rows, Total: len(rows)}
}

func extractField(o *types.TaskOutput, rule types.TransformRule) any {
	for _, src := range strings.Split(rule.Source, "|") {
		if val := resolveSource(o, strings.TrimSpace(src)); val != nil && val != "" {
			if rule.Extract != "" {
				val = applyExtract(fmt.Sprintf("%v", val), rule.Extract)
			}
			return val
		}
	}
	return nil
}

func resolveSource(o *types.TaskOutput, source string) any {
	switch source {
	case "title":
		return o.Title
	case "artifact_key":
		return ArtifactOf(o).Key()
	case "artifact_label":
		return ArtifactOf(o).Label()
	case "artifact_kind":
		return ArtifactOf(o).Kind()
	case "output_type":
		return o.OutputType
	case "uri":
		if o.URI != nil {
			return *o.URI
		}
		return nil
	case "summary":
		if o.Summary != nil {
			return *o.Summary
		}
		return nil
	case "created_at":
		return o.CreatedAt.Format(time.RFC3339)
	case "task_id":
		return o.TaskID
	case "agent_id":
		if o.AgentID != nil {
			return *o.AgentID
		}
		return nil
	case "agent_name":
		return o.AgentName
	}
	if strings.HasPrefix(source, "data.") {
		return dotGet(o.Data, strings.TrimPrefix(source, "data."))
	}
	if strings.HasPrefix(source, "metadata.") {
		return dotGet(o.Metadata, strings.TrimPrefix(source, "metadata."))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

func filterOutputsByArtifactKey(outputs []*types.TaskOutput, artifactKey string) []*types.TaskOutput {
	if len(outputs) == 0 || strings.TrimSpace(artifactKey) == "" {
		return outputs
	}
	filtered := make([]*types.TaskOutput, 0, len(outputs))
	for _, o := range outputs {
		if o != nil && ArtifactOf(o).MatchesKey(artifactKey) {
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
// Dot-path navigation
// ---------------------------------------------------------------------------

func dotGet(m map[string]any, path string) any {
	if m == nil {
		return nil
	}
	return pathGet(m, splitPath(path))
}

func splitPath(path string) []string {
	normalized := strings.ReplaceAll(path, "[]", ".[].")
	normalized = regexp.MustCompile(`\[(\d+|\*)\]`).ReplaceAllString(normalized, `.$1`)
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
	return firstNonEmptyValue(collectArrayValues(items, append([]string{part}, rest...)))
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
			if !isEmptyValue(typed) {
				values = append(values, typed)
			}
		}
	}
	return values
}

func firstNonEmptyValue(values []any) any {
	for _, v := range values {
		if !isEmptyValue(v) {
			return v
		}
	}
	return nil
}

func isEmptyValue(value any) bool {
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
// Extract / column meta
// ---------------------------------------------------------------------------

func applyExtract(val, pattern string) any {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return val
	}
	matches := re.FindStringSubmatch(val)
	if len(matches) > 1 {
		return matches[1]
	}
	if len(matches) == 1 {
		return matches[0]
	}
	return val
}

func buildColumnMeta(columns []string, rules []types.TransformRule, config map[string]any) []ColumnMeta {
	hiddenCols := map[string]bool{"task_id": true, "output_id": true}
	ruleByCol := make(map[string]types.TransformRule, len(rules))
	for _, r := range rules {
		ruleByCol[r.Column] = r
	}

	configCols := parseConfigColumns(config)
	configByKey := make(map[string]configColumn, len(configCols))
	for _, cc := range configCols {
		configByKey[cc.Key] = cc
	}

	meta := make([]ColumnMeta, 0, len(columns))
	for _, col := range columns {
		cm := ColumnMeta{
			Key:    col,
			Label:  humanizeColumn(col),
			Type:   "text",
			Hidden: hiddenCols[col],
		}
		if rule, ok := ruleByCol[col]; ok {
			if rule.Type != "" {
				cm.Type = normalizeColumnType(rule.Type)
			}
			if rule.Format != "" {
				cm.Format = rule.Format
			}
		}
		if cc, ok := configByKey[col]; ok {
			if cc.Label != "" {
				cm.Label = cc.Label
			}
			if cc.Type != "" {
				cm.Type = normalizeColumnType(cc.Type)
			}
			if cc.Format != "" {
				cm.Format = cc.Format
			}
			cm.Frozen = cc.Frozen
			if len(cc.Options) > 0 {
				cm.Options = cc.Options
			}
		}
		if col == "created_at" && cm.Type == "text" {
			cm.Type = "date"
		}
		meta = append(meta, cm)
	}
	return meta
}

type ColumnMeta = types.ColumnMeta

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
