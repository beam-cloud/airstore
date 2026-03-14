package views

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
	"github.com/rs/zerolog/log"
)

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

func (c *mappingCache) cacheKey(outputIDs []string, schemaHash string) string {
	sorted := make([]string, len(outputIDs))
	copy(sorted, outputIDs)
	sort.Strings(sorted)

	h := sha256.New()
	for _, id := range sorted {
		h.Write([]byte(id))
		h.Write([]byte{0})
	}
	h.Write([]byte(schemaHash))
	return mappingCachePrefix + hex.EncodeToString(h.Sum(nil))[:24]
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
	if json.Unmarshal(raw, &cached) != nil {
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
		return
	}
	c.rdb.Set(ctx, key, raw, mappingCacheTTL)
}

type cachedMapping struct {
	Columns    []string         `json:"columns"`
	ColumnMeta []types.ColumnMeta `json:"column_meta"`
	Rows       [][]any          `json:"rows"`
	CachedAt   time.Time        `json:"cached_at"`
}

// mapOutputsToSchema calls the BAML MapOutputsToSchema function and returns
// ResolvedData. It serializes outputs into a compact JSON representation for
// the LLM, then parses the mapped rows back into the tabular format.
func mapOutputsToSchema(
	ctx context.Context,
	comp types.ComponentSpec,
	outputs []*types.TaskOutput,
) (*types.ResolvedData, error) {
	columns := buildColumnSchemas(comp)
	if len(columns) == 0 {
		return fallbackResolve(comp, outputs)
	}

	outputsJSON, err := serializeOutputsForMapping(outputs)
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
		log.Warn().Err(err).Str("component", comp.ID).Msg("BAML MapOutputsToSchema failed, falling back to rule-based mapping")
		return fallbackResolve(comp, outputs)
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

func serializeOutputsForMapping(outputs []*types.TaskOutput) (string, error) {
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

	compact := make([]compactOutput, 0, len(outputs))
	for _, o := range outputs {
		if o == nil {
			continue
		}
		filteredMeta := filterMetadataForMapping(o.Metadata)
		compact = append(compact, compactOutput{
			ID:         o.ID,
			Title:      o.Title,
			OutputType: o.OutputType,
			Summary:    o.Summary,
			URI:        o.URI,
			Data:       filterDataForMapping(o.Data),
			Metadata:   filteredMeta,
			CreatedAt:  o.CreatedAt.Format(time.RFC3339),
		})
	}

	raw, err := json.Marshal(compact)
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

func filterMetadataForMapping(metadata map[string]any) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	keep := map[string]bool{
		"artifact_key":   true,
		"artifact_label": true,
		"artifact_kind":  true,
		"artifact_role":  true,
		"tags":           true,
		"deeplink":       true,
	}
	filtered := make(map[string]any)
	for k, v := range metadata {
		if keep[k] {
			filtered[k] = v
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

func convertMappedResult(result bamltypes.MappedResult, columns []bamltypes.ColumnSchema) (*types.ResolvedData, error) {
	colNames := make([]string, len(columns)+2)
	for i, col := range columns {
		colNames[i] = col.Key
	}
	colNames[len(columns)] = "task_id"
	colNames[len(columns)+1] = "output_id"

	colIndex := make(map[string]int, len(columns))
	for i, col := range columns {
		colIndex[col.Key] = i
	}

	meta := make([]types.ColumnMeta, len(colNames))
	for i, col := range columns {
		meta[i] = types.ColumnMeta{
			Key:   col.Key,
			Label: col.Description,
			Type:  normalizeColumnType(col.Type),
		}
	}
	meta[len(columns)] = types.ColumnMeta{Key: "task_id", Type: "text", Hidden: true}
	meta[len(columns)+1] = types.ColumnMeta{Key: "output_id", Type: "text", Hidden: true}

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
		row[len(columns)] = ""
		row[len(columns)+1] = mappedRow.Output_id
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

// fallbackResolve uses the old rule-based transform logic when BAML
// mapping fails or when no column schemas are defined.
func fallbackResolve(comp types.ComponentSpec, outputs []*types.TaskOutput) (*types.ResolvedData, error) {
	rules := resolveRules(comp, outputs)
	result := applyTransform(outputs, rules)
	result.ColumnMeta = buildColumnMeta(result.Columns, rules, comp.Config)
	result.Status = types.ResolvedDataStatusOK
	return result, nil
}

func schemaHash(comp types.ComponentSpec) string {
	payload := struct {
		Transform []types.TransformRule `json:"t,omitempty"`
		Config    map[string]any       `json:"c,omitempty"`
	}{
		Config: comp.Config,
	}
	if comp.DataSource != nil {
		payload.Transform = comp.DataSource.Transform
	}
	raw, _ := json.Marshal(payload)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])[:16]
}
