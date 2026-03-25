package views

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	baml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	bamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/net/context"
)

const MaxVisibleColumns = 25

type ImportParams struct {
	Store       *ViewStore
	Backend     repository.BackendRepository
	Data        []byte
	FilePath    string
	ViewID      string
	WorkspaceID uint
	SheetID     string
	ComponentID string
	ColMapping  map[string]string // optional caller override
}

type ImportResult struct {
	ImportID    string   `json:"import_id"`
	RowCount    int      `json:"row_count"`
	ColumnCount int      `json:"column_count"`
	NewColumns  []string `json:"new_columns,omitempty"`
	ParseErrors []string `json:"parse_errors,omitempty"`
}

func ImportData(ctx context.Context, p ImportParams) (*ImportResult, error) {
	if p.Store == nil || !p.Store.Available() {
		return nil, fmt.Errorf("data store not configured")
	}
	if len(p.Data) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	ext := strings.ToLower(filepath.Ext(p.FilePath))
	var records []map[string]string
	var headers []string
	var parseErrors []string

	switch ext {
	case ".json":
		h, recs, errs, err := parseJSON(p.Data)
		if err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
		headers, records, parseErrors = h, recs, errs
	default:
		delimiter := ','
		if ext == ".tsv" || ext == ".tab" {
			delimiter = '\t'
		}
		h, recs, errs, err := parseCSV(p.Data, delimiter)
		if err != nil {
			return nil, fmt.Errorf("parse CSV: %w", err)
		}
		headers, records, parseErrors = h, recs, errs
	}

	if len(records) == 0 {
		return nil, fmt.Errorf("no data rows found")
	}

	colMapping := p.ColMapping
	if len(colMapping) == 0 {
		colMapping = resolveColumnMapping(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, headers)
	}

	newCols, err := syncColumnsToView(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, headers, colMapping)
	if err != nil {
		log.Warn().Err(err).Str("view_id", p.ViewID).Msg("import: failed to sync columns to view definition (data will still be imported)")
	}

	if len(newCols) > 0 {
		stampSchemaHash(ctx, p.Store, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID)
	}

	importID := uuid.New().String()
	rows := buildImportRows(records, colMapping, p.SheetID, p.ComponentID, importID)

	if err := p.Store.UpsertRows(ctx, p.ViewID, rows); err != nil {
		return nil, fmt.Errorf("upsert rows: %w", err)
	}

	log.Info().
		Str("view_id", p.ViewID).
		Str("sheet_id", p.SheetID).
		Str("component_id", p.ComponentID).
		Str("import_id", importID).
		Int("rows", len(rows)).
		Int("new_columns", len(newCols)).
		Msg("import: data imported")

	return &ImportResult{
		ImportID:    importID,
		RowCount:    len(rows),
		ColumnCount: len(colMapping),
		NewColumns:  newCols,
		ParseErrors: parseErrors,
	}, nil
}

func parseCSV(data []byte, delimiter rune) ([]string, []map[string]string, []string, error) {
	reader := csv.NewReader(strings.NewReader(string(data)))
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	reader.Comma = delimiter

	rawHeaders, err := reader.Read()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("read headers: %w", err)
	}
	for i := range rawHeaders {
		rawHeaders[i] = strings.TrimSpace(rawHeaders[i])
	}

	var headers []string
	for _, h := range rawHeaders {
		if h != "" {
			headers = append(headers, h)
		}
	}

	var records []map[string]string
	var parseErrors []string
	rowIndex := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			parseErrors = append(parseErrors, fmt.Sprintf("row %d: %v", rowIndex+1, err))
			rowIndex++
			continue
		}

		row := make(map[string]string, len(headers))
		hasValue := false
		for i, header := range rawHeaders {
			if i >= len(record) || header == "" {
				continue
			}
			val := strings.TrimSpace(record[i])
			if val != "" {
				row[header] = val
				hasValue = true
			}
		}
		if hasValue {
			records = append(records, row)
		}
		rowIndex++
	}

	return headers, records, parseErrors, nil
}

func parseJSON(data []byte) ([]string, []map[string]string, []string, error) {
	var raw []map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		var single map[string]any
		if err2 := json.Unmarshal(data, &single); err2 != nil {
			return nil, nil, nil, fmt.Errorf("expected JSON array or object: %w", err)
		}
		raw = []map[string]any{single}
	}

	if len(raw) == 0 {
		return nil, nil, nil, fmt.Errorf("empty JSON array")
	}

	headerSet := make(map[string]struct{})
	var headerOrder []string
	var records []map[string]string
	var parseErrors []string

	for i, obj := range raw {
		row := make(map[string]string)
		flattenJSON("", obj, row)
		if len(row) == 0 {
			parseErrors = append(parseErrors, fmt.Sprintf("row %d: empty after flattening", i))
			continue
		}
		for k := range row {
			if _, ok := headerSet[k]; !ok {
				headerSet[k] = struct{}{}
				headerOrder = append(headerOrder, k)
			}
		}
		records = append(records, row)
	}

	return headerOrder, records, parseErrors, nil
}

func flattenJSON(prefix string, obj map[string]any, out map[string]string) {
	for k, v := range obj {
		key := k
		if prefix != "" {
			key = prefix + "_" + k
		}
		switch val := v.(type) {
		case map[string]any:
			flattenJSON(key, val, out)
		case []any:
			b, _ := json.Marshal(val)
			out[key] = string(b)
		case nil:
			// skip
		default:
			out[key] = fmt.Sprintf("%v", val)
		}
	}
}

func buildImportRows(records []map[string]string, colMapping map[string]string, sheetID, componentID, importID string) []ViewRow {
	rows := make([]ViewRow, 0, len(records))
	for i, record := range records {
		pinned := make(map[string]string, len(record))
		for header, val := range record {
			if colKey, ok := colMapping[header]; ok {
				pinned[colKey] = val
			}
		}
		if len(pinned) == 0 {
			continue
		}

		rowID := fmt.Sprintf("%s:%s:import-%s:%d", sheetID, componentID, importID, i)
		rows = append(rows, ViewRow{
			ID:          rowID,
			SheetID:     sheetID,
			ComponentID: componentID,
			GroupID:     "import:" + importID,
			RowKey:      fmt.Sprintf("import-%d", i),
			SchemaHash:  "",
			Cells:       map[string]string{},
			Pinned:      pinned,
			Source:      "import",
			ImportID:    importID,
			UpdatedAt:   time.Now(),
		})
	}
	return rows
}

// capColumns returns at most maxVisible keys, preserving the original order
// from the source file. No domain-specific heuristics — the file author's
// column ordering is respected as-is.
func capColumns(keys []string, maxVisible int) []string {
	if len(keys) <= maxVisible {
		return keys
	}
	return keys[:maxVisible]
}

// syncColumnsToView adds any missing column definitions to the view's table
// component config so that the resolver can render imported data. Returns the
// list of newly added column keys.
func syncColumnsToView(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, headers []string, colMapping map[string]string) ([]string, error) {
	if backend == nil {
		return nil, nil
	}

	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return nil, fmt.Errorf("get view: %w", err)
	}

	var targetSheet *types.SheetSpec
	var targetComp *types.ComponentSpec
	for si := range v.Definition.Sheets {
		if v.Definition.Sheets[si].ID == sheetID {
			targetSheet = &v.Definition.Sheets[si]
			for ci := range targetSheet.Components {
				if targetSheet.Components[ci].ID == componentID {
					targetComp = &targetSheet.Components[ci]
					break
				}
			}
			break
		}
	}
	if targetComp == nil {
		return nil, fmt.Errorf("component %s not found in sheet %s", componentID, sheetID)
	}

	existingKeys := existingColumnKeys(targetComp.Config)
	existingCount := len(existingKeys)

	var candidateKeys []string
	labelByKey := make(map[string]string)
	for _, header := range headers {
		key := colMapping[header]
		if key == "" || existingKeys[key] {
			continue
		}
		existingKeys[key] = true
		candidateKeys = append(candidateKeys, key)
		labelByKey[key] = header
	}

	if len(candidateKeys) == 0 {
		return nil, nil
	}

	budget := MaxVisibleColumns - existingCount
	if budget < 0 {
		budget = 0
	}
	visibleKeys := capColumns(candidateKeys, budget)
	visibleSet := make(map[string]bool, len(visibleKeys))
	for _, k := range visibleKeys {
		visibleSet[k] = true
	}

	var newKeys []string
	var newColEntries []map[string]any
	for _, key := range candidateKeys {
		if !visibleSet[key] {
			continue
		}
		newKeys = append(newKeys, key)
		newColEntries = append(newColEntries, map[string]any{
			"key":   key,
			"label": labelByKey[key],
			"type":  "text",
		})
	}

	if len(newColEntries) == 0 {
		return nil, nil
	}

	if len(candidateKeys) > len(newColEntries) {
		log.Info().
			Str("view_id", viewID).
			Int("total_columns", len(candidateKeys)).
			Int("visible_columns", len(newColEntries)).
			Msg("import: capped visible columns (all data stored in MongoDB)")
	}

	if targetComp.Config == nil {
		targetComp.Config = make(map[string]any)
	}

	existing, _ := targetComp.Config["columns"].([]any)
	for _, entry := range newColEntries {
		existing = append(existing, entry)
	}
	targetComp.Config["columns"] = existing

	NormalizeDefinition(&v.Definition)
	if err := backend.UpdateView(ctx, v); err != nil {
		return nil, fmt.Errorf("update view: %w", err)
	}

	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Strs("new_columns", newKeys).
		Msg("import: synced columns to view definition")

	return newKeys, nil
}

func existingColumnKeys(config map[string]any) map[string]bool {
	keys := make(map[string]bool)
	if config == nil {
		return keys
	}
	raw, ok := config["columns"]
	if !ok {
		return keys
	}
	cols, ok := raw.([]any)
	if !ok {
		return keys
	}
	for _, c := range cols {
		if m, ok := c.(map[string]any); ok {
			if k, ok := m["key"].(string); ok {
				keys[strings.TrimSpace(k)] = true
			}
		}
	}
	return keys
}

func importColumnKey(header string) string {
	key := strings.ToLower(strings.TrimSpace(header))
	key = strings.ReplaceAll(key, " ", "_")
	key = strings.ReplaceAll(key, "-", "_")
	var clean strings.Builder
	for _, r := range key {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			clean.WriteRune(r)
		}
	}
	result := clean.String()
	if result == "" {
		return "col"
	}
	return result
}

// resolveColumnMapping uses BAML to semantically map CSV headers to existing
// view columns when columns are already defined, falling back to naive string
// normalization if no existing columns exist or if BAML fails.
func resolveColumnMapping(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, headers []string) map[string]string {
	existing := loadExistingColumnSchemas(ctx, backend, workspaceID, viewID, sheetID, componentID)

	if len(existing) > 0 {
		result, err := baml.MapImportColumns(ctx, headers, existing)
		if err == nil && len(result.Mappings) == len(headers) {
			mapping := make(map[string]string, len(result.Mappings))
			seen := make(map[string]bool, len(result.Mappings))
			valid := true
			for _, m := range result.Mappings {
				key := strings.TrimSpace(m.Column_key)
				if key == "" || seen[key] {
					valid = false
					break
				}
				seen[key] = true
				mapping[m.Header] = key
			}
			if valid {
				log.Info().
					Str("view_id", viewID).
					Int("headers", len(headers)).
					Int("existing_cols", len(existing)).
					Msg("import: BAML column mapping succeeded")
				return mapping
			}
		}
		if err != nil {
			log.Warn().Err(err).Str("view_id", viewID).Msg("import: BAML column mapping failed, falling back to string normalization")
		} else {
			log.Warn().Str("view_id", viewID).Msg("import: BAML column mapping returned invalid result, falling back")
		}
	}

	mapping := make(map[string]string, len(headers))
	for _, h := range headers {
		mapping[h] = importColumnKey(h)
	}
	return mapping
}

// loadExistingColumnSchemas reads the view definition and extracts column
// schemas from the target component, formatted for the BAML mapper.
func loadExistingColumnSchemas(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string) []bamltypes.ColumnSchema {
	if backend == nil {
		return nil
	}
	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return nil
	}
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID != componentID {
				continue
			}
			return extractColumnSchemas(comp.Config)
		}
	}
	return nil
}

func extractColumnSchemas(config map[string]any) []bamltypes.ColumnSchema {
	if config == nil {
		return nil
	}
	cols, ok := config["columns"].([]any)
	if !ok {
		return nil
	}
	var schemas []bamltypes.ColumnSchema
	for _, c := range cols {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		key, _ := m["key"].(string)
		if key == "" {
			continue
		}
		label, _ := m["label"].(string)
		if label == "" {
			label = key
		}
		colType, _ := m["type"].(string)
		if colType == "" {
			colType = "text"
		}
		schemas = append(schemas, bamltypes.ColumnSchema{
			Name:        label,
			Key:         key,
			Type:        colType,
			Description: label,
		})
	}
	return schemas
}

// stampSchemaHash re-reads the view definition after column changes and
// stamps the new schema hash onto all existing rows for the component.
// Without this, every cached task-mapped row looks "stale" after an import
// adds columns, triggering a full BAML re-map on the next data fetch.
func stampSchemaHash(ctx context.Context, store *ViewStore, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string) {
	if store == nil || backend == nil {
		return
	}

	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Msg("import: could not re-read view for schema hash stamp")
		return
	}

	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID != componentID || !comp.IsTable() {
				continue
			}
			newHash := MappingSchemaHash(sheet, comp)
			if newHash == "" {
				return
			}
			if err := store.UpdateSchemaHash(ctx, viewID, sheetID, componentID, newHash); err != nil {
				log.Warn().Err(err).Str("view_id", viewID).Str("hash", newHash).Msg("import: failed to stamp schema hash")
			}
			return
		}
	}
}
