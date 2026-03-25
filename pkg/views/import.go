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

type columnCreateSpec struct {
	Key     string
	Label   string
	ColType string
}

type schemaUpdate struct {
	NewCols       []columnCreateSpec
	RemoveCols    []string
	ColumnOrder   []string
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
	var update schemaUpdate
	if len(colMapping) == 0 {
		colMapping, update = resolveColumnMapping(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, headers, records)
	}

	newCols, err := syncColumnsToView(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, update)
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

// syncColumnsToView applies a schema update to the view's table component:
// removes redundant columns, adds new columns, and reorders everything per
// the BAML mapper's recommendation. Returns the list of newly added column keys.
func syncColumnsToView(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, update schemaUpdate) ([]string, error) {
	if backend == nil {
		return nil, nil
	}
	if len(update.NewCols) == 0 && len(update.RemoveCols) == 0 && len(update.ColumnOrder) == 0 {
		return nil, nil
	}

	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return nil, fmt.Errorf("get view: %w", err)
	}

	var targetComp *types.ComponentSpec
	for si := range v.Definition.Sheets {
		if v.Definition.Sheets[si].ID == sheetID {
			for ci := range v.Definition.Sheets[si].Components {
				if v.Definition.Sheets[si].Components[ci].ID == componentID {
					targetComp = &v.Definition.Sheets[si].Components[ci]
					break
				}
			}
			break
		}
	}
	if targetComp == nil {
		return nil, fmt.Errorf("component %s not found in sheet %s", componentID, sheetID)
	}

	if targetComp.Config == nil {
		targetComp.Config = make(map[string]any)
	}

	// Build a map of all column entries keyed by column key for fast lookup.
	colsByKey := make(map[string]map[string]any)
	existing, _ := targetComp.Config["columns"].([]any)
	for _, c := range existing {
		if m, ok := c.(map[string]any); ok {
			if k, ok := m["key"].(string); ok && k != "" {
				colsByKey[k] = m
			}
		}
	}

	// Remove redundant columns.
	removeSet := make(map[string]bool, len(update.RemoveCols))
	for _, k := range update.RemoveCols {
		if _, exists := colsByKey[k]; exists {
			removeSet[k] = true
			delete(colsByKey, k)
		}
	}

	// Add new columns (that don't already exist).
	var newKeys []string
	for _, spec := range update.NewCols {
		if _, exists := colsByKey[spec.Key]; exists {
			continue
		}
		entry := map[string]any{
			"key":   spec.Key,
			"label": spec.Label,
			"type":  spec.ColType,
		}
		colsByKey[spec.Key] = entry
		newKeys = append(newKeys, spec.Key)
	}

	// Cap total visible columns.
	if len(colsByKey) > MaxVisibleColumns {
		excess := len(colsByKey) - MaxVisibleColumns
		if excess > len(newKeys) {
			excess = len(newKeys)
		}
		for i := len(newKeys) - 1; i >= 0 && excess > 0; i-- {
			delete(colsByKey, newKeys[i])
			newKeys = newKeys[:i]
			excess--
		}
	}

	// Reorder: use BAML's suggested order, appending any keys it missed.
	var ordered []any
	seen := make(map[string]bool, len(colsByKey))
	for _, key := range update.ColumnOrder {
		if removeSet[key] || seen[key] {
			continue
		}
		if entry, ok := colsByKey[key]; ok {
			ordered = append(ordered, entry)
			seen[key] = true
		}
	}
	for _, c := range existing {
		if m, ok := c.(map[string]any); ok {
			if k, ok := m["key"].(string); ok && !seen[k] && !removeSet[k] {
				if entry, exists := colsByKey[k]; exists {
					ordered = append(ordered, entry)
					seen[k] = true
					_ = entry
				}
			}
		}
	}
	for _, spec := range update.NewCols {
		if !seen[spec.Key] {
			if entry, exists := colsByKey[spec.Key]; exists {
				ordered = append(ordered, entry)
				seen[spec.Key] = true
				_ = entry
			}
		}
	}
	targetComp.Config["columns"] = ordered

	NormalizeDefinition(&v.Definition)
	if err := backend.UpdateView(ctx, v); err != nil {
		return nil, fmt.Errorf("update view: %w", err)
	}

	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Int("added", len(newKeys)).
		Int("removed", len(removeSet)).
		Int("total", len(ordered)).
		Strs("new_columns", newKeys).
		Strs("removed_columns", update.RemoveCols).
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

const dataPreviewRows = 5

// resolveColumnMapping uses BAML to semantically map CSV headers to existing
// view columns. The BAML mapper sees the sheet name, existing columns, all
// headers, AND a sample of actual data rows — enabling fuzzy matching by
// content, not just header names. Also returns schema cleanup instructions:
// redundant columns to remove and a recommended column order.
func resolveColumnMapping(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, headers []string, records []map[string]string) (map[string]string, schemaUpdate) {
	sheetName, existing := loadImportContext(ctx, backend, workspaceID, viewID, sheetID, componentID)
	preview := buildDataPreview(headers, records, dataPreviewRows)

	result, err := baml.MapImportColumns(ctx, sheetName, headers, existing, preview)
	if err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Msg("import: BAML column mapping failed, falling back to string normalization")
		return fallbackMapping(headers)
	}
	if len(result.Mappings) != len(headers) {
		log.Warn().Str("view_id", viewID).Int("expected", len(headers)).Int("got", len(result.Mappings)).Msg("import: BAML returned wrong mapping count, falling back")
		return fallbackMapping(headers)
	}

	mapping := make(map[string]string, len(result.Mappings))
	seen := make(map[string]bool, len(result.Mappings))
	var specs []columnCreateSpec
	valid := true

	for _, m := range result.Mappings {
		key := strings.TrimSpace(m.Column_key)
		if key == "" || seen[key] {
			valid = false
			break
		}
		seen[key] = true
		mapping[m.Header] = key

		if m.Action == bamltypes.ColumnActionCreate {
			label := m.Label
			if label == "" {
				label = m.Header
			}
			colType := m.Column_type
			if colType == "" {
				colType = "text"
			}
			specs = append(specs, columnCreateSpec{Key: key, Label: label, ColType: colType})
		}
	}

	if !valid {
		log.Warn().Str("view_id", viewID).Msg("import: BAML returned duplicate or empty keys, falling back")
		return fallbackMapping(headers)
	}

	matched, created, skipped := 0, 0, 0
	for _, m := range result.Mappings {
		switch m.Action {
		case bamltypes.ColumnActionMatch:
			matched++
		case bamltypes.ColumnActionCreate:
			created++
		case bamltypes.ColumnActionSkip:
			skipped++
		}
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet", sheetName).
		Int("headers", len(headers)).
		Int("matched", matched).
		Int("created", created).
		Int("skipped", skipped).
		Int("remove", len(result.Remove_columns)).
		Int("order_len", len(result.Column_order)).
		Msg("import: BAML column mapping succeeded")

	return mapping, schemaUpdate{
		NewCols:     specs,
		RemoveCols:  result.Remove_columns,
		ColumnOrder: result.Column_order,
	}
}

// buildDataPreview formats the first N rows as a compact table so the BAML
// mapper can see actual values and make data-informed matching decisions.
func buildDataPreview(headers []string, records []map[string]string, maxRows int) string {
	if len(records) == 0 {
		return ""
	}
	n := maxRows
	if n > len(records) {
		n = len(records)
	}

	var b strings.Builder
	for i, h := range headers {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString(h)
	}
	b.WriteByte('\n')

	for i, h := range headers {
		if i > 0 {
			b.WriteString(" | ")
		}
		for j := 0; j < len(h) && j < 20; j++ {
			b.WriteByte('-')
		}
	}
	b.WriteByte('\n')

	for row := 0; row < n; row++ {
		rec := records[row]
		for i, h := range headers {
			if i > 0 {
				b.WriteString(" | ")
			}
			val := rec[h]
			if len(val) > 80 {
				val = val[:77] + "..."
			}
			b.WriteString(val)
		}
		b.WriteByte('\n')
	}

	return b.String()
}

func fallbackMapping(headers []string) (map[string]string, schemaUpdate) {
	mapping := make(map[string]string, len(headers))
	specs := make([]columnCreateSpec, 0, len(headers))
	for _, h := range headers {
		key := importColumnKey(h)
		mapping[h] = key
		specs = append(specs, columnCreateSpec{Key: key, Label: h, ColType: "text"})
	}
	return mapping, schemaUpdate{NewCols: specs}
}

// loadImportContext reads the view definition once and extracts both the sheet
// name and existing column schemas for the BAML mapper.
func loadImportContext(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string) (string, []bamltypes.ColumnSchema) {
	if backend == nil {
		return "", nil
	}
	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return "", nil
	}
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID != componentID {
				continue
			}
			return sheet.Name, extractColumnSchemas(comp.Config)
		}
		return sheet.Name, nil
	}
	return "", nil
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
