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

const (
	dataPreviewRows       = 5
	importRowChunkMinSize = 5
	importRowChunkMaxSize = 50
)

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
	NewCols     []columnCreateSpec
	RemoveCols  []string
	ColumnOrder []string
}

type schemaSyncResult struct {
	NewColumns []string
	Changed    bool
}

type parsedImportData struct {
	Headers     []string
	Records     []map[string]string
	ParseErrors []string
}

func ImportData(ctx context.Context, p ImportParams) (*ImportResult, error) {
	if p.Store == nil || !p.Store.Available() {
		return nil, fmt.Errorf("data store not configured")
	}
	if len(p.Data) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	parsed, err := parseImportData(p.Data, p.FilePath)
	if err != nil {
		return nil, err
	}
	if len(parsed.Records) == 0 {
		return nil, fmt.Errorf("no data rows found")
	}

	importID := uuid.New().String()
	colMapping := p.ColMapping
	var update schemaUpdate
	if len(colMapping) == 0 {
		colMapping, update = resolveColumnMapping(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, parsed.Headers, parsed.Records)
	}

	syncResult, err := syncColumnsToView(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, update)
	if err != nil {
		log.Warn().Err(err).Str("view_id", p.ViewID).Msg("import: failed to sync columns to view definition (data will still be imported)")
	}

	if syncResult.Changed {
		stampSchemaHash(ctx, p.Store, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID)
	}

	rowCount, err := upsertImportRowsInChunks(ctx, p.Store, p.ViewID, p.SheetID, p.ComponentID, importID, parsed.Records, colMapping)
	if err != nil {
		return nil, err
	}

	log.Info().
		Str("view_id", p.ViewID).
		Str("sheet_id", p.SheetID).
		Str("component_id", p.ComponentID).
		Str("import_id", importID).
		Int("rows", rowCount).
		Int("new_columns", len(syncResult.NewColumns)).
		Msg("import: data imported")

	return &ImportResult{
		ImportID:    importID,
		RowCount:    rowCount,
		ColumnCount: len(colMapping),
		NewColumns:  syncResult.NewColumns,
		ParseErrors: parsed.ParseErrors,
	}, nil
}

func parseImportData(data []byte, filePath string) (*parsedImportData, error) {
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".json":
		headers, records, parseErrors, err := parseJSON(data)
		if err != nil {
			return nil, fmt.Errorf("parse JSON: %w", err)
		}
		return &parsedImportData{
			Headers:     headers,
			Records:     records,
			ParseErrors: parseErrors,
		}, nil
	default:
		delimiter := ','
		if ext == ".tsv" || ext == ".tab" {
			delimiter = '\t'
		}
		headers, records, parseErrors, err := parseCSV(data, delimiter)
		if err != nil {
			return nil, fmt.Errorf("parse CSV: %w", err)
		}
		return &parsedImportData{
			Headers:     headers,
			Records:     records,
			ParseErrors: parseErrors,
		}, nil
	}
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

func buildImportRows(records []map[string]string, colMapping map[string]string, sheetID, componentID, importID string, rowOffset int) []ViewRow {
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

		rowIndex := rowOffset + i
		rowID := fmt.Sprintf("%s:%s:import-%s:%d", sheetID, componentID, importID, rowIndex)
		rows = append(rows, ViewRow{
			ID:          rowID,
			SheetID:     sheetID,
			ComponentID: componentID,
			GroupID:     "import:" + importID,
			RowKey:      fmt.Sprintf("import-%d", rowIndex),
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

func upsertImportRowsInChunks(ctx context.Context, store *ViewStore, viewID, sheetID, componentID, importID string, records []map[string]string, colMapping map[string]string) (int, error) {
	if len(records) == 0 {
		return 0, nil
	}

	chunkSize := importRowChunkSize(len(records))
	totalRows := 0
	for start := 0; start < len(records); start += chunkSize {
		end := start + chunkSize
		if end > len(records) {
			end = len(records)
		}

		rows := buildImportRows(records[start:end], colMapping, sheetID, componentID, importID, start)
		if err := store.UpsertRows(ctx, viewID, rows); err != nil {
			return totalRows, fmt.Errorf("upsert rows %d-%d: %w", start, end-1, err)
		}
		totalRows += len(rows)

		log.Debug().
			Str("view_id", viewID).
			Str("sheet_id", sheetID).
			Str("component_id", componentID).
			Str("import_id", importID).
			Int("chunk_start", start).
			Int("chunk_end", end).
			Int("chunk_rows", len(rows)).
			Int("rows_written", totalRows).
			Msg("import: upserted row chunk")
	}

	return totalRows, nil
}

func importRowChunkSize(total int) int {
	if total <= 0 {
		return 1
	}
	size := total / 4
	if size < importRowChunkMinSize {
		size = importRowChunkMinSize
	}
	if size > importRowChunkMaxSize {
		size = importRowChunkMaxSize
	}
	return size
}

// syncColumnsToView adds new columns to the view's table component and
// persists the updated definition. Existing columns keep their position;
// new columns are appended in the order provided by the schema update.
func syncColumnsToView(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, update schemaUpdate) (schemaSyncResult, error) {
	if backend == nil {
		return schemaSyncResult{}, nil
	}
	if len(update.NewCols) == 0 {
		return schemaSyncResult{}, nil
	}

	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return schemaSyncResult{}, fmt.Errorf("get view: %w", err)
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
		return schemaSyncResult{}, fmt.Errorf("component %s not found in sheet %s", componentID, sheetID)
	}

	if targetComp.Config == nil {
		targetComp.Config = make(map[string]any)
	}

	existingKeys := make(map[string]bool)
	existing, _ := targetComp.Config["columns"].([]any)
	for _, c := range existing {
		if m, ok := c.(map[string]any); ok {
			if k, ok := m["key"].(string); ok && k != "" {
				existingKeys[k] = true
			}
		}
	}

	var newKeys []string
	ordered := append([]any(nil), existing...)
	for _, spec := range update.NewCols {
		if existingKeys[spec.Key] {
			continue
		}
		ordered = append(ordered, map[string]any{
			"key":   spec.Key,
			"label": spec.Label,
			"type":  spec.ColType,
		})
		newKeys = append(newKeys, spec.Key)
		existingKeys[spec.Key] = true
	}

	if len(newKeys) == 0 {
		return schemaSyncResult{}, nil
	}

	targetComp.Config["columns"] = ordered
	NormalizeDefinition(&v.Definition)
	if err := backend.UpdateView(ctx, v); err != nil {
		return schemaSyncResult{}, fmt.Errorf("update view: %w", err)
	}

	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Int("added", len(newKeys)).
		Int("total", len(ordered)).
		Msg("import: synced columns to view definition")

	return schemaSyncResult{
		NewColumns: newKeys,
		Changed:    true,
	}, nil
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

// resolveColumnMapping runs a single BAML call to match headers against
// existing columns, then deterministically creates columns for everything
// that wasn't matched or skipped. Fast path: one LLM call regardless of
// header count.
func resolveColumnMapping(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, headers []string, records []map[string]string) (map[string]string, schemaUpdate) {
	sheetName, existing := loadImportContext(ctx, backend, workspaceID, viewID, sheetID, componentID)
	preview := buildDataPreview(headers, records, dataPreviewRows)

	matchMap, skipSet := callImportMapper(ctx, viewID, sheetName, existing, headers, preview)

	existingKeys := make(map[string]bool, len(existing))
	for _, col := range existing {
		existingKeys[col.Key] = true
	}

	mapping := make(map[string]string, len(headers))
	seenKeys := make(map[string]bool, len(headers))
	var newCols []columnCreateSpec
	matched, skipped, created := 0, 0, 0

	for _, header := range headers {
		if existingKey, ok := matchMap[header]; ok {
			mapping[header] = existingKey
			seenKeys[existingKey] = true
			matched++
			continue
		}
		if skipSet[header] {
			key := uniqueKey(importColumnKey(header), seenKeys)
			mapping[header] = key
			skipped++
			continue
		}

		key := uniqueKey(importColumnKey(header), seenKeys)
		mapping[header] = key
		if !existingKeys[key] {
			newCols = append(newCols, columnCreateSpec{Key: key, Label: header, ColType: "text"})
		}
		created++
	}

	order := make([]string, 0, len(existing)+len(newCols))
	for _, col := range existing {
		order = append(order, col.Key)
	}
	for _, spec := range newCols {
		order = append(order, spec.Key)
	}

	log.Info().
		Str("view_id", viewID).
		Str("sheet", sheetName).
		Int("headers", len(headers)).
		Int("matched", matched).
		Int("skipped", skipped).
		Int("created", created).
		Msg("import: resolved column mapping")

	return mapping, schemaUpdate{NewCols: newCols, ColumnOrder: order}
}

// callImportMapper calls the single-pass BAML function to match headers
// to existing columns and identify headers to skip. Falls back to
// all-create on error.
func callImportMapper(ctx context.Context, viewID, sheetName string, existing []bamltypes.ColumnSchema, headers []string, preview string) (matchMap map[string]string, skipSet map[string]bool) {
	matchMap = make(map[string]string)
	skipSet = make(map[string]bool)

	if len(existing) == 0 {
		return
	}

	result, err := baml.MapImportColumns(ctx, sheetName, existing, headers, preview)
	if err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Msg("import: BAML mapper failed, falling back to deterministic mapping")
		return
	}

	existingKeys := make(map[string]bool, len(existing))
	for _, col := range existing {
		existingKeys[col.Key] = true
	}

	usedKeys := make(map[string]bool)
	for _, m := range result.Matches {
		header := strings.TrimSpace(m.Header)
		key := strings.TrimSpace(m.Existing_key)
		if header == "" || key == "" || !existingKeys[key] || usedKeys[key] {
			continue
		}
		matchMap[header] = key
		usedKeys[key] = true
	}

	for _, h := range result.Skip {
		h = strings.TrimSpace(h)
		if h != "" {
			skipSet[h] = true
		}
	}
	return
}

func uniqueKey(base string, seen map[string]bool) string {
	if base == "" {
		base = "col"
	}
	key := base
	for seen[key] {
		key += "_"
	}
	seen[key] = true
	return key
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

// buildDataPreview formats the first N rows as a compact table so the BAML
// mapper can see actual values and make data-informed matching decisions.
func buildDataPreview(headers []string, records []map[string]string, maxRows int) string {
	if len(records) == 0 || len(headers) == 0 {
		return ""
	}
	n := maxRows
	if n > len(records) {
		n = len(records)
	}

	previewHeaders := headers
	if len(previewHeaders) > 40 {
		previewHeaders = previewHeaders[:40]
	}

	var b strings.Builder
	for i, h := range previewHeaders {
		if i > 0 {
			b.WriteString(" | ")
		}
		if len(h) > 25 {
			b.WriteString(h[:22])
			b.WriteString("...")
		} else {
			b.WriteString(h)
		}
	}
	b.WriteByte('\n')

	for row := 0; row < n; row++ {
		rec := records[row]
		for i, h := range previewHeaders {
			if i > 0 {
				b.WriteString(" | ")
			}
			val := rec[h]
			if len(val) > 30 {
				val = val[:27] + "..."
			}
			b.WriteString(val)
		}
		b.WriteByte('\n')
	}

	return b.String()
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
