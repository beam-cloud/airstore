package views

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
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

const bamlFuzzyThreshold = 5

type ImportParams struct {
	Store       *ViewStore
	Backend     repository.BackendRepository
	Data        []byte
	FilePath    string
	ViewID      string
	WorkspaceID uint
	SheetID     string
	ComponentID string
	ColMapping  map[string]string // caller-provided mapping bypasses resolution
}

type ImportResult struct {
	ImportID    string   `json:"import_id"`
	RowCount    int      `json:"row_count"`
	ColumnCount int      `json:"column_count"`
	NewColumns  []string `json:"new_columns,omitempty"`
	ParseErrors []string `json:"parse_errors,omitempty"`
}

type columnSpec struct{ Key, Label, Type string }

func ImportData(ctx context.Context, p ImportParams) (*ImportResult, error) {
	if p.Store == nil || !p.Store.Available() {
		return nil, fmt.Errorf("data store not configured")
	}
	if len(p.Data) == 0 {
		return nil, fmt.Errorf("empty file")
	}

	headers, records, parseErrors, err := parseFile(p.Data, p.FilePath)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("no data rows found")
	}

	importID := uuid.New().String()

	colMapping := p.ColMapping
	var newCols []columnSpec
	if len(colMapping) == 0 {
		colMapping, newCols = resolveColumns(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, headers, records)
	}

	if len(newCols) > 0 {
		if err := syncColumns(ctx, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID, newCols); err != nil {
			log.Warn().Err(err).Str("view_id", p.ViewID).Msg("import: column sync failed")
		} else {
			stampSchemaHash(ctx, p.Store, p.Backend, p.WorkspaceID, p.ViewID, p.SheetID, p.ComponentID)
		}
	}

	now := time.Now()
	rows := make([]ViewRow, 0, len(records))
	for i, record := range records {
		pinned := make(map[string]string, len(record))
		for h, v := range record {
			if k, ok := colMapping[h]; ok {
				pinned[k] = v
			}
		}
		if len(pinned) == 0 {
			continue
		}
		rowKey := deriveRowKey(pinned)
		if rowKey == "" {
			rowKey = "import-" + strconv.Itoa(i)
		}
		rows = append(rows, ViewRow{
			ID:          fmt.Sprintf("%s:%s:import:%d", p.SheetID, p.ComponentID, i),
			SheetID:     p.SheetID,
			ComponentID: p.ComponentID,
			GroupID:     "import:" + importID,
			RowKey:      NormalizeRowKey(rowKey),
			Cells:       map[string]string{},
			Pinned:      pinned,
			Source:      RowSourceImport,
			ImportID:    importID,
			UpdatedAt:   now,
		})
	}

	if err := p.Store.UpsertRows(ctx, p.ViewID, rows); err != nil {
		return nil, fmt.Errorf("upsert rows: %w", err)
	}

	keepIDs := make([]string, len(rows))
	for i := range rows {
		keepIDs[i] = rows[i].ID
	}
	if stale, err := p.Store.CleanupStaleImportRows(ctx, p.ViewID, p.SheetID, p.ComponentID, keepIDs); err == nil && stale > 0 {
		log.Info().Str("view_id", p.ViewID).Int64("cleaned", stale).Msg("import: removed stale rows")
	}

	newColKeys := make([]string, len(newCols))
	for i := range newCols {
		newColKeys[i] = newCols[i].Key
	}
	log.Info().Str("view_id", p.ViewID).Int("rows", len(rows)).Int("new_cols", len(newCols)).Msg("import: complete")
	return &ImportResult{
		ImportID:    importID,
		RowCount:    len(rows),
		ColumnCount: len(colMapping),
		NewColumns:  newColKeys,
		ParseErrors: parseErrors,
	}, nil
}

func parseFile(data []byte, path string) ([]string, []map[string]string, []string, error) {
	ext := strings.ToLower(filepath.Ext(path))
	if ext == ".json" {
		return parseJSON(data)
	}
	delim := ','
	if ext == ".tsv" || ext == ".tab" {
		delim = '\t'
	}
	return parseCSV(data, delim)
}

func parseCSV(data []byte, delim rune) ([]string, []map[string]string, []string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	r.Comma = delim

	var raw []string
	for {
		var err error
		raw, err = r.Read()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("read headers: %w", err)
		}
		hasContent := false
		for _, f := range raw {
			if strings.TrimSpace(f) != "" {
				hasContent = true
				break
			}
		}
		if hasContent {
			break
		}
	}
	for i := range raw {
		raw[i] = strings.TrimSpace(raw[i])
	}

	headers := make([]string, 0, len(raw))
	for _, h := range raw {
		if h != "" {
			headers = append(headers, h)
		}
	}

	var records []map[string]string
	var errs []string
	for idx := 0; ; idx++ {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("row %d: %v", idx+1, err))
			continue
		}
		row := make(map[string]string, len(headers))
		nonEmpty := false
		for i, h := range raw {
			if i >= len(rec) || h == "" {
				continue
			}
			if v := strings.TrimSpace(rec[i]); v != "" {
				row[h] = v
				nonEmpty = true
			}
		}
		if nonEmpty {
			records = append(records, row)
		}
	}
	return headers, records, errs, nil
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

	seen := map[string]struct{}{}
	var order []string
	var records []map[string]string
	var errs []string
	for i, obj := range raw {
		row := map[string]string{}
		flattenJSON("", obj, row)
		if len(row) == 0 {
			errs = append(errs, fmt.Sprintf("row %d: empty", i))
			continue
		}
		for k := range row {
			if _, dup := seen[k]; !dup {
				seen[k] = struct{}{}
				order = append(order, k)
			}
		}
		records = append(records, row)
	}
	return order, records, errs, nil
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
		case string:
			out[key] = val
		case nil:
		default:
			out[key] = fmt.Sprintf("%v", val)
		}
	}
}

// resolveColumns does deterministic key matching first. BAML fuzzy matching
// only fires when >bamlFuzzyThreshold unmatched headers remain — re-imports
// and exact-name CSVs skip it entirely.
func resolveColumns(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, headers []string, records []map[string]string) (map[string]string, []columnSpec) {
	sheetName, existing := loadSchema(ctx, backend, workspaceID, viewID, sheetID, componentID)

	existingKeys := make(map[string]bool, len(existing))
	for _, col := range existing {
		existingKeys[col.Key] = true
	}

	mapping := make(map[string]string, len(headers))
	claimed := make(map[string]bool, len(existing))
	var unmatched []string

	for _, h := range headers {
		k := toKey(h)
		if existingKeys[k] && !claimed[k] {
			mapping[h] = k
			claimed[k] = true
		} else {
			unmatched = append(unmatched, h)
		}
	}

	if len(unmatched) > bamlFuzzyThreshold && len(existing) > 0 {
		var candidates []bamltypes.ColumnSchema
		for _, col := range existing {
			if !claimed[col.Key] {
				candidates = append(candidates, col)
			}
		}
		if len(candidates) > 0 {
			result, err := baml.MapImportColumns(ctx, sheetName, candidates, unmatched, dataPreview(headers, records))
			if err != nil {
				log.Warn().Err(err).Str("view_id", viewID).Msg("import: BAML column mapper failed")
			} else {
				matched := 0
				for _, m := range result.Matches {
					h, k := strings.TrimSpace(m.Header), strings.TrimSpace(m.Existing_key)
					if h != "" && k != "" && existingKeys[k] && !claimed[k] {
						mapping[h] = k
						claimed[k] = true
						matched++
					}
				}
				log.Info().Str("view_id", viewID).Int("fuzzy_matched", matched).Int("candidates", len(candidates)).Msg("import: BAML mapping")
			}
		}
	}

	var newCols []columnSpec
	for _, h := range headers {
		if _, ok := mapping[h]; ok {
			continue
		}
		k := dedupKey(toKey(h), claimed)
		mapping[h] = k
		if !existingKeys[k] {
			newCols = append(newCols, columnSpec{k, h, "text"})
		}
	}

	log.Info().Str("view_id", viewID).Int("matched", len(mapping)-len(newCols)).Int("new", len(newCols)).Int("headers", len(headers)).Msg("import: columns resolved")
	return mapping, newCols
}

func dataPreview(headers []string, records []map[string]string) string {
	if len(records) == 0 {
		return ""
	}
	cols := headers
	if len(cols) > 40 {
		cols = cols[:40]
	}
	n := min(5, len(records))
	var b strings.Builder
	for i, h := range cols {
		if i > 0 {
			b.WriteString(" | ")
		}
		if len(h) > 25 {
			b.WriteString(h[:22] + "...")
		} else {
			b.WriteString(h)
		}
	}
	b.WriteByte('\n')
	for row := 0; row < n; row++ {
		for i, h := range cols {
			if i > 0 {
				b.WriteString(" | ")
			}
			v := records[row][h]
			if len(v) > 30 {
				v = v[:27] + "..."
			}
			b.WriteString(v)
		}
		b.WriteByte('\n')
	}
	return b.String()
}

func toKey(header string) string {
	s := strings.ToLower(strings.TrimSpace(header))
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "-", "_")
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "col"
	}
	return b.String()
}

func dedupKey(base string, claimed map[string]bool) string {
	if base == "" {
		base = "col"
	}
	if !claimed[base] {
		claimed[base] = true
		return base
	}
	for i := 2; ; i++ {
		k := base + "_" + strconv.Itoa(i)
		if !claimed[k] {
			claimed[k] = true
			return k
		}
	}
}

func syncColumns(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string, newCols []columnSpec) error {
	if backend == nil || len(newCols) == 0 {
		return nil
	}
	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return fmt.Errorf("get view: %w", err)
	}

	comp := findComponent(&v.Definition, sheetID, componentID)
	if comp == nil {
		return fmt.Errorf("component %s not found", componentID)
	}
	if comp.Config == nil {
		comp.Config = make(map[string]any)
	}

	existing, _ := comp.Config["columns"].([]any)
	have := make(map[string]bool, len(existing))
	for _, c := range existing {
		if m, ok := c.(map[string]any); ok {
			if k, _ := m["key"].(string); k != "" {
				have[k] = true
			}
		}
	}

	cols := append([]any(nil), existing...)
	for _, c := range newCols {
		if !have[c.Key] {
			cols = append(cols, map[string]any{"key": c.Key, "label": c.Label, "type": c.Type})
		}
	}

	comp.Config["columns"] = cols
	NormalizeDefinition(&v.Definition)
	return backend.UpdateView(ctx, v)
}

func findComponent(def *types.ViewDefinition, sheetID, componentID string) *types.ComponentSpec {
	for si := range def.Sheets {
		if def.Sheets[si].ID != sheetID {
			continue
		}
		for ci := range def.Sheets[si].Components {
			if def.Sheets[si].Components[ci].ID == componentID {
				return &def.Sheets[si].Components[ci]
			}
		}
	}
	return nil
}

func loadSchema(ctx context.Context, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string) (string, []bamltypes.ColumnSchema) {
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
			cols, _ := comp.Config["columns"].([]any)
			out := make([]bamltypes.ColumnSchema, 0, len(cols))
			for _, c := range cols {
				m, _ := c.(map[string]any)
				key, _ := m["key"].(string)
				if key == "" {
					continue
				}
				label, _ := m["label"].(string)
				if label == "" {
					label = key
				}
				t, _ := m["type"].(string)
				if t == "" {
					t = "text"
				}
				out = append(out, bamltypes.ColumnSchema{Name: label, Key: key, Type: t, Description: label})
			}
			return sheet.Name, out
		}
		return sheet.Name, nil
	}
	return "", nil
}

func stampSchemaHash(ctx context.Context, store *ViewStore, backend repository.BackendRepository, workspaceID uint, viewID, sheetID, componentID string) {
	if store == nil || backend == nil {
		return
	}
	v, err := backend.GetView(ctx, workspaceID, viewID)
	if err != nil {
		return
	}
	for _, sheet := range v.Definition.Sheets {
		if sheet.ID != sheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID == componentID && comp.IsTable() {
				if h := MappingSchemaHash(sheet, comp); h != "" {
					_ = store.UpdateSchemaHash(ctx, viewID, sheetID, componentID, h)
				}
				return
			}
		}
	}
}
