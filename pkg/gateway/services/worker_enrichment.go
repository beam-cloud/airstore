package services

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/views"
	viewbaml "github.com/beam-cloud/airstore/pkg/views/baml_client"
	viewbamltypes "github.com/beam-cloud/airstore/pkg/views/baml_client/types"
	"github.com/rs/zerolog/log"
)

// enrichViewRows runs async BAML mapping on the gateway side after an output
// is persisted. It classifies which rows are affected, then populates cells
// for each matched row (or inserts new ones for unmatched entities).
func (s *WorkerService) enrichViewRows(ctx context.Context, output *types.TaskOutput) {
	if s.viewStore == nil || !s.viewStore.Available() || s.backend == nil || output == nil {
		return
	}
	if strings.TrimSpace(output.OutputType) == "" {
		return
	}

	agentID := ""
	if output.AgentID != nil {
		agentID = strings.TrimSpace(*output.AgentID)
	}
	if agentID == "" {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	contexts, err := types.LoadViewOutputSchemaContexts(ctx, s.backend, output.WorkspaceID, agentID)
	if err != nil {
		log.Warn().Err(err).Str("task_id", output.TaskID).Msg("enrichViewRows: failed to load schema contexts")
		return
	}

	for _, schemaCtx := range contexts {
		if err := s.enrichViewRowsForContext(ctx, output, schemaCtx); err != nil {
			log.Warn().Err(err).
				Str("task_id", output.TaskID).
				Str("view_id", schemaCtx.ViewID).
				Str("sheet_id", schemaCtx.SheetID).
				Msg("enrichViewRows: failed for context")
		}
	}
}

func (s *WorkerService) enrichViewRowsForContext(
	ctx context.Context,
	output *types.TaskOutput,
	schemaCtx types.ViewOutputSchemaContext,
) error {
	if schemaCtx.ViewID == "" || schemaCtx.SheetID == "" || len(schemaCtx.Columns) == 0 {
		return nil
	}
	if !outputMatchesSchemaContext(output, schemaCtx) {
		return nil
	}

	existingRows, err := s.viewStore.GetRows(ctx, schemaCtx.ViewID, schemaCtx.SheetID, schemaCtx.ComponentID)
	if err != nil {
		return fmt.Errorf("load existing rows: %w", err)
	}

	sort.Slice(existingRows, func(i, j int) bool {
		return existingRows[i].UpdatedAt.After(existingRows[j].UpdatedAt)
	})
	if len(existingRows) > 200 {
		existingRows = existingRows[:200]
	}

	schemaKeys := schemaCtx.ColumnKeys()
	columns := make([]viewbamltypes.ViewColumn, len(schemaCtx.Columns))
	for i, c := range schemaCtx.Columns {
		columns[i] = viewbamltypes.ViewColumn{Key: c.Key, Label: c.Label, Type: c.Type}
	}

	outputData := serializeOutputForBAML(output)
	summary := ""
	if output.Summary != nil {
		summary = *output.Summary
	}

	// Step 1: classify which rows are affected
	classification, err := viewbaml.ClassifyAffectedRows(
		ctx, columns,
		output.OutputType, output.Title, summary, outputData,
		serializeRows(existingRows, schemaKeys),
	)
	if err != nil {
		return fmt.Errorf("BAML ClassifyAffectedRows: %w", err)
	}

	log.Info().
		Str("task_id", output.TaskID).
		Str("view_id", schemaCtx.ViewID).
		Int("matched_rows", len(classification.Affected_row_ids)).
		Int("unmatched_entities", len(classification.Unmatched_entities)).
		Msg("enrichViewRows: classification complete")

	rowIndex := make(map[string]*views.ViewRow, len(existingRows))
	for i := range existingRows {
		rowIndex[existingRows[i].ID] = &existingRows[i]
	}

	schemaHash := s.computeSchemaHash(ctx, output.WorkspaceID, schemaCtx)
	updated := false

	// Step 2: populate cells for each matched row
	for _, rowID := range classification.Affected_row_ids {
		row, ok := rowIndex[rowID]
		if !ok {
			log.Warn().Str("row_id", rowID).Msg("enrichViewRows: classified row not found, treating as unmatched")
			if output.Title != "" {
				classification.Unmatched_entities = append(classification.Unmatched_entities, output.Title)
			}
			continue
		}

		result, err := viewbaml.PopulateRowCells(
			ctx, columns,
			output.OutputType, output.Title, summary, outputData,
			rowID, serializeCells(row, schemaKeys), "",
		)
		if err != nil {
			log.Warn().Err(err).Str("row_id", rowID).Msg("enrichViewRows: PopulateRowCells failed")
			continue
		}

		cells := extractCells(result.Cells)
		if err := s.viewStore.MergeCells(ctx, schemaCtx.ViewID, rowID, cells, output.ID); err != nil {
			log.Warn().Err(err).Str("row_id", rowID).Msg("enrichViewRows: merge cells failed")
			continue
		}

		log.Info().Str("task_id", output.TaskID).Str("view_id", schemaCtx.ViewID).
			Str("row_id", rowID).Int("cells", len(cells)).
			Msg("enrichViewRows: enriched existing row")
		updated = true
	}

	// Step 3: insert new rows for unmatched entities
	for _, entity := range classification.Unmatched_entities {
		entity = strings.TrimSpace(entity)
		if entity == "" {
			continue
		}

		result, err := viewbaml.PopulateRowCells(
			ctx, columns,
			output.OutputType, output.Title, summary, outputData,
			"", "", entity,
		)
		if err != nil {
			log.Warn().Err(err).Str("entity", entity).Msg("enrichViewRows: PopulateRowCells for insert failed")
			continue
		}

		cells := extractCells(result.Cells)
		if len(cells) == 0 {
			continue
		}

		if reason := rejectInsert(cells, existingRows); reason != "" {
			log.Info().Str("task_id", output.TaskID).Str("entity", entity).
				Str("reason", reason).Msg("enrichViewRows: skipping insert")
			continue
		}

		if match := findBestRowMatch(existingRows, cells); match != nil {
			if err := s.viewStore.MergeCells(ctx, schemaCtx.ViewID, match.ID, cells, output.ID); err != nil {
				log.Warn().Err(err).Str("row_id", match.ID).Msg("enrichViewRows: merge into matched row failed")
				continue
			}
			if match.Cells == nil {
				match.Cells = make(map[string]string, len(cells))
			}
			for k, v := range cells {
				match.Cells[k] = v
			}
			log.Info().Str("task_id", output.TaskID).Str("row_id", match.ID).
				Str("entity", entity).Int("cells", len(cells)).
				Msg("enrichViewRows: merged into existing row by identity")
			updated = true
			continue
		}

		rowKey := result.Row_key
		if rowKey == "" {
			rowKey = "task"
		}
		normalizedKey := views.NormalizeRowKey(rowKey)

		if found, _ := s.viewStore.FindRowByKey(ctx, schemaCtx.ViewID, schemaCtx.SheetID, schemaCtx.ComponentID, normalizedKey); found != nil {
			if err := s.viewStore.MergeCells(ctx, schemaCtx.ViewID, found.ID, cells, output.ID); err != nil {
				log.Warn().Err(err).Str("row_id", found.ID).Msg("enrichViewRows: merge by key failed")
				continue
			}
			log.Info().Str("task_id", output.TaskID).Str("row_id", found.ID).
				Str("row_key", normalizedKey).Int("cells", len(cells)).
				Msg("enrichViewRows: merged into existing row by key")
			updated = true
			continue
		}

		rowID := fmt.Sprintf("%s:%s:%s", schemaCtx.SheetID, schemaCtx.ComponentID, normalizedKey)
		row := views.ViewRow{
			ID: rowID, SheetID: schemaCtx.SheetID, ComponentID: schemaCtx.ComponentID,
			GroupID: output.TaskID, TaskID: output.TaskID,
			RowKey: normalizedKey, SchemaHash: schemaHash,
			OutputIDs: []string{output.ID}, OutputSignature: output.ID,
			SourceOutputIDs: []string{output.ID},
			Cells: cells, UpdatedAt: time.Now(),
		}
		if err := s.viewStore.UpsertRows(ctx, schemaCtx.ViewID, []views.ViewRow{row}); err != nil {
			log.Warn().Err(err).Str("entity", entity).Msg("enrichViewRows: insert failed")
			continue
		}
		existingRows = append(existingRows, row)
		log.Info().Str("task_id", output.TaskID).Str("row_id", rowID).
			Int("cells", len(cells)).Msg("enrichViewRows: inserted new row")
		updated = true
	}

	if updated {
		s.publishTaskUpdate(ctx, output.WorkspaceID, output.TaskID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Insert guards
// ---------------------------------------------------------------------------

// rejectInsert returns a reason if the proposed row should not be inserted.
func rejectInsert(cells map[string]string, existingRows []views.ViewRow) string {
	if countSubstantive(cells) < 3 {
		return "fragment row (too few populated cells)"
	}
	if allValuesPresent(cells, existingRows) {
		return "redundant (all values already exist in table)"
	}
	if hasConcatenatedData(cells, existingRows) {
		return "concatenated identity data from multiple rows"
	}
	return ""
}

// ---------------------------------------------------------------------------
// Row matching
// ---------------------------------------------------------------------------

// findBestRowMatch finds an existing row that likely represents the same
// entity as the proposed cells. Requires at least 4 points of overlap where
// only values >= 10 chars are scored (exact match = 2, substring = 1).
func findBestRowMatch(existingRows []views.ViewRow, proposed map[string]string) *views.ViewRow {
	var best *views.ViewRow
	bestScore := 0
	for i := range existingRows {
		if s := overlapScore(existingRows[i].MergedCells(), proposed); s > bestScore {
			bestScore = s
			best = &existingRows[i]
		}
	}
	if bestScore >= 4 {
		return best
	}
	return nil
}

func overlapScore(existing, proposed map[string]string) int {
	score := 0
	for col, pv := range proposed {
		pn := strings.ToLower(strings.TrimSpace(pv))
		if len(pn) < 10 || isTrivial(pv) {
			continue
		}
		ev, ok := existing[col]
		if !ok {
			continue
		}
		en := strings.ToLower(strings.TrimSpace(ev))
		if len(en) < 10 || isTrivial(ev) {
			continue
		}
		if pn == en {
			score += 2
		} else if strings.Contains(pn, en) || strings.Contains(en, pn) {
			score++
		}
	}
	return score
}

// ---------------------------------------------------------------------------
// Cell analysis
// ---------------------------------------------------------------------------

var trivialValues = map[string]bool{
	"": true, "n/a": true, "new": true, "none": true,
	"true": true, "false": true, "yes": true, "no": true,
	"sent": true, "draft": true,
}

func isTrivial(v string) bool {
	return trivialValues[strings.ToLower(strings.TrimSpace(v))]
}

func countSubstantive(cells map[string]string) int {
	n := 0
	for _, v := range cells {
		if len(strings.TrimSpace(v)) >= 2 && !isTrivial(v) {
			n++
		}
	}
	return n
}

// allValuesPresent checks whether every non-trivial cell value already exists
// somewhere in the existing rows. Only does forward containment (needle IN
// haystack), not reverse, to avoid false positives on short substrings.
func allValuesPresent(cells map[string]string, rows []views.ViewRow) bool {
	if len(rows) == 0 {
		return false
	}
	checked, found := 0, 0
	for _, v := range cells {
		norm := strings.ToLower(strings.TrimSpace(v))
		if len(norm) < 4 || isTrivial(v) {
			continue
		}
		checked++
		for i := range rows {
			if cellContains(norm, rows[i].MergedCells()) {
				found++
				break
			}
		}
	}
	return checked > 0 && checked == found
}

// cellContains checks if needle appears (exact or as a substring) in any
// cell of the row. Only checks needle-in-haystack direction to avoid matching
// a 3-word proposed value just because it contains some short existing value.
func cellContains(needle string, rowCells map[string]string) bool {
	for _, v := range rowCells {
		hay := strings.ToLower(strings.TrimSpace(v))
		if len(hay) < 4 {
			continue
		}
		if needle == hay || strings.Contains(hay, needle) {
			return true
		}
	}
	return false
}

// hasConcatenatedData detects when a proposed row aggregates data from
// multiple existing rows into a single cell (e.g. several addresses
// comma-separated into one value).
func hasConcatenatedData(cells map[string]string, rows []views.ViewRow) bool {
	for col, pv := range cells {
		if len(pv) < 15 {
			continue
		}
		pn := strings.ToLower(pv)
		matches := 0
		for i := range rows {
			ev := rows[i].MergedCells()[col]
			en := strings.ToLower(strings.TrimSpace(ev))
			if len(en) < 4 || isTrivial(ev) {
				continue
			}
			if pn != en && strings.Contains(pn, en) {
				matches++
			}
		}
		if matches >= 2 {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Serialization
// ---------------------------------------------------------------------------

const maxCellLen = 200

func extractCells(baml []viewbamltypes.ViewCell) map[string]string {
	out := make(map[string]string, len(baml))
	for _, c := range baml {
		if c.Column != "" && c.Value != "" {
			out[c.Column] = c.Value
		}
	}
	return out
}

func outputMatchesSchemaContext(output *types.TaskOutput, ctx types.ViewOutputSchemaContext) bool {
	ot := strings.TrimSpace(strings.ToLower(output.OutputType))
	if ot == "" {
		return false
	}
	if ctx.OutputType != "" && strings.TrimSpace(strings.ToLower(ctx.OutputType)) == ot {
		return true
	}
	ak := ""
	if output.Metadata != nil {
		if v, ok := output.Metadata["artifact_key"].(string); ok {
			ak = strings.TrimSpace(strings.ToLower(v))
		}
	}
	if ctx.ArtifactKey != "" && ak != "" && strings.TrimSpace(strings.ToLower(ctx.ArtifactKey)) == ak {
		return true
	}
	return ctx.OutputType == "" && ctx.ArtifactKey == ""
}

func serializeOutputForBAML(output *types.TaskOutput) string {
	compact := map[string]any{
		"id":          output.ID,
		"output_type": output.OutputType,
		"title":       output.Title,
	}
	if output.Summary != nil && *output.Summary != "" {
		compact["summary"] = *output.Summary
	}
	if output.URI != nil && *output.URI != "" {
		compact["uri"] = *output.URI
	}
	if len(output.Data) > 0 {
		compact["data"] = output.Data
	}
	if output.Metadata != nil {
		filtered := make(map[string]any)
		for k, v := range output.Metadata {
			if !strings.HasPrefix(k, "_") {
				filtered[k] = v
			}
		}
		if len(filtered) > 0 {
			compact["metadata"] = filtered
		}
	}
	b, _ := json.Marshal(compact)
	if b == nil {
		return "{}"
	}
	return string(b)
}

// filterCells returns schema-filtered, truncated cells from a row.
func filterCells(merged map[string]string, schemaKeys []string) map[string]string {
	schemaSet := make(map[string]struct{}, len(schemaKeys))
	for _, k := range schemaKeys {
		schemaSet[k] = struct{}{}
	}
	out := make(map[string]string)
	for k, v := range merged {
		if v == "" {
			continue
		}
		if _, ok := schemaSet[k]; !ok && len(schemaSet) > 0 {
			continue
		}
		if len(v) > maxCellLen {
			v = v[:maxCellLen] + "..."
		}
		out[k] = v
	}
	return out
}

// serializeRows produces a compact JSON array for the classifier.
func serializeRows(rows []views.ViewRow, schemaKeys []string) string {
	if len(rows) == 0 {
		return ""
	}
	type compactRow struct {
		ID    string            `json:"_id"`
		Cells map[string]string `json:"cells"`
	}
	compact := make([]compactRow, 0, len(rows))
	for _, r := range rows {
		cells := filterCells(r.MergedCells(), schemaKeys)
		if len(cells) > 0 {
			compact = append(compact, compactRow{ID: r.ID, Cells: cells})
		}
	}
	if len(compact) == 0 {
		return ""
	}
	b, _ := json.Marshal(compact)
	return string(b)
}

// serializeCells returns a JSON object of a single row's schema-filtered cells.
func serializeCells(row *views.ViewRow, schemaKeys []string) string {
	if row == nil {
		return "{}"
	}
	b, _ := json.Marshal(filterCells(row.MergedCells(), schemaKeys))
	if b == nil {
		return "{}"
	}
	return string(b)
}

func (s *WorkerService) computeSchemaHash(
	ctx context.Context,
	workspaceID uint,
	schemaCtx types.ViewOutputSchemaContext,
) string {
	if s.backend == nil {
		return ""
	}
	view, err := s.backend.GetView(ctx, workspaceID, schemaCtx.ViewID)
	if err != nil || view == nil {
		return ""
	}
	for _, sheet := range view.Definition.Sheets {
		if sheet.ID != schemaCtx.SheetID {
			continue
		}
		for _, comp := range sheet.Components {
			if comp.ID == schemaCtx.ComponentID && comp.IsTable() {
				return views.MappingSchemaHash(sheet, comp)
			}
		}
		for _, comp := range sheet.Components {
			if comp.IsTable() {
				return views.MappingSchemaHash(sheet, comp)
			}
		}
	}
	return ""
}
