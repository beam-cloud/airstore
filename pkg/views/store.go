package views

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var ErrViewRowNotFound = errors.New("view row not found")
var ErrInvalidViewColumnKey = errors.New("invalid view column key")

func mongoColumnFieldPath(prefix, key string) (string, error) {
	key = strings.TrimSpace(key)
	switch {
	case key == "":
		return "", fmt.Errorf("%w: empty key", ErrInvalidViewColumnKey)
	case strings.Contains(key, "."):
		return "", fmt.Errorf("%w: %q contains '.'", ErrInvalidViewColumnKey, key)
	case strings.Contains(key, "$"):
		return "", fmt.Errorf("%w: %q contains '$'", ErrInvalidViewColumnKey, key)
	case strings.ContainsRune(key, 0):
		return "", fmt.Errorf("%w: %q contains NUL", ErrInvalidViewColumnKey, key)
	default:
		return fmt.Sprintf("%s.%s", prefix, key), nil
	}
}

// ViewRow is the MongoDB document schema for a single rendered row in a sheet.
type ViewRow struct {
	ID              string            `bson:"_id"`
	SheetID         string            `bson:"sheet_id"`
	ComponentID     string            `bson:"component_id,omitempty"`
	GroupID         string            `bson:"group_id"`
	TaskID          string            `bson:"task_id"`
	RowKey          string            `bson:"row_key"`
	SchemaHash      string            `bson:"schema_hash"`
	OutputIDs       []string          `bson:"output_ids"`
	SourceOutputIDs []string          `bson:"source_output_ids,omitempty"`
	Cells           map[string]string `bson:"cells"`
	Manual          map[string]string `bson:"manual,omitempty"`
	UpdatedAt       time.Time         `bson:"updated_at"`
}

// ExcludedRowSnapshot is the data we store when a user deletes a row so the
// BAML mapper knows not to regenerate it.
type ExcludedRowSnapshot struct {
	TaskID string            `bson:"task_id"`
	RowKey string            `bson:"row_key"`
	Cells  map[string]string `bson:"cells"`
}

// MergedCells returns cells with manual edits overlaid on top of BAML-mapped cells.
func (r *ViewRow) MergedCells() map[string]string {
	if len(r.Manual) == 0 {
		return r.Cells
	}
	merged := make(map[string]string, len(r.Cells))
	for k, v := range r.Cells {
		merged[k] = v
	}
	for k, v := range r.Manual {
		merged[k] = v
	}
	return merged
}

type ViewStore struct {
	mongo *common.MongoClient
}

func NewViewStore(mongo *common.MongoClient) *ViewStore {
	return &ViewStore{mongo: mongo}
}

func (s *ViewStore) collectionName(viewID string) string {
	return fmt.Sprintf("view_%s", viewID)
}

func (s *ViewStore) Available() bool {
	return s != nil && s.mongo != nil
}

func rowScopeFilter(sheetID, componentID string) bson.D {
	filter := bson.D{}
	if strings.TrimSpace(sheetID) != "" {
		filter = append(filter, bson.E{Key: "sheet_id", Value: sheetID})
	}
	if strings.TrimSpace(componentID) != "" {
		filter = append(filter, bson.E{Key: "component_id", Value: componentID})
	}
	return filter
}

func (s *ViewStore) GetRows(ctx context.Context, viewID, sheetID, componentID string) ([]ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := rowScopeFilter(sheetID, componentID)
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find rows: %w", err)
	}
	defer cursor.Close(ctx)

	var all []ViewRow
	if err := cursor.All(ctx, &all); err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	rows := make([]ViewRow, 0, len(all))
	for _, r := range all {
		if !strings.HasPrefix(r.ID, "__") {
			rows = append(rows, r)
		}
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Str("collection", s.collectionName(viewID)).
		Int("rows", len(rows)).
		Msg("view: loaded rows")
	return rows, nil
}

func (s *ViewStore) GetRow(ctx context.Context, viewID, sheetID, rowID string) (*ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	var row ViewRow
	if err := coll.FindOne(ctx, bson.D{
		{Key: "_id", Value: rowID},
		{Key: "sheet_id", Value: sheetID},
	}).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("find row: %w", err)
	}
	return &row, nil
}

// UpsertRows bulk-upserts rows into the view collection.
// Existing manual edits and excluded flags are preserved — only computed
// row data is replaced.
func (s *ViewStore) UpsertRows(ctx context.Context, viewID string, rows []ViewRow) error {
	if !s.Available() || len(rows) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	models := make([]mongo.WriteModel, 0, len(rows))
	for _, row := range rows {
		setFields := bson.D{
			{Key: "sheet_id", Value: row.SheetID},
			{Key: "component_id", Value: row.ComponentID},
			{Key: "group_id", Value: row.GroupID},
			{Key: "task_id", Value: row.TaskID},
			{Key: "row_key", Value: row.RowKey},
			{Key: "schema_hash", Value: row.SchemaHash},
			{Key: "output_ids", Value: row.OutputIDs},
			{Key: "source_output_ids", Value: row.SourceOutputIDs},
			{Key: "cells", Value: row.Cells},
			{Key: "updated_at", Value: row.UpdatedAt},
		}

		if len(row.Manual) > 0 {
			setFields = append(setFields, bson.E{Key: "manual", Value: row.Manual})
		}

		update := bson.D{{Key: "$set", Value: setFields}}
		if len(row.Manual) == 0 {
			update = append(update, bson.E{Key: "$setOnInsert", Value: bson.D{
				{Key: "manual", Value: bson.M{}},
			}})
		}

		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: row.ID}}).
			SetUpdate(update).
			SetUpsert(true),
		)
	}

	result, err := coll.BulkWrite(ctx, models)
	if err != nil {
		return fmt.Errorf("bulk upsert rows: %w", err)
	}
	log.Info().
		Str("view_id", viewID).
		Str("collection", s.collectionName(viewID)).
		Int("rows", len(rows)).
		Int64("upserted", result.UpsertedCount).
		Int64("modified", result.ModifiedCount).
		Msg("view: upserted rows")
	return nil
}

// DeleteRowsNotInGroups removes stale rows for the remapped groups.
func (s *ViewStore) DeleteRowsNotInGroups(ctx context.Context, viewID, sheetID, componentID string, groupIDs, keepRowIDs []string) error {
	if !s.Available() || len(groupIDs) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := rowScopeFilter(sheetID, componentID)
	filter = append(filter, bson.E{Key: "group_id", Value: bson.D{{Key: "$in", Value: groupIDs}}})
	if len(keepRowIDs) > 0 {
		filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$nin", Value: keepRowIDs}}})
	}
	res, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete stale rows: %w", err)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Int("groups", len(groupIDs)).
		Int64("deleted", res.DeletedCount).
		Msg("view: deleted stale rows")
	return nil
}

// UpdateCells writes user-edited values into the manual overlay.
func (s *ViewStore) UpdateCells(ctx context.Context, viewID, sheetID, rowID string, cells map[string]string) error {
	if !s.Available() {
		return fmt.Errorf("MongoDB not configured")
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	setFields := bson.D{}
	for k, v := range cells {
		fieldPath, err := mongoColumnFieldPath("manual", k)
		if err != nil {
			return err
		}
		setFields = append(setFields, bson.E{Key: fieldPath, Value: v})
	}
	setFields = append(setFields, bson.E{Key: "updated_at", Value: time.Now()})

	res, err := coll.UpdateOne(ctx,
		bson.D{
			{Key: "_id", Value: rowID},
			{Key: "sheet_id", Value: sheetID},
		},
		bson.D{{Key: "$set", Value: setFields}},
	)
	if err != nil {
		return fmt.Errorf("update cells: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrViewRowNotFound
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("row_id", rowID).
		Int("fields", len(cells)).
		Int64("matched", res.MatchedCount).
		Int64("modified", res.ModifiedCount).
		Msg("view: manual cell edit")
	return nil
}

// ClearManualCells removes manual overlays for the given rows and columns.
func (s *ViewStore) ClearManualCells(ctx context.Context, viewID, sheetID, componentID string, rowIDs, columnKeys []string) error {
	if !s.Available() || len(rowIDs) == 0 || len(columnKeys) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	unsetFields := bson.D{}
	for _, key := range columnKeys {
		fieldPath, err := mongoColumnFieldPath("manual", key)
		if err != nil {
			return err
		}
		unsetFields = append(unsetFields, bson.E{Key: fieldPath, Value: ""})
	}

	res, err := coll.UpdateMany(ctx,
		append(rowScopeFilter(sheetID, componentID), bson.E{Key: "_id", Value: bson.D{{Key: "$in", Value: rowIDs}}}),
		bson.D{
			{Key: "$unset", Value: unsetFields},
			{Key: "$set", Value: bson.D{{Key: "updated_at", Value: time.Now()}}},
		},
	)
	if err != nil {
		return fmt.Errorf("clear manual cells: %w", err)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Int("rows", len(rowIDs)).
		Int("columns", len(columnKeys)).
		Int64("matched", res.MatchedCount).
		Int64("modified", res.ModifiedCount).
		Msg("view: cleared manual cell overlays")
	return nil
}

// RenameColumn renames a column key in cells and manual across a component scope.
func (s *ViewStore) RenameColumn(ctx context.Context, viewID, sheetID, componentID, oldKey, newKey, schemaHash string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	cellsOldPath, err := mongoColumnFieldPath("cells", oldKey)
	if err != nil {
		return err
	}
	cellsNewPath, err := mongoColumnFieldPath("cells", newKey)
	if err != nil {
		return err
	}
	manualOldPath, err := mongoColumnFieldPath("manual", oldKey)
	if err != nil {
		return err
	}
	manualNewPath, err := mongoColumnFieldPath("manual", newKey)
	if err != nil {
		return err
	}
	update := bson.D{{Key: "$rename", Value: bson.D{
		{Key: cellsOldPath, Value: cellsNewPath},
		{Key: manualOldPath, Value: manualNewPath},
	}}}
	if strings.TrimSpace(schemaHash) != "" {
		update = append(update, bson.E{Key: "$set", Value: bson.D{{Key: "schema_hash", Value: schemaHash}}})
	}
	_, err = coll.UpdateMany(ctx,
		rowScopeFilter(sheetID, componentID),
		update,
	)
	if err != nil {
		return fmt.Errorf("rename column: %w", err)
	}
	return nil
}

// DeleteColumn removes a column key from cells and manual across a component scope.
func (s *ViewStore) DeleteColumn(ctx context.Context, viewID, sheetID, componentID, key, schemaHash string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	cellsPath, err := mongoColumnFieldPath("cells", key)
	if err != nil {
		return err
	}
	manualPath, err := mongoColumnFieldPath("manual", key)
	if err != nil {
		return err
	}
	update := bson.D{{Key: "$unset", Value: bson.D{
		{Key: cellsPath, Value: ""},
		{Key: manualPath, Value: ""},
	}}}
	if strings.TrimSpace(schemaHash) != "" {
		update = append(update, bson.E{Key: "$set", Value: bson.D{{Key: "schema_hash", Value: schemaHash}}})
	}
	_, err = coll.UpdateMany(ctx,
		rowScopeFilter(sheetID, componentID),
		update,
	)
	if err != nil {
		return fmt.Errorf("delete column: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Row exclusions — snapshot-based
// ---------------------------------------------------------------------------

func exclusionDocID(sheetID string) string {
	return "__exclusions:" + sheetID
}

// ExcludeRow snapshots a row's data into the per-sheet exclusion document,
// then hard-deletes the row. The snapshot is passed to the BAML mapper so it
// knows not to regenerate matching rows.
func (s *ViewStore) ExcludeRow(ctx context.Context, viewID, sheetID, rowID string) error {
	if !s.Available() {
		return fmt.Errorf("MongoDB not configured")
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	var row ViewRow
	if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: rowID}, {Key: "sheet_id", Value: sheetID}}).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return ErrViewRowNotFound
		}
		return fmt.Errorf("exclude row: read: %w", err)
	}

	snapshot := ExcludedRowSnapshot{
		TaskID: row.TaskID,
		RowKey: row.RowKey,
		Cells:  row.MergedCells(),
	}

	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: exclusionDocID(sheetID)}},
		bson.D{
			{Key: "$push", Value: bson.D{{Key: "rows", Value: snapshot}}},
			{Key: "$set", Value: bson.D{
				{Key: "sheet_id", Value: sheetID},
				{Key: "updated_at", Value: time.Now()},
			}},
		},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("exclude row: store snapshot: %w", err)
	}

	if _, err := coll.DeleteOne(ctx, bson.D{{Key: "_id", Value: rowID}}); err != nil {
		log.Warn().Err(err).Str("row_id", rowID).Msg("exclude row: failed to delete row document after snapshot")
	}
	return nil
}

// RestoreRow removes a snapshot from the exclusion document by matching
// task_id and row_key. The row will be regenerated on the next mapping pass.
func (s *ViewStore) RestoreRow(ctx context.Context, viewID, sheetID, rowID string) error {
	if !s.Available() {
		return fmt.Errorf("MongoDB not configured")
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	parts := strings.SplitN(rowID, ":", 4)
	var taskID, rowKey string
	if len(parts) >= 4 {
		taskID = parts[2]
		rowKey = parts[3]
	}
	if taskID == "" {
		return fmt.Errorf("restore row: cannot parse task_id from row_id %q", rowID)
	}

	pullFilter := bson.D{{Key: "task_id", Value: taskID}}
	if rowKey != "" {
		pullFilter = append(pullFilter, bson.E{Key: "row_key", Value: rowKey})
	}

	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: exclusionDocID(sheetID)}},
		bson.D{
			{Key: "$pull", Value: bson.D{{Key: "rows", Value: pullFilter}}},
			{Key: "$set", Value: bson.D{{Key: "updated_at", Value: time.Now()}}},
		},
	)
	if err != nil {
		return fmt.Errorf("restore row: %w", err)
	}
	return nil
}

// GetExcludedRows returns the exclusion snapshots for a sheet.
func (s *ViewStore) GetExcludedRows(ctx context.Context, viewID, sheetID string) ([]ExcludedRowSnapshot, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	var doc struct {
		Rows []ExcludedRowSnapshot `bson:"rows"`
	}
	err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: exclusionDocID(sheetID)}}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("get excluded rows: %w", err)
	}
	return doc.Rows, nil
}

// DeleteSheet removes all rows for a sheet.
func (s *ViewStore) DeleteSheet(ctx context.Context, viewID, sheetID string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	_, err := coll.DeleteMany(ctx, bson.D{{Key: "sheet_id", Value: sheetID}})
	if err != nil {
		return fmt.Errorf("delete sheet rows: %w", err)
	}
	return nil
}

// DropView drops the entire MongoDB collection for a view.
func (s *ViewStore) DropView(ctx context.Context, viewID string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	if err := coll.Drop(ctx); err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Msg("failed to drop view collection")
		return fmt.Errorf("drop view collection: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Widget data cache
// ---------------------------------------------------------------------------
// Widget rows — stored in the same collection as view rows, keyed by
// __widget:<sheetID>:<widgetID>.  Typed fields, no raw blobs.
// ---------------------------------------------------------------------------

// WidgetRow is the MongoDB document for resolved widget data.
type WidgetRow struct {
	ID         string              `bson:"_id"`
	SheetID    string              `bson:"sheet_id"`
	WidgetID   string              `bson:"widget_id"`
	Type       string              `bson:"type"`
	Status     string              `bson:"status"`
	Error      string              `bson:"error,omitempty"`
	SchemaHash string              `bson:"schema_hash"`
	Metric     *WidgetMetric       `bson:"metric,omitempty"`
	MapData    *WidgetMapData      `bson:"map_data,omitempty"`
	ListData   *WidgetListData     `bson:"list_data,omitempty"`
	UpdatedAt  time.Time           `bson:"updated_at"`
}

type WidgetMetric struct {
	Value      string `bson:"value"`
	Label      string `bson:"label"`
	Comparison string `bson:"comparison,omitempty"`
}

type WidgetMapData struct {
	Markers []WidgetMapMarker `bson:"markers"`
}

type WidgetMapMarker struct {
	Lat    float64 `bson:"lat"`
	Lng    float64 `bson:"lng"`
	Label  string  `bson:"label"`
	Detail string  `bson:"detail,omitempty"`
}

type WidgetListData struct {
	Items []WidgetListItem `bson:"items"`
}

type WidgetListItem struct {
	Label  string `bson:"label"`
	Value  string `bson:"value"`
	Detail string `bson:"detail,omitempty"`
}

func widgetRowID(sheetID, widgetID string) string {
	return "__widget:" + sheetID + ":" + widgetID
}

// GetWidgetRows loads all widget rows for a sheet.
func (s *ViewStore) GetWidgetRows(ctx context.Context, viewID, sheetID string) ([]WidgetRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	cursor, err := coll.Find(ctx, bson.D{
		{Key: "sheet_id", Value: sheetID},
		{Key: "widget_id", Value: bson.D{{Key: "$exists", Value: true}}},
	})
	if err != nil {
		return nil, fmt.Errorf("find widget rows: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []WidgetRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode widget rows: %w", err)
	}
	return rows, nil
}

// UpsertWidgetRow writes a single widget row.
func (s *ViewStore) UpsertWidgetRow(ctx context.Context, viewID string, row WidgetRow) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	row.ID = widgetRowID(row.SheetID, row.WidgetID)
	_, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: row.ID}},
		bson.D{{Key: "$set", Value: row}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("upsert widget row: %w", err)
	}
	return nil
}

// DeleteWidgetRows removes all widget rows for a sheet.
func (s *ViewStore) DeleteWidgetRows(ctx context.Context, viewID, sheetID string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	_, err := coll.DeleteMany(ctx, bson.D{
		{Key: "sheet_id", Value: sheetID},
		{Key: "widget_id", Value: bson.D{{Key: "$exists", Value: true}}},
	})
	if err != nil {
		return fmt.Errorf("delete widget rows: %w", err)
	}
	return nil
}
