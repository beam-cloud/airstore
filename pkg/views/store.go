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

func (s *ViewStore) GetRows(ctx context.Context, viewID, sheetID string) ([]ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{}
	if sheetID != "" {
		filter = append(filter, bson.E{Key: "sheet_id", Value: sheetID})
	}
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find rows: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode rows: %w", err)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("collection", s.collectionName(viewID)).
		Int("rows", len(rows)).
		Msg("mongo: loaded rows")
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
// Existing manual edits are preserved — only computed row data is replaced.
func (s *ViewStore) UpsertRows(ctx context.Context, viewID string, rows []ViewRow) error {
	if !s.Available() || len(rows) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	models := make([]mongo.WriteModel, 0, len(rows))
	for _, row := range rows {
		setFields := bson.D{
			{Key: "sheet_id", Value: row.SheetID},
			{Key: "group_id", Value: row.GroupID},
			{Key: "task_id", Value: row.TaskID},
			{Key: "row_key", Value: row.RowKey},
			{Key: "schema_hash", Value: row.SchemaHash},
			{Key: "output_ids", Value: row.OutputIDs},
			{Key: "source_output_ids", Value: row.SourceOutputIDs},
			{Key: "cells", Value: row.Cells},
			{Key: "updated_at", Value: row.UpdatedAt},
		}
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: row.ID}}).
			SetUpdate(bson.D{
				{Key: "$set", Value: setFields},
				{Key: "$setOnInsert", Value: bson.D{
					{Key: "manual", Value: bson.M{}},
				}},
			}).
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
		Msg("mongo: upserted rows")
	return nil
}

// DeleteRowsNotInGroups removes stale rows for the remapped groups.
func (s *ViewStore) DeleteRowsNotInGroups(ctx context.Context, viewID, sheetID string, groupIDs, keepRowIDs []string) error {
	if !s.Available() || len(groupIDs) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{
		{Key: "sheet_id", Value: sheetID},
		{Key: "group_id", Value: bson.D{{Key: "$in", Value: groupIDs}}},
	}
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
		Int("groups", len(groupIDs)).
		Int64("deleted", res.DeletedCount).
		Msg("mongo: deleted stale rows")
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
		Msg("mongo: manual cell edit")
	return nil
}

// ClearManualCells removes manual overlays for the given rows and columns.
func (s *ViewStore) ClearManualCells(ctx context.Context, viewID, sheetID string, rowIDs, columnKeys []string) error {
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
		bson.D{
			{Key: "sheet_id", Value: sheetID},
			{Key: "_id", Value: bson.D{{Key: "$in", Value: rowIDs}}},
		},
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
		Int("rows", len(rowIDs)).
		Int("columns", len(columnKeys)).
		Int64("matched", res.MatchedCount).
		Int64("modified", res.ModifiedCount).
		Msg("mongo: cleared manual cell overlays")
	return nil
}

// RenameColumn renames a column key in cells and manual across a sheet.
func (s *ViewStore) RenameColumn(ctx context.Context, viewID, sheetID, oldKey, newKey string) error {
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
	_, err = coll.UpdateMany(ctx,
		bson.D{{Key: "sheet_id", Value: sheetID}},
		bson.D{{Key: "$rename", Value: bson.D{
			{Key: cellsOldPath, Value: cellsNewPath},
			{Key: manualOldPath, Value: manualNewPath},
		}}},
	)
	if err != nil {
		return fmt.Errorf("rename column: %w", err)
	}
	return nil
}

// DeleteColumn removes a column key from cells and manual across a sheet.
func (s *ViewStore) DeleteColumn(ctx context.Context, viewID, sheetID, key string) error {
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
	_, err = coll.UpdateMany(ctx,
		bson.D{{Key: "sheet_id", Value: sheetID}},
		bson.D{{Key: "$unset", Value: bson.D{
			{Key: cellsPath, Value: ""},
			{Key: manualPath, Value: ""},
		}}},
	)
	if err != nil {
		return fmt.Errorf("delete column: %w", err)
	}
	return nil
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
