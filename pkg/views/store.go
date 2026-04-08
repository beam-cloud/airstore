package views

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/google/uuid"
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
	StableRef       string            `bson:"stable_ref,omitempty"`
	SheetID         string            `bson:"sheet_id"`
	ComponentID     string            `bson:"component_id,omitempty"`
	Marker          bool              `bson:"marker,omitempty"`
	GroupID         string            `bson:"group_id"`
	TaskID          string            `bson:"task_id"`
	RowKey          string            `bson:"row_key"`
	SchemaHash      string            `bson:"schema_hash"`
	OutputIDs       []string          `bson:"output_ids"`
	OutputSignature string            `bson:"output_signature,omitempty"`
	SourceOutputIDs []string          `bson:"source_output_ids,omitempty"`
	Cells           map[string]string `bson:"cells"`
	Manual          map[string]string `bson:"manual,omitempty"`
	Pinned          map[string]string `bson:"pinned,omitempty"`
	Source          string            `bson:"source,omitempty"`
	ImportID        string            `bson:"import_id,omitempty"`
	SearchText      string            `bson:"search_text,omitempty"`
	Embedding       []float64         `bson:"embedding,omitempty"`
	UpdatedAt       time.Time         `bson:"updated_at"`
}

const (
	RowSourceImport = "import"
	RowSourceSync   = "sync"
)

func (r *ViewRow) IsImport() bool {
	return r.Source == RowSourceImport
}

// ExcludedRowSnapshot is the data we store when a user deletes a row so the
// BAML mapper knows not to regenerate it.
type ExcludedRowSnapshot struct {
	ComponentID     string            `bson:"component_id,omitempty"`
	TaskID          string            `bson:"task_id"`
	RowKey          string            `bson:"row_key"`
	SourceOutputIDs []string          `bson:"source_output_ids,omitempty"`
	Cells           map[string]string `bson:"cells"`
}

// MergedCells returns the three-layer merge: pinned (import seed) -> cells (BAML-computed) -> manual (user edits).
func (r *ViewRow) MergedCells() map[string]string {
	if len(r.Pinned) == 0 && len(r.Manual) == 0 {
		return r.Cells
	}
	merged := make(map[string]string, len(r.Pinned)+len(r.Cells)+len(r.Manual))
	for k, v := range r.Pinned {
		merged[k] = v
	}
	for k, v := range r.Cells {
		merged[k] = v
	}
	for k, v := range r.Manual {
		merged[k] = v
	}
	return merged
}

type ViewStore struct {
	mongo    *common.MongoClient
	embedder *EmbeddingClient
}

func NewViewStore(mongo *common.MongoClient, openAIKey string) *ViewStore {
	return &ViewStore{
		mongo:    mongo,
		embedder: NewEmbeddingClient(openAIKey),
	}
}

func (s *ViewStore) Embedder() *EmbeddingClient {
	if s == nil {
		return nil
	}
	return s.embedder
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
	log.Debug().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Int("rows", len(rows)).
		Msg("view: loaded rows")
	return rows, nil
}

// GetRowsPage returns a paginated slice of rows and the total count.
// Uses MongoDB Skip/Limit for server-side pagination.
func (s *ViewStore) GetRowsPage(ctx context.Context, viewID, sheetID, componentID string, offset, limit int) ([]ViewRow, int, error) {
	if !s.Available() {
		return nil, 0, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := rowScopeFilter(sheetID, componentID)
	filter = append(filter, bson.E{Key: "_id", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$regex", Value: "^__"}}}}})

	total, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count rows: %w", err)
	}

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}

	opts := options.Find().
		SetSkip(int64(offset)).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "updated_at", Value: -1}})

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, int(total), fmt.Errorf("find rows page: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, int(total), fmt.Errorf("decode rows page: %w", err)
	}

	log.Debug().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Int("offset", offset).
		Int("limit", limit).
		Int("returned", len(rows)).
		Int("total", int(total)).
		Msg("view: loaded rows page")
	return rows, int(total), nil
}

// SearchRowsText performs a case-insensitive free-text search across the
// search_text field (populated by autoEmbed) and falls back to a regex scan
// across all cell values. Returns paginated results with total count.
func (s *ViewStore) SearchRowsText(ctx context.Context, viewID, sheetID, componentID, query string, offset, limit int) ([]ViewRow, int, error) {
	if !s.Available() || strings.TrimSpace(query) == "" {
		return nil, 0, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	// Split query into meaningful words and require ALL to appear in
	// search_text. Stop words and very short tokens are filtered so that
	// queries like "email about 201 3rd St" work (only "201", "3rd", "St"
	// are required).
	words := strings.Fields(strings.TrimSpace(query))
	var andClauses bson.A
	for _, w := range words {
		if len(w) < 2 || searchStopWords[strings.ToLower(w)] {
			continue
		}
		andClauses = append(andClauses, bson.M{
			"search_text": bson.M{"$regex": regexEscape(w), "$options": "i"},
		})
	}
	if len(andClauses) == 0 {
		return nil, 0, nil
	}

	filter := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$regex", Value: "^__"}}}}},
		{Key: "$and", Value: andClauses},
	}
	if strings.TrimSpace(sheetID) != "" {
		filter = append(filter, bson.E{Key: "sheet_id", Value: sheetID})
	}
	if strings.TrimSpace(componentID) != "" {
		filter = append(filter, bson.E{Key: "component_id", Value: componentID})
	}

	total, err := coll.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, fmt.Errorf("count search rows: %w", err)
	}

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}

	opts := options.Find().
		SetSkip(int64(offset)).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "updated_at", Value: -1}})

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, int(total), fmt.Errorf("search rows text: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, int(total), fmt.Errorf("decode search rows: %w", err)
	}

	log.Debug().
		Str("view_id", viewID).
		Str("query", query).
		Int("offset", offset).
		Int("limit", limit).
		Int("returned", len(rows)).
		Int("total", int(total)).
		Msg("view: text search rows")
	return rows, int(total), nil
}

// GetRowTaskIndex loads only the _id, task_id, and thread_id fields from all
// rows in a view. Used by the mailbox to map tasks to rows without loading
// full cell data.
func (s *ViewStore) GetRowTaskIndex(ctx context.Context, viewID string) ([]ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$regex", Value: "^__"}}}}}}

	opts := options.Find().SetProjection(bson.D{
		{Key: "_id", Value: 1},
		{Key: "task_id", Value: 1},
		{Key: "sheet_id", Value: 1},
		{Key: "component_id", Value: 1},
		{Key: "row_key", Value: 1},
		{Key: "stable_ref", Value: 1},
		{Key: "source", Value: 1},
	})

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find row task index: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode row task index: %w", err)
	}
	return rows, nil
}

// GetRowsForMailbox loads all rows with cell data but excludes the large
// embedding and search_text fields. The mailbox needs MergedCells() for
// thread_id extraction and row labels.
func (s *ViewStore) GetRowsForMailbox(ctx context.Context, viewID string) ([]ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	// Only fetch rows that are relevant to the mailbox: rows with a task_id
	// (for task→output association) or a thread_id cell (for direct thread
	// discovery). This avoids loading the entire collection for large views.
	filter := bson.D{
		{Key: "_id", Value: bson.D{{Key: "$not", Value: bson.D{{Key: "$regex", Value: "^__"}}}}},
		{Key: "$or", Value: bson.A{
			bson.D{{Key: "task_id", Value: bson.D{{Key: "$exists", Value: true}, {Key: "$ne", Value: ""}}}},
			bson.D{{Key: "cells.thread_id", Value: bson.D{{Key: "$exists", Value: true}, {Key: "$ne", Value: ""}}}},
			bson.D{{Key: "agent_cells.thread_id", Value: bson.D{{Key: "$exists", Value: true}, {Key: "$ne", Value: ""}}}},
		}},
	}

	opts := options.Find().SetProjection(bson.D{
		{Key: "embedding", Value: 0},
		{Key: "search_text", Value: 0},
	})

	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find mailbox rows: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode mailbox rows: %w", err)
	}
	return rows, nil
}

// GetRowByID loads a row by _id only (no sheet filter). Used for detail layout cache lookups.
func (s *ViewStore) GetRowByID(ctx context.Context, viewID, rowID string) (*ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	var row ViewRow
	if err := coll.FindOne(ctx, bson.D{
		{Key: "_id", Value: rowID},
	}).Decode(&row); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("find row by id: %w", err)
	}
	return &row, nil
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
// row data is replaced. Automatically computes embeddings for rows that
// don't already have them when an embedding client is configured.
func (s *ViewStore) UpsertRows(ctx context.Context, viewID string, rows []ViewRow) error {
	if !s.Available() || len(rows) == 0 {
		return nil
	}

	s.autoEmbed(ctx, rows)

	return s.upsertRowsBulk(ctx, viewID, rows)
}

// UpsertRowsNoEmbed inserts/updates rows without running embeddings.
// Use when embeddings will be generated asynchronously (e.g. during large imports).
func (s *ViewStore) UpsertRowsNoEmbed(ctx context.Context, viewID string, rows []ViewRow) error {
	if !s.Available() || len(rows) == 0 {
		return nil
	}

	// Still build search_text for text-based search even without embeddings.
	for i := range rows {
		rows[i].SearchText = buildSearchText(rows[i])
	}

	return s.upsertRowsBulk(ctx, viewID, rows)
}

// EmbedRowsAsync kicks off embedding generation in the background for the given view.
func (s *ViewStore) EmbedRowsAsync(viewID string) {
	if s == nil || !s.Available() || s.embedder == nil || !s.embedder.Available() {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		rows, err := s.GetRows(ctx, viewID, "", "")
		if err != nil {
			log.Warn().Err(err).Str("view_id", viewID).Msg("async embed: failed to load rows")
			return
		}

		needEmbed := make([]ViewRow, 0)
		for _, r := range rows {
			if len(r.Embedding) == 0 && r.SearchText != "" {
				needEmbed = append(needEmbed, r)
			}
		}
		if len(needEmbed) == 0 {
			return
		}

		log.Info().Str("view_id", viewID).Int("rows", len(needEmbed)).Msg("async embed: starting background embedding")

		const batchSize = 2000
		embedded := 0
		for i := 0; i < len(needEmbed); i += batchSize {
			if ctx.Err() != nil {
				break
			}
			end := i + batchSize
			if end > len(needEmbed) {
				end = len(needEmbed)
			}
			batch := needEmbed[i:end]
			s.autoEmbed(ctx, batch)

			updates := make([]EmbeddingUpdate, 0, len(batch))
			for _, r := range batch {
				if len(r.Embedding) > 0 {
					updates = append(updates, EmbeddingUpdate{
						RowID:      r.ID,
						SearchText: r.SearchText,
						Embedding:  r.Embedding,
					})
				}
			}
			if len(updates) == 0 {
				continue
			}
			if err := s.BulkUpdateEmbeddings(ctx, viewID, updates); err != nil {
				log.Warn().Err(err).Str("view_id", viewID).Int("batch", i/batchSize).Msg("async embed: failed to persist batch")
				continue
			}
			embedded += len(updates)
			log.Info().Str("view_id", viewID).Int("embedded", embedded).Int("total", len(needEmbed)).Msg("async embed: progress")
		}
		log.Info().Str("view_id", viewID).Int("embedded", embedded).Msg("async embed: complete")
	}()
}

func buildSearchText(row ViewRow) string {
	return RowSearchText(&row)
}

func (s *ViewStore) upsertRowsBulk(ctx context.Context, viewID string, rows []ViewRow) error {
	coll := s.mongo.Collection(s.collectionName(viewID))

	models := make([]mongo.WriteModel, 0, len(rows))
	for _, row := range rows {
		setFields := bson.D{
			{Key: "sheet_id", Value: row.SheetID},
			{Key: "component_id", Value: row.ComponentID},
			{Key: "marker", Value: row.Marker},
			{Key: "group_id", Value: row.GroupID},
			{Key: "task_id", Value: row.TaskID},
			{Key: "row_key", Value: row.RowKey},
			{Key: "schema_hash", Value: row.SchemaHash},
			{Key: "output_ids", Value: row.OutputIDs},
			{Key: "output_signature", Value: row.OutputSignature},
			{Key: "source_output_ids", Value: row.SourceOutputIDs},
			{Key: "cells", Value: row.Cells},
			{Key: "updated_at", Value: row.UpdatedAt},
		}

		if len(row.Manual) > 0 {
			setFields = append(setFields, bson.E{Key: "manual", Value: row.Manual})
		}
		if row.Source != "" {
			setFields = append(setFields, bson.E{Key: "source", Value: row.Source})
		}
		if row.ImportID != "" {
			setFields = append(setFields, bson.E{Key: "import_id", Value: row.ImportID})
		}
		if len(row.Pinned) > 0 {
			setFields = append(setFields, bson.E{Key: "pinned", Value: row.Pinned})
		}
		if row.SearchText != "" {
			setFields = append(setFields, bson.E{Key: "search_text", Value: row.SearchText})
		}
		if len(row.Embedding) > 0 {
			setFields = append(setFields, bson.E{Key: "embedding", Value: row.Embedding})
		}

		stableRef := row.StableRef
		if stableRef == "" {
			stableRef = uuid.New().String()
		}
		insertOnly := bson.D{
			{Key: "stable_ref", Value: stableRef},
		}
		if len(row.Manual) == 0 {
			insertOnly = append(insertOnly, bson.E{Key: "manual", Value: bson.M{}})
		}

		update := bson.D{
			{Key: "$set", Value: setFields},
			{Key: "$setOnInsert", Value: insertOnly},
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

// UpdateSchemaHash stamps a new schema_hash on all rows in a component scope
// without remapping cells. Used when columns are added — missing cell keys
// render as empty so no cell mutation is needed.
func (s *ViewStore) UpdateSchemaHash(ctx context.Context, viewID, sheetID, componentID, schemaHash string) error {
	if !s.Available() || strings.TrimSpace(schemaHash) == "" {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	res, err := coll.UpdateMany(ctx,
		rowScopeFilter(sheetID, componentID),
		bson.D{{Key: "$set", Value: bson.D{
			{Key: "schema_hash", Value: schemaHash},
			{Key: "updated_at", Value: time.Now()},
		}}},
	)
	if err != nil {
		return fmt.Errorf("update schema hash: %w", err)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("component_id", componentID).
		Str("schema_hash", schemaHash).
		Int64("matched", res.MatchedCount).
		Int64("modified", res.ModifiedCount).
		Msg("view: updated schema hash for column addition")
	return nil
}

// EnsureIndexes creates indexes required for the view collection, including
// a unique index on stable_ref.
func (s *ViewStore) EnsureIndexes(ctx context.Context, viewID string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "stable_ref", Value: 1}},
		Options: options.Index().SetUnique(true).SetSparse(true),
	})
	if err != nil {
		return fmt.Errorf("ensure stable_ref index: %w", err)
	}
	return nil
}

// DeleteRowsNotInGroups removes stale rows for the remapped groups.
// Import-sourced rows are always protected from deletion.
func (s *ViewStore) DeleteRowsNotInGroups(ctx context.Context, viewID, sheetID, componentID string, groupIDs, keepRowIDs []string) error {
	if !s.Available() || len(groupIDs) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := rowScopeFilter(sheetID, componentID)
	filter = append(filter, bson.E{Key: "group_id", Value: bson.D{{Key: "$in", Value: groupIDs}}})
	filter = append(filter, bson.E{Key: "source", Value: bson.D{{Key: "$ne", Value: RowSourceImport}}})
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

// DeleteRowsByIDs removes specific rows by their document IDs.
func (s *ViewStore) DeleteRowsByIDs(ctx context.Context, viewID string, rowIDs []string) error {
	if !s.Available() || len(rowIDs) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: rowIDs}}}}
	_, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete rows by IDs: %w", err)
	}
	return nil
}

// GetRowsBySource returns rows filtered by source (e.g. "import") within a component scope.
func (s *ViewStore) GetRowsBySource(ctx context.Context, viewID, sheetID, componentID, source string) ([]ViewRow, error) {
	if !s.Available() {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := rowScopeFilter(sheetID, componentID)
	filter = append(filter, bson.E{Key: "source", Value: source})
	cursor, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("find rows by source: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode rows by source: %w", err)
	}
	return rows, nil
}

// CleanupStaleImportRows removes import-source rows that weren't part of the
// current import batch. Called after upsert so valid rows are already written —
// this cleans up orphans from previous imports (different row count or legacy
// ID format) without a destructive delete-before-insert window.
func (s *ViewStore) CleanupStaleImportRows(ctx context.Context, viewID, sheetID, componentID string, keepIDs []string) (int64, error) {
	if !s.Available() || len(keepIDs) == 0 {
		return 0, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := rowScopeFilter(sheetID, componentID)
	filter = append(filter,
		bson.E{Key: "source", Value: RowSourceImport},
		bson.E{Key: "_id", Value: bson.D{{Key: "$nin", Value: keepIDs}}},
	)
	res, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("cleanup stale import rows: %w", err)
	}
	return res.DeletedCount, nil
}

// DeleteImport removes all rows from a specific import batch.
func (s *ViewStore) DeleteImport(ctx context.Context, viewID, sheetID, importID string) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{
		{Key: "sheet_id", Value: sheetID},
		{Key: "source", Value: RowSourceImport},
		{Key: "import_id", Value: importID},
	}
	res, err := coll.DeleteMany(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete import: %w", err)
	}
	log.Info().
		Str("view_id", viewID).
		Str("sheet_id", sheetID).
		Str("import_id", importID).
		Int64("deleted", res.DeletedCount).
		Msg("view: deleted import rows")
	return nil
}

// EnrichImportRow updates the Cells and task association on an existing import row
// without changing its Pinned values or identity.
func (s *ViewStore) EnrichImportRow(ctx context.Context, viewID string, row ViewRow) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	setFields := bson.D{
		{Key: "cells", Value: row.Cells},
		{Key: "task_id", Value: row.TaskID},
		{Key: "source_output_ids", Value: row.SourceOutputIDs},
		{Key: "output_ids", Value: row.OutputIDs},
		{Key: "output_signature", Value: row.OutputSignature},
		{Key: "schema_hash", Value: row.SchemaHash},
		{Key: "updated_at", Value: time.Now()},
	}
	res, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: row.ID}},
		bson.D{{Key: "$set", Value: setFields}},
	)
	if err != nil {
		return fmt.Errorf("enrich import row: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrViewRowNotFound
	}
	return nil
}

// MergeCells performs a cell-level merge on an existing row's computed cells.
// Only non-empty values in newCells are written; empty strings are skipped
// (meaning "leave the existing value as-is"). The outputID is appended to
// the row's output tracking arrays.
func (s *ViewStore) MergeCells(ctx context.Context, viewID, rowID string, newCells map[string]string, outputID string) error {
	if !s.Available() {
		return fmt.Errorf("MongoDB not configured")
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	setFields := bson.D{}
	for k, v := range newCells {
		if v == "" {
			continue
		}
		fieldPath, err := mongoColumnFieldPath("cells", k)
		if err != nil {
			return err
		}
		setFields = append(setFields, bson.E{Key: fieldPath, Value: v})
	}
	if len(setFields) == 0 && outputID == "" {
		return nil
	}
	setFields = append(setFields, bson.E{Key: "updated_at", Value: time.Now()})

	update := bson.D{{Key: "$set", Value: setFields}}
	if outputID != "" {
		update = append(update, bson.E{
			Key: "$addToSet",
			Value: bson.D{
				{Key: "output_ids", Value: outputID},
				{Key: "source_output_ids", Value: outputID},
			},
		})
	}

	res, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: rowID}},
		update,
	)
	if err != nil && outputID != "" && strings.Contains(err.Error(), "non-array") {
		// output_ids field was null (e.g. import rows). Initialize it and retry.
		initArrays := bson.D{
			{Key: "$set", Value: bson.D{
				{Key: "output_ids", Value: bson.A{outputID}},
				{Key: "source_output_ids", Value: bson.A{outputID}},
			}},
		}
		for _, f := range setFields {
			initArrays[0].Value = append(initArrays[0].Value.(bson.D), f)
		}
		res, err = coll.UpdateOne(ctx, bson.D{{Key: "_id", Value: rowID}}, initArrays)
	}
	if err != nil {
		return fmt.Errorf("merge cells: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrViewRowNotFound
	}
	log.Debug().
		Str("view_id", viewID).
		Str("row_id", rowID).
		Int("cells", len(newCells)).
		Msg("view: merged cells")
	return nil
}

// ReembedRow fetches the latest state of a row, recomputes its search_text
// and embedding, and writes them back. Call after MergeCells to keep the
// vector index current. No-op if the embedder is not configured.
func (s *ViewStore) ReembedRow(ctx context.Context, viewID, rowID string) {
	if s.embedder == nil || !s.embedder.Available() || rowID == "" {
		return
	}
	row, err := s.GetRowByID(ctx, viewID, rowID)
	if err != nil || row == nil {
		return
	}
	searchText := RowSearchText(row)
	if searchText == "" {
		return
	}
	vec, err := s.embedder.EmbedOne(ctx, searchText)
	if err != nil {
		log.Debug().Err(err).Str("row_id", rowID).Msg("reembed: embed failed")
		return
	}
	if err := s.UpdateRowEmbedding(ctx, viewID, rowID, searchText, vec); err != nil {
		log.Debug().Err(err).Str("row_id", rowID).Msg("reembed: update failed")
	}
}

// autoEmbed computes search_text and embedding for rows that don't already
// have them. Called automatically by UpsertRows.
func (s *ViewStore) autoEmbed(ctx context.Context, rows []ViewRow) {
	if s.embedder == nil || !s.embedder.Available() || len(rows) == 0 {
		return
	}

	var needIdx []int
	var texts []string
	for i := range rows {
		if len(rows[i].Embedding) > 0 {
			continue
		}
		st := RowSearchText(&rows[i])
		if st == "" {
			continue
		}
		rows[i].SearchText = st
		needIdx = append(needIdx, i)
		texts = append(texts, st)
	}
	if len(texts) == 0 {
		return
	}

	vecs, err := s.embedder.Embed(ctx, texts)
	if err != nil {
		log.Warn().Err(err).Int("rows", len(texts)).Msg("autoEmbed: failed, proceeding without")
		return
	}
	for j, idx := range needIdx {
		if j < len(vecs) {
			rows[idx].Embedding = vecs[j]
		}
	}
}

// UpdateRowEmbedding recomputes and stores the search_text and embedding for
// a single row. Call after MergeCells to keep the vector index current.
func (s *ViewStore) UpdateRowEmbedding(ctx context.Context, viewID, rowID string, searchText string, embedding []float64) error {
	if !s.Available() || rowID == "" {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	setFields := bson.D{
		{Key: "search_text", Value: searchText},
		{Key: "embedding", Value: embedding},
	}

	res, err := coll.UpdateOne(ctx,
		bson.D{{Key: "_id", Value: rowID}},
		bson.D{{Key: "$set", Value: setFields}},
	)
	if err != nil {
		return fmt.Errorf("update row embedding: %w", err)
	}
	if res.MatchedCount == 0 {
		return ErrViewRowNotFound
	}
	return nil
}

// EmbeddingUpdate holds the fields needed to persist an embedding without
// touching any other row data (avoids overwriting concurrent cell updates).
type EmbeddingUpdate struct {
	RowID      string
	SearchText string
	Embedding  []float64
}

// BulkUpdateEmbeddings writes search_text and embedding for many rows in a
// single bulk operation. Only those two fields are $set — cells, pinned, manual
// etc. are untouched, eliminating the read-modify-write race in EmbedRowsAsync.
func (s *ViewStore) BulkUpdateEmbeddings(ctx context.Context, viewID string, updates []EmbeddingUpdate) error {
	if !s.Available() || len(updates) == 0 {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	models := make([]mongo.WriteModel, 0, len(updates))
	for _, u := range updates {
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.D{{Key: "_id", Value: u.RowID}}).
			SetUpdate(bson.D{{Key: "$set", Value: bson.D{
				{Key: "search_text", Value: u.SearchText},
				{Key: "embedding", Value: u.Embedding},
			}}}))
	}

	_, err := coll.BulkWrite(ctx, models)
	if err != nil {
		return fmt.Errorf("bulk update embeddings: %w", err)
	}
	return nil
}

// FindByStableRef looks up a row by its stable_ref within a sheet scope.
// Returns nil if not found.
func (s *ViewStore) FindByStableRef(ctx context.Context, viewID, sheetID, ref string) (*ViewRow, error) {
	if !s.Available() || ref == "" {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{
		{Key: "sheet_id", Value: sheetID},
		{Key: "stable_ref", Value: ref},
	}
	var row ViewRow
	err := coll.FindOne(ctx, filter).Decode(&row)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("find by stable ref: %w", err)
	}
	return &row, nil
}

// FindRows performs an indexed text search for rows matching a column/value
// pair within a sheet scope. Uses regex-based SearchRows under the hood.
func (s *ViewStore) FindRows(ctx context.Context, viewID, sheetID, column, value string, limit int) ([]ViewRow, error) {
	if !s.Available() || column == "" || value == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = 50
	}
	return s.SearchRows(ctx, viewID, sheetID, "", []SearchCriterion{{Column: column, Value: value}}, limit)
}

// UpsertOpts carries optional metadata for UpsertRow. ViewSync passes these;
// ViewClient can leave them empty.
type UpsertOpts struct {
	TaskID     string
	GroupID    string
	OutputID   string
	SchemaHash string
	RowKey     string // override auto-derived key
}

// UpsertRow is the single entry point for creating or merging a row.
// It derives the row key from cells (schema-agnostic), normalizes it, sets
// StableRef, checks for an existing row via FindRowByKey / FindByStableRef,
// and either merges into the existing row or creates a new one.
// Returns (rowID, created, error).
func (s *ViewStore) UpsertRow(ctx context.Context, viewID, sheetID, componentID string, cells map[string]string, opts UpsertOpts) (string, bool, error) {
	if !s.Available() {
		return "", false, fmt.Errorf("MongoDB not configured")
	}
	if len(cells) == 0 {
		return "", false, fmt.Errorf("cells cannot be empty")
	}

	rowKey := opts.RowKey
	if rowKey == "" {
		rowKey = deriveRowKey(cells)
	}
	nk := NormalizeRowKey(rowKey)
	if nk == "" {
		nk = fmt.Sprintf("auto-%d", time.Now().UnixMilli())
	}

	// Try to find an existing row by key or stable ref.
	existing, _ := s.FindRowByKey(ctx, viewID, sheetID, componentID, nk)
	if existing == nil {
		existing, _ = s.FindByStableRef(ctx, viewID, sheetID, nk)
	}

	if existing != nil {
		if err := s.MergeCells(ctx, viewID, existing.ID, cells, opts.OutputID); err != nil {
			return "", false, fmt.Errorf("upsert merge: %w", err)
		}
		s.ReembedRow(ctx, viewID, existing.ID)
		return existing.ID, false, nil
	}

	rowID := fmt.Sprintf("%s:%s:%s", sheetID, componentID, nk)
	groupID := opts.GroupID
	if groupID == "" {
		groupID = opts.TaskID
	}
	row := ViewRow{
		ID:          rowID,
		StableRef:   nk,
		SheetID:     sheetID,
		ComponentID: componentID,
		GroupID:     groupID,
		TaskID:      opts.TaskID,
		RowKey:      nk,
		SchemaHash:  opts.SchemaHash,
		Cells:       cells,
		UpdatedAt:   time.Now(),
	}
	if opts.OutputID != "" {
		row.OutputIDs = []string{opts.OutputID}
		row.OutputSignature = opts.OutputID
		row.SourceOutputIDs = []string{opts.OutputID}
	}

	if err := s.UpsertRows(ctx, viewID, []ViewRow{row}); err != nil {
		return "", false, fmt.Errorf("upsert insert: %w", err)
	}
	return rowID, true, nil
}

// UpdateRow merges cells into an existing row and recomputes its embedding.
// A convenience wrapper around MergeCells + ReembedRow.
func (s *ViewStore) UpdateRow(ctx context.Context, viewID, rowID string, cells map[string]string, outputID string) error {
	if err := s.MergeCells(ctx, viewID, rowID, cells, outputID); err != nil {
		return err
	}
	s.ReembedRow(ctx, viewID, rowID)
	return nil
}

// maxRowKeyCells caps how many cells contribute to the row key hash.
// Using only the first N alphabetically sorted cells makes the key stable when
// new columns are added later (e.g., enrichment adds zip+rent to an existing
// address row). The real dedup intelligence lives in ClassifyRowMatch/vector
// search; this is the last-resort deterministic fallback.
const maxRowKeyCells = 4

func deriveRowKey(cells map[string]string) string {
	if len(cells) == 0 {
		return ""
	}

	keys := make([]string, 0, len(cells))
	for k, v := range cells {
		if strings.TrimSpace(v) == "" {
			continue
		}
		if strings.HasPrefix(k, "_") || k == "thread_id" {
			continue
		}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return ""
	}

	sort.Strings(keys)
	if len(keys) > maxRowKeyCells {
		keys = keys[:maxRowKeyCells]
	}

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value := canonicalRowKeyValue(cells[key])
		if value == "" {
			continue
		}
		parts = append(parts, key+"="+value)
	}
	if len(parts) == 0 {
		return ""
	}

	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return "rk_" + hex.EncodeToString(sum[:16])
}

func canonicalRowKeyValue(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	return strings.Join(strings.Fields(value), " ")
}

// FindRowByKey looks up a row by its normalized row_key within a specific
// sheet/component scope. Returns nil if not found.
func (s *ViewStore) FindRowByKey(ctx context.Context, viewID, sheetID, componentID, rowKey string) (*ViewRow, error) {
	if !s.Available() || rowKey == "" {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))
	filter := bson.D{
		{Key: "sheet_id", Value: sheetID},
		{Key: "component_id", Value: componentID},
		{Key: "row_key", Value: rowKey},
	}
	var row ViewRow
	err := coll.FindOne(ctx, filter).Decode(&row)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, fmt.Errorf("find row by key: %w", err)
	}
	return &row, nil
}

// NormalizeRowKey normalizes a row key for consistent dedup across tasks.
func NormalizeRowKey(key string) string {
	return normalizeToken(key)
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

	// Rebuild search_text so the updated values are searchable.
	go func() {
		bgCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		row, err := s.GetRow(bgCtx, viewID, sheetID, rowID)
		if err != nil || row == nil {
			return
		}
		newSearchText := buildSearchText(*row)
		if newSearchText == row.SearchText {
			return
		}
		_, _ = coll.UpdateOne(bgCtx,
			bson.D{{Key: "_id", Value: rowID}},
			bson.D{{Key: "$set", Value: bson.D{{Key: "search_text", Value: newSearchText}}}},
		)
	}()

	return nil
}

// ClearManualCells removes manual overlays for the given rows and columns.
// Import-sourced rows are skipped — their pinned values are never cleared.
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

	filter := append(rowScopeFilter(sheetID, componentID),
		bson.E{Key: "_id", Value: bson.D{{Key: "$in", Value: rowIDs}}},
		bson.E{Key: "source", Value: bson.D{{Key: "$ne", Value: RowSourceImport}}},
	)

	res, err := coll.UpdateMany(ctx,
		filter,
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
		ComponentID:     row.ComponentID,
		TaskID:          row.TaskID,
		RowKey:          row.RowKey,
		SourceOutputIDs: append([]string(nil), row.SourceOutputIDs...),
		Cells:           row.MergedCells(),
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

// SearchCriterion is a column + value pair for targeted row lookups.
type SearchCriterion struct {
	Column string
	Value  string
}

// SearchRowsAnd finds rows where ALL criteria match (AND logic). Each
// criterion checks cells.column OR pinned.column. This is used for identity-
// based dedup where we need precise matches (e.g. same address AND city).
func (s *ViewStore) SearchRowsAnd(ctx context.Context, viewID, sheetID, componentID string, criteria []SearchCriterion, maxResults int) ([]ViewRow, error) {
	if !s.Available() || len(criteria) == 0 {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	andClauses := make(bson.A, 0, len(criteria))
	for _, c := range criteria {
		if c.Column == "" || c.Value == "" {
			continue
		}
		if strings.Contains(c.Column, ".") || strings.Contains(c.Column, "$") {
			continue
		}
		escaped := regexEscape(c.Value)
		pattern := bson.M{"$regex": escaped, "$options": "i"}
		andClauses = append(andClauses, bson.M{"$or": bson.A{
			bson.M{"cells." + c.Column: pattern},
			bson.M{"pinned." + c.Column: pattern},
		}})
	}
	if len(andClauses) == 0 {
		return nil, nil
	}

	filter := bson.M{"$and": andClauses}
	if sheetID != "" {
		filter["sheet_id"] = sheetID
	}
	if componentID != "" {
		filter["component_id"] = componentID
	}

	if maxResults <= 0 {
		maxResults = 50
	}
	opts := options.Find().SetLimit(int64(maxResults))
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("search rows and: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode search rows and: %w", err)
	}

	log.Debug().
		Str("view_id", viewID).
		Int("criteria", len(criteria)).
		Int("results", len(rows)).
		Msg("view: search rows (AND)")
	return rows, nil
}

// SearchRows finds rows where any cell (cells or pinned) matches one of the
// search criteria using case-insensitive regex. Returns at most maxResults rows.
// This enables targeted lookups instead of loading the entire collection.
func (s *ViewStore) SearchRows(ctx context.Context, viewID, sheetID, componentID string, criteria []SearchCriterion, maxResults int) ([]ViewRow, error) {
	if !s.Available() || len(criteria) == 0 {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	orClauses := make([]bson.M, 0, len(criteria)*2)
	for _, c := range criteria {
		if c.Column == "" || c.Value == "" {
			continue
		}
		if strings.Contains(c.Column, ".") || strings.Contains(c.Column, "$") {
			continue
		}
		escaped := regexEscape(c.Value)
		pattern := bson.M{"$regex": escaped, "$options": "i"}
		orClauses = append(orClauses,
			bson.M{"cells." + c.Column: pattern},
			bson.M{"pinned." + c.Column: pattern},
		)
	}
	if len(orClauses) == 0 {
		return nil, nil
	}

	filter := bson.M{"$or": orClauses}
	if sheetID != "" {
		filter["sheet_id"] = sheetID
	}
	if componentID != "" {
		filter["component_id"] = componentID
	}

	if maxResults <= 0 {
		maxResults = 50
	}
	opts := options.Find().SetLimit(int64(maxResults))
	cursor, err := coll.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("search rows: %w", err)
	}
	defer cursor.Close(ctx)

	var rows []ViewRow
	if err := cursor.All(ctx, &rows); err != nil {
		return nil, fmt.Errorf("decode search rows: %w", err)
	}

	log.Debug().
		Str("view_id", viewID).
		Int("criteria", len(criteria)).
		Int("results", len(rows)).
		Msg("view: search rows")
	return rows, nil
}

var searchStopWords = map[string]bool{
	"a": true, "an": true, "the": true, "is": true, "at": true,
	"in": true, "on": true, "of": true, "to": true, "for": true,
	"by": true, "or": true, "and": true, "but": true, "not": true,
	"with": true, "from": true, "about": true, "into": true,
	"this": true, "that": true, "it": true, "its": true,
	"was": true, "were": true, "are": true, "been": true,
	"has": true, "had": true, "have": true, "will": true,
	"can": true, "may": true, "do": true, "does": true, "did": true,
	"email": true, "sent": true, "inquiry": true, "regarding": true,
	"property": true, "space": true, "commercial": true, "retail": true,
	"lease": true, "rental": true, "available": true, "listing": true,
	"outreach": true, "update": true, "status": true,
}

func regexEscape(s string) string {
	special := `\.+*?^$()[]{}|`
	var b strings.Builder
	for _, c := range s {
		if strings.ContainsRune(special, c) {
			b.WriteRune('\\')
		}
		b.WriteRune(c)
	}
	return b.String()
}

// ---------------------------------------------------------------------------
// Vector Search (Atlas)
// ---------------------------------------------------------------------------

const vectorIndexName = "embedding_vector_index"

// EnsureVectorIndex creates the vectorSearch index for a view collection if it
// doesn't already exist. Safe to call repeatedly — it's a no-op when the index
// is already present.
func (s *ViewStore) EnsureVectorIndex(ctx context.Context, viewID string, dims int) error {
	if !s.Available() {
		return nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	cursor, err := coll.SearchIndexes().List(ctx, options.SearchIndexes().SetName(vectorIndexName))
	if err == nil {
		var existing []bson.M
		if err := cursor.All(ctx, &existing); err == nil && len(existing) > 0 {
			return nil
		}
	}

	definition := bson.D{
		{Key: "fields", Value: bson.A{
			bson.D{
				{Key: "type", Value: "vector"},
				{Key: "path", Value: "embedding"},
				{Key: "numDimensions", Value: dims},
				{Key: "similarity", Value: "cosine"},
			},
			bson.D{
				{Key: "type", Value: "filter"},
				{Key: "path", Value: "sheet_id"},
			},
			bson.D{
				{Key: "type", Value: "filter"},
				{Key: "path", Value: "component_id"},
			},
		}},
	}

	opts := options.SearchIndexes().
		SetName(vectorIndexName).
		SetType("vectorSearch")

	model := mongo.SearchIndexModel{
		Definition: definition,
		Options:    opts,
	}

	_, err = coll.SearchIndexes().CreateOne(ctx, model)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "duplicate") {
			return nil
		}
		return fmt.Errorf("create vector search index: %w", err)
	}

	log.Info().
		Str("view_id", viewID).
		Str("index", vectorIndexName).
		Int("dims", dims).
		Msg("view: created vector search index")
	return nil
}

// VectorSearchResult wraps a ViewRow with its similarity score.
type VectorSearchResult struct {
	ViewRow `bson:",inline"`
	Score   float64 `bson:"vs_score"`
}

// VectorSearch runs a $vectorSearch aggregation against the view collection.
// Returns up to `limit` rows pre-filtered by sheetID, ranked by cosine similarity.
func (s *ViewStore) VectorSearch(ctx context.Context, viewID, sheetID string, queryEmbedding []float64, limit int) ([]VectorSearchResult, error) {
	if !s.Available() || len(queryEmbedding) == 0 {
		return nil, nil
	}
	coll := s.mongo.Collection(s.collectionName(viewID))

	if limit <= 0 {
		limit = 20
	}
	numCandidates := limit * 5
	if numCandidates < 100 {
		numCandidates = 100
	}

	vectorSearchStage := bson.D{
		{Key: "$vectorSearch", Value: bson.D{
			{Key: "index", Value: vectorIndexName},
			{Key: "path", Value: "embedding"},
			{Key: "queryVector", Value: queryEmbedding},
			{Key: "numCandidates", Value: numCandidates},
			{Key: "limit", Value: limit},
		}},
	}

	if sheetID != "" {
		vectorSearchStage = bson.D{
			{Key: "$vectorSearch", Value: bson.D{
				{Key: "index", Value: vectorIndexName},
				{Key: "path", Value: "embedding"},
				{Key: "queryVector", Value: queryEmbedding},
				{Key: "filter", Value: bson.D{
					{Key: "sheet_id", Value: sheetID},
				}},
				{Key: "numCandidates", Value: numCandidates},
				{Key: "limit", Value: limit},
			}},
		}
	}

	scoreStage := bson.D{
		{Key: "$addFields", Value: bson.D{
			{Key: "vs_score", Value: bson.D{{Key: "$meta", Value: "vectorSearchScore"}}},
		}},
	}

	pipeline := mongo.Pipeline{vectorSearchStage, scoreStage}

	cursor, err := coll.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("vector search: %w", err)
	}
	defer cursor.Close(ctx)

	var results []VectorSearchResult
	if err := cursor.All(ctx, &results); err != nil {
		return nil, fmt.Errorf("decode vector search results: %w", err)
	}

	log.Debug().
		Str("view_id", viewID).
		Int("results", len(results)).
		Msg("view: vector search")

	return results, nil
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
	ID         string          `bson:"_id"`
	SheetID    string          `bson:"sheet_id"`
	WidgetID   string          `bson:"widget_id"`
	Type       string          `bson:"type"`
	Status     string          `bson:"status"`
	Error      string          `bson:"error,omitempty"`
	SchemaHash string          `bson:"schema_hash"`
	Metric     *WidgetMetric   `bson:"metric,omitempty"`
	MapData    *WidgetMapData  `bson:"map_data,omitempty"`
	ListData   *WidgetListData `bson:"list_data,omitempty"`
	UpdatedAt  time.Time       `bson:"updated_at"`
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
