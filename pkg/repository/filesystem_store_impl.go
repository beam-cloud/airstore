package repository

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

const (
	defaultCacheTTL       = 30 * time.Second
	maxIndexedContentSize = 1 * 1024 * 1024 // 1MB safety cap for search indexing
)

// nullableString converts an empty string to sql.NullString{Valid: false}
// and a non-empty string to sql.NullString{String: s, Valid: true}.
func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func nullableStringPtr(s *string) sql.NullString {
	if s == nil {
		return sql.NullString{}
	}
	trimmed := strings.TrimSpace(*s)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}

// ElasticsearchClient is an optional interface for Elasticsearch operations.
type ElasticsearchClient interface {
	Index(ctx context.Context, index, docID string, body []byte) error
	Search(ctx context.Context, index string, query map[string]interface{}, size int) ([]json.RawMessage, error)
	Get(ctx context.Context, index, docID string) ([]byte, error)
	Delete(ctx context.Context, index, docID string) error
	DeleteByQuery(ctx context.Context, index string, query map[string]interface{}) error
}

// filesystemStore implements FilesystemStore with layered storage:
// - Postgres: query definitions (persistent, nil for memory mode)
// - Elasticsearch: materialized results + content (optional)
// - Redis: cache layer (nil for memory mode)
// - Memory: fallback when Redis/Postgres unavailable
type filesystemStore struct {
	db      *sql.DB             // Postgres (nil for memory mode)
	redis   *common.RedisClient // Redis cache (nil for memory mode)
	elastic ElasticsearchClient // Elasticsearch (optional)
	ttl     time.Duration

	// In-memory fallback storage (used when db/redis are nil)
	mu           sync.RWMutex
	memQueries   map[string]*types.FilesystemQuery // by external_id
	memQueryPath map[string]string                 // path -> external_id
	memDirs      map[string]*types.DirMeta
	memFiles     map[string]*types.FileMeta
	memSymlinks  map[string]string
	memListings  map[string][]types.DirEntry
	memResults   map[string][]QueryResult // cacheKey -> results
	memContent   map[string][]byte        // cacheKey -> content
	memHooks     map[string]*types.Hook   // by external_id
}

// NewFilesystemStore creates a unified filesystem store.
// Pass nil for db/redis to use memory-only mode.
func NewFilesystemStore(db *sql.DB, redis *common.RedisClient, elastic ElasticsearchClient) FilesystemStore {
	return &filesystemStore{
		db:           db,
		redis:        redis,
		elastic:      elastic,
		ttl:          defaultCacheTTL,
		memQueries:   make(map[string]*types.FilesystemQuery),
		memQueryPath: make(map[string]string),
		memDirs:      make(map[string]*types.DirMeta),
		memFiles:     make(map[string]*types.FileMeta),
		memSymlinks:  make(map[string]string),
		memListings:  make(map[string][]types.DirEntry),
		memResults:   make(map[string][]QueryResult),
		memContent:   make(map[string][]byte),
		memHooks:     make(map[string]*types.Hook),
	}
}

// NewFilesystemStoreWithTTL creates a store with custom cache TTL.
func NewFilesystemStoreWithTTL(db *sql.DB, redis *common.RedisClient, elastic ElasticsearchClient, ttl time.Duration) FilesystemStore {
	s := NewFilesystemStore(db, redis, elastic).(*filesystemStore)
	s.ttl = ttl
	return s
}

// NewMemoryFilesystemStore creates a memory-only filesystem store (for local mode).
func NewMemoryFilesystemStore() FilesystemStore {
	return NewFilesystemStore(nil, nil, nil)
}

func (s *filesystemStore) isMemoryMode() bool {
	return s.db == nil
}

func (s *filesystemStore) elasticIndex(workspaceId uint) string {
	return fmt.Sprintf("airstore_results_%d", workspaceId)
}

// ===== Query Definitions =====

func (s *filesystemStore) CreateQuery(ctx context.Context, query *types.FilesystemQuery) (*types.FilesystemQuery, error) {
	query.ExternalId = uuid.New().String()
	query.CreatedAt = time.Now()
	query.UpdatedAt = time.Now()

	if query.OutputFormat == "" {
		query.OutputFormat = types.ViewOutputFolder
	}

	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Check for duplicate path
		if _, exists := s.memQueryPath[query.Path]; exists {
			return nil, fmt.Errorf("query already exists at path: %s", query.Path)
		}

		query.Id = uint(len(s.memQueries) + 1)
		s.memQueries[query.ExternalId] = query
		s.memQueryPath[query.Path] = query.ExternalId
		return query, nil
	}

	err := s.db.QueryRowContext(ctx, `
		INSERT INTO filesystem_queries (external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, mode, filter, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		RETURNING id
	`, query.ExternalId, query.WorkspaceId, query.Integration, query.Path, query.Name,
		query.QuerySpec, query.Guidance, query.OutputFormat, query.FileExt, query.FilenameFormat, query.CacheTTL,
		query.Mode, nullableString(query.Filter),
		query.CreatedAt, query.UpdatedAt).Scan(&query.Id)
	if err != nil {
		return nil, fmt.Errorf("create filesystem query: %w", err)
	}

	return query, nil
}

func (s *filesystemStore) GetQuery(ctx context.Context, workspaceId uint, path string) (*types.FilesystemQuery, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Try exact match first, then case-insensitive
		extId, exists := s.memQueryPath[path]
		if !exists {
			// Case-insensitive fallback
			pathLower := strings.ToLower(path)
			for p, id := range s.memQueryPath {
				if strings.ToLower(p) == pathLower {
					extId = id
					exists = true
					break
				}
			}
		}
		if !exists {
			return nil, nil
		}
		q := s.memQueries[extId]
		if q != nil && q.WorkspaceId == workspaceId {
			return q, nil
		}
		return nil, nil
	}

	query := &types.FilesystemQuery{}
	var lastExecuted sql.NullTime
	var filenameFormat sql.NullString

	// Use ILIKE for case-insensitive path matching (handles old lowercase paths)
	var filterStr sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, mode, filter, created_at, updated_at, last_executed
		FROM filesystem_queries WHERE workspace_id = $1 AND LOWER(path) = LOWER($2)
	`, workspaceId, path).Scan(
		&query.Id, &query.ExternalId, &query.WorkspaceId, &query.Integration,
		&query.Path, &query.Name, &query.QuerySpec, &query.Guidance,
		&query.OutputFormat, &query.FileExt, &filenameFormat, &query.CacheTTL,
		&query.Mode, &filterStr,
		&query.CreatedAt, &query.UpdatedAt, &lastExecuted,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get filesystem query: %w", err)
	}

	if lastExecuted.Valid {
		query.LastExecuted = &lastExecuted.Time
	}
	if filenameFormat.Valid {
		query.FilenameFormat = filenameFormat.String
	}
	if filterStr.Valid {
		query.Filter = filterStr.String
	}
	return query, nil
}

func (s *filesystemStore) GetQueryByExternalId(ctx context.Context, externalId string) (*types.FilesystemQuery, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memQueries[externalId], nil
	}

	query := &types.FilesystemQuery{}
	var lastExecuted sql.NullTime
	var filenameFormat sql.NullString

	var filterStr sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, mode, filter, created_at, updated_at, last_executed
		FROM filesystem_queries WHERE external_id = $1
	`, externalId).Scan(
		&query.Id, &query.ExternalId, &query.WorkspaceId, &query.Integration,
		&query.Path, &query.Name, &query.QuerySpec, &query.Guidance,
		&query.OutputFormat, &query.FileExt, &filenameFormat, &query.CacheTTL,
		&query.Mode, &filterStr,
		&query.CreatedAt, &query.UpdatedAt, &lastExecuted,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get filesystem query by external id: %w", err)
	}

	if lastExecuted.Valid {
		query.LastExecuted = &lastExecuted.Time
	}
	if filenameFormat.Valid {
		query.FilenameFormat = filenameFormat.String
	}
	if filterStr.Valid {
		query.Filter = filterStr.String
	}
	return query, nil
}

func (s *filesystemStore) ListQueries(ctx context.Context, workspaceId uint, parentPath string) ([]*types.FilesystemQuery, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		// Use lowercase for case-insensitive matching
		prefixLower := strings.ToLower(parentPath) + "/"
		var queries []*types.FilesystemQuery
		for _, q := range s.memQueries {
			if q.WorkspaceId == workspaceId && strings.HasPrefix(strings.ToLower(q.Path), prefixLower) {
				// Ensure it's a direct child (no additional /)
				rel := strings.ToLower(q.Path)[len(prefixLower):]
				if !strings.Contains(rel, "/") {
					queries = append(queries, q)
				}
			}
		}
		return queries, nil
	}

	// Use ILIKE for case-insensitive matching (handles old lowercase paths)
	pattern := parentPath + "/%"
	excludePattern := parentPath + "/%/%"

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, mode, filter, created_at, updated_at, last_executed
		FROM filesystem_queries 
		WHERE workspace_id = $1 AND path ILIKE $2 AND path NOT ILIKE $3
		ORDER BY name
	`, workspaceId, pattern, excludePattern)
	if err != nil {
		return nil, fmt.Errorf("list filesystem queries: %w", err)
	}
	defer rows.Close()

	var queries []*types.FilesystemQuery
	for rows.Next() {
		query := &types.FilesystemQuery{}
		var lastExecuted sql.NullTime
		var filenameFormat sql.NullString
		var filterStr sql.NullString
		err := rows.Scan(
			&query.Id, &query.ExternalId, &query.WorkspaceId, &query.Integration,
			&query.Path, &query.Name, &query.QuerySpec, &query.Guidance,
			&query.OutputFormat, &query.FileExt, &filenameFormat, &query.CacheTTL,
			&query.Mode, &filterStr,
			&query.CreatedAt, &query.UpdatedAt, &lastExecuted,
		)
		if err != nil {
			return nil, fmt.Errorf("scan filesystem query: %w", err)
		}
		if lastExecuted.Valid {
			query.LastExecuted = &lastExecuted.Time
		}
		if filenameFormat.Valid {
			query.FilenameFormat = filenameFormat.String
		}
		if filterStr.Valid {
			query.Filter = filterStr.String
		}
		queries = append(queries, query)
	}

	return queries, rows.Err()
}

func (s *filesystemStore) CountQueries(ctx context.Context, workspaceId uint) (int, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		count := 0
		for _, q := range s.memQueries {
			if q.WorkspaceId == workspaceId {
				count++
			}
		}
		return count, nil
	}

	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM filesystem_queries WHERE workspace_id = $1`,
		workspaceId,
	).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *filesystemStore) UpdateQuery(ctx context.Context, query *types.FilesystemQuery) error {
	query.UpdatedAt = time.Now()

	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()

		if existing, ok := s.memQueries[query.ExternalId]; ok {
			// If path changed, update the path index
			if existing.Path != query.Path {
				delete(s.memQueryPath, existing.Path)
				s.memQueryPath[query.Path] = query.ExternalId
			}

			existing.Name = query.Name
			existing.Path = query.Path
			existing.QuerySpec = query.QuerySpec
			existing.Guidance = query.Guidance
			existing.OutputFormat = query.OutputFormat
			existing.FileExt = query.FileExt
			existing.FilenameFormat = query.FilenameFormat
			existing.CacheTTL = query.CacheTTL
			existing.Mode = query.Mode
			existing.Filter = query.Filter
			existing.UpdatedAt = query.UpdatedAt
			existing.LastExecuted = query.LastExecuted
		}
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
		UPDATE filesystem_queries SET
			name = $1, path = $2, query_spec = $3, guidance = $4, output_format = $5, 
			file_ext = $6, filename_format = $7, cache_ttl = $8, mode = $9, filter = $10, updated_at = $11, last_executed = $12
		WHERE external_id = $13
	`, query.Name, query.Path, query.QuerySpec, query.Guidance, query.OutputFormat,
		query.FileExt, query.FilenameFormat, query.CacheTTL, query.Mode, nullableString(query.Filter),
		query.UpdatedAt, query.LastExecuted, query.ExternalId)
	if err != nil {
		return fmt.Errorf("update filesystem query: %w", err)
	}

	return nil
}

func (s *filesystemStore) DeleteQuery(ctx context.Context, externalId string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if q, ok := s.memQueries[externalId]; ok {
			delete(s.memQueryPath, q.Path)
			delete(s.memQueries, externalId)
		}
		return nil
	}

	_, err := s.db.ExecContext(ctx, `DELETE FROM filesystem_queries WHERE external_id = $1`, externalId)
	if err != nil {
		return fmt.Errorf("delete filesystem query: %w", err)
	}
	return nil
}

// ===== Query Results =====

func (s *filesystemStore) resultCacheKey(workspaceId uint, queryPath string) string {
	return fmt.Sprintf("%d:%s", workspaceId, queryPath)
}

func (s *filesystemStore) GetQueryResults(ctx context.Context, workspaceId uint, queryPath string) ([]QueryResult, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memResults[s.resultCacheKey(workspaceId, queryPath)], nil
	}

	// Try Redis cache first
	cacheKey := common.Keys.FsQueryResult(workspaceId, queryPath)
	data, err := s.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		var results []QueryResult
		if err := json.Unmarshal(data, &results); err == nil {
			return results, nil
		}
	}
	if err != nil && err != redis.Nil {
		// Log but don't fail
	}

	// Fall back to Elasticsearch if available
	if s.elastic != nil {
		return s.fetchResultsFromElastic(ctx, workspaceId, queryPath)
	}

	return nil, nil
}

func (s *filesystemStore) fetchResultsFromElastic(ctx context.Context, workspaceId uint, queryPath string) ([]QueryResult, error) {
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"query_path": queryPath,
			},
		},
	}

	docs, err := s.elastic.Search(ctx, s.elasticIndex(workspaceId), query, 1000)
	if err != nil {
		return nil, err
	}

	var results []QueryResult
	for _, doc := range docs {
		var result QueryResult
		if err := json.Unmarshal(doc, &result); err == nil {
			results = append(results, result)
		}
	}

	return results, nil
}

func (s *filesystemStore) StoreQueryResults(ctx context.Context, workspaceId uint, queryPath string, results []QueryResult, ttl time.Duration) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.memResults[s.resultCacheKey(workspaceId, queryPath)] = results
		return nil
	}

	// Store in Redis cache
	cacheKey := common.Keys.FsQueryResult(workspaceId, queryPath)
	data, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshal query results: %w", err)
	}

	if ttl == 0 {
		ttl = s.ttl
	}
	if err := s.redis.Set(ctx, cacheKey, data, ttl).Err(); err != nil {
		// Log but don't fail
	}

	// Index in Elasticsearch if available
	if s.elastic != nil {
		for _, result := range results {
			doc := map[string]interface{}{
				"query_path": queryPath,
				"result_id":  result.ID,
				"filename":   result.Filename,
				"metadata":   result.Metadata,
				"size":       result.Size,
				"mtime":      result.Mtime,
				"indexed_at": time.Now().UTC(),
			}
			docData, _ := json.Marshal(doc)
			docID := fmt.Sprintf("%s_%s", types.GeneratePathID(queryPath), result.ID)
			_ = s.elastic.Index(ctx, s.elasticIndex(workspaceId), docID, docData)
		}
	}

	return nil
}

// ===== Result Content =====

func (s *filesystemStore) contentCacheKey(workspaceId uint, queryPath, resultID string) string {
	return fmt.Sprintf("%d:%s:%s", workspaceId, queryPath, resultID)
}

func (s *filesystemStore) GetResultContent(ctx context.Context, workspaceId uint, queryPath, resultID string) ([]byte, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memContent[s.contentCacheKey(workspaceId, queryPath, resultID)], nil
	}

	// Try Redis cache first
	cacheKey := common.Keys.FsResultBody(workspaceId, queryPath, resultID)
	data, err := s.redis.Get(ctx, cacheKey).Bytes()
	if err == nil {
		return data, nil
	}
	if err != redis.Nil {
		// Log but don't fail
	}

	// Try Elasticsearch if available
	if s.elastic != nil {
		docID := fmt.Sprintf("%s_%s_content", types.GeneratePathID(queryPath), resultID)
		return s.elastic.Get(ctx, s.elasticIndex(workspaceId), docID)
	}

	return nil, nil
}

func (s *filesystemStore) StoreResultContent(ctx context.Context, workspaceId uint, queryPath, resultID string, content []byte) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.memContent[s.contentCacheKey(workspaceId, queryPath, resultID)] = content
		return nil
	}

	// Cache in Redis
	cacheKey := common.Keys.FsResultBody(workspaceId, queryPath, resultID)
	contentTTL := s.ttl * 10 // Longer TTL for content
	if err := s.redis.Set(ctx, cacheKey, content, contentTTL).Err(); err != nil {
		// Log but don't fail
	}
	// Track cache key in an index for O(1) invalidation by query path.
	s.addIndexMember(ctx, common.Keys.FsResultBodyIndex(workspaceId, queryPath), cacheKey, contentTTL*2)

	// Index in Elasticsearch if available and safe to index as text.
	if s.elastic != nil && shouldIndexResultContent(resultID, content) {
		doc := map[string]interface{}{
			"query_path": queryPath,
			"result_id":  resultID,
			"content":    string(content),
			"indexed_at": time.Now().UTC(),
		}
		docData, _ := json.Marshal(doc)
		docID := fmt.Sprintf("%s_%s_content", types.GeneratePathID(queryPath), resultID)
		return s.elastic.Index(ctx, s.elasticIndex(workspaceId), docID, docData)
	}

	return nil
}

// ===== Full-Text Search =====

func (s *filesystemStore) SearchContent(ctx context.Context, workspaceId uint, query string, limit int) ([]SearchHit, error) {
	if s.elastic == nil {
		return nil, fmt.Errorf("elasticsearch not configured")
	}

	esQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"content": query,
			},
		},
		"highlight": map[string]interface{}{
			"fields": map[string]interface{}{
				"content": map[string]interface{}{},
			},
		},
	}

	docs, err := s.elastic.Search(ctx, s.elasticIndex(workspaceId), esQuery, limit)
	if err != nil {
		return nil, err
	}

	var hits []SearchHit
	for _, doc := range docs {
		var hit struct {
			QueryPath string  `json:"query_path"`
			ResultID  string  `json:"result_id"`
			Filename  string  `json:"filename"`
			Score     float64 `json:"_score"`
		}
		if err := json.Unmarshal(doc, &hit); err == nil {
			hits = append(hits, SearchHit{
				WorkspaceID: workspaceId,
				QueryPath:   hit.QueryPath,
				ResultID:    hit.ResultID,
				Filename:    hit.Filename,
				Score:       hit.Score,
			})
		}
	}

	return hits, nil
}

func (s *filesystemStore) IndexContent(ctx context.Context, workspaceId uint, queryPath, resultID, filename string, content []byte) error {
	if s.elastic == nil || !shouldIndexResultContent(resultID, content) {
		return nil
	}

	doc := map[string]interface{}{
		"query_path": queryPath,
		"result_id":  resultID,
		"filename":   filename,
		"content":    string(content),
		"indexed_at": time.Now().UTC(),
	}
	docData, _ := json.Marshal(doc)
	docID := fmt.Sprintf("%s_%s_content", types.GeneratePathID(queryPath), resultID)

	return s.elastic.Index(ctx, s.elasticIndex(workspaceId), docID, docData)
}

func shouldIndexResultContent(resultID string, content []byte) bool {
	if len(content) == 0 || len(content) > maxIndexedContentSize {
		return false
	}
	// Gmail attachments are frequently binary; skip text indexing entirely.
	if strings.HasPrefix(resultID, "att:") {
		return false
	}
	return !looksBinaryContent(content)
}

func looksBinaryContent(content []byte) bool {
	sample := content
	if len(sample) > 8192 {
		sample = sample[:8192]
	}
	if len(sample) == 0 {
		return false
	}
	if bytes.IndexByte(sample, 0x00) >= 0 {
		return true
	}
	if !utf8.Valid(sample) {
		return true
	}

	controlBytes := 0
	for _, b := range sample {
		if b < 0x09 || (b > 0x0D && b < 0x20) {
			controlBytes++
		}
	}
	return float64(controlBytes)/float64(len(sample)) > 0.10
}

// ===== Filesystem Metadata =====

func (s *filesystemStore) StatPath(ctx context.Context, path string) (*types.DirMeta, *types.FileMeta, string, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		if dm := s.memDirs[path]; dm != nil {
			return dm, nil, "", nil
		}
		if fm := s.memFiles[path]; fm != nil {
			return nil, fm, "", nil
		}
		if target := s.memSymlinks[path]; target != "" {
			return nil, nil, target, nil
		}
		return nil, nil, "", nil
	}

	// Pipeline all three lookups into a single Redis round-trip
	pipe := s.redis.Pipeline()
	dirCmd := pipe.Get(ctx, common.Keys.FsDirMeta(path))
	fileCmd := pipe.Get(ctx, common.Keys.FsFileMeta(path))
	symlinkCmd := pipe.Get(ctx, common.Keys.FsSymlink(path))
	_, execErr := pipe.Exec(ctx)

	// redis.Nil is expected (most keys won't exist for a given path).
	// Any other error means Redis is unreachable — propagate it so the
	// caller returns an IO error instead of a false "not found".
	if execErr != nil && execErr != redis.Nil {
		return nil, nil, "", fmt.Errorf("redis pipeline: %w", execErr)
	}

	// Check directory
	if data, err := dirCmd.Bytes(); err == nil {
		var meta types.DirMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, nil, "", fmt.Errorf("corrupt dir metadata at %s: %w", path, err)
		}
		return &meta, nil, "", nil
	}

	// Check file
	if data, err := fileCmd.Bytes(); err == nil {
		var meta types.FileMeta
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil, nil, "", fmt.Errorf("corrupt file metadata at %s: %w", path, err)
		}
		return nil, &meta, "", nil
	}

	// Check symlink
	if target, err := symlinkCmd.Result(); err == nil && target != "" {
		return nil, nil, target, nil
	}

	return nil, nil, "", nil
}

func (s *filesystemStore) GetFileMeta(ctx context.Context, path string) (*types.FileMeta, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memFiles[path], nil
	}

	key := common.Keys.FsFileMeta(path)
	data, err := s.redis.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var meta types.FileMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *filesystemStore) GetDirMeta(ctx context.Context, path string) (*types.DirMeta, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memDirs[path], nil
	}

	key := common.Keys.FsDirMeta(path)
	data, err := s.redis.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var meta types.DirMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func (s *filesystemStore) SaveFileMeta(ctx context.Context, meta *types.FileMeta) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.memFiles[meta.Path] = meta
		return nil
	}

	key := common.Keys.FsFileMeta(meta.Path)
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return s.redis.Set(ctx, key, data, s.ttl).Err()
}

func (s *filesystemStore) SaveDirMeta(ctx context.Context, meta *types.DirMeta) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.memDirs[meta.Path] = meta
		return nil
	}

	key := common.Keys.FsDirMeta(meta.Path)
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return s.redis.Set(ctx, key, data, s.ttl).Err()
}

func (s *filesystemStore) DeleteFileMeta(ctx context.Context, path string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memFiles, path)
		return nil
	}

	return s.redis.Del(ctx, common.Keys.FsFileMeta(path)).Err()
}

func (s *filesystemStore) DeleteDirMeta(ctx context.Context, path string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memDirs, path)
		return nil
	}

	return s.redis.Del(ctx, common.Keys.FsDirMeta(path)).Err()
}

func (s *filesystemStore) ListDir(ctx context.Context, path string) ([]types.DirEntry, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memListings[path], nil
	}

	key := common.Keys.FsDirChildren(path)
	data, err := s.redis.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var entries []types.DirEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *filesystemStore) SaveDirListing(ctx context.Context, path string, entries []types.DirEntry) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.memListings[path] = entries
		return nil
	}

	key := common.Keys.FsDirChildren(path)
	data, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	return s.redis.Set(ctx, key, data, s.ttl).Err()
}

// ===== Symlinks =====

func (s *filesystemStore) GetSymlink(ctx context.Context, path string) (string, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memSymlinks[path], nil
	}

	target, err := s.redis.Get(ctx, common.Keys.FsSymlink(path)).Result()
	if err == redis.Nil {
		return "", nil
	}
	return target, err
}

func (s *filesystemStore) SaveSymlink(ctx context.Context, path, target string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.memSymlinks[path] = target
		return nil
	}

	return s.redis.Set(ctx, common.Keys.FsSymlink(path), target, s.ttl).Err()
}

func (s *filesystemStore) DeleteSymlink(ctx context.Context, path string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memSymlinks, path)
		return nil
	}

	return s.redis.Del(ctx, common.Keys.FsSymlink(path)).Err()
}

// ===== Cache Invalidation =====

func (s *filesystemStore) InvalidatePath(ctx context.Context, path string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memDirs, path)
		delete(s.memFiles, path)
		delete(s.memSymlinks, path)
		delete(s.memListings, path)
		return nil
	}

	keys := []string{
		common.Keys.FsDirMeta(path),
		common.Keys.FsFileMeta(path),
		common.Keys.FsSymlink(path),
		common.Keys.FsDirChildren(path),
	}
	return s.redis.Del(ctx, keys...).Err()
}

func (s *filesystemStore) InvalidatePrefix(ctx context.Context, prefix string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		// Invalidate parent listing
		parent := parentPath(prefix)
		delete(s.memListings, parent)
		return nil
	}

	parent := parentPath(prefix)
	return s.redis.Del(ctx, common.Keys.FsDirChildren(parent)).Err()
}

func (s *filesystemStore) InvalidateQuery(ctx context.Context, workspaceId uint, queryPath string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		cacheKey := s.resultCacheKey(workspaceId, queryPath)
		delete(s.memResults, cacheKey)
		// Delete all content for this query
		prefix := fmt.Sprintf("%d:%s:", workspaceId, queryPath)
		for k := range s.memContent {
			if strings.HasPrefix(k, prefix) {
				delete(s.memContent, k)
			}
		}
		return nil
	}

	// Invalidate Redis results listing cache
	listingKey := common.Keys.FsQueryResult(workspaceId, queryPath)
	if err := s.redis.Del(ctx, listingKey).Err(); err != nil {
		// Log but continue
	}

	// Invalidate indexed result-content keys for this path.
	s.deleteIndexedKeys(ctx, common.Keys.FsResultBodyIndex(workspaceId, queryPath))

	// Invalidate indexed compressed keys (pointer + content) for this path.
	s.deleteIndexedKeys(ctx, common.Keys.FsCompressedIndex(workspaceId, queryPath))

	// Delete from Elasticsearch if available
	if s.elastic != nil {
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"term": map[string]interface{}{
					"query_path": queryPath,
				},
			},
		}
		_ = s.elastic.DeleteByQuery(ctx, s.elasticIndex(workspaceId), query)
	}

	return nil
}

const indexDeleteBatchSize = 500

// addIndexMember best-effort adds a cache key to a Redis set index and extends
// index TTL so it outlives members slightly (helpful for eager invalidation).
func (s *filesystemStore) addIndexMember(ctx context.Context, indexKey, member string, ttl time.Duration) {
	if err := s.redis.SAdd(ctx, indexKey, member).Err(); err != nil {
		return
	}
	if ttl > 0 {
		_ = s.redis.Expire(ctx, indexKey, ttl).Err()
	}
}

// deleteIndexedKeys deletes all keys currently referenced by an index set,
// then deletes the set itself. Best-effort by design.
func (s *filesystemStore) deleteIndexedKeys(ctx context.Context, indexKey string) {
	keys, err := s.redis.SMembers(ctx, indexKey).Result()
	if err != nil {
		_ = s.redis.Del(ctx, indexKey).Err()
		return
	}
	for i := 0; i < len(keys); i += indexDeleteBatchSize {
		end := i + indexDeleteBatchSize
		if end > len(keys) {
			end = len(keys)
		}
		_ = s.redis.Del(ctx, keys[i:end]...).Err()
	}
	_ = s.redis.Del(ctx, indexKey).Err()
}

// parentPath returns the parent directory of a path
func parentPath(path string) string {
	if path == "/" || path == "" {
		return "/"
	}
	path = strings.TrimSuffix(path, "/")
	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return "/"
	}
	return path[:idx]
}

// NewElasticsearchClient creates a new Elasticsearch client.
func NewElasticsearchClient(url string) ElasticsearchClient {
	return &elasticsearchHTTPClient{baseURL: strings.TrimSuffix(url, "/")}
}

// elasticsearchHTTPClient is a simple HTTP-based Elasticsearch client placeholder.
type elasticsearchHTTPClient struct {
	baseURL string
}

func (c *elasticsearchHTTPClient) Index(ctx context.Context, index, docID string, body []byte) error {
	return nil
}

func (c *elasticsearchHTTPClient) Search(ctx context.Context, index string, query map[string]interface{}, size int) ([]json.RawMessage, error) {
	return nil, nil
}

func (c *elasticsearchHTTPClient) Get(ctx context.Context, index, docID string) ([]byte, error) {
	return nil, nil
}

func (c *elasticsearchHTTPClient) Delete(ctx context.Context, index, docID string) error {
	return nil
}

func (c *elasticsearchHTTPClient) DeleteByQuery(ctx context.Context, index string, query map[string]interface{}) error {
	return nil
}

// ===== Source Polling =====

func (s *filesystemStore) GetWatchedSourceQueries(ctx context.Context, staleAfter time.Duration, limit int) ([]*types.FilesystemQuery, error) {
	if s.isMemoryMode() {
		return nil, nil // not supported in memory mode
	}

	// Use ILIKE for case-insensitive path matching (handles old lowercase paths)
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT q.id, q.external_id, q.workspace_id, q.integration, q.path, q.name,
		       q.query_spec, q.guidance, q.output_format, q.file_ext, q.filename_format,
		       q.cache_ttl, q.created_at, q.updated_at, q.last_executed
		FROM filesystem_queries q
		JOIN filesystem_hooks h
		  ON h.workspace_id = q.workspace_id
		  AND h.active = true
		  AND (LOWER(q.path) = LOWER(h.path) OR LOWER(q.path) LIKE LOWER(replace(replace(h.path, '%', '\%'), '_', '\_') || '/%'))
		WHERE q.last_executed IS NULL
		   OR q.last_executed < NOW() - $1::interval
		ORDER BY q.last_executed ASC NULLS FIRST
		LIMIT $2
	`, fmt.Sprintf("%d seconds", int(staleAfter.Seconds())), limit)
	if err != nil {
		return nil, fmt.Errorf("get watched source queries: %w", err)
	}
	defer rows.Close()

	var queries []*types.FilesystemQuery
	for rows.Next() {
		q := &types.FilesystemQuery{}
		var lastExecuted sql.NullTime
		var filenameFormat sql.NullString
		if err := rows.Scan(
			&q.Id, &q.ExternalId, &q.WorkspaceId, &q.Integration,
			&q.Path, &q.Name, &q.QuerySpec, &q.Guidance,
			&q.OutputFormat, &q.FileExt, &filenameFormat, &q.CacheTTL,
			&q.CreatedAt, &q.UpdatedAt, &lastExecuted,
		); err != nil {
			return nil, fmt.Errorf("scan watched query: %w", err)
		}
		if lastExecuted.Valid {
			q.LastExecuted = &lastExecuted.Time
		}
		if filenameFormat.Valid {
			q.FilenameFormat = filenameFormat.String
		}
		queries = append(queries, q)
	}
	return queries, rows.Err()
}

// ===== Hooks =====

func (s *filesystemStore) CreateHook(ctx context.Context, hook *types.Hook) (*types.Hook, error) {
	hook.ExternalId = uuid.New().String()
	hook.CreatedAt = time.Now()
	hook.UpdatedAt = time.Now()
	normalizeHookSkillFields(hook)

	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		hook.Id = uint(len(s.memHooks) + 1)
		s.memHooks[hook.ExternalId] = hook
		return hook, nil
	}

	agentID := nullableStringPtr(hook.AgentId)
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO filesystem_hooks (external_id, workspace_id, path, prompt, skill_path, skill_paths, agent_id, active, event_types, created_by_member_id, token_id, encrypted_token, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		RETURNING id
	`, hook.ExternalId, hook.WorkspaceId, hook.Path, hook.Prompt, hook.SkillPath,
		pq.Array(hook.SkillPaths), agentID,
		hook.Active, pq.Array(hook.EventTypes), hook.CreatedByMemberId, hook.TokenId, hook.EncryptedToken,
		hook.CreatedAt, hook.UpdatedAt).Scan(&hook.Id)
	if err != nil {
		return nil, fmt.Errorf("create hook: %w", err)
	}

	return hook, nil
}

func (s *filesystemStore) GetHook(ctx context.Context, externalId string) (*types.Hook, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		h, ok := s.memHooks[externalId]
		if !ok {
			return nil, nil
		}
		return h, nil
	}

	h := &types.Hook{}
	var skillPaths pq.StringArray
	var eventTypes pq.StringArray
	var agentID sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, external_id, workspace_id, path, prompt, skill_path, skill_paths, agent_id, active,
		       event_types, created_by_member_id, token_id, encrypted_token, created_at, updated_at
		FROM filesystem_hooks WHERE external_id = $1
	`, externalId).Scan(
		&h.Id, &h.ExternalId, &h.WorkspaceId, &h.Path, &h.Prompt, &h.SkillPath, &skillPaths, &agentID,
		&h.Active, &eventTypes, &h.CreatedByMemberId, &h.TokenId, &h.EncryptedToken,
		&h.CreatedAt, &h.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get hook: %w", err)
	}
	h.SkillPaths = []string(skillPaths)
	h.EventTypes = []string(eventTypes)
	if agentID.Valid {
		v := strings.TrimSpace(agentID.String)
		if v != "" {
			h.AgentId = &v
		}
	}
	normalizeHookSkillFields(h)
	return h, nil
}

func (s *filesystemStore) GetHookById(ctx context.Context, id uint) (*types.Hook, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, h := range s.memHooks {
			if h.Id == id {
				return h, nil
			}
		}
		return nil, nil
	}

	h := &types.Hook{}
	var skillPaths pq.StringArray
	var eventTypes pq.StringArray
	var agentID sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, external_id, workspace_id, path, prompt, skill_path, skill_paths, agent_id, active,
		       event_types, created_by_member_id, token_id, encrypted_token, created_at, updated_at
		FROM filesystem_hooks WHERE id = $1
	`, id).Scan(
		&h.Id, &h.ExternalId, &h.WorkspaceId, &h.Path, &h.Prompt, &h.SkillPath, &skillPaths, &agentID,
		&h.Active, &eventTypes, &h.CreatedByMemberId, &h.TokenId, &h.EncryptedToken,
		&h.CreatedAt, &h.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get hook by id: %w", err)
	}
	h.SkillPaths = []string(skillPaths)
	h.EventTypes = []string(eventTypes)
	if agentID.Valid {
		v := strings.TrimSpace(agentID.String)
		if v != "" {
			h.AgentId = &v
		}
	}
	normalizeHookSkillFields(h)
	return h, nil
}

func (s *filesystemStore) ListHooks(ctx context.Context, workspaceId uint) ([]*types.Hook, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		var hooks []*types.Hook
		for _, h := range s.memHooks {
			if h.WorkspaceId == workspaceId {
				hooks = append(hooks, h)
			}
		}
		sort.Slice(hooks, func(i, j int) bool {
			return hooks[i].CreatedAt.Before(hooks[j].CreatedAt)
		})
		return hooks, nil
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, external_id, workspace_id, path, prompt, skill_path, skill_paths, agent_id, active,
		       event_types, created_by_member_id, token_id, encrypted_token, created_at, updated_at
		FROM filesystem_hooks WHERE workspace_id = $1
		ORDER BY created_at
	`, workspaceId)
	if err != nil {
		return nil, fmt.Errorf("list hooks: %w", err)
	}
	defer rows.Close()

	var hooks []*types.Hook
	for rows.Next() {
		h := &types.Hook{}
		var skillPaths pq.StringArray
		var eventTypes pq.StringArray
		var agentID sql.NullString
		err := rows.Scan(
			&h.Id, &h.ExternalId, &h.WorkspaceId, &h.Path, &h.Prompt, &h.SkillPath, &skillPaths, &agentID,
			&h.Active, &eventTypes, &h.CreatedByMemberId, &h.TokenId, &h.EncryptedToken,
			&h.CreatedAt, &h.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan hook: %w", err)
		}
		h.SkillPaths = []string(skillPaths)
		h.EventTypes = []string(eventTypes)
		if agentID.Valid {
			v := strings.TrimSpace(agentID.String)
			if v != "" {
				h.AgentId = &v
			}
		}
		normalizeHookSkillFields(h)
		hooks = append(hooks, h)
	}
	return hooks, rows.Err()
}

func (s *filesystemStore) UpdateHook(ctx context.Context, hook *types.Hook) error {
	hook.UpdatedAt = time.Now()
	normalizeHookSkillFields(hook)

	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if existing, ok := s.memHooks[hook.ExternalId]; ok {
			existing.Prompt = hook.Prompt
			existing.SkillPath = hook.SkillPath
			existing.SkillPaths = append([]string(nil), hook.SkillPaths...)
			existing.EventTypes = append([]string(nil), hook.EventTypes...)
			existing.AgentId = hook.AgentId
			existing.Active = hook.Active
			existing.UpdatedAt = hook.UpdatedAt
		}
		return nil
	}

	agentID := nullableStringPtr(hook.AgentId)
	_, err := s.db.ExecContext(ctx, `
		UPDATE filesystem_hooks SET
			prompt = $1, skill_path = $2, skill_paths = $3, agent_id = $4, active = $5, event_types = $6, updated_at = $7
		WHERE external_id = $8
	`, hook.Prompt, hook.SkillPath, pq.Array(hook.SkillPaths), agentID, hook.Active, pq.Array(hook.EventTypes), hook.UpdatedAt, hook.ExternalId)
	if err != nil {
		return fmt.Errorf("update hook: %w", err)
	}
	return nil
}

func (s *filesystemStore) DeleteHook(ctx context.Context, externalId string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memHooks, externalId)
		return nil
	}

	_, err := s.db.ExecContext(ctx, `DELETE FROM filesystem_hooks WHERE external_id = $1`, externalId)
	if err != nil {
		return fmt.Errorf("delete hook: %w", err)
	}
	return nil
}

func normalizeHookSkillFields(hook *types.Hook) {
	if hook == nil {
		return
	}
	hook.SkillPaths = types.NormalizeSkillPaths(hook.SkillPaths, hook.SkillPath)
	if len(hook.SkillPaths) > 0 {
		hook.SkillPath = hook.SkillPaths[0]
	} else {
		hook.SkillPath = ""
	}
}

var _ FilesystemStore = (*filesystemStore)(nil)
