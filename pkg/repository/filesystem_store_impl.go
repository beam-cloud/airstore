package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// Redis key prefixes
	keyFsDirMeta     = "airstore:fs:dir:%s"   // path hash
	keyFsFileMeta    = "airstore:fs:file:%s"  // path hash
	keyFsSymlink     = "airstore:fs:link:%s"  // path hash
	keyFsDirChildren = "airstore:fs:ls:%s"    // path hash
	keyQueryResults  = "airstore:qr:%d:%s"    // workspace_id, path hash
	keyResultContent = "airstore:rc:%d:%s:%s" // workspace_id, path hash, result_id
	defaultCacheTTL  = 30 * time.Second
)

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

func (s *filesystemStore) redisKey(format string, args ...interface{}) string {
	for i, arg := range args {
		if path, ok := arg.(string); ok && strings.HasPrefix(path, "/") {
			args[i] = types.GeneratePathID(path)
		}
	}
	return fmt.Sprintf(format, args...)
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
		query.OutputFormat = types.QueryOutputFolder
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
		INSERT INTO filesystem_queries (external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		RETURNING id
	`, query.ExternalId, query.WorkspaceId, query.Integration, query.Path, query.Name,
		query.QuerySpec, query.Guidance, query.OutputFormat, query.FileExt, query.FilenameFormat, query.CacheTTL,
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

		extId, exists := s.memQueryPath[path]
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

	err := s.db.QueryRowContext(ctx, `
		SELECT id, external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, created_at, updated_at, last_executed
		FROM filesystem_queries WHERE workspace_id = $1 AND path = $2
	`, workspaceId, path).Scan(
		&query.Id, &query.ExternalId, &query.WorkspaceId, &query.Integration,
		&query.Path, &query.Name, &query.QuerySpec, &query.Guidance,
		&query.OutputFormat, &query.FileExt, &filenameFormat, &query.CacheTTL,
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

	err := s.db.QueryRowContext(ctx, `
		SELECT id, external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, created_at, updated_at, last_executed
		FROM filesystem_queries WHERE external_id = $1
	`, externalId).Scan(
		&query.Id, &query.ExternalId, &query.WorkspaceId, &query.Integration,
		&query.Path, &query.Name, &query.QuerySpec, &query.Guidance,
		&query.OutputFormat, &query.FileExt, &filenameFormat, &query.CacheTTL,
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
	return query, nil
}

func (s *filesystemStore) ListQueries(ctx context.Context, workspaceId uint, parentPath string) ([]*types.FilesystemQuery, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()

		prefix := parentPath + "/"
		var queries []*types.FilesystemQuery
		for _, q := range s.memQueries {
			if q.WorkspaceId == workspaceId && strings.HasPrefix(q.Path, prefix) {
				// Ensure it's a direct child (no additional /)
				rel := strings.TrimPrefix(q.Path, prefix)
				if !strings.Contains(rel, "/") {
					queries = append(queries, q)
				}
			}
		}
		return queries, nil
	}

	pattern := parentPath + "/%"
	excludePattern := parentPath + "/%/%"

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, external_id, workspace_id, integration, path, name, query_spec, guidance, output_format, file_ext, filename_format, cache_ttl, created_at, updated_at, last_executed
		FROM filesystem_queries 
		WHERE workspace_id = $1 AND path LIKE $2 AND path NOT LIKE $3
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
		err := rows.Scan(
			&query.Id, &query.ExternalId, &query.WorkspaceId, &query.Integration,
			&query.Path, &query.Name, &query.QuerySpec, &query.Guidance,
			&query.OutputFormat, &query.FileExt, &filenameFormat, &query.CacheTTL,
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
		queries = append(queries, query)
	}

	return queries, rows.Err()
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
			existing.UpdatedAt = query.UpdatedAt
			existing.LastExecuted = query.LastExecuted
		}
		return nil
	}

	_, err := s.db.ExecContext(ctx, `
		UPDATE filesystem_queries SET
			name = $1, path = $2, query_spec = $3, guidance = $4, output_format = $5, 
			file_ext = $6, filename_format = $7, cache_ttl = $8, updated_at = $9, last_executed = $10
		WHERE external_id = $11
	`, query.Name, query.Path, query.QuerySpec, query.Guidance, query.OutputFormat,
		query.FileExt, query.FilenameFormat, query.CacheTTL, query.UpdatedAt, query.LastExecuted, query.ExternalId)
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
	cacheKey := s.redisKey(keyQueryResults, workspaceId, queryPath)
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
	cacheKey := s.redisKey(keyQueryResults, workspaceId, queryPath)
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
	cacheKey := s.redisKey(keyResultContent, workspaceId, queryPath, resultID)
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
	cacheKey := s.redisKey(keyResultContent, workspaceId, queryPath, resultID)
	contentTTL := s.ttl * 10 // Longer TTL for content
	if err := s.redis.Set(ctx, cacheKey, content, contentTTL).Err(); err != nil {
		// Log but don't fail
	}

	// Index in Elasticsearch if available
	if s.elastic != nil {
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
	if s.elastic == nil {
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

// ===== Filesystem Metadata =====

func (s *filesystemStore) GetFileMeta(ctx context.Context, path string) (*types.FileMeta, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memFiles[path], nil
	}

	key := s.redisKey(keyFsFileMeta, path)
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

	key := s.redisKey(keyFsDirMeta, path)
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

	key := s.redisKey(keyFsFileMeta, meta.Path)
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

	key := s.redisKey(keyFsDirMeta, meta.Path)
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

	return s.redis.Del(ctx, s.redisKey(keyFsFileMeta, path)).Err()
}

func (s *filesystemStore) DeleteDirMeta(ctx context.Context, path string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memDirs, path)
		return nil
	}

	return s.redis.Del(ctx, s.redisKey(keyFsDirMeta, path)).Err()
}

func (s *filesystemStore) ListDir(ctx context.Context, path string) ([]types.DirEntry, error) {
	if s.isMemoryMode() {
		s.mu.RLock()
		defer s.mu.RUnlock()
		return s.memListings[path], nil
	}

	key := s.redisKey(keyFsDirChildren, path)
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

	key := s.redisKey(keyFsDirChildren, path)
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

	target, err := s.redis.Get(ctx, s.redisKey(keyFsSymlink, path)).Result()
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

	return s.redis.Set(ctx, s.redisKey(keyFsSymlink, path), target, s.ttl).Err()
}

func (s *filesystemStore) DeleteSymlink(ctx context.Context, path string) error {
	if s.isMemoryMode() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.memSymlinks, path)
		return nil
	}

	return s.redis.Del(ctx, s.redisKey(keyFsSymlink, path)).Err()
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
		s.redisKey(keyFsDirMeta, path),
		s.redisKey(keyFsFileMeta, path),
		s.redisKey(keyFsSymlink, path),
		s.redisKey(keyFsDirChildren, path),
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
	return s.redis.Del(ctx, s.redisKey(keyFsDirChildren, parent)).Err()
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

	// Invalidate Redis cache
	pattern := s.redisKey(keyQueryResults, workspaceId, queryPath)
	if err := s.redis.Del(ctx, pattern).Err(); err != nil {
		// Log but continue
	}

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

var _ FilesystemStore = (*filesystemStore)(nil)
