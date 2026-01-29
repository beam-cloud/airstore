package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// ElasticsearchIndexStore implements IndexStore using Elasticsearch.
// This is the scalable backend for large deployments.
type ElasticsearchIndexStore struct {
	baseURL    string
	indexName  string
	httpClient *http.Client
}

// ElasticsearchConfig holds configuration for the Elasticsearch store
type ElasticsearchConfig struct {
	URL       string // e.g., "http://localhost:9200"
	IndexName string // e.g., "airstore"
	Timeout   time.Duration
}

// NewElasticsearchIndexStore creates a new Elasticsearch-backed index store
func NewElasticsearchIndexStore(cfg ElasticsearchConfig) (*ElasticsearchIndexStore, error) {
	if cfg.URL == "" {
		cfg.URL = "http://localhost:9200"
	}
	if cfg.IndexName == "" {
		cfg.IndexName = "airstore"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	store := &ElasticsearchIndexStore{
		baseURL:   strings.TrimSuffix(cfg.URL, "/"),
		indexName: cfg.IndexName,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
	}

	if err := store.ensureIndex(); err != nil {
		return nil, fmt.Errorf("failed to ensure index: %w", err)
	}

	return store, nil
}

// ensureIndex creates the index with appropriate mappings if it doesn't exist
func (s *ElasticsearchIndexStore) ensureIndex() error {
	// Check if index exists
	resp, err := s.httpClient.Head(s.baseURL + "/" + s.indexName)
	if err != nil {
		return fmt.Errorf("failed to check index: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode == 200 {
		return nil // Index exists
	}

	// Create index with mappings
	mapping := map[string]any{
		"settings": map[string]any{
			"number_of_shards":   1,
			"number_of_replicas": 0,
			"analysis": map[string]any{
				"analyzer": map[string]any{
					"content_analyzer": map[string]any{
						"type":      "custom",
						"tokenizer": "standard",
						"filter":    []string{"lowercase", "porter_stem"},
					},
				},
			},
		},
		"mappings": map[string]any{
			"properties": map[string]any{
				"integration": map[string]any{"type": "keyword"},
				"entity_id":   map[string]any{"type": "keyword"},
				"path":        map[string]any{"type": "keyword"},
				"parent_path": map[string]any{"type": "keyword"},
				"name":        map[string]any{"type": "text", "analyzer": "content_analyzer"},
				"type":        map[string]any{"type": "keyword"},
				"size":        map[string]any{"type": "long"},
				"mod_time":    map[string]any{"type": "date"},
				"title":       map[string]any{"type": "text", "analyzer": "content_analyzer"},
				"body":        map[string]any{"type": "text", "analyzer": "content_analyzer"},
				"metadata":    map[string]any{"type": "text"},
				"content_ref": map[string]any{"type": "keyword"},
			},
		},
	}

	body, _ := json.Marshal(mapping)
	req, _ := http.NewRequest("PUT", s.baseURL+"/"+s.indexName, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err = s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to create index: %s", string(respBody))
	}

	// Create sync_status index
	syncMapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				"integration": map[string]any{"type": "keyword"},
				"last_sync":   map[string]any{"type": "date"},
				"sync_token":  map[string]any{"type": "keyword"},
			},
		},
	}

	body, _ = json.Marshal(syncMapping)
	req, _ = http.NewRequest("PUT", s.baseURL+"/"+s.indexName+"_sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err = s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create sync index: %w", err)
	}
	defer resp.Body.Close()

	return nil
}

// docID generates a unique document ID from integration and entity ID
func (s *ElasticsearchIndexStore) docID(integration, entityID string) string {
	return integration + ":" + entityID
}

// Upsert inserts or updates an entry in the index
func (s *ElasticsearchIndexStore) Upsert(ctx context.Context, entry *IndexEntry) error {
	doc := map[string]any{
		"integration": entry.Integration,
		"entity_id":   entry.EntityID,
		"path":        entry.Path,
		"parent_path": entry.ParentPath,
		"name":        entry.Name,
		"type":        string(entry.Type),
		"size":        entry.Size,
		"mod_time":    entry.ModTime.Format(time.RFC3339),
		"title":       entry.Title,
		"body":        entry.Body,
		"metadata":    entry.Metadata,
		"content_ref": entry.ContentRef,
	}

	body, _ := json.Marshal(doc)
	docID := s.docID(entry.Integration, entry.EntityID)

	req, _ := http.NewRequestWithContext(ctx, "PUT",
		fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, s.indexName, docID),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upsert entry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to upsert entry: %s", string(respBody))
	}

	return nil
}

// Delete removes an entry from the index
func (s *ElasticsearchIndexStore) Delete(ctx context.Context, integration, entityID string) error {
	docID := s.docID(integration, entityID)

	req, _ := http.NewRequestWithContext(ctx, "DELETE",
		fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, s.indexName, docID), nil)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete entry: %w", err)
	}
	defer resp.Body.Close()

	// 404 is ok - entry didn't exist
	if resp.StatusCode >= 400 && resp.StatusCode != 404 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete entry: %s", string(respBody))
	}

	return nil
}

// DeleteByIntegration removes all entries for an integration
func (s *ElasticsearchIndexStore) DeleteByIntegration(ctx context.Context, integration string) error {
	query := map[string]any{
		"query": map[string]any{
			"term": map[string]any{
				"integration": integration,
			},
		},
	}

	body, _ := json.Marshal(query)
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/%s/_delete_by_query", s.baseURL, s.indexName),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to delete entries: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete entries: %s", string(respBody))
	}

	return nil
}

// List returns all entries under a path prefix
func (s *ElasticsearchIndexStore) List(ctx context.Context, integration, pathPrefix string) ([]*IndexEntry, error) {
	// Normalize path
	parentPath := strings.TrimSuffix(pathPrefix, "/")

	query := map[string]any{
		"size": 10000,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{"term": map[string]any{"integration": integration}},
					map[string]any{"term": map[string]any{"parent_path": parentPath}},
				},
			},
		},
		"sort": []any{
			map[string]any{"type": "desc"},
			map[string]any{"name": "asc"},
		},
	}

	return s.search(ctx, query)
}

// ListAll returns ALL entries for an integration (for sync/export)
func (s *ElasticsearchIndexStore) ListAll(ctx context.Context, integration string) ([]*IndexEntry, error) {
	query := map[string]any{
		"size": 10000,
		"query": map[string]any{
			"term": map[string]any{"integration": integration},
		},
		"sort": []any{
			map[string]any{"path": "asc"},
		},
	}

	return s.search(ctx, query)
}

// Get retrieves a single entry by integration and entity ID
func (s *ElasticsearchIndexStore) Get(ctx context.Context, integration, entityID string) (*IndexEntry, error) {
	docID := s.docID(integration, entityID)

	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/%s/_doc/%s", s.baseURL, s.indexName, docID), nil)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, nil
	}

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get entry: %s", string(respBody))
	}

	var result struct {
		Source esDocument `json:"_source"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode entry: %w", err)
	}

	return result.Source.toIndexEntry(), nil
}

// GetByPath retrieves an entry by its virtual filesystem path
func (s *ElasticsearchIndexStore) GetByPath(ctx context.Context, path string) (*IndexEntry, error) {
	query := map[string]any{
		"size": 1,
		"query": map[string]any{
			"term": map[string]any{
				"path": path,
			},
		},
	}

	entries, err := s.search(ctx, query)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, nil
	}
	return entries[0], nil
}

// Search performs full-text search across entries
func (s *ElasticsearchIndexStore) Search(ctx context.Context, integration, queryStr string) ([]*IndexEntry, error) {
	log.Debug().Str("integration", integration).Str("query", queryStr).Msg("searching elasticsearch index")

	query := map[string]any{
		"size": 1000,
		"query": map[string]any{
			"bool": map[string]any{
				"must": []any{
					map[string]any{"term": map[string]any{"integration": integration}},
					map[string]any{
						"multi_match": map[string]any{
							"query":  queryStr,
							"fields": []string{"title^3", "body", "name^2"},
							"type":   "best_fields",
						},
					},
				},
			},
		},
	}

	return s.search(ctx, query)
}

// LastSync returns the last sync time for an integration
func (s *ElasticsearchIndexStore) LastSync(ctx context.Context, integration string) (time.Time, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET",
		fmt.Sprintf("%s/%s_sync/_doc/%s", s.baseURL, s.indexName, integration), nil)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to get last sync: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return time.Time{}, nil
	}

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return time.Time{}, fmt.Errorf("failed to get last sync: %s", string(respBody))
	}

	var result struct {
		Source struct {
			LastSync string `json:"last_sync"`
		} `json:"_source"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return time.Time{}, fmt.Errorf("failed to decode last sync: %w", err)
	}

	t, _ := time.Parse(time.RFC3339, result.Source.LastSync)
	return t, nil
}

// SetLastSync records the last sync time for an integration
func (s *ElasticsearchIndexStore) SetLastSync(ctx context.Context, integration string, t time.Time) error {
	doc := map[string]any{
		"integration": integration,
		"last_sync":   t.Format(time.RFC3339),
	}

	body, _ := json.Marshal(doc)
	req, _ := http.NewRequestWithContext(ctx, "PUT",
		fmt.Sprintf("%s/%s_sync/_doc/%s", s.baseURL, s.indexName, integration),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to set last sync: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to set last sync: %s", string(respBody))
	}

	return nil
}

// Close closes the store (no-op for HTTP client)
func (s *ElasticsearchIndexStore) Close() error {
	return nil
}

// search executes a search query and returns entries
func (s *ElasticsearchIndexStore) search(ctx context.Context, query map[string]any) ([]*IndexEntry, error) {
	body, _ := json.Marshal(query)
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("%s/%s/_search", s.baseURL, s.indexName),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to search: %s", string(respBody))
	}

	var result struct {
		Hits struct {
			Hits []struct {
				Source esDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode search results: %w", err)
	}

	entries := make([]*IndexEntry, 0, len(result.Hits.Hits))
	for _, hit := range result.Hits.Hits {
		entries = append(entries, hit.Source.toIndexEntry())
	}

	return entries, nil
}

// esDocument represents a document in Elasticsearch
type esDocument struct {
	Integration string `json:"integration"`
	EntityID    string `json:"entity_id"`
	Path        string `json:"path"`
	ParentPath  string `json:"parent_path"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Size        int64  `json:"size"`
	ModTime     string `json:"mod_time"`
	Title       string `json:"title"`
	Body        string `json:"body"`
	Metadata    string `json:"metadata"`
	ContentRef  string `json:"content_ref"`
}

// toIndexEntry converts an Elasticsearch document to an IndexEntry
func (d *esDocument) toIndexEntry() *IndexEntry {
	modTime, _ := time.Parse(time.RFC3339, d.ModTime)

	return &IndexEntry{
		Integration: d.Integration,
		EntityID:    d.EntityID,
		Path:        d.Path,
		ParentPath:  d.ParentPath,
		Name:        d.Name,
		Type:        EntryType(d.Type),
		Size:        d.Size,
		ModTime:     modTime,
		Title:       d.Title,
		Body:        d.Body,
		Metadata:    d.Metadata,
		ContentRef:  d.ContentRef,
	}
}
