package views

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	defaultEmbeddingModel = "text-embedding-3-small"
	defaultEmbeddingDims  = 1536
	openAIEmbeddingsURL   = "https://api.openai.com/v1/embeddings"
	embeddingMaxBatch     = 2048
)

type EmbeddingClient struct {
	apiKey string
	model  string
	dims   int
	url    string
	http   *http.Client
}

func NewEmbeddingClient(apiKey string) *EmbeddingClient {
	if apiKey == "" {
		return nil
	}
	return &EmbeddingClient{
		apiKey: apiKey,
		model:  defaultEmbeddingModel,
		dims:   defaultEmbeddingDims,
		url:    openAIEmbeddingsURL,
		http: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *EmbeddingClient) Available() bool {
	return c != nil && c.apiKey != ""
}

func (c *EmbeddingClient) Dims() int {
	if c == nil {
		return defaultEmbeddingDims
	}
	return c.dims
}

type embeddingRequest struct {
	Input []string `json:"input"`
	Model string   `json:"model"`
}

type embeddingResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
		Index     int       `json:"index"`
	} `json:"data"`
	Usage struct {
		TotalTokens int `json:"total_tokens"`
	} `json:"usage"`
}

func (c *EmbeddingClient) Embed(ctx context.Context, texts []string) ([][]float64, error) {
	if !c.Available() || len(texts) == 0 {
		return nil, nil
	}

	if len(texts) <= embeddingMaxBatch {
		return c.embedBatch(ctx, texts)
	}

	result := make([][]float64, 0, len(texts))
	for i := 0; i < len(texts); i += embeddingMaxBatch {
		end := i + embeddingMaxBatch
		if end > len(texts) {
			end = len(texts)
		}
		batch, err := c.embedBatch(ctx, texts[i:end])
		if err != nil {
			return nil, err
		}
		result = append(result, batch...)
	}
	return result, nil
}

func (c *EmbeddingClient) embedBatch(ctx context.Context, texts []string) ([][]float64, error) {
	body, err := json.Marshal(embeddingRequest{
		Input: texts,
		Model: c.model,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal embedding request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create embedding request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("embedding API %d: %s", resp.StatusCode, string(b))
	}

	var result embeddingResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode embedding response: %w", err)
	}

	sort.Slice(result.Data, func(i, j int) bool {
		return result.Data[i].Index < result.Data[j].Index
	})

	embeddings := make([][]float64, len(result.Data))
	for i, d := range result.Data {
		embeddings[i] = d.Embedding
	}

	log.Debug().
		Int("texts", len(texts)).
		Int("tokens", result.Usage.TotalTokens).
		Msg("embedding: batch complete")

	return embeddings, nil
}

func (c *EmbeddingClient) EmbedOne(ctx context.Context, text string) ([]float64, error) {
	vecs, err := c.Embed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(vecs) == 0 {
		return nil, fmt.Errorf("embedding returned no vectors")
	}
	return vecs[0], nil
}

// RowSearchText builds a deterministic, schema-agnostic search string from a
// row's merged cells. Keys are sorted for reproducibility.
func RowSearchText(row *ViewRow) string {
	if row == nil {
		return ""
	}
	merged := row.MergedCells()
	if len(merged) == 0 {
		return ""
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var parts []string
	for _, k := range keys {
		v := strings.TrimSpace(merged[k])
		if v == "" {
			continue
		}
		parts = append(parts, k+": "+v)
	}
	return strings.Join(parts, " | ")
}

// OutputSearchText builds a query string from a task output for vector search.
func OutputSearchText(outputType, title, summary, data string) string {
	var parts []string
	if t := strings.TrimSpace(outputType); t != "" {
		parts = append(parts, t)
	}
	if t := strings.TrimSpace(title); t != "" {
		parts = append(parts, t)
	}
	if s := strings.TrimSpace(summary); s != "" {
		if len(s) > 500 {
			s = s[:500]
		}
		parts = append(parts, s)
	}
	if d := strings.TrimSpace(data); d != "" {
		if len(d) > 1000 {
			d = d[:1000]
		}
		parts = append(parts, d)
	}
	return strings.Join(parts, " | ")
}
