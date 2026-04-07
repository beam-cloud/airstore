package hooks

import "context"

type ContextEnricher interface {
	FetchSourceContent(ctx context.Context, workspaceID uint, integration string, data map[string]any) string
	FetchViewRows(ctx context.Context, workspaceID uint, taskID string, queryHint string) string
}
