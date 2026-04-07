package services

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/views"
	"github.com/rs/zerolog/log"
)

const (
	maxContentBytes    = 8 * 1024
	maxViewRows        = 20
	maxBodyTruncateLen = 2000
)

type sourceContextEnricher struct {
	registry  *sources.Registry
	fsStore   repository.FilesystemStore
	viewStore *views.ViewStore
	backend   repository.BackendRepository
}

func NewSourceContextEnricher(
	registry *sources.Registry,
	fsStore repository.FilesystemStore,
	viewStore *views.ViewStore,
	backend repository.BackendRepository,
) hooks.ContextEnricher {
	return &sourceContextEnricher{registry, fsStore, viewStore, backend}
}

func (e *sourceContextEnricher) FetchSourceContent(ctx context.Context, workspaceID uint, integration string, data map[string]any) string {
	if workspaceID == 0 || integration == "" {
		return ""
	}

	var sections []string

	if s := e.providerEnrich(ctx, workspaceID, integration, data); s != "" {
		sections = append(sections, s)
	} else if s := e.fetchGenericContent(ctx, workspaceID, data); s != "" {
		sections = append(sections, s)
	}

	if atts := e.attachmentLines(ctx, workspaceID, data); len(atts) > 0 {
		sections = append(sections, "### Attachments\n"+strings.Join(atts, "\n"))
	}

	if len(sections) == 0 {
		return ""
	}
	return truncate("## Source Content\n\n"+strings.Join(sections, "\n\n"), maxContentBytes)
}

func (e *sourceContextEnricher) providerEnrich(ctx context.Context, workspaceID uint, integration string, data map[string]any) string {
	if e.registry == nil {
		return ""
	}
	provider := e.registry.Get(integration)
	if provider == nil {
		return ""
	}
	enricher, ok := provider.(sources.HookEnricher)
	if !ok {
		return ""
	}

	pctx := &sources.ProviderContext{WorkspaceId: workspaceID}
	conn, err := e.backend.GetConnection(ctx, workspaceID, 0, integration)
	if err != nil || conn == nil {
		return ""
	}
	creds, err := repository.DecryptCredentials(conn)
	if err != nil || creds.AccessToken == "" {
		return ""
	}
	pctx.Credentials = creds

	return enricher.EnrichHookContent(ctx, pctx, data)
}

func (e *sourceContextEnricher) FetchViewRows(ctx context.Context, workspaceID uint, taskID string, queryHint string) string {
	taskID = strings.TrimSpace(taskID)
	if e.backend == nil || e.viewStore == nil || !e.viewStore.Available() || taskID == "" || workspaceID == 0 {
		return ""
	}

	task, err := e.backend.GetTask(ctx, workspaceID, taskID)
	if err != nil || task == nil || task.PayloadJSON == nil {
		return ""
	}
	viewID, _ := task.PayloadJSON["source_view_id"].(string)
	if viewID = strings.TrimSpace(viewID); viewID == "" {
		return ""
	}

	queryHint = strings.TrimSpace(queryHint)

	if ec := e.viewStore.Embedder(); ec != nil && ec.Available() && queryHint != "" {
		vec, err := ec.EmbedOne(ctx, queryHint)
		if err == nil && len(vec) > 0 {
			results, err := e.viewStore.VectorSearch(ctx, viewID, "", vec, maxViewRows)
			if err == nil && len(results) > 0 {
				rows := make([]views.ViewRow, 0, len(results))
				for _, r := range results {
					rows = append(rows, r.ViewRow)
				}
				return formatViewRows(rows)
			}
		}
	}

	// Fallback: scope to the first sheet instead of loading all rows unfiltered.
	view, err := e.backend.GetView(ctx, workspaceID, viewID)
	if err != nil || view == nil || len(view.Definition.Sheets) == 0 {
		return ""
	}
	firstSheet := view.Definition.Sheets[0]
	var componentID string
	for _, comp := range firstSheet.Components {
		if comp.Type == "table" {
			componentID = comp.ID
			break
		}
	}
	rows, _, err := e.viewStore.GetRowsPage(ctx, viewID, firstSheet.ID, componentID, 0, maxViewRows)
	if err != nil {
		log.Warn().Err(err).Str("view_id", viewID).Msg("context enrichment: failed to fetch view rows")
		return ""
	}
	return formatViewRows(rows)
}

func (e *sourceContextEnricher) fetchGenericContent(ctx context.Context, workspaceID uint, data map[string]any) string {
	queryPath := hooks.NormalizePath(dataString(data, "path"))
	if queryPath == "" || e.fsStore == nil {
		return ""
	}
	results, err := e.fsStore.GetQueryResults(ctx, workspaceID, queryPath)
	if err != nil || len(results) == 0 {
		return ""
	}

	newSet := make(map[string]struct{})
	for _, item := range splitCSV(dataString(data, "new_items")) {
		newSet[item] = struct{}{}
	}
	if len(newSet) == 0 {
		return ""
	}

	var sections []string
	for _, r := range results {
		fullPath := queryPath + "/" + r.Filename
		_, byPath := newSet[fullPath]
		_, byID := newSet[r.ID]
		if !byPath && !byID {
			continue
		}
		if r.Metadata["result_type"] == "attachment" {
			continue
		}

		content, _ := e.fsStore.GetResultContent(ctx, workspaceID, queryPath, r.ID)
		if len(content) > 0 {
			sections = append(sections, fmt.Sprintf("**%s**\n```\n%s\n```", r.Filename, truncate(string(content), maxBodyTruncateLen)))
		} else {
			sections = append(sections, fmt.Sprintf("**%s** (read from `%s`)", r.Filename, "/workspace"+fullPath))
		}
	}
	if len(sections) == 0 {
		return ""
	}
	return strings.Join(sections, "\n\n")
}

func (e *sourceContextEnricher) attachmentLines(ctx context.Context, workspaceID uint, data map[string]any) []string {
	queryPath := hooks.NormalizePath(dataString(data, "path"))
	if queryPath == "" || e.fsStore == nil {
		return nil
	}
	results, err := e.fsStore.GetQueryResults(ctx, workspaceID, queryPath)
	if err != nil {
		return nil
	}
	var lines []string
	for _, r := range results {
		if r.Metadata["result_type"] == "attachment" {
			name := r.Metadata["attachment_name"]
			if name == "" {
				name = r.Filename
			}
			lines = append(lines, fmt.Sprintf("- `%s` (%s, %s)",
				"/workspace"+queryPath+"/"+r.Filename,
				r.Metadata["attachment_mime"],
				humanBytes(r.Size)))
		}
	}
	return lines
}

func formatViewRows(rows []views.ViewRow) string {
	if len(rows) == 0 {
		return ""
	}
	limit := min(len(rows), maxViewRows)

	var parts []string
	for i, row := range rows[:limit] {
		merged := row.MergedCells()
		var cells []string
		for k, v := range merged {
			if v = strings.TrimSpace(v); v != "" {
				cells = append(cells, fmt.Sprintf("- %s: %s", k, v))
			}
		}
		if len(cells) > 0 {
			parts = append(parts, fmt.Sprintf("Row %d:\n%s", i+1, strings.Join(cells, "\n")))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	result := "## Relevant View Data\n\n" + strings.Join(parts, "\n\n")
	if len(rows) > maxViewRows {
		result += fmt.Sprintf("\n\n_(%d more rows not shown)_", len(rows)-maxViewRows)
	}
	return result
}

func dataString(data map[string]any, key string) string {
	s, _ := data[key].(string)
	return strings.TrimSpace(s)
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

var _ hooks.ContextEnricher = (*sourceContextEnricher)(nil)
