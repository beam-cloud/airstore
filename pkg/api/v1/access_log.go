package apiv1

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/gateway/services"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/labstack/echo/v4"
)

// AccessLogGroup exposes workspace access logs from S2 streams.
type AccessLogGroup struct {
	routerGroup   *echo.Group
	backend       repository.BackendRepository
	s2Client      *common.S2Client
	sourceService *services.SourceService
}

// NewAccessLogGroup creates and registers the access log API group.
func NewAccessLogGroup(
	routerGroup *echo.Group,
	backend repository.BackendRepository,
	s2Client *common.S2Client,
	sourceService *services.SourceService,
) *AccessLogGroup {
	g := &AccessLogGroup{
		routerGroup:   routerGroup,
		backend:       backend,
		s2Client:      s2Client,
		sourceService: sourceService,
	}
	g.registerRoutes()
	return g
}

func (g *AccessLogGroup) registerRoutes() {
	g.routerGroup.GET("", g.ListReads)
	g.routerGroup.GET("/sessions", g.ListSessions)
	g.routerGroup.GET("/summary", g.GetSummary)
	g.routerGroup.GET("/read", g.ReadSource)
}

type listReadsResponse struct {
	Reads      []instrumentation.AccessEvent `json:"reads"`
	NextCursor string                        `json:"next_cursor"`
	HasMore    bool                          `json:"has_more"`
}

func resolveAccessSession(workspaceID, requested string) string {
	if session := strings.TrimSpace(requested); session != "" {
		return session
	}
	return workspaceID
}

func accessEventInScope(ev instrumentation.AccessEvent, workspaceID, sessionID string) bool {
	if ev.WorkspaceID != workspaceID {
		return false
	}
	expectedSession := resolveAccessSession(workspaceID, sessionID)
	return resolveAccessSession(workspaceID, ev.SessionID) == expectedSession
}

// ListReads returns a page of access log entries from S2.
//
//	GET /api/v1/workspaces/:workspace_id/access-log
//	Query params: start, end (unix ms), cursor (seq_num), limit, session
func (g *AccessLogGroup) ListReads(c echo.Context) error {
	ctx := c.Request().Context()

	if g.s2Client == nil || !g.s2Client.Enabled() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "access log unavailable")
	}

	wsExtId := c.Param("workspace_id")
	ws, err := g.backend.GetWorkspaceByExternalId(ctx, wsExtId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	startMs := parseIntParam(c, "start", 0)
	endMs := parseIntParam(c, "end", 0)
	cursor := parseIntParam(c, "cursor", 0)
	limit := parseIntParam(c, "limit", 100)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	session := resolveAccessSession(wsExtId, c.QueryParam("session"))
	stream := instrumentation.AccessWorkspaceStreamName(wsExtId, session)

	// Fetch more than limit to account for time-window filtering
	fetchCount := int(limit) * 2
	if fetchCount < 200 {
		fetchCount = 200
	}

	records, err := g.s2Client.Read(ctx, stream, cursor, fetchCount)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to read access log: "+err.Error())
	}

	reads := make([]instrumentation.AccessEvent, 0, limit)
	var nextSeqNum int64 = cursor

	for _, r := range records {
		if r.SeqNum >= nextSeqNum {
			nextSeqNum = r.SeqNum + 1
		}

		var ev instrumentation.AccessEvent
		if err := json.Unmarshal([]byte(r.Body), &ev); err != nil {
			continue
		}
		if !accessEventInScope(ev, wsExtId, session) {
			continue
		}

		if startMs > 0 && ev.Timestamp < startMs {
			continue
		}
		if endMs > 0 && ev.Timestamp > endMs {
			continue
		}

		reads = append(reads, ev)

		if int64(len(reads)) >= limit {
			break
		}
	}

	hasMore := len(records) > 0 && int64(len(reads)) >= limit

	return SuccessResponse(c, listReadsResponse{
		Reads:      reads,
		NextCursor: strconv.FormatInt(nextSeqNum, 10),
		HasMore:    hasMore,
	})
}

// --- Sessions ---

type listSessionsResponse struct {
	Sessions []string `json:"sessions"`
}

// ListSessions returns distinct session IDs that have access log streams.
//
//	GET /api/v1/workspaces/:workspace_id/access-log/sessions
//
// It lists S2 streams matching the workspace-scoped prefix and extracts the
// session ID component from stream names (format: access.{workspace}.{session}.events).
func (g *AccessLogGroup) ListSessions(c echo.Context) error {
	ctx := c.Request().Context()

	if g.s2Client == nil || !g.s2Client.Enabled() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "access log unavailable")
	}

	wsExtId := c.Param("workspace_id")
	ws, err := g.backend.GetWorkspaceByExternalId(ctx, wsExtId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	streams, err := g.s2Client.ListStreams(ctx, instrumentation.AccessWorkspaceStreamPrefix(wsExtId))
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list streams: "+err.Error())
	}

	sessionSet := map[string]struct{}{
		wsExtId: {},
	}
	for _, s := range streams {
		sessionID := instrumentation.SessionIDFromWorkspaceStreamName(s.Name, wsExtId)
		if sessionID == "" {
			continue
		}
		sessionSet[sessionID] = struct{}{}
	}

	sessions := make([]string, 0, len(sessionSet))
	for sessionID := range sessionSet {
		sessions = append(sessions, sessionID)
	}

	// Sort for deterministic output
	sort.Strings(sessions)

	return SuccessResponse(c, listSessionsResponse{Sessions: sessions})
}

// --- Summary ---

type integrationStats struct {
	Events           int `json:"events"`
	OriginalTokens   int `json:"original_tokens"`
	CompressedTokens int `json:"compressed_tokens"`
}

type pathStats struct {
	Path        string `json:"path"`
	SourceURI   string `json:"source_uri"`
	Events      int    `json:"events"`
	TotalTokens int    `json:"total_tokens"`
}

type summaryResponse struct {
	TotalReads       int                         `json:"total_reads"`
	BackendReads     int                         `json:"backend_reads"`
	CacheServedReads int                         `json:"cache_served_reads"`
	OriginalTokens   int                         `json:"total_original_tokens"`
	CompressedTokens int                         `json:"total_compressed_tokens"`
	CompressionRatio float64                     `json:"compression_ratio"`
	ByIntegration    map[string]integrationStats `json:"by_integration"`
	ByOutcome        map[string]int              `json:"by_outcome"`
	ByCacheSource    map[string]int              `json:"by_cache_source"`
	TopPaths         []pathStats                 `json:"top_paths"`
}

// GetSummary aggregates access log entries within a time window.
//
//	GET /api/v1/workspaces/:workspace_id/access-log/summary
//	Query params: start, end (unix ms), session
func (g *AccessLogGroup) GetSummary(c echo.Context) error {
	ctx := c.Request().Context()

	if g.s2Client == nil || !g.s2Client.Enabled() {
		return ErrorResponse(c, http.StatusServiceUnavailable, "access log unavailable")
	}

	wsExtId := c.Param("workspace_id")
	ws, err := g.backend.GetWorkspaceByExternalId(ctx, wsExtId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	startMs := parseIntParam(c, "start", 0)
	endMs := parseIntParam(c, "end", 0)

	session := resolveAccessSession(wsExtId, c.QueryParam("session"))
	stream := instrumentation.AccessWorkspaceStreamName(wsExtId, session)

	var seqNum int64 = 0
	const pageSize = 1000
	const maxReads = 50000

	summary := summaryResponse{
		ByIntegration: make(map[string]integrationStats),
		ByOutcome:     make(map[string]int),
		ByCacheSource: make(map[string]int),
	}
	type pathAcc struct {
		sourceURI   string
		events      int
		totalTokens int
	}
	pathCounts := make(map[string]*pathAcc)

	for {
		records, err := g.s2Client.Read(ctx, stream, seqNum, pageSize)
		if err != nil {
			return ErrorResponse(c, http.StatusInternalServerError, "failed to read access log: "+err.Error())
		}
		if len(records) == 0 {
			break
		}

		for _, r := range records {
			if r.SeqNum >= seqNum {
				seqNum = r.SeqNum + 1
			}

			var ev instrumentation.AccessEvent
			if err := json.Unmarshal([]byte(r.Body), &ev); err != nil {
				continue
			}
			if !accessEventInScope(ev, wsExtId, session) {
				continue
			}

			if startMs > 0 && ev.Timestamp < startMs {
				continue
			}
			if endMs > 0 && ev.Timestamp > endMs {
				continue
			}

			summary.TotalReads++
			summary.OriginalTokens += ev.OriginalTokens
			summary.CompressedTokens += ev.CompressedTokens

			if ev.Integration != "" {
				is := summary.ByIntegration[ev.Integration]
				is.Events++
				is.OriginalTokens += ev.OriginalTokens
				is.CompressedTokens += ev.CompressedTokens
				summary.ByIntegration[ev.Integration] = is
			}

			if ev.Outcome != "" {
				summary.ByOutcome[ev.Outcome]++
			}
			if ev.CacheSource != "" {
				summary.ByCacheSource[ev.CacheSource]++
			} else {
				summary.ByCacheSource["unknown"]++
			}
			if ev.CacheSource == "backend_rpc" {
				summary.BackendReads++
			}

			if ev.Path != "" {
				pa, ok := pathCounts[ev.Path]
				if !ok {
					pa = &pathAcc{sourceURI: ev.SourceURI}
					pathCounts[ev.Path] = pa
				}
				pa.events++
				pa.totalTokens += ev.OriginalTokens
			}
		}

		if summary.TotalReads >= maxReads {
			break
		}
		if len(records) < pageSize {
			break
		}
	}

	if summary.OriginalTokens > 0 {
		summary.CompressionRatio = float64(summary.CompressedTokens) / float64(summary.OriginalTokens)
	}
	summary.CacheServedReads = summary.TotalReads - summary.BackendReads

	allPaths := make([]pathStats, 0, len(pathCounts))
	for p, pa := range pathCounts {
		allPaths = append(allPaths, pathStats{
			Path:        p,
			SourceURI:   pa.sourceURI,
			Events:      pa.events,
			TotalTokens: pa.totalTokens,
		})
	}
	sort.Slice(allPaths, func(i, j int) bool {
		return allPaths[i].TotalTokens > allPaths[j].TotalTokens
	})
	if len(allPaths) > 20 {
		allPaths = allPaths[:20]
	}
	summary.TopPaths = allPaths

	return SuccessResponse(c, summary)
}

// ReadSource fetches content directly from an upstream integration using a
// source_uri. This bypasses the source-view layer, so it works even if the
// query results have changed since the original read.
//
//	GET /api/v1/workspaces/:workspace_id/access-log/read?uri=github://abc123
func (g *AccessLogGroup) ReadSource(c echo.Context) error {
	ctx := c.Request().Context()

	if g.sourceService == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "source service unavailable")
	}

	uri := c.QueryParam("uri")
	if uri == "" {
		return ErrorResponse(c, http.StatusBadRequest, "uri parameter required")
	}

	wsExtId := c.Param("workspace_id")
	ws, err := g.backend.GetWorkspaceByExternalId(ctx, wsExtId)
	if err != nil || ws == nil {
		return ErrorResponse(c, http.StatusNotFound, "workspace not found")
	}

	memberId := auth.MemberId(ctx)

	content, err := g.sourceService.ReadBySourceURI(ctx, ws.Id, memberId, uri)
	if err != nil {
		return ErrorResponse(c, http.StatusBadRequest, err.Error())
	}

	return c.Blob(http.StatusOK, "application/octet-stream", content)
}

// parseIntParam reads an integer query parameter with a default fallback.
func parseIntParam(c echo.Context, name string, defaultVal int64) int64 {
	s := c.QueryParam(name)
	if s == "" {
		return defaultVal
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return defaultVal
	}
	return v
}
