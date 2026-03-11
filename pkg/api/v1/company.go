package apiv1

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/company"
	gamepkg "github.com/beam-cloud/airstore/pkg/company/game"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog/log"
)

// CompanyGroup handles /company/* routes for the Company Copilot.
type CompanyGroup struct {
	g            *echo.Group
	copilot      *company.Copilot
	world        *gamepkg.WorldRuntime
	gameServer   *gamepkg.GameServer
	tickLoop     *gamepkg.TickLoop
	agentAPI     *orchestration.AgentAPI
	backend      repository.BackendRepository
	integrations repository.IntegrationRepository
	store        *repository.OrchestrationStore
}

func NewCompanyGroup(
	g *echo.Group,
	copilot *company.Copilot,
	world *gamepkg.WorldRuntime,
	agentAPI *orchestration.AgentAPI,
	backend repository.BackendRepository,
	integrations repository.IntegrationRepository,
	store *repository.OrchestrationStore,
) *CompanyGroup {
	gs := gamepkg.NewGameServer(world)
	tl := gamepkg.NewTickLoop(gs)
	tl.Start(context.Background())

	cg := &CompanyGroup{
		g:            g,
		copilot:      copilot,
		world:        world,
		gameServer:   gs,
		tickLoop:     tl,
		agentAPI:     agentAPI,
		backend:      backend,
		integrations: integrations,
		store:        store,
	}
	cg.g.POST("/sessions", cg.CreateSession)
	cg.g.GET("/sessions", cg.ListSessions)
	cg.g.GET("/sessions/:session_id", cg.GetSession)
	cg.g.POST("/sessions/:session_id/chat", cg.ChatSession)
	cg.g.POST("/sessions/:session_id/confirm", cg.ConfirmChangeset)
	cg.g.GET("/stream", cg.StreamCompanyState)
	cg.g.GET("/snapshot", cg.GetSnapshot)
	cg.g.GET("/world/stream", cg.StreamCompanyWorld)
	cg.g.GET("/world/snapshot", cg.GetWorldSnapshot)
	cg.g.GET("/world/activity", cg.GetWorldActivity)
	cg.g.POST("/world/commands", cg.SubmitWorldCommand)
	cg.g.GET("/world/ws", cg.WorldWebSocket)
	return cg
}

// ---------------------------------------------------------------------------
// Session Cache (mirrors skill draftsStore)
// ---------------------------------------------------------------------------

const companySessionTTL = 30 * time.Minute

type companySession struct {
	mu          sync.Mutex
	session     *company.CopilotSession
	pending     *company.CompanyChangeset
	lastTouched time.Time
}

var companySessionsStore = struct {
	sync.Mutex
	m map[string]*companySession
}{m: make(map[string]*companySession)}

func putCompanySession(s *company.CopilotSession) *companySession {
	companySessionsStore.Lock()
	defer companySessionsStore.Unlock()

	// Evict expired entries opportunistically
	now := time.Now()
	for id, cs := range companySessionsStore.m {
		if now.Sub(cs.lastTouched) > companySessionTTL {
			delete(companySessionsStore.m, id)
		}
	}

	cs := &companySession{session: s, lastTouched: now}
	companySessionsStore.m[s.ID] = cs
	return cs
}

func (cg *CompanyGroup) getSessionCached(c echo.Context, sessionID string) (*companySession, error) {
	companySessionsStore.Lock()
	cs, ok := companySessionsStore.m[sessionID]
	companySessionsStore.Unlock()

	if ok {
		cs.lastTouched = time.Now()
		return cs, nil
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return nil, err
	}

	session, err := cg.copilot.LoadSession(c.Request().Context(), fmt.Sprintf("%d", workspaceID), sessionID)
	if err != nil {
		return nil, err
	}
	return putCompanySession(session), nil
}

// ---------------------------------------------------------------------------
// Request / Response types
// ---------------------------------------------------------------------------

type createSessionRequest struct {
	Name string `json:"name,omitempty"`
}

type createSessionResponse struct {
	SessionID string `json:"session_id"`
}

type companyChatRequest struct {
	Message string `json:"message"`
}

type worldCommandRequest struct {
	Message  string `json:"message"`
	Channel  string `json:"channel,omitempty"`
	EntityID string `json:"entity_id,omitempty"`
}

type companySSEEvent = company.CopilotSSEEvent

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func (cg *CompanyGroup) CreateSession(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	var req createSessionRequest
	if err := c.Bind(&req); err != nil {
		req = createSessionRequest{}
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = "Company Session"
	}

	session, err := cg.copilot.CreateSession(c.Request().Context(), fmt.Sprintf("%d", workspaceID), name)
	if err != nil {
		log.Error().Err(err).Msg("failed to create company session")
		return ErrorResponse(c, http.StatusInternalServerError, "failed to create session")
	}

	putCompanySession(session)
	return SuccessResponse(c, createSessionResponse{SessionID: session.ID})
}

func (cg *CompanyGroup) ListSessions(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	sessions, err := cg.copilot.ListSessions(c.Request().Context(), fmt.Sprintf("%d", workspaceID))
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to list sessions")
	}
	return SuccessResponse(c, sessions)
}

func (cg *CompanyGroup) GetSession(c echo.Context) error {
	cs, err := cg.getSessionCached(c, c.Param("session_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "session not found")
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return SuccessResponse(c, cs.session)
}

func (cg *CompanyGroup) ChatSession(c echo.Context) error {
	var req companyChatRequest
	if err := c.Bind(&req); err != nil || strings.TrimSpace(req.Message) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "message is required")
	}

	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	cs, err := cg.getSessionCached(c, c.Param("session_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "session not found")
	}

	w := c.Response()
	flusher, ok := w.Writer.(http.Flusher)
	if !ok {
		return ErrorResponse(c, http.StatusInternalServerError, "streaming not supported")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Minute))

	writeSSE := func(evt companySSEEvent) {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
		_ = rc.SetWriteDeadline(time.Now().Add(5 * time.Minute))
	}

	genCtx, genCancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer genCancel()
	go func() {
		<-c.Request().Context().Done()
		genCancel()
	}()

	cs.mu.Lock()
	defer cs.mu.Unlock()

	message := strings.TrimSpace(req.Message)
	session := cs.session

	_ = cg.copilot.PersistMessage(genCtx, session.ID, "user", message)
	session.Messages = append(session.Messages, company.CopilotMessage{
		Role:      "user",
		Content:   message,
		Timestamp: time.Now().UnixMilli(),
	})
	if cg.world != nil {
		_, _, _ = cg.world.RecordActivity(genCtx, workspaceID, "guild", fmt.Sprintf("You: %s", message), "")
	}

	writeSSE(companySSEEvent{Event: "thinking"})

	snap, err := cg.copilot.BuildSnapshot(genCtx, workspaceID)
	if err != nil {
		log.Error().Err(err).Msg("company copilot: snapshot failed")
		writeSSE(companySSEEvent{Event: "error", Error: "failed to build company snapshot"})
		writeSSE(companySSEEvent{Event: "done"})
		return nil
	}

	history := cg.copilot.FormatHistory(session.Messages[:len(session.Messages)-1])
	stateContext := company.FormatSnapshotContext(snap)

	// For now, without BAML generated code, we respond with a helpful
	// status-based response. When baml_client is generated, this will
	// call ClassifyCompanyIntent and PlanCompanyChanges.
	responseMessage := generateCopilotResponse(message, snap, history, stateContext)

	writeSSE(companySSEEvent{
		Event:   "chunk",
		Message: responseMessage,
	})

	_ = cg.copilot.PersistMessage(genCtx, session.ID, "assistant", responseMessage)
	session.Messages = append(session.Messages, company.CopilotMessage{
		Role:      "assistant",
		Content:   responseMessage,
		Timestamp: time.Now().UnixMilli(),
	})
	if cg.world != nil {
		_, _, _ = cg.world.RecordActivity(genCtx, workspaceID, "copilot", responseMessage, "")
	}

	writeSSE(companySSEEvent{
		Event:    "done",
		Message:  responseMessage,
		Snapshot: snap,
	})

	return nil
}

// ConfirmChangeset executes a pending changeset for a session.
func (cg *CompanyGroup) ConfirmChangeset(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	cs, err := cg.getSessionCached(c, c.Param("session_id"))
	if err != nil {
		return ErrorResponse(c, http.StatusNotFound, "session not found")
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()

	if cs.pending == nil {
		return ErrorResponse(c, http.StatusBadRequest, "no pending changeset")
	}

	ctx := c.Request().Context()
	executor := cg.copilot.Executor()
	results := executor.ExecuteAll(ctx, workspaceID, cs.pending.Actions)

	for _, r := range results {
		_ = cg.copilot.PersistAction(ctx, cs.session.ID, r)
		cs.session.Actions = append(cs.session.Actions, r)
	}
	if cg.world != nil {
		_, _, _ = cg.world.RecordActionResults(ctx, workspaceID, results)
		_, _, _ = cg.world.SyncWorkspace(ctx, workspaceID)
	}

	cs.pending = nil
	return SuccessResponse(c, results)
}

func (cg *CompanyGroup) StreamCompanyState(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	w := c.Response()
	flusher, ok := w.Writer.(http.Flusher)
	if !ok {
		return ErrorResponse(c, http.StatusInternalServerError, "streaming not supported")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	company.StreamCompanyState(
		c.Request().Context(),
		w,
		flusher,
		workspaceID,
		cg.agentAPI,
		cg.backend,
		cg.integrations,
		cg.store,
	)
	return nil
}

func (cg *CompanyGroup) GetSnapshot(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}

	snap, err := company.BuildSnapshot(c.Request().Context(), workspaceID, cg.agentAPI, cg.backend, cg.integrations)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to build snapshot")
	}
	return SuccessResponse(c, snap)
}

func (cg *CompanyGroup) StreamCompanyWorld(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if cg.world == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "company world runtime not configured")
	}

	w := c.Response()
	flusher, ok := w.Writer.(http.Flusher)
	if !ok {
		return ErrorResponse(c, http.StatusInternalServerError, "streaming not supported")
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	writeSSE := func(evt company.CompanyWorldStreamEvent) {
		data, _ := json.Marshal(evt)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}

	initialSnapshot, _, err := cg.world.SyncWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to build world snapshot")
	}
	writeSSE(company.CompanyWorldStreamEvent{
		Event:         company.CompanyWorldStreamEventSnapshot,
		WorldSnapshot: initialSnapshot,
		Timestamp:     time.Now().UnixMilli(),
	})

	msgCh, cancel, err := cg.world.Subscribe(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to subscribe world runtime")
	}
	defer cancel()

	heartbeat := time.NewTicker(2 * time.Second)
	defer heartbeat.Stop()

	refresh := time.NewTicker(3 * time.Second)
	defer refresh.Stop()

	var notifyCh <-chan struct{}
	var unsubscribe func()
	if cg.store != nil {
		ch, unsub, subErr := cg.store.SubscribeWorkspaceLive(c.Request().Context(), workspaceID)
		if subErr == nil {
			notifyCh = ch
			unsubscribe = unsub
		}
	}
	if unsubscribe != nil {
		defer unsubscribe()
	}

	for {
		select {
		case <-c.Request().Context().Done():
			return nil
		case <-heartbeat.C:
			writeSSE(company.CompanyWorldStreamEvent{
				Event:     company.CompanyWorldStreamEventHeartbeat,
				Timestamp: time.Now().UnixMilli(),
			})
		case <-refresh.C:
			_, _, _ = cg.world.SyncWorkspace(c.Request().Context(), workspaceID)
		case _, ok := <-notifyCh:
			if !ok {
				notifyCh = nil
				continue
			}
			_, _, _ = cg.world.SyncWorkspace(c.Request().Context(), workspaceID)
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			switch msg.Event {
			case "world_delta":
				writeSSE(company.CompanyWorldStreamEvent{
					Event:      company.CompanyWorldStreamEventDelta,
					WorldDelta: msg.Delta,
					Timestamp:  msg.Timestamp,
				})
			case "world_snapshot":
				writeSSE(company.CompanyWorldStreamEvent{
					Event:         company.CompanyWorldStreamEventSnapshot,
					WorldSnapshot: msg.Snapshot,
					Timestamp:     msg.Timestamp,
				})
			}
		}
	}
}

func (cg *CompanyGroup) GetWorldSnapshot(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if cg.world == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "company world runtime not configured")
	}
	worldSnapshot, _, err := cg.world.SyncWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to build world snapshot")
	}
	return SuccessResponse(c, worldSnapshot)
}

func (cg *CompanyGroup) GetWorldActivity(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if cg.world == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "company world runtime not configured")
	}
	worldSnapshot, _, err := cg.world.SyncWorkspace(c.Request().Context(), workspaceID)
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to build world activity")
	}
	return SuccessResponse(c, worldSnapshot.Activity)
}

func (cg *CompanyGroup) SubmitWorldCommand(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if cg.world == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "company world runtime not configured")
	}

	var req worldCommandRequest
	if err := c.Bind(&req); err != nil || strings.TrimSpace(req.Message) == "" {
		return ErrorResponse(c, http.StatusBadRequest, "message is required")
	}
	channel := strings.TrimSpace(req.Channel)
	if channel == "" {
		channel = "system"
	}
	worldSnapshot, _, err := cg.world.RecordActivity(c.Request().Context(), workspaceID, channel, strings.TrimSpace(req.Message), strings.TrimSpace(req.EntityID))
	if err != nil {
		return ErrorResponse(c, http.StatusInternalServerError, "failed to record world command")
	}
	return SuccessResponse(c, worldSnapshot)
}

func (cg *CompanyGroup) WorldWebSocket(c echo.Context) error {
	workspaceID, err := requireWorkspaceID(c)
	if err != nil {
		return err
	}
	if cg.gameServer == nil {
		return ErrorResponse(c, http.StatusServiceUnavailable, "game server not configured")
	}

	wid := fmt.Sprintf("%d", workspaceID)
	cg.tickLoop.RegisterWorkspace(wid)

	return cg.gameServer.HandleUpgrade(c.Response(), c.Request(), workspaceID)
}

// generateCopilotResponse is a placeholder that creates a helpful response
// without BAML. Once baml_client is generated, this will be replaced with
// ClassifyCompanyIntent + PlanCompanyChanges streaming calls.
func generateCopilotResponse(message string, snap *company.CompanySnapshot, history, stateContext string) string {
	lower := strings.ToLower(message)

	if strings.Contains(lower, "status") || strings.Contains(lower, "how") || strings.Contains(lower, "what's running") {
		agentCount := len(snap.Agents)
		taskCount := len(snap.RunningTasks)
		schedCount := len(snap.ScheduledTasks)
		return fmt.Sprintf("Here's your company overview: %d agents configured, %d tasks currently active, %d scheduled tasks. Total spend: $%.2f.",
			agentCount, taskCount, schedCount, snap.CostSummary.TotalUSD)
	}

	return fmt.Sprintf("I understand your request: \"%s\". Once the BAML functions are compiled, I'll be able to classify your intent, plan changes, and execute actions against your %d agents. For now, use the snapshot endpoint to see the full company state.",
		message, len(snap.Agents))
}
