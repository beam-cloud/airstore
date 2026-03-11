package company

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/google/uuid"
)

func init() {
	common.Streams = common.Streams // ensure Streams helpers are available
}

// companyCopilotStream returns the S2 stream name for a copilot session.
func companyCopilotStream(sessionID string) string {
	return fmt.Sprintf("company-copilot.%s", sessionID)
}

// companyCopilotIndex returns the S2 stream name for a workspace's session index.
func companyCopilotIndex(workspaceID string) string {
	return fmt.Sprintf("company-copilot-index.%s", workspaceID)
}

// Copilot manages persistent company copilot sessions backed by S2.
type Copilot struct {
	s2           *common.S2Client
	agentAPI     *orchestration.AgentAPI
	backend      repository.BackendRepository
	integrations repository.IntegrationRepository
	executor     *ActionExecutor
}

// NewCopilot creates a new company copilot.
func NewCopilot(
	s2 *common.S2Client,
	agentAPI *orchestration.AgentAPI,
	backend repository.BackendRepository,
	integrations repository.IntegrationRepository,
) *Copilot {
	c := &Copilot{
		s2:           s2,
		agentAPI:     agentAPI,
		backend:      backend,
		integrations: integrations,
	}
	c.executor = NewActionExecutor(agentAPI, backend)
	return c
}

// CreateSession initializes a new copilot session.
func (c *Copilot) CreateSession(ctx context.Context, workspaceID, name string) (*CopilotSession, error) {
	now := nowMs()
	session := &CopilotSession{
		ID:          uuid.New().String(),
		WorkspaceID: workspaceID,
		Status:      SessionStatusActive,
		Name:        name,
		Messages:    []CopilotMessage{},
		Actions:     []ActionResult{},
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := c.appendEntry(ctx, session.ID, sessionStreamEntry{
		Type:        "meta",
		WorkspaceID: workspaceID,
		Name:        name,
		Timestamp:   now,
	}); err != nil {
		return nil, err
	}

	if err := c.indexSessionCreated(ctx, workspaceID, session.ID, name); err != nil {
		return nil, err
	}

	return session, nil
}

// LoadSession reconstructs a session from S2 records.
func (c *Copilot) LoadSession(ctx context.Context, workspaceID, sessionID string) (*CopilotSession, error) {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil, fmt.Errorf("S2 not configured")
	}

	records, err := c.s2.Read(ctx, companyCopilotStream(sessionID), 0, 5000)
	if err != nil {
		return nil, fmt.Errorf("read session stream: %w", err)
	}
	if len(records) == 0 {
		return nil, fmt.Errorf("session not found")
	}

	return decodeSessionRecords(workspaceID, sessionID, records)
}

func decodeSessionRecords(workspaceID, sessionID string, records []common.ReadRecord) (*CopilotSession, error) {
	session := &CopilotSession{
		ID:       sessionID,
		Status:   SessionStatusActive,
		Messages: []CopilotMessage{},
		Actions:  []ActionResult{},
	}

	for _, rec := range records {
		var entry sessionStreamEntry
		if err := json.Unmarshal([]byte(rec.Body), &entry); err != nil {
			continue
		}
		switch entry.Type {
		case "meta":
			session.WorkspaceID = entry.WorkspaceID
			session.Name = entry.Name
			session.CreatedAt = entry.Timestamp
			bumpUpdatedAt(session, entry.Timestamp)
		case "message":
			session.Messages = append(session.Messages, CopilotMessage{
				Role:      entry.Role,
				Content:   entry.Content,
				Timestamp: entry.Timestamp,
			})
			bumpUpdatedAt(session, entry.Timestamp)
		case "action":
			if entry.Action != nil {
				session.Actions = append(session.Actions, *entry.Action)
			}
			bumpUpdatedAt(session, entry.Timestamp)
		case "status":
			if entry.Content == "archived" {
				session.Status = SessionStatusArchived
			}
			bumpUpdatedAt(session, entry.Timestamp)
		}
	}

	if session.WorkspaceID == "" || (workspaceID != "" && session.WorkspaceID != workspaceID) {
		return nil, fmt.Errorf("session not found")
	}

	return session, nil
}

// ListSessions returns summaries of all sessions for a workspace.
func (c *Copilot) ListSessions(ctx context.Context, workspaceID string) ([]SessionSummary, error) {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil, nil
	}

	records, err := c.s2.Read(ctx, companyCopilotIndex(workspaceID), 0, 1000)
	if err != nil {
		return nil, err
	}

	sessions := make(map[string]*SessionSummary)
	for _, rec := range records {
		var entry SessionIndexEntry
		if err := json.Unmarshal([]byte(rec.Body), &entry); err != nil {
			continue
		}
		switch entry.Type {
		case "created":
			sessions[entry.SessionID] = &SessionSummary{
				ID:        entry.SessionID,
				Name:      entry.Name,
				Status:    "active",
				CreatedAt: entry.Timestamp,
				UpdatedAt: entry.Timestamp,
			}
		case "archived":
			if s, ok := sessions[entry.SessionID]; ok {
				s.Status = "archived"
				s.UpdatedAt = entry.Timestamp
			}
		}
	}

	result := make([]SessionSummary, 0, len(sessions))
	for _, s := range sessions {
		result = append(result, *s)
	}
	return result, nil
}

// PersistMessage appends a chat message to the session stream.
func (c *Copilot) PersistMessage(ctx context.Context, sessionID, role, content string) error {
	return c.appendEntry(ctx, sessionID, sessionStreamEntry{
		Type:      "message",
		Role:      role,
		Content:   content,
		Timestamp: nowMs(),
	})
}

// PersistAction appends an action result to the session stream.
func (c *Copilot) PersistAction(ctx context.Context, sessionID string, result ActionResult) error {
	return c.appendEntry(ctx, sessionID, sessionStreamEntry{
		Type:      "action",
		Action:    &result,
		Timestamp: result.Timestamp,
	})
}

// BuildSnapshot assembles the current company state.
func (c *Copilot) BuildSnapshot(ctx context.Context, workspaceID uint) (*CompanySnapshot, error) {
	return BuildSnapshot(ctx, workspaceID, c.agentAPI, c.backend, c.integrations)
}

// FormatHistory serializes conversation messages for BAML context injection.
func (c *Copilot) FormatHistory(messages []CopilotMessage) string {
	if len(messages) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, m := range messages {
		role := "User"
		if m.Role == "assistant" {
			role = "Assistant"
		}
		sb.WriteString(fmt.Sprintf("[%s] %s: %s\n",
			time.UnixMilli(m.Timestamp).Format("Jan 2 15:04"),
			role,
			m.Content,
		))
	}
	return sb.String()
}

// FormatSnapshotContext serializes a CompanySnapshot for BAML prompt injection.
func FormatSnapshotContext(snap *CompanySnapshot) string {
	if snap == nil {
		return "(no company data)"
	}
	var sb strings.Builder

	sb.WriteString("AGENTS:\n")
	if len(snap.Agents) == 0 {
		sb.WriteString("  (none)\n")
	}
	for _, a := range snap.Agents {
		status := "active"
		if !a.Active {
			status = "inactive"
		}
		sb.WriteString(fmt.Sprintf("  - %s (id: %s, key: %s, role: %s, state: %s, status: %s, tasks: %d, cost: $%.2f)\n",
			a.Name, a.ID, a.Key, a.Role, a.State, status, a.ActiveTaskCount, a.TotalCostUSD))
		if a.Model != "" {
			sb.WriteString(fmt.Sprintf("    model: %s\n", a.Model))
		}
		if len(a.Skills) > 0 {
			sb.WriteString(fmt.Sprintf("    skills: %s\n", strings.Join(a.Skills, ", ")))
		}
	}

	sb.WriteString("\nRUNNING TASKS:\n")
	if len(snap.RunningTasks) == 0 {
		sb.WriteString("  (none)\n")
	}
	for _, t := range snap.RunningTasks {
		sb.WriteString(fmt.Sprintf("  - [%s] %s (id: %s, agent: %s, priority: %s, cost: $%.2f)\n",
			t.State, t.PromptSummary, t.ID, t.AgentName, t.Priority, t.CostUSD))
	}

	sb.WriteString("\nSCHEDULED TASKS:\n")
	if len(snap.ScheduledTasks) == 0 {
		sb.WriteString("  (none)\n")
	}
	for _, s := range snap.ScheduledTasks {
		active := "active"
		if !s.Active {
			active = "paused"
		}
		sb.WriteString(fmt.Sprintf("  - %s for %s (cron: %s, tz: %s, %s)\n",
			s.Prompt, s.AgentName, s.CronExpr, s.Timezone, active))
	}

	sb.WriteString("\nRECENT RESULTS:\n")
	if len(snap.RecentResults) == 0 {
		sb.WriteString("  (none)\n")
	}
	for _, r := range snap.RecentResults {
		sb.WriteString(fmt.Sprintf("  - [%s] %s by %s (cost: $%.2f)\n",
			r.State, r.Prompt, r.AgentName, r.CostUSD))
	}

	sb.WriteString("\nCONNECTED SOURCES:\n")
	if len(snap.Sources) == 0 {
		sb.WriteString("  (none)\n")
	}
	for _, s := range snap.Sources {
		sb.WriteString(fmt.Sprintf("  - %s (%s)\n", s.IntegrationType, s.Status))
	}

	sb.WriteString("\nCHANNEL BINDINGS:\n")
	if len(snap.Channels) == 0 {
		sb.WriteString("  (none)\n")
	}
	for _, ch := range snap.Channels {
		sb.WriteString(fmt.Sprintf("  - %s: %s (agent: %s)\n", ch.ChannelType, ch.Address, ch.AgentName))
	}

	sb.WriteString(fmt.Sprintf("\nCOST: $%.2f total\n", snap.CostSummary.TotalUSD))

	return sb.String()
}

// Executor returns the action executor for the copilot.
func (c *Copilot) Executor() *ActionExecutor {
	return c.executor
}

func (c *Copilot) appendEntry(ctx context.Context, sessionID string, entry sessionStreamEntry) error {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil
	}
	return c.s2.Append(ctx, companyCopilotStream(sessionID), entry)
}

func (c *Copilot) indexSessionCreated(ctx context.Context, workspaceID, sessionID, name string) error {
	if c.s2 == nil || !c.s2.Enabled() {
		return nil
	}
	return c.s2.Append(ctx, companyCopilotIndex(workspaceID), SessionIndexEntry{
		Type:      "created",
		SessionID: sessionID,
		Name:      name,
		Timestamp: nowMs(),
	})
}

func bumpUpdatedAt(session *CopilotSession, ts int64) {
	if session != nil && ts > session.UpdatedAt {
		session.UpdatedAt = ts
	}
}
