package providers

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

const amCacheTTL = 2 * time.Minute

const agentmailReadme = `# AgentMail Integration

## Quick Start
- ` + "`cat recent.json`" + ` - See recent messages across all inboxes
- ` + "`ls inboxes/`" + ` - List available inboxes
- ` + "`cat README.md`" + ` - This help file

## Structure
- ` + "`inboxes/{address}/messages/{sender}/{date_subject}/`" + ` - Messages grouped by sender
- ` + "`inboxes/{address}/threads/{thread_id}/`" + ` - Threads with all messages

## File Types
- ` + "`meta.json`" + ` - Email metadata (from, to, subject, date)
- ` + "`body.txt`" + ` - Plain text email body
- ` + "`messages.txt`" + ` - All messages in a thread
`

// AgentMailProvider implements sources.Provider for AgentMail.
// Structure: /sources/agentmail/inboxes/{address}/messages/{sender}/{date_subject}/
type AgentMailProvider struct {
	client *clients.AgentMailClient

	cacheMu sync.RWMutex
	cache   map[string]*amInboxCache // keyed by inbox_id
}

type amInboxCache struct {
	messages  []amMessage
	fetchedAt time.Time
}

type amMessage struct {
	ID        string
	ThreadID  string
	InboxID   string
	From      string
	To        string
	Subject   string
	Text      string
	CreatedAt string
	Timestamp time.Time

	SenderFolder  string
	SubjectFolder string
}

func NewAgentMailProvider(client *clients.AgentMailClient) *AgentMailProvider {
	return &AgentMailProvider{
		client: client,
		cache:  make(map[string]*amInboxCache),
	}
}

func (a *AgentMailProvider) Name() string { return types.AgentMail.String() }

var _ sources.Provider = (*AgentMailProvider)(nil)
var _ sources.QueryExecutor = (*AgentMailProvider)(nil)
var _ sources.NativeBrowsable = (*AgentMailProvider)(nil)

func (a *AgentMailProvider) checkClient() error {
	if a.client == nil {
		return sources.ErrNotConnected
	}
	return nil
}

// ---------------------------------------------------------------------------
// Provider interface
// ---------------------------------------------------------------------------

func (a *AgentMailProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if err := a.checkClient(); err != nil {
		return nil, err
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "README.md":
		return sources.FileInfoFromBytes([]byte(agentmailReadme)), nil
	case "recent.json":
		data, err := a.generateRecentJSON(ctx)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	case "inboxes":
		return a.statInboxes(ctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

func (a *AgentMailProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if err := a.checkClient(); err != nil {
		return nil, err
	}

	if path == "" {
		return []sources.DirEntry{
			fileEntry("README.md", int64(len(agentmailReadme))),
			fileEntry("recent.json", 0),
			dirEntry("inboxes"),
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "inboxes":
		return a.readdirInboxes(ctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

func (a *AgentMailProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if err := a.checkClient(); err != nil {
		return nil, err
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "README.md":
		return sliceData([]byte(agentmailReadme), offset, length), nil
	case "recent.json":
		data, err := a.generateRecentJSON(ctx)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	case "inboxes":
		return a.readInboxes(ctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

func (a *AgentMailProvider) Readlink(_ context.Context, _ *sources.ProviderContext, _ string) (string, error) {
	return "", sources.ErrNotFound
}

func (a *AgentMailProvider) Search(_ context.Context, _ *sources.ProviderContext, _ string, _ int) ([]sources.SearchResult, error) {
	return nil, sources.ErrSearchNotSupported
}

func (a *AgentMailProvider) IsNativeBrowsable() bool { return true }

// ---------------------------------------------------------------------------
// QueryExecutor interface
// ---------------------------------------------------------------------------

func (a *AgentMailProvider) ExecuteQuery(ctx context.Context, _ *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if err := a.checkClient(); err != nil {
		return nil, err
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}

	// Determine which inboxes to query.
	inboxFilter := strings.TrimSpace(spec.Metadata["inbox_filter"])
	var inboxIDs []string
	if inboxFilter != "" {
		inboxIDs = []string{inboxFilter}
	} else {
		inboxes, _, err := a.client.ListInboxes(ctx, 100, "")
		if err != nil {
			return nil, err
		}
		for _, inbox := range inboxes {
			inboxIDs = append(inboxIDs, inbox.InboxID)
		}
	}

	// Collect messages from all target inboxes.
	var all []amMessage
	for _, id := range inboxIDs {
		msgs, err := a.getCachedMessages(ctx, id)
		if err != nil {
			continue
		}
		all = append(all, msgs...)
	}

	// Client-side text filter.
	query := strings.TrimSpace(strings.ToLower(spec.Query))
	if query != "" {
		filtered := all[:0]
		for _, m := range all {
			if matchesQuery(m, query) {
				filtered = append(filtered, m)
			}
		}
		all = filtered
	}

	// Sort newest first.
	sort.Slice(all, func(i, j int) bool {
		return all[i].Timestamp.After(all[j].Timestamp)
	})

	if len(all) > limit {
		all = all[:limit]
	}

	results := make([]sources.QueryResult, 0, len(all))
	for _, m := range all {
		metadata := map[string]string{
			"id":      m.ID,
			"date":    m.Timestamp.Format("2006-01-02"),
			"from":    extractSenderName(m.From),
			"to":      m.To,
			"subject": m.Subject,
			"inbox":   m.InboxID,
		}
		if m.ThreadID != "" {
			metadata["thread_id"] = m.ThreadID
		}
		results = append(results, sources.QueryResult{
			ID:       m.InboxID + ":" + m.ID,
			Filename: a.FormatFilename(spec.FilenameFormat, metadata),
			Metadata: metadata,
			Mtime:    m.Timestamp.Unix(),
		})
	}

	return &sources.QueryResponse{Results: results}, nil
}

func (a *AgentMailProvider) ReadResult(ctx context.Context, _ *sources.ProviderContext, resultID string) ([]byte, error) {
	if err := a.checkClient(); err != nil {
		return nil, err
	}

	inboxID, messageID, err := parseAgentMailResultID(resultID)
	if err != nil {
		return nil, err
	}

	msg, err := a.client.GetMessage(ctx, inboxID, messageID)
	if err != nil {
		return nil, err
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "From: %s\n", msg.From)
	if len(msg.To) > 0 {
		fmt.Fprintf(&sb, "To: %s\n", strings.Join(msg.To, ", "))
	}
	fmt.Fprintf(&sb, "Subject: %s\n", msg.Subject)
	fmt.Fprintf(&sb, "Date: %s\n", msg.CreatedAt)
	fmt.Fprintf(&sb, "Inbox: %s\n", msg.InboxID)
	if msg.ThreadID != "" {
		fmt.Fprintf(&sb, "Thread-ID: %s\n", msg.ThreadID)
	}
	sb.WriteString("\n")
	sb.WriteString(msg.Text)
	sb.WriteString("\n")

	return []byte(sb.String()), nil
}

func (a *AgentMailProvider) FormatFilename(format string, metadata map[string]string) string {
	if format == "" {
		format = "{date}_{from}_{subject}_{id}.txt"
	}

	result := format
	for key, value := range metadata {
		placeholder := "{" + key + "}"
		safeValue := sources.SanitizeFilename(value)
		if key != "id" && len(safeValue) > 40 {
			safeValue = safeValue[:40]
		}
		result = strings.ReplaceAll(result, placeholder, safeValue)
	}

	if result == "" || result == ".txt" {
		if id, ok := metadata["id"]; ok {
			result = id + ".txt"
		} else {
			result = "unknown.txt"
		}
	}

	return result
}

func parseAgentMailResultID(resultID string) (inboxID, messageID string, err error) {
	idx := strings.Index(resultID, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("invalid agentmail result ID: %s", resultID)
	}
	return resultID[:idx], resultID[idx+1:], nil
}

func matchesQuery(m amMessage, query string) bool {
	return strings.Contains(strings.ToLower(m.From), query) ||
		strings.Contains(strings.ToLower(m.Subject), query) ||
		strings.Contains(strings.ToLower(m.Text), query)
}

// ---------------------------------------------------------------------------
// inboxes/ routing
// ---------------------------------------------------------------------------

// statInboxes handles Stat for paths under inboxes/
func (a *AgentMailProvider) statInboxes(ctx context.Context, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	inboxID := parts[0]

	if len(parts) == 1 {
		return sources.DirInfo(), nil
	}

	switch parts[1] {
	case "messages":
		return a.statMessages(ctx, inboxID, parts[2:])
	case "threads":
		return a.statThreads(ctx, inboxID, parts[2:])
	default:
		return nil, sources.ErrNotFound
	}
}

// readdirInboxes handles ReadDir for paths under inboxes/
func (a *AgentMailProvider) readdirInboxes(ctx context.Context, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		inboxes, _, err := a.client.ListInboxes(ctx, 100, "")
		if err != nil {
			return nil, err
		}
		entries := make([]sources.DirEntry, 0, len(inboxes))
		for _, inbox := range inboxes {
			entries = append(entries, dirEntry(inbox.InboxID))
		}
		return entries, nil
	}

	inboxID := parts[0]

	if len(parts) == 1 {
		return []sources.DirEntry{
			dirEntry("messages"),
			dirEntry("threads"),
		}, nil
	}

	switch parts[1] {
	case "messages":
		return a.readdirMessages(ctx, inboxID, parts[2:])
	case "threads":
		return a.readdirThreads(ctx, inboxID, parts[2:])
	default:
		return nil, sources.ErrNotFound
	}
}

// readInboxes handles Read for paths under inboxes/
func (a *AgentMailProvider) readInboxes(ctx context.Context, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 3 {
		return nil, sources.ErrNotFound
	}

	inboxID := parts[0]

	switch parts[1] {
	case "messages":
		return a.readMessages(ctx, inboxID, parts[2:], offset, length)
	case "threads":
		return a.readThreads(ctx, inboxID, parts[2:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// ---------------------------------------------------------------------------
// messages/ — grouped by sender
// ---------------------------------------------------------------------------

func (a *AgentMailProvider) statMessages(ctx context.Context, inboxID string, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	msgs, err := a.getCachedMessages(ctx, inboxID)
	if err != nil {
		return nil, err
	}

	senderFolder := parts[0]
	if len(parts) == 1 {
		for _, m := range msgs {
			if m.SenderFolder == senderFolder {
				return sources.DirInfo(), nil
			}
		}
		return nil, sources.ErrNotFound
	}

	subjectFolder := parts[1]
	if len(parts) == 2 {
		for _, m := range msgs {
			if m.SenderFolder == senderFolder && m.SubjectFolder == subjectFolder {
				return sources.DirInfo(), nil
			}
		}
		return nil, sources.ErrNotFound
	}

	msg := a.findMessage(msgs, senderFolder, subjectFolder)
	if msg == nil {
		return nil, sources.ErrNotFound
	}

	switch parts[2] {
	case "meta.json":
		data := a.renderMeta(msg)
		return sources.FileInfoFromBytes(data), nil
	case "body.txt":
		return sources.FileInfoFromBytes([]byte(msg.Text)), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (a *AgentMailProvider) readdirMessages(ctx context.Context, inboxID string, parts []string) ([]sources.DirEntry, error) {
	msgs, err := a.getCachedMessages(ctx, inboxID)
	if err != nil {
		return nil, err
	}

	if len(parts) == 0 {
		// List senders
		seen := make(map[string]bool)
		var entries []sources.DirEntry
		for _, m := range msgs {
			if m.SenderFolder != "" && !seen[m.SenderFolder] {
				seen[m.SenderFolder] = true
				entries = append(entries, dirEntry(m.SenderFolder))
			}
		}
		return entries, nil
	}

	senderFolder := parts[0]
	if len(parts) == 1 {
		// List messages from this sender
		var entries []sources.DirEntry
		for _, m := range msgs {
			if m.SenderFolder == senderFolder {
				entries = append(entries, dirEntry(m.SubjectFolder))
			}
		}
		return entries, nil
	}

	subjectFolder := parts[1]
	if len(parts) == 2 {
		msg := a.findMessage(msgs, senderFolder, subjectFolder)
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		return []sources.DirEntry{
			fileEntry("meta.json", 0),
			fileEntry("body.txt", int64(len(msg.Text))),
		}, nil
	}

	return nil, sources.ErrNotFound
}

func (a *AgentMailProvider) readMessages(ctx context.Context, inboxID string, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 3 {
		return nil, sources.ErrNotFound
	}

	senderFolder := parts[0]
	subjectFolder := parts[1]

	msgs, err := a.getCachedMessages(ctx, inboxID)
	if err != nil {
		return nil, err
	}

	msg := a.findMessage(msgs, senderFolder, subjectFolder)
	if msg == nil {
		return nil, sources.ErrNotFound
	}

	switch parts[2] {
	case "meta.json":
		return sliceData(a.renderMeta(msg), offset, length), nil
	case "body.txt":
		return sliceData([]byte(msg.Text), offset, length), nil
	default:
		return nil, sources.ErrNotFound
	}
}

// ---------------------------------------------------------------------------
// threads/
// ---------------------------------------------------------------------------

func (a *AgentMailProvider) statThreads(ctx context.Context, inboxID string, parts []string) (*sources.FileInfo, error) {
	if len(parts) == 0 {
		return sources.DirInfo(), nil
	}

	if len(parts) == 1 {
		// Thread folder
		return sources.DirInfo(), nil
	}

	threadID := parts[0]
	thread, err := a.client.GetThread(ctx, inboxID, threadID)
	if err != nil {
		return nil, sources.ErrNotFound
	}

	switch parts[1] {
	case "meta.json":
		data := a.renderThreadMeta(thread)
		return sources.FileInfoFromBytes(data), nil
	case "messages.txt":
		data := a.renderThreadMessages(thread)
		return sources.FileInfoFromBytes(data), nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (a *AgentMailProvider) readdirThreads(ctx context.Context, inboxID string, parts []string) ([]sources.DirEntry, error) {
	if len(parts) == 0 {
		// List threads
		threads, _, err := a.client.ListThreads(ctx, inboxID, 50, "")
		if err != nil {
			return nil, err
		}
		entries := make([]sources.DirEntry, 0, len(threads))
		for _, t := range threads {
			entries = append(entries, dirEntry(t.ThreadID))
		}
		return entries, nil
	}

	if len(parts) == 1 {
		// Files in a thread folder
		return []sources.DirEntry{
			fileEntry("meta.json", 0),
			fileEntry("messages.txt", 0),
		}, nil
	}

	return nil, sources.ErrNotFound
}

func (a *AgentMailProvider) readThreads(ctx context.Context, inboxID string, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) < 2 {
		return nil, sources.ErrNotFound
	}

	threadID := parts[0]
	thread, err := a.client.GetThread(ctx, inboxID, threadID)
	if err != nil {
		return nil, sources.ErrNotFound
	}

	switch parts[1] {
	case "meta.json":
		return sliceData(a.renderThreadMeta(thread), offset, length), nil
	case "messages.txt":
		return sliceData(a.renderThreadMessages(thread), offset, length), nil
	default:
		return nil, sources.ErrNotFound
	}
}

// ---------------------------------------------------------------------------
// Caching
// ---------------------------------------------------------------------------

func (a *AgentMailProvider) getCachedMessages(ctx context.Context, inboxID string) ([]amMessage, error) {
	a.cacheMu.RLock()
	if c, ok := a.cache[inboxID]; ok && time.Since(c.fetchedAt) < amCacheTTL {
		a.cacheMu.RUnlock()
		return c.messages, nil
	}
	a.cacheMu.RUnlock()

	apiMsgs, _, err := a.client.ListMessages(ctx, inboxID, 50, "")
	if err != nil {
		return nil, err
	}

	msgs := make([]amMessage, 0, len(apiMsgs))
	for _, m := range apiMsgs {
		from := m.From
		to := ""
		if len(m.To) > 0 {
			to = m.To[0]
		}
		ts, _ := time.Parse(time.RFC3339, m.CreatedAt)

		msg := amMessage{
			ID:        m.MessageID,
			ThreadID:  m.ThreadID,
			InboxID:   m.InboxID,
			From:      from,
			To:        to,
			Subject:   m.Subject,
			Text:      m.Text,
			CreatedAt: m.CreatedAt,
			Timestamp: ts,
		}
		msg.SenderFolder = extractSenderName(from)
		msg.SubjectFolder = formatSubjectFolder(m.Subject, m.CreatedAt, m.MessageID)
		msgs = append(msgs, msg)
	}

	a.cacheMu.Lock()
	a.cache[inboxID] = &amInboxCache{messages: msgs, fetchedAt: time.Now()}
	a.cacheMu.Unlock()

	return msgs, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (a *AgentMailProvider) findMessage(msgs []amMessage, senderFolder, subjectFolder string) *amMessage {
	for i := range msgs {
		if msgs[i].SenderFolder == senderFolder && msgs[i].SubjectFolder == subjectFolder {
			return &msgs[i]
		}
	}
	return nil
}

func (a *AgentMailProvider) renderMeta(msg *amMessage) []byte {
	meta := map[string]any{
		"id":         msg.ID,
		"thread_id":  msg.ThreadID,
		"from":       msg.From,
		"to":         msg.To,
		"subject":    msg.Subject,
		"created_at": msg.CreatedAt,
	}
	data, _ := json.MarshalIndent(meta, "", "  ")
	return data
}

func (a *AgentMailProvider) renderThreadMeta(thread *clients.AgentMailThread) []byte {
	meta := map[string]any{
		"thread_id":     thread.ThreadID,
		"message_count": len(thread.Messages),
	}
	if len(thread.Messages) > 0 {
		meta["subject"] = thread.Messages[0].Subject
	}
	data, _ := json.MarshalIndent(meta, "", "  ")
	return data
}

func (a *AgentMailProvider) renderThreadMessages(thread *clients.AgentMailThread) []byte {
	var sb strings.Builder
	for i, m := range thread.Messages {
		from := m.From
		if i > 0 {
			sb.WriteString("\n---\n\n")
		}
		fmt.Fprintf(&sb, "From: %s\nDate: %s\nSubject: %s\n\n%s\n", from, m.CreatedAt, m.Subject, m.Text)
	}
	return []byte(sb.String())
}

func (a *AgentMailProvider) generateRecentJSON(ctx context.Context) ([]byte, error) {
	inboxes, _, err := a.client.ListInboxes(ctx, 100, "")
	if err != nil {
		return nil, err
	}

	type recentMsg struct {
		ID      string `json:"id"`
		Inbox   string `json:"inbox"`
		From    string `json:"from"`
		Subject string `json:"subject"`
		Date    string `json:"date"`
		Path    string `json:"path"`
	}

	var all []recentMsg
	for _, inbox := range inboxes {
		msgs, err := a.getCachedMessages(ctx, inbox.InboxID)
		if err != nil {
			continue
		}
		for _, m := range msgs {
			all = append(all, recentMsg{
				ID:      m.ID,
				Inbox:   m.InboxID,
				From:    m.From,
				Subject: m.Subject,
				Date:    m.CreatedAt,
				Path:    fmt.Sprintf("inboxes/%s/messages/%s/%s", m.InboxID, m.SenderFolder, m.SubjectFolder),
			})
		}
	}

	sort.Slice(all, func(i, j int) bool {
		return all[i].Date > all[j].Date
	})
	if len(all) > 20 {
		all = all[:20]
	}

	data, _ := json.MarshalIndent(all, "", "  ")
	return data, nil
}
