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
		from := ""
		if len(m.From) > 0 {
			from = m.From[0]
		}
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
		from := ""
		if len(m.From) > 0 {
			from = m.From[0]
		}
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
