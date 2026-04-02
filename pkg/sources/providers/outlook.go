package providers

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/sources/clients"
	"github.com/beam-cloud/airstore/pkg/types"
)

// Outlook categories (folders under /messages/)
var outlookCategories = []string{"inbox", "unread", "sent", "flagged", "drafts"}

// outlookCategoryConfig maps category name to Graph API folder or filter expression.
type outlookCategoryConfig struct {
	Folder string // well-known folder name (empty = use filter on all messages)
	Filter string // OData $filter (empty = no filter)
}

var outlookCategoryConfigs = map[string]outlookCategoryConfig{
	"inbox":   {Folder: "Inbox"},
	"unread":  {Filter: "isRead eq false"},
	"sent":    {Folder: "SentItems"},
	"flagged": {Filter: "flag/flagStatus eq 'flagged'"},
	"drafts":  {Folder: "Drafts"},
}

const outlookReadme = `# Outlook Integration

## Quick Start
- ` + "`cat unread.json`" + ` - See all unread emails
- ` + "`cat recent.json`" + ` - See 20 most recent emails
- ` + "`cat README.md`" + ` - Check connection status

## Structure
- ` + "`messages/inbox/`" + ` - Inbox emails by sender
- ` + "`messages/unread/`" + ` - Unread emails by sender
- ` + "`messages/sent/`" + ` - Sent emails
- ` + "`messages/flagged/`" + ` - Flagged emails
- ` + "`messages/drafts/`" + ` - Draft emails

## Finding Emails
- By sender: ` + "`ls messages/inbox/`" + ` shows senders
- By date: Folders sorted as ` + "`YYYY-MM-DD_Subject_id`" + `
- Full email: ` + "`cat messages/inbox/Sender/2026-01-29_Subject_id/body.txt`" + `
- Attachments: ` + "`ls messages/inbox/Sender/2026-01-29_Subject_id/attachments/`" + `

## File Types
- ` + "`meta.json`" + ` - Email metadata (from, to, subject, date, attachments list)
- ` + "`body.txt`" + ` - Plain text email body
- ` + "`attachments/`" + ` - Directory containing downloadable file attachments
- ` + "`index.json`" + ` - Summary of all emails in a category
`

// OutlookProvider implements sources.Provider and sources.QueryExecutor for Outlook/Microsoft 365.
// Structure: /sources/outlook/messages/{category}/{sender}/{subject}/
type OutlookProvider struct {
	client *clients.OutlookClient

	// Cache for message metadata (keyed by category)
	cacheMu      sync.RWMutex
	messageCache map[string]*outlookCategoryCache

	// Cache for attachment metadata (keyed by messageID)
	attachmentCacheMu sync.RWMutex
	attachmentCache   map[string]*outlookAttachmentCache
}

type outlookCategoryCache struct {
	messages  []outlookMessage
	fetchedAt time.Time
}

type outlookAttachmentCache struct {
	attachments []outlookAttachmentMeta
	fetchedAt   time.Time
}

// outlookAttachmentMeta holds parsed attachment data for filesystem display.
type outlookAttachmentMeta struct {
	ID          string
	Name        string
	ContentType string
	Size        int
	IsInline    bool
	SafeName    string // sanitized filename for filesystem
}

// outlookMessage holds parsed message data for folder organization
type outlookMessage struct {
	ID            string
	Subject       string
	From          string
	FromEmail     string
	To            string
	Date          string
	ReceivedTime  time.Time
	Snippet       string
	IsRead        bool
	IsFlagged     bool
	HasAttachment bool

	// Derived folder names
	SenderFolder  string
	SubjectFolder string
}

// NewOutlookProvider creates a new Outlook source provider.
func NewOutlookProvider() *OutlookProvider {
	return &OutlookProvider{
		client:          clients.NewOutlookClient(),
		messageCache:    make(map[string]*outlookCategoryCache),
		attachmentCache: make(map[string]*outlookAttachmentCache),
	}
}

func (o *OutlookProvider) Name() string {
	return types.Outlook.String()
}

// Compile-time interface checks
var (
	_ sources.Provider      = (*OutlookProvider)(nil)
	_ sources.QueryExecutor = (*OutlookProvider)(nil)
)

// Stat returns file/directory attributes
func (o *OutlookProvider) Stat(ctx context.Context, pctx *sources.ProviderContext, path string) (*sources.FileInfo, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	if path == "" {
		return sources.DirInfo(), nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "README.md":
		return sources.FileInfoFromBytes([]byte(outlookReadme)), nil
	case "unread.json":
		data, err := o.generateUnreadJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	case "recent.json":
		data, err := o.generateRecentJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	case "messages":
		return o.statMessages(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// ReadDir lists directory contents
func (o *OutlookProvider) ReadDir(ctx context.Context, pctx *sources.ProviderContext, path string) ([]sources.DirEntry, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	if path == "" {
		return []sources.DirEntry{
			fileEntry("README.md", int64(len(outlookReadme))),
			fileEntry("unread.json", 0),
			fileEntry("recent.json", 0),
			dirEntry("messages"),
		}, nil
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "messages":
		return o.readdirMessages(ctx, pctx, parts[1:])
	default:
		return nil, sources.ErrNotFound
	}
}

// Read reads file content
func (o *OutlookProvider) Read(ctx context.Context, pctx *sources.ProviderContext, path string, offset, length int64) ([]byte, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	parts := strings.Split(path, "/")

	switch parts[0] {
	case "README.md":
		return sliceData([]byte(outlookReadme), offset, length), nil
	case "unread.json":
		data, err := o.generateUnreadJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	case "recent.json":
		data, err := o.generateRecentJSON(ctx, pctx)
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	case "messages":
		return o.readMessages(ctx, pctx, parts[1:], offset, length)
	default:
		return nil, sources.ErrNotFound
	}
}

// Readlink is not supported for Outlook
func (o *OutlookProvider) Readlink(ctx context.Context, pctx *sources.ProviderContext, path string) (string, error) {
	return "", sources.ErrNotFound
}

// Search executes a search query using Microsoft Graph $search
func (o *OutlookProvider) Search(ctx context.Context, pctx *sources.ProviderContext, query string, limit int) ([]sources.SearchResult, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = 50
	}

	list, err := o.client.SearchMessages(ctx, pctx.Credentials, query, limit)
	if err != nil {
		return nil, err
	}

	results := make([]sources.SearchResult, 0, len(list.Messages))
	for _, msg := range list.Messages {
		om := convertOutlookMessage(&msg)
		filename := outlookSearchResultFilename(om)
		results = append(results, sources.SearchResult{
			Name:    filename,
			Id:      msg.ID,
			Mode:    sources.ModeFile,
			Size:    0,
			Mtime:   om.ReceivedTime.Unix(),
			Preview: msg.BodyPreview,
		})
	}
	return results, nil
}

// ============================================================================
// QueryExecutor implementation
// ============================================================================

const outlookResultAttachPrefix = "att:"

func formatOutlookAttachmentResultID(messageID, attachmentID string) string {
	return outlookResultAttachPrefix + messageID + ":" + attachmentID
}

// parseOutlookResultID parses a result ID into message and optional attachment IDs.
// Formats: "att:msgID:attID" (attachment), "msgID" (message, backward compat).
func parseOutlookResultID(resultID string) (messageID, attachmentID string, isAttachment bool) {
	if strings.HasPrefix(resultID, outlookResultAttachPrefix) {
		rest := resultID[len(outlookResultAttachPrefix):]
		if idx := strings.Index(rest, ":"); idx > 0 {
			return rest[:idx], rest[idx+1:], true
		}
	}
	return resultID, "", false
}

// ExecuteQuery runs a query against the Microsoft Graph Mail API.
func (o *OutlookProvider) ExecuteQuery(ctx context.Context, pctx *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	limit := spec.Limit
	if limit <= 0 {
		limit = 50
	}

	var list *clients.OutlookMessageList
	var err error

	if spec.PageToken != "" {
		// Use pagination URL directly
		list, err = o.client.ListMessagesPage(ctx, pctx.Credentials, spec.PageToken)
	} else {
		// Build initial query
		folder := strings.TrimSpace(spec.Metadata["folder"])
		list, err = o.client.ListMessages(ctx, pctx.Credentials, folder, limit, spec.Query)
	}
	if err != nil {
		return nil, err
	}

	includeAttachments := spec.Metadata["include_attachments"] == "true"

	results := make([]sources.QueryResult, 0, len(list.Messages))
	for _, msg := range list.Messages {
		om := convertOutlookMessage(&msg)
		metadata := map[string]string{
			"id":      msg.ID,
			"date":    om.Date,
			"from":    om.FromEmail,
			"to":      om.To,
			"subject": om.Subject,
		}
		results = append(results, sources.QueryResult{
			ID:       msg.ID,
			Filename: o.FormatFilename(spec.FilenameFormat, metadata),
			Metadata: metadata,
			Size:     0,
			Mtime:    om.ReceivedTime.Unix(),
		})

		if includeAttachments && msg.HasAttachments {
			atts, err := o.getMessageAttachments(ctx, pctx, msg.ID)
			if err != nil {
				continue // skip attachment errors, still return the message
			}
			msgFilename := o.FormatFilename(spec.FilenameFormat, metadata)
			for _, att := range atts {
				attMetadata := map[string]string{
					"id":              msg.ID,
					"date":            om.Date,
					"from":            om.FromEmail,
					"to":              om.To,
					"subject":         om.Subject,
					"result_type":     "attachment",
					"attachment_id":   att.ID,
					"attachment_name": att.Name,
					"attachment_mime": att.ContentType,
				}
				attFilename := buildOutlookAttachmentFilename(msgFilename, att.SafeName)
				results = append(results, sources.QueryResult{
					ID:       formatOutlookAttachmentResultID(msg.ID, att.ID),
					Filename: attFilename,
					Metadata: attMetadata,
					Size:     int64(att.Size),
					Mtime:    om.ReceivedTime.Unix(),
				})
			}
		}
	}

	return &sources.QueryResponse{
		Results:       results,
		NextPageToken: list.NextPageToken,
		HasMore:       list.NextPageToken != "",
	}, nil
}

// buildOutlookAttachmentFilename creates a filename for an attachment query result.
func buildOutlookAttachmentFilename(msgFilename, attSafeName string) string {
	base := strings.TrimSuffix(msgFilename, ".txt")
	return base + "__att__" + attSafeName
}

// ReadResult fetches content for a single message or attachment by ID.
// Attachment IDs use the format "att:messageID:attachmentID".
func (o *OutlookProvider) ReadResult(ctx context.Context, pctx *sources.ProviderContext, resultID string) ([]byte, error) {
	if err := checkAuth(pctx); err != nil {
		return nil, err
	}

	messageID, attachmentID, isAttachment := parseOutlookResultID(resultID)
	if isAttachment {
		return o.fetchAttachmentContent(ctx, pctx, messageID, attachmentID)
	}

	msg, err := o.client.GetMessage(ctx, pctx.Credentials, messageID)
	if err != nil {
		return nil, err
	}

	if msg.Body != nil {
		return []byte(msg.Body.Content), nil
	}
	return []byte(msg.BodyPreview), nil
}

// FormatFilename generates a filename from metadata using a format template.
// Supported placeholders: {id}, {date}, {from}, {to}, {subject}
func (o *OutlookProvider) FormatFilename(format string, metadata map[string]string) string {
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

// ============================================================================
// Messages filesystem tree
// ============================================================================

func (o *OutlookProvider) statMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string) (*sources.FileInfo, error) {
	switch len(parts) {
	case 0:
		return sources.DirInfo(), nil
	case 1:
		if !isValidOutlookCategory(parts[0]) {
			return nil, sources.ErrNotFound
		}
		return sources.DirInfo(), nil
	case 2:
		if parts[1] == "index.json" {
			data, err := o.generateCategoryIndexJSON(ctx, pctx, parts[0])
			if err != nil {
				return nil, err
			}
			return sources.FileInfoFromBytes(data), nil
		}
		return sources.DirInfo(), nil
	case 3:
		return sources.DirInfo(), nil
	case 4:
		category, senderFolder, subjectFolder, file := parts[0], parts[1], parts[2], parts[3]
		msg, err := o.findMessage(ctx, pctx, category, senderFolder, subjectFolder)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		if file == "attachments" {
			return sources.DirInfo(), nil
		}
		data, err := o.getMessageFileData(ctx, pctx, msg.ID, file)
		if err != nil {
			return nil, err
		}
		return sources.FileInfoFromBytes(data), nil
	case 5:
		if parts[3] != "attachments" {
			return nil, sources.ErrNotFound
		}
		msg, err := o.findMessage(ctx, pctx, parts[0], parts[1], parts[2])
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		att, err := o.findAttachmentByName(ctx, pctx, msg.ID, parts[4])
		if err != nil {
			return nil, err
		}
		if att == nil {
			return nil, sources.ErrNotFound
		}
		return &sources.FileInfo{
			Size:  int64(att.Size),
			Mode:  sources.ModeFile,
			Mtime: sources.NowUnix(),
			IsDir: false,
		}, nil
	default:
		return nil, sources.ErrNotFound
	}
}

func (o *OutlookProvider) readdirMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string) ([]sources.DirEntry, error) {
	switch len(parts) {
	case 0:
		entries := make([]sources.DirEntry, len(outlookCategories))
		for i, cat := range outlookCategories {
			entries[i] = dirEntry(cat)
		}
		return entries, nil

	case 1:
		messages, err := o.getValidatedCategoryMessages(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		entries := []sources.DirEntry{fileEntry("index.json", 0)}
		entries = append(entries, o.listSenders(messages)...)
		return entries, nil

	case 2:
		messages, err := o.getValidatedCategoryMessages(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		return o.listSubjectsFromSender(messages, parts[1]), nil

	case 3:
		msg, err := o.findMessage(ctx, pctx, parts[0], parts[1], parts[2])
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		metaData, _ := o.getMessageFileData(ctx, pctx, msg.ID, "meta.json")
		bodyData, _ := o.getMessageFileData(ctx, pctx, msg.ID, "body.txt")
		entries := []sources.DirEntry{
			fileEntry("meta.json", int64(len(metaData))),
			fileEntry("body.txt", int64(len(bodyData))),
		}
		if msg.HasAttachment {
			entries = append(entries, dirEntry("attachments"))
		}
		return entries, nil

	case 4:
		if parts[3] != "attachments" {
			return nil, sources.ErrNotDir
		}
		msg, err := o.findMessage(ctx, pctx, parts[0], parts[1], parts[2])
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, sources.ErrNotFound
		}
		atts, err := o.getMessageAttachments(ctx, pctx, msg.ID)
		if err != nil {
			return nil, err
		}
		entries := make([]sources.DirEntry, 0, len(atts))
		for _, att := range atts {
			entries = append(entries, fileEntry(att.SafeName, int64(att.Size)))
		}
		return entries, nil

	default:
		return nil, sources.ErrNotDir
	}
}

func (o *OutlookProvider) readMessages(ctx context.Context, pctx *sources.ProviderContext, parts []string, offset, length int64) ([]byte, error) {
	if len(parts) == 2 && parts[1] == "index.json" {
		data, err := o.generateCategoryIndexJSON(ctx, pctx, parts[0])
		if err != nil {
			return nil, err
		}
		return sliceData(data, offset, length), nil
	}

	if len(parts) < 4 {
		return nil, sources.ErrIsDir
	}

	category, senderFolder, subjectFolder, file := parts[0], parts[1], parts[2], parts[3]

	// Handle attachments directory and files
	if file == "attachments" {
		if len(parts) == 4 {
			return nil, sources.ErrIsDir
		}
		if len(parts) == 5 {
			msg, err := o.findMessage(ctx, pctx, category, senderFolder, subjectFolder)
			if err != nil {
				return nil, err
			}
			if msg == nil {
				return nil, sources.ErrNotFound
			}
			att, err := o.findAttachmentByName(ctx, pctx, msg.ID, parts[4])
			if err != nil {
				return nil, err
			}
			if att == nil {
				return nil, sources.ErrNotFound
			}
			data, err := o.fetchAttachmentContent(ctx, pctx, msg.ID, att.ID)
			if err != nil {
				return nil, err
			}
			return sliceData(data, offset, length), nil
		}
		return nil, sources.ErrNotFound
	}

	if len(parts) > 4 {
		return nil, sources.ErrNotFound
	}

	msg, err := o.findMessage(ctx, pctx, category, senderFolder, subjectFolder)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, sources.ErrNotFound
	}

	data, err := o.getMessageFileData(ctx, pctx, msg.ID, file)
	if err != nil {
		return nil, err
	}

	return sliceData(data, offset, length), nil
}

// ============================================================================
// Message helpers
// ============================================================================

const outlookCacheTTL = 10 * time.Minute

func outlookCacheKey(pctx *sources.ProviderContext, category string) string {
	return fmt.Sprintf("%d:%d:%s", pctx.WorkspaceId, pctx.MemberId, category)
}

func (o *OutlookProvider) getValidatedCategoryMessages(ctx context.Context, pctx *sources.ProviderContext, category string) ([]outlookMessage, error) {
	if !isValidOutlookCategory(category) {
		return nil, sources.ErrNotFound
	}
	return o.getCategoryMessages(ctx, pctx, category)
}

func (o *OutlookProvider) getCategoryMessages(ctx context.Context, pctx *sources.ProviderContext, category string) ([]outlookMessage, error) {
	key := outlookCacheKey(pctx, category)

	o.cacheMu.RLock()
	if cached, ok := o.messageCache[key]; ok && time.Since(cached.fetchedAt) < outlookCacheTTL {
		o.cacheMu.RUnlock()
		return cached.messages, nil
	}
	o.cacheMu.RUnlock()

	messages, err := o.fetchCategoryMessages(ctx, pctx, category)
	if err != nil {
		return nil, err
	}

	o.cacheMu.Lock()
	o.messageCache[key] = &outlookCategoryCache{
		messages:  messages,
		fetchedAt: time.Now(),
	}
	o.cacheMu.Unlock()

	return messages, nil
}

func (o *OutlookProvider) fetchCategoryMessages(ctx context.Context, pctx *sources.ProviderContext, category string) ([]outlookMessage, error) {
	cfg, ok := outlookCategoryConfigs[category]
	if !ok {
		return nil, sources.ErrNotFound
	}

	list, err := o.client.ListMessages(ctx, pctx.Credentials, cfg.Folder, 50, cfg.Filter)
	if err != nil {
		return nil, err
	}

	messages := make([]outlookMessage, 0, len(list.Messages))
	for i := range list.Messages {
		messages = append(messages, *convertOutlookMessage(&list.Messages[i]))
	}
	return messages, nil
}

// findMessage locates a message by its category, sender folder, and subject folder names.
func (o *OutlookProvider) findMessage(ctx context.Context, pctx *sources.ProviderContext, category, senderFolder, subjectFolder string) (*outlookMessage, error) {
	messages, err := o.getValidatedCategoryMessages(ctx, pctx, category)
	if err != nil {
		return nil, err
	}

	for i := range messages {
		if messages[i].SenderFolder == senderFolder && messages[i].SubjectFolder == subjectFolder {
			return &messages[i], nil
		}
	}
	return nil, nil
}

// getMessageAttachments returns cached attachment metadata for a message, fetching if needed.
func (o *OutlookProvider) getMessageAttachments(ctx context.Context, pctx *sources.ProviderContext, messageID string) ([]outlookAttachmentMeta, error) {
	o.attachmentCacheMu.RLock()
	if cached, ok := o.attachmentCache[messageID]; ok && time.Since(cached.fetchedAt) < outlookCacheTTL {
		o.attachmentCacheMu.RUnlock()
		return cached.attachments, nil
	}
	o.attachmentCacheMu.RUnlock()

	rawAtts, err := o.client.ListAttachments(ctx, pctx.Credentials, messageID)
	if err != nil {
		return nil, err
	}

	// Filter to file attachments only and build metadata
	seen := make(map[string]int)
	atts := make([]outlookAttachmentMeta, 0, len(rawAtts))
	for _, a := range rawAtts {
		if a.ODataType != "#microsoft.graph.fileAttachment" {
			continue
		}
		safeName := sources.SanitizeFilename(a.Name)
		if safeName == "" || safeName == "_unknown_" {
			safeName = "attachment"
		}
		// Deduplicate filenames within this message
		seen[safeName]++
		if seen[safeName] > 1 {
			ext := ""
			if dot := strings.LastIndex(safeName, "."); dot >= 0 {
				ext = safeName[dot:]
				safeName = safeName[:dot]
			}
			safeName = fmt.Sprintf("%s_%d%s", safeName, seen[safeName], ext)
		}
		atts = append(atts, outlookAttachmentMeta{
			ID:          a.ID,
			Name:        a.Name,
			ContentType: a.ContentType,
			Size:        a.Size,
			IsInline:    a.IsInline,
			SafeName:    safeName,
		})
	}

	o.attachmentCacheMu.Lock()
	o.attachmentCache[messageID] = &outlookAttachmentCache{
		attachments: atts,
		fetchedAt:   time.Now(),
	}
	o.attachmentCacheMu.Unlock()

	return atts, nil
}

// findAttachmentByName finds an attachment by its sanitized filename.
func (o *OutlookProvider) findAttachmentByName(ctx context.Context, pctx *sources.ProviderContext, messageID, safeName string) (*outlookAttachmentMeta, error) {
	atts, err := o.getMessageAttachments(ctx, pctx, messageID)
	if err != nil {
		return nil, err
	}
	for i := range atts {
		if atts[i].SafeName == safeName {
			return &atts[i], nil
		}
	}
	return nil, nil
}

// fetchAttachmentContent downloads the raw bytes of an attachment.
func (o *OutlookProvider) fetchAttachmentContent(ctx context.Context, pctx *sources.ProviderContext, messageID, attachmentID string) ([]byte, error) {
	att, err := o.client.GetAttachment(ctx, pctx.Credentials, messageID, attachmentID)
	if err != nil {
		return nil, err
	}
	if att.ContentBytes != "" {
		data, err := base64.StdEncoding.DecodeString(att.ContentBytes)
		if err != nil {
			return nil, fmt.Errorf("decoding attachment %s: %w", attachmentID, err)
		}
		return data, nil
	}
	// Fallback to /$value endpoint for large attachments
	return o.client.GetAttachmentContent(ctx, pctx.Credentials, messageID, attachmentID)
}

// getMessageFileData returns file content for a message (meta.json or body.txt).
func (o *OutlookProvider) getMessageFileData(ctx context.Context, pctx *sources.ProviderContext, messageID, file string) ([]byte, error) {
	switch file {
	case "meta.json":
		msg, err := o.client.GetMessage(ctx, pctx.Credentials, messageID)
		if err != nil {
			return nil, err
		}
		meta := map[string]any{
			"id":              msg.ID,
			"subject":         msg.Subject,
			"from":            msg.SenderString(),
			"from_email":      msg.SenderEmail(),
			"received":        msg.ReceivedDateTime,
			"is_read":         msg.IsRead,
			"has_attachments": msg.HasAttachments,
			"importance":      msg.Importance,
			"web_link":        msg.WebLink,
		}
		if len(msg.ToRecipients) > 0 {
			to := make([]string, 0, len(msg.ToRecipients))
			for _, r := range msg.ToRecipients {
				to = append(to, r.EmailAddress.Address)
			}
			meta["to"] = to
		}
		if len(msg.CcRecipients) > 0 {
			cc := make([]string, 0, len(msg.CcRecipients))
			for _, r := range msg.CcRecipients {
				cc = append(cc, r.EmailAddress.Address)
			}
			meta["cc"] = cc
		}
		if msg.HasAttachments {
			atts, err := o.getMessageAttachments(ctx, pctx, messageID)
			if err == nil && len(atts) > 0 {
				attList := make([]map[string]any, 0, len(atts))
				for _, att := range atts {
					attList = append(attList, map[string]any{
						"id":           att.ID,
						"name":         att.Name,
						"content_type": att.ContentType,
						"size":         att.Size,
						"is_inline":    att.IsInline,
					})
				}
				meta["attachments"] = attList
			}
		}
		data, err := json.MarshalIndent(meta, "", "  ")
		if err != nil {
			return nil, err
		}
		return append(data, '\n'), nil

	case "body.txt":
		msg, err := o.client.GetMessage(ctx, pctx.Credentials, messageID)
		if err != nil {
			return nil, err
		}
		if msg.Body != nil {
			return []byte(msg.Body.Content), nil
		}
		return []byte(msg.BodyPreview), nil

	default:
		return nil, sources.ErrNotFound
	}
}

// listSenders returns unique sender directories from messages.
func (o *OutlookProvider) listSenders(messages []outlookMessage) []sources.DirEntry {
	seen := make(map[string]bool)
	entries := make([]sources.DirEntry, 0)
	for _, msg := range messages {
		if msg.SenderFolder == "" || seen[msg.SenderFolder] {
			continue
		}
		seen[msg.SenderFolder] = true
		entries = append(entries, dirEntry(msg.SenderFolder))
	}
	return entries
}

// listSubjectsFromSender returns subject directories for a sender.
func (o *OutlookProvider) listSubjectsFromSender(messages []outlookMessage, senderFolder string) []sources.DirEntry {
	entries := make([]sources.DirEntry, 0)
	for _, msg := range messages {
		if msg.SenderFolder == senderFolder {
			entries = append(entries, dirEntry(msg.SubjectFolder))
		}
	}
	return entries
}

// ============================================================================
// JSON generation helpers
// ============================================================================

func (o *OutlookProvider) generateUnreadJSON(ctx context.Context, pctx *sources.ProviderContext) ([]byte, error) {
	list, err := o.client.ListMessages(ctx, pctx.Credentials, "", 20, "isRead eq false")
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(list.Messages))
	for _, msg := range list.Messages {
		summaries = append(summaries, map[string]any{
			"id":       msg.ID,
			"from":     msg.SenderString(),
			"subject":  msg.Subject,
			"received": msg.ReceivedDateTime,
			"preview":  truncateString(msg.BodyPreview, 100),
		})
	}

	data, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (o *OutlookProvider) generateRecentJSON(ctx context.Context, pctx *sources.ProviderContext) ([]byte, error) {
	list, err := o.client.ListMessages(ctx, pctx.Credentials, "", 20, "")
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(list.Messages))
	for _, msg := range list.Messages {
		summaries = append(summaries, map[string]any{
			"id":       msg.ID,
			"from":     msg.SenderString(),
			"subject":  msg.Subject,
			"received": msg.ReceivedDateTime,
			"is_read":  msg.IsRead,
			"preview":  truncateString(msg.BodyPreview, 100),
		})
	}

	data, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func (o *OutlookProvider) generateCategoryIndexJSON(ctx context.Context, pctx *sources.ProviderContext, category string) ([]byte, error) {
	messages, err := o.getValidatedCategoryMessages(ctx, pctx, category)
	if err != nil {
		return nil, err
	}

	summaries := make([]map[string]any, 0, len(messages))
	for _, msg := range messages {
		summaries = append(summaries, map[string]any{
			"id":      msg.ID,
			"from":    msg.From,
			"subject": msg.Subject,
			"date":    msg.Date,
			"is_read": msg.IsRead,
			"flagged": msg.IsFlagged,
			"preview": truncateString(msg.Snippet, 100),
		})
	}

	data, err := json.MarshalIndent(summaries, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// ============================================================================
// Conversion and utility helpers
// ============================================================================

// convertOutlookMessage converts a Graph API message to our internal type.
func convertOutlookMessage(msg *clients.OutlookMessage) *outlookMessage {
	receivedTime := msg.ReceivedTime()
	dateStr := receivedTime.Format("2006-01-02")

	senderName := msg.SenderString()
	senderEmail := msg.SenderEmail()
	senderFolder := sources.SanitizeFilename(extractOutlookSenderName(senderName, senderEmail))

	to := ""
	if len(msg.ToRecipients) > 0 {
		to = msg.ToRecipients[0].EmailAddress.Address
	}

	subject := msg.Subject
	if subject == "" {
		subject = "(no subject)"
	}

	subjectFolder := outlookSubjectFolder(dateStr, subject, msg.ID)

	return &outlookMessage{
		ID:            msg.ID,
		Subject:       subject,
		From:          senderName,
		FromEmail:     senderEmail,
		To:            to,
		Date:          dateStr,
		ReceivedTime:  receivedTime,
		Snippet:       msg.BodyPreview,
		IsRead:        msg.IsRead,
		IsFlagged:     msg.IsFlagged(),
		HasAttachment: msg.HasAttachments,
		SenderFolder:  senderFolder,
		SubjectFolder: subjectFolder,
	}
}

// extractOutlookSenderName returns a clean folder name for a sender.
func extractOutlookSenderName(name, email string) string {
	if name != "" && !isOutlookGenericName(name) {
		return name
	}
	// Use the part before @ from the email
	if idx := strings.Index(email, "@"); idx > 0 {
		local := email[:idx]
		// Try to extract domain name for noreply-like addresses
		if isOutlookGenericName(local) {
			domain := email[idx+1:]
			if dotIdx := strings.Index(domain, "."); dotIdx > 0 {
				return domain[:dotIdx]
			}
			return domain
		}
		return local
	}
	return email
}

var outlookGenericNames = []string{
	"noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
	"notifications", "notification", "mailer", "mailer-daemon",
	"postmaster", "bounce", "auto", "automated", "system",
}

func isOutlookGenericName(name string) bool {
	lower := strings.ToLower(name)
	for _, pattern := range outlookGenericNames {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// outlookSubjectFolder creates a folder name from date, subject and message ID.
func outlookSubjectFolder(date, subject, id string) string {
	safeSubject := sources.SanitizeFilename(subject)
	if len(safeSubject) > 50 {
		safeSubject = safeSubject[:50]
	}

	idSuffix := id
	if len(idSuffix) > 8 {
		idSuffix = idSuffix[:8]
	}

	return fmt.Sprintf("%s_%s_%s", date, safeSubject, idSuffix)
}

func outlookSearchResultFilename(msg *outlookMessage) string {
	sender := sources.SanitizeFilename(msg.From)
	if len(sender) > 20 {
		sender = sender[:20]
	}

	subj := sources.SanitizeFilename(msg.Subject)
	if len(subj) > 30 {
		subj = subj[:30]
	}

	idSuffix := msg.ID
	if len(idSuffix) > 8 {
		idSuffix = idSuffix[:8]
	}

	return fmt.Sprintf("%s_%s_%s_%s.txt", msg.Date, sender, subj, idSuffix)
}

func isValidOutlookCategory(cat string) bool {
	for _, c := range outlookCategories {
		if c == cat {
			return true
		}
	}
	return false
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
