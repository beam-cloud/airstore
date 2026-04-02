package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

const graphAPIBase = "https://graph.microsoft.com/v1.0"

// OutlookClient provides access to the Microsoft Graph Mail API.
type OutlookClient struct {
	HTTPClient *http.Client
}

func NewOutlookClient() *OutlookClient {
	return &OutlookClient{
		HTTPClient: &http.Client{Timeout: 60 * time.Second},
	}
}

func (c *OutlookClient) Integration() types.IntegrationName {
	return types.Outlook
}

// OutlookMessage represents a mail message from Microsoft Graph.
type OutlookMessage struct {
	ID               string              `json:"id"`
	Subject          string              `json:"subject"`
	BodyPreview      string              `json:"bodyPreview"`
	Body             *OutlookMessageBody `json:"body,omitempty"`
	From             *OutlookRecipient   `json:"from,omitempty"`
	ToRecipients     []OutlookRecipient  `json:"toRecipients,omitempty"`
	CcRecipients     []OutlookRecipient  `json:"ccRecipients,omitempty"`
	ReceivedDateTime string              `json:"receivedDateTime"`
	SentDateTime     string              `json:"sentDateTime,omitempty"`
	IsRead           bool                `json:"isRead"`
	IsDraft          bool                `json:"isDraft"`
	HasAttachments   bool                `json:"hasAttachments"`
	Importance       string              `json:"importance"`
	Flag             *OutlookFlag        `json:"flag,omitempty"`
	ConversationID   string              `json:"conversationId,omitempty"`
	ParentFolderID   string              `json:"parentFolderId,omitempty"`
	WebLink          string              `json:"webLink,omitempty"`
}

type OutlookMessageBody struct {
	ContentType string `json:"contentType"`
	Content     string `json:"content"`
}

type OutlookRecipient struct {
	EmailAddress OutlookEmailAddress `json:"emailAddress"`
}

type OutlookEmailAddress struct {
	Name    string `json:"name"`
	Address string `json:"address"`
}

type OutlookFlag struct {
	FlagStatus string `json:"flagStatus"` // "notFlagged", "flagged", "complete"
}

// OutlookMailFolder represents a mail folder from Microsoft Graph.
type OutlookMailFolder struct {
	ID              string `json:"id"`
	DisplayName     string `json:"displayName"`
	TotalItemCount  int    `json:"totalItemCount"`
	UnreadItemCount int    `json:"unreadItemCount"`
	ParentFolderID  string `json:"parentFolderId,omitempty"`
}

// OutlookAttachment represents a mail attachment from Microsoft Graph.
type OutlookAttachment struct {
	ODataType    string `json:"@odata.type"`
	ID           string `json:"id"`
	Name         string `json:"name"`
	ContentType  string `json:"contentType"`
	Size         int    `json:"size"`
	IsInline     bool   `json:"isInline"`
	ContentBytes string `json:"contentBytes,omitempty"` // base64-encoded, present for file attachments
}

// OutlookMessageList wraps a Graph API list response with pagination.
type OutlookMessageList struct {
	Messages      []OutlookMessage
	NextPageToken string
}

// messageSelect is the default $select for message queries.
var messageSelect = strings.Join([]string{
	"id", "subject", "bodyPreview", "from", "toRecipients", "ccRecipients",
	"receivedDateTime", "sentDateTime", "isRead", "isDraft",
	"hasAttachments", "importance", "flag", "conversationId",
	"parentFolderId", "webLink",
}, ",")

// messageSelectWithBody includes the body field.
var messageSelectWithBody = messageSelect + ",body"

// request performs an authenticated GET against the Graph API.
func (c *OutlookClient) request(ctx context.Context, creds *types.IntegrationCredentials, path string, result any) error {
	reqURL := graphAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+creds.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Prefer", `outlook.body-content-type="text"`)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return graphAPIError(resp.StatusCode, body)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// requestURL performs an authenticated GET to an absolute URL (for pagination).
func (c *OutlookClient) requestURL(ctx context.Context, creds *types.IntegrationCredentials, fullURL string, result any) error {
	parsed, err := url.Parse(fullURL)
	if err != nil {
		return fmt.Errorf("invalid pagination URL: %w", err)
	}
	if parsed.Scheme != "https" || parsed.Hostname() != "graph.microsoft.com" {
		return fmt.Errorf("refusing to follow pagination URL to non-Microsoft host: %s", fullURL)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+creds.AccessToken)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Prefer", `outlook.body-content-type="text"`)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return graphAPIError(resp.StatusCode, body)
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// requestRaw performs an authenticated GET and returns raw bytes (for binary endpoints like /$value).
func (c *OutlookClient) requestRaw(ctx context.Context, creds *types.IntegrationCredentials, path string) ([]byte, error) {
	reqURL := graphAPIBase + path
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+creds.AccessToken)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, graphAPIError(resp.StatusCode, body)
	}

	return body, nil
}

// ListMessages lists messages, optionally from a well-known folder.
// folder can be a well-known name (Inbox, SentItems, Drafts) or empty for all messages.
// filter is an OData $filter expression (e.g., "isRead eq false").
func (c *OutlookClient) ListMessages(ctx context.Context, creds *types.IntegrationCredentials, folder string, top int, filter string) (*OutlookMessageList, error) {
	if top <= 0 {
		top = 25
	}

	params := url.Values{}
	params.Set("$select", messageSelect)
	params.Set("$top", strconv.Itoa(top))
	params.Set("$orderby", "receivedDateTime desc")
	if filter != "" {
		params.Set("$filter", filter)
	}

	path := "/me/messages"
	if folder != "" {
		path = fmt.Sprintf("/me/mailFolders/%s/messages", folder)
	}

	var resp struct {
		Value    []OutlookMessage `json:"value"`
		NextLink string           `json:"@odata.nextLink"`
	}
	if err := c.request(ctx, creds, path+"?"+params.Encode(), &resp); err != nil {
		return nil, err
	}

	return &OutlookMessageList{
		Messages:      resp.Value,
		NextPageToken: resp.NextLink,
	}, nil
}

// ListMessagesPage fetches the next page of messages using a pagination URL.
func (c *OutlookClient) ListMessagesPage(ctx context.Context, creds *types.IntegrationCredentials, nextLink string) (*OutlookMessageList, error) {
	var resp struct {
		Value    []OutlookMessage `json:"value"`
		NextLink string           `json:"@odata.nextLink"`
	}
	if err := c.requestURL(ctx, creds, nextLink, &resp); err != nil {
		return nil, err
	}

	return &OutlookMessageList{
		Messages:      resp.Value,
		NextPageToken: resp.NextLink,
	}, nil
}

// GetMessage fetches a single message by ID including the body.
func (c *OutlookClient) GetMessage(ctx context.Context, creds *types.IntegrationCredentials, messageID string) (*OutlookMessage, error) {
	params := url.Values{}
	params.Set("$select", messageSelectWithBody)

	var msg OutlookMessage
	if err := c.request(ctx, creds, "/me/messages/"+messageID+"?"+params.Encode(), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}

// ListConversationMessages fetches all messages for a specific Outlook conversation.
// Microsoft Graph rejects some conversationId filters when combined with $orderby,
// so we omit server-side ordering and sort client-side by receivedDateTime.
func (c *OutlookClient) ListConversationMessages(ctx context.Context, creds *types.IntegrationCredentials, conversationID string) (*OutlookMessageList, error) {
	params := url.Values{}
	params.Set("$select", messageSelectWithBody)
	params.Set("$top", "100")
	sanitized := strings.ReplaceAll(strings.TrimSpace(conversationID), "'", "''")
	params.Set("$filter", fmt.Sprintf("conversationId eq '%s'", sanitized))

	var resp struct {
		Value []OutlookMessage `json:"value"`
	}
	if err := c.request(ctx, creds, "/me/messages?"+params.Encode(), &resp); err != nil {
		return nil, err
	}
	sort.SliceStable(resp.Value, func(i, j int) bool {
		return resp.Value[i].ReceivedDateTime < resp.Value[j].ReceivedDateTime
	})

	return &OutlookMessageList{
		Messages: resp.Value,
	}, nil
}

// SearchMessages searches messages using the $search query parameter.
func (c *OutlookClient) SearchMessages(ctx context.Context, creds *types.IntegrationCredentials, query string, top int) (*OutlookMessageList, error) {
	if top <= 0 {
		top = 25
	}

	params := url.Values{}
	params.Set("$select", messageSelect)
	params.Set("$top", strconv.Itoa(top))
	params.Set("$search", fmt.Sprintf("%q", query))

	var resp struct {
		Value    []OutlookMessage `json:"value"`
		NextLink string           `json:"@odata.nextLink"`
	}
	if err := c.request(ctx, creds, "/me/messages?"+params.Encode(), &resp); err != nil {
		return nil, err
	}

	return &OutlookMessageList{
		Messages:      resp.Value,
		NextPageToken: resp.NextLink,
	}, nil
}

// ListMailFolders lists the user's mail folders.
func (c *OutlookClient) ListMailFolders(ctx context.Context, creds *types.IntegrationCredentials) ([]OutlookMailFolder, error) {
	var resp struct {
		Value []OutlookMailFolder `json:"value"`
	}
	if err := c.request(ctx, creds, "/me/mailFolders?$top=100", &resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// ListAttachments lists attachments for a message (metadata only, no content bytes).
func (c *OutlookClient) ListAttachments(ctx context.Context, creds *types.IntegrationCredentials, messageID string) ([]OutlookAttachment, error) {
	params := url.Values{}
	params.Set("$select", "id,name,contentType,size,isInline,@odata.type")

	var resp struct {
		Value []OutlookAttachment `json:"value"`
	}
	path := fmt.Sprintf("/me/messages/%s/attachments?%s", messageID, params.Encode())
	if err := c.request(ctx, creds, path, &resp); err != nil {
		return nil, err
	}
	return resp.Value, nil
}

// GetAttachment fetches a single attachment by ID including content bytes.
func (c *OutlookClient) GetAttachment(ctx context.Context, creds *types.IntegrationCredentials, messageID, attachmentID string) (*OutlookAttachment, error) {
	var att OutlookAttachment
	path := fmt.Sprintf("/me/messages/%s/attachments/%s", messageID, attachmentID)
	if err := c.request(ctx, creds, path, &att); err != nil {
		return nil, err
	}
	return &att, nil
}

// GetAttachmentContent fetches raw attachment bytes via the /$value endpoint.
// This is the fallback for large attachments where contentBytes may be omitted.
func (c *OutlookClient) GetAttachmentContent(ctx context.Context, creds *types.IntegrationCredentials, messageID, attachmentID string) ([]byte, error) {
	path := fmt.Sprintf("/me/messages/%s/attachments/%s/$value", messageID, attachmentID)
	return c.requestRaw(ctx, creds, path)
}

// SenderString returns a display string for the message sender.
func (m *OutlookMessage) SenderString() string {
	if m.From == nil {
		return "unknown"
	}
	if m.From.EmailAddress.Name != "" {
		return m.From.EmailAddress.Name
	}
	return m.From.EmailAddress.Address
}

// SenderEmail returns the sender's email address.
func (m *OutlookMessage) SenderEmail() string {
	if m.From == nil {
		return "unknown"
	}
	return m.From.EmailAddress.Address
}

// ReceivedTime parses the receivedDateTime field.
func (m *OutlookMessage) ReceivedTime() time.Time {
	t, err := time.Parse(time.RFC3339, m.ReceivedDateTime)
	if err != nil {
		return time.Time{}
	}
	return t
}

// IsFlagged returns true if the message is flagged.
func (m *OutlookMessage) IsFlagged() bool {
	return m.Flag != nil && m.Flag.FlagStatus == "flagged"
}

func graphAPIError(statusCode int, body []byte) error {
	var apiErr struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &apiErr) == nil && apiErr.Error.Message != "" {
		return fmt.Errorf("graph API: %s: %s (HTTP %d)", apiErr.Error.Code, apiErr.Error.Message, statusCode)
	}

	snippet := strings.TrimSpace(string(body))
	if len(snippet) > 2048 {
		snippet = snippet[:2048] + "..."
	}
	if snippet != "" {
		return fmt.Errorf("graph API: HTTP %d - %s", statusCode, snippet)
	}
	return fmt.Errorf("graph API: HTTP %d", statusCode)
}
