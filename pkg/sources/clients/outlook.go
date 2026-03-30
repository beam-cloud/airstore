package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	ID                 string              `json:"id"`
	Subject            string              `json:"subject"`
	BodyPreview        string              `json:"bodyPreview"`
	Body               *OutlookMessageBody `json:"body,omitempty"`
	From               *OutlookRecipient   `json:"from,omitempty"`
	ToRecipients       []OutlookRecipient  `json:"toRecipients,omitempty"`
	CcRecipients       []OutlookRecipient  `json:"ccRecipients,omitempty"`
	ReceivedDateTime   string              `json:"receivedDateTime"`
	SentDateTime       string              `json:"sentDateTime,omitempty"`
	IsRead             bool                `json:"isRead"`
	IsDraft            bool                `json:"isDraft"`
	HasAttachments     bool                `json:"hasAttachments"`
	Importance         string              `json:"importance"`
	Flag               *OutlookFlag        `json:"flag,omitempty"`
	ConversationID     string              `json:"conversationId,omitempty"`
	ParentFolderID     string              `json:"parentFolderId,omitempty"`
	WebLink            string              `json:"webLink,omitempty"`
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
	ID               string `json:"id"`
	DisplayName      string `json:"displayName"`
	TotalItemCount   int    `json:"totalItemCount"`
	UnreadItemCount  int    `json:"unreadItemCount"`
	ParentFolderID   string `json:"parentFolderId,omitempty"`
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
	if parsed.Hostname() != "graph.microsoft.com" {
		return fmt.Errorf("refusing to follow pagination URL to non-Microsoft host: %s", parsed.Hostname())
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
