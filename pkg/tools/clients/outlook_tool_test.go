package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

type outlookRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn outlookRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonOutlookResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func outlookTestClient(t *testing.T) *OutlookToolClient {
	t.Helper()
	client := NewOutlookToolClient()
	client.api.baseURL = "https://graph.microsoft.com/v1.0"
	client.api.httpClient = &http.Client{
		Transport: outlookRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			switch {
			// Search
			case r.Method == http.MethodGet && r.URL.Path == "/v1.0/me/messages" && r.URL.Query().Get("$search") != "":
				return jsonOutlookResponse(`{"value":[
				{
					"id":"msg-001",
					"subject":"Test Email",
					"bodyPreview":"Hello world",
					"from":{"emailAddress":{"name":"Alice","address":"alice@example.com"}},
					"toRecipients":[{"emailAddress":{"address":"bob@example.com"}}],
					"receivedDateTime":"2026-03-15T10:00:00Z",
					"isRead":false,
					"hasAttachments":true,
					"conversationId":"conv-001",
					"webLink":"https://outlook.office.com/mail/id/msg-001"
				}
			]}`), nil

			// Get thread (filter by conversationId)
			case r.Method == http.MethodGet && r.URL.Path == "/v1.0/me/messages" && strings.Contains(r.URL.Query().Get("$filter"), "conversationId"):
				if got := r.URL.Query().Get("$orderby"); got != "" {
					t.Fatalf("expected get-thread to omit $orderby, got %q", got)
				}
				return jsonOutlookResponse(`{"value":[
				{
					"id":"msg-002",
					"subject":"Re: Thread Subject",
					"bodyPreview":"Reply message",
					"body":{"contentType":"text","content":"Reply message body"},
					"from":{"emailAddress":{"name":"Bob","address":"bob@example.com"}},
					"toRecipients":[{"emailAddress":{"address":"alice@example.com"}}],
					"receivedDateTime":"2026-03-15T11:00:00Z",
					"isRead":true,
					"hasAttachments":true,
					"conversationId":"conv-001",
					"webLink":"https://outlook.office.com/mail/id/msg-002"
				},
				{
					"id":"msg-001",
					"subject":"Thread Subject",
					"bodyPreview":"First message",
					"body":{"contentType":"text","content":"First message body"},
					"from":{"emailAddress":{"name":"Alice","address":"alice@example.com"}},
					"toRecipients":[{"emailAddress":{"address":"bob@example.com"}}],
					"receivedDateTime":"2026-03-15T10:00:00Z",
					"isRead":true,
					"hasAttachments":false,
					"conversationId":"conv-001",
					"webLink":"https://outlook.office.com/mail/id/msg-001"
				}
			]}`), nil

			// Get single message
			case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1.0/me/messages/msg-"):
				return jsonOutlookResponse(`{
				"id":"msg-001",
				"subject":"Test Email",
				"bodyPreview":"Hello world",
				"body":{"contentType":"text","content":"Full message body here"},
				"from":{"emailAddress":{"name":"Alice","address":"alice@example.com"}},
				"toRecipients":[{"emailAddress":{"address":"bob@example.com"}}],
				"receivedDateTime":"2026-03-15T10:00:00Z",
				"isRead":false,
				"hasAttachments":true,
				"conversationId":"conv-001",
				"webLink":"https://outlook.office.com/mail/id/msg-001"
			}`), nil

			// Get sender profile
			case r.Method == http.MethodGet && r.URL.Path == "/v1.0/me":
				return jsonOutlookResponse(`{"mail":"bob@example.com","userPrincipalName":"bob@example.com"}`), nil

				// Create draft
			case r.Method == http.MethodPost && r.URL.Path == "/v1.0/me/messages":
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Errorf("decode draft payload: %v", err)
					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{"Content-Type": []string{"application/json"}},
						Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"bad request"}}`)),
					}, nil
				}
				if _, ok := payload["subject"]; !ok {
					t.Error("missing subject in draft payload")
				}
				if _, ok := payload["toRecipients"]; !ok {
					t.Error("missing toRecipients in draft payload")
				}
				return jsonOutlookResponse(`{
				"id":"draft-001",
				"subject":"Draft Subject",
				"conversationId":"conv-002",
				"webLink":"https://outlook.office.com/mail/id/draft-001"
			}`), nil

			// Send draft
			case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/send"):
				return &http.Response{
					StatusCode: http.StatusAccepted,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader("")),
				}, nil

			default:
				t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
				return nil, nil
			}
		}),
	}
	return client
}

func TestOutlookSearchReturnsFormattedResults(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdSearch, map[string]any{
		"query": "test",
		"limit": 10,
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["query"]; got != "test" {
		t.Errorf("query = %v, want test", got)
	}
	results, ok := out["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("expected 1 result, got %v", out["results"])
	}
	first := results[0].(map[string]any)
	if got := first["message_id"]; got != "msg-001" {
		t.Errorf("message_id = %v, want msg-001", got)
	}
	if got := first["from"]; got != "Alice <alice@example.com>" {
		t.Errorf("from = %v, want Alice <alice@example.com>", got)
	}
	if got := first["thread_id"]; got != "conv-001" {
		t.Errorf("thread_id = %v, want conv-001", got)
	}
	if got := first["has_attachments"]; got != true {
		t.Errorf("has_attachments = %v, want true", got)
	}
}

func TestOutlookGetMessageIncludesBody(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdGetMessage, map[string]any{
		"message_id": "msg-001",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["body"]; got != "Full message body here" {
		t.Errorf("body = %v, want 'Full message body here'", got)
	}
	if got := out["message_id"]; got != "msg-001" {
		t.Errorf("message_id = %v, want msg-001", got)
	}
	if got := out["thread_id"]; got != "conv-001" {
		t.Errorf("thread_id = %v, want conv-001", got)
	}
	if got := out["has_attachments"]; got != true {
		t.Errorf("has_attachments = %v, want true", got)
	}
}

func TestOutlookGetThreadGroupsByConversation(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdGetThread, map[string]any{
		"conversation_id": "conv-001",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["conversation_id"]; got != "conv-001" {
		t.Errorf("conversation_id = %v, want conv-001", got)
	}
	if got := out["thread_id"]; got != "conv-001" {
		t.Errorf("thread_id = %v, want conv-001", got)
	}
	messages, ok := out["messages"].([]any)
	if !ok || len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %v", out["messages"])
	}
	if got := messages[0].(map[string]any)["message_id"]; got != "msg-001" {
		t.Fatalf("first message_id = %v, want msg-001 after client-side sort", got)
	}
	// Second message from bob@example.com should be outbound (sender is bob)
	second := messages[1].(map[string]any)
	if got := second["is_outbound"]; got != true {
		t.Errorf("second message is_outbound = %v, want true", got)
	}
	if got := second["has_attachments"]; got != true {
		t.Errorf("second message has_attachments = %v, want true", got)
	}
	// First message from alice should not be outbound
	first := messages[0].(map[string]any)
	if got := first["is_outbound"]; got != false {
		t.Errorf("first message is_outbound = %v, want false", got)
	}
	if got := first["has_attachments"]; got != false {
		t.Errorf("first message has_attachments = %v, want false", got)
	}
}

func TestOutlookCreateDraft(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdCreateDraft, map[string]any{
		"to":              "luke@beam.cloud",
		"subject":         "Test Draft",
		"body":            "Draft body.",
		"conversation_id": "conv-002",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["message_id"]; got != "draft-001" {
		t.Errorf("message_id = %v, want draft-001", got)
	}
	if got := out["conversation_id"]; got != "conv-002" {
		t.Errorf("conversation_id = %v, want conv-002", got)
	}
	if got := out["thread_id"]; got != "conv-002" {
		t.Errorf("thread_id = %v, want conv-002", got)
	}
	if got := out["draft_id"]; got != "draft-001" {
		t.Errorf("draft_id = %v, want draft-001", got)
	}
}

func TestOutlookSendNewEmail(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdSendEmail, map[string]any{
		"to":      "luke@beam.cloud",
		"subject": "Test Send",
		"body":    "Sending now.",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["status"]; got != "sent" {
		t.Errorf("status = %v, want sent", got)
	}
	if got := out["to"]; got != "luke@beam.cloud" {
		t.Errorf("to = %v, want luke@beam.cloud", got)
	}
	if got := out["conversation_id"]; got != "conv-002" {
		t.Errorf("conversation_id = %v, want conv-002", got)
	}
	if got := out["thread_id"]; got != "conv-002" {
		t.Errorf("thread_id = %v, want conv-002", got)
	}
	if got := out["message_id"]; got != "draft-001" {
		t.Errorf("message_id = %v, want draft-001", got)
	}
}

func TestOutlookSendDraft(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdSendEmail, map[string]any{
		"to":       "luke@beam.cloud",
		"subject":  "Test Send Draft",
		"body":     "Sending draft.",
		"draft_id": "draft-001",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["status"]; got != "sent" {
		t.Errorf("status = %v, want sent", got)
	}
	if got := out["message_id"]; got != "draft-001" {
		t.Errorf("message_id = %v, want draft-001", got)
	}
}

func TestOutlookSendDraftOnly(t *testing.T) {
	client := outlookTestClient(t)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), outlookCmdSendEmail, map[string]any{
		"draft_id": "draft-001",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["status"]; got != "sent" {
		t.Errorf("status = %v, want sent", got)
	}
	if got := out["message_id"]; got != "draft-001" {
		t.Errorf("message_id = %v, want draft-001", got)
	}
}

func TestOutlookStripHTML(t *testing.T) {
	html := `<html><body><p>Hello</p><br><div>World</div></body></html>`
	got := stripOutlookHTML(html)
	if !strings.Contains(got, "Hello") || !strings.Contains(got, "World") {
		t.Errorf("stripOutlookHTML failed, got %q", got)
	}
	if strings.Contains(got, "<") {
		t.Errorf("stripOutlookHTML left HTML tags: %q", got)
	}
}
