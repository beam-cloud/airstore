package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func outlookTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		// Search
		case r.Method == http.MethodGet && r.URL.Path == "/me/messages" && r.URL.Query().Get("$search") != "":
			_, _ = io.WriteString(w, `{"value":[
				{
					"id":"msg-001",
					"subject":"Test Email",
					"bodyPreview":"Hello world",
					"from":{"emailAddress":{"name":"Alice","address":"alice@example.com"}},
					"toRecipients":[{"emailAddress":{"address":"bob@example.com"}}],
					"receivedDateTime":"2026-03-15T10:00:00Z",
					"isRead":false,
					"conversationId":"conv-001",
					"webLink":"https://outlook.office.com/mail/id/msg-001"
				}
			]}`)

		// Get thread (filter by conversationId)
		case r.Method == http.MethodGet && r.URL.Path == "/me/messages" && strings.Contains(r.URL.Query().Get("$filter"), "conversationId"):
			_, _ = io.WriteString(w, `{"value":[
				{
					"id":"msg-001",
					"subject":"Thread Subject",
					"bodyPreview":"First message",
					"body":{"contentType":"text","content":"First message body"},
					"from":{"emailAddress":{"name":"Alice","address":"alice@example.com"}},
					"toRecipients":[{"emailAddress":{"address":"bob@example.com"}}],
					"receivedDateTime":"2026-03-15T10:00:00Z",
					"isRead":true,
					"conversationId":"conv-001",
					"webLink":"https://outlook.office.com/mail/id/msg-001"
				},
				{
					"id":"msg-002",
					"subject":"Re: Thread Subject",
					"bodyPreview":"Reply message",
					"body":{"contentType":"text","content":"Reply message body"},
					"from":{"emailAddress":{"name":"Bob","address":"bob@example.com"}},
					"toRecipients":[{"emailAddress":{"address":"alice@example.com"}}],
					"receivedDateTime":"2026-03-15T11:00:00Z",
					"isRead":true,
					"conversationId":"conv-001",
					"webLink":"https://outlook.office.com/mail/id/msg-002"
				}
			]}`)

		// Get single message
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/me/messages/msg-"):
			_, _ = io.WriteString(w, `{
				"id":"msg-001",
				"subject":"Test Email",
				"bodyPreview":"Hello world",
				"body":{"contentType":"text","content":"Full message body here"},
				"from":{"emailAddress":{"name":"Alice","address":"alice@example.com"}},
				"toRecipients":[{"emailAddress":{"address":"bob@example.com"}}],
				"receivedDateTime":"2026-03-15T10:00:00Z",
				"isRead":false,
				"conversationId":"conv-001",
				"webLink":"https://outlook.office.com/mail/id/msg-001"
			}`)

		// Get sender profile
		case r.Method == http.MethodGet && r.URL.Path == "/me":
			_, _ = io.WriteString(w, `{"mail":"bob@example.com","userPrincipalName":"bob@example.com"}`)

		// Create draft
		case r.Method == http.MethodPost && r.URL.Path == "/me/messages":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode draft payload: %v", err)
				http.Error(w, "bad request", 400)
				return
			}
			if _, ok := payload["subject"]; !ok {
				t.Error("missing subject in draft payload")
			}
			if _, ok := payload["toRecipients"]; !ok {
				t.Error("missing toRecipients in draft payload")
			}
			_, _ = io.WriteString(w, `{
				"id":"draft-001",
				"subject":"Draft Subject",
				"conversationId":"conv-002",
				"webLink":"https://outlook.office.com/mail/id/draft-001"
			}`)

		// Send draft
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/send"):
			w.WriteHeader(http.StatusAccepted)

		// Send new email
		case r.Method == http.MethodPost && r.URL.Path == "/me/sendMail":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode sendMail payload: %v", err)
				http.Error(w, "bad request", 400)
				return
			}
			msg, _ := payload["message"].(map[string]any)
			if msg == nil {
				t.Error("missing message in sendMail payload")
			}
			if save, _ := payload["saveToSentItems"].(bool); !save {
				t.Error("expected saveToSentItems to be true")
			}
			w.WriteHeader(http.StatusAccepted)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", 404)
		}
	}))
}

func newTestOutlookToolClient(t *testing.T, server *httptest.Server) *OutlookToolClient {
	t.Helper()
	c := NewOutlookToolClient()
	c.api.baseURL = server.URL
	c.api.httpClient = server.Client()
	return c
}

func TestOutlookSearchReturnsFormattedResults(t *testing.T) {
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
}

func TestOutlookGetMessageIncludesBody(t *testing.T) {
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
}

func TestOutlookGetThreadGroupsByConversation(t *testing.T) {
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
	messages, ok := out["messages"].([]any)
	if !ok || len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %v", out["messages"])
	}
	// Second message from bob@example.com should be outbound (sender is bob)
	second := messages[1].(map[string]any)
	if got := second["is_outbound"]; got != true {
		t.Errorf("second message is_outbound = %v, want true", got)
	}
	// First message from alice should not be outbound
	first := messages[0].(map[string]any)
	if got := first["is_outbound"]; got != false {
		t.Errorf("first message is_outbound = %v, want false", got)
	}
}

func TestOutlookCreateDraft(t *testing.T) {
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
}

func TestOutlookSendNewEmail(t *testing.T) {
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
}

func TestOutlookSendDraft(t *testing.T) {
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
	server := outlookTestServer(t)
	defer server.Close()

	client := newTestOutlookToolClient(t, server)
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
