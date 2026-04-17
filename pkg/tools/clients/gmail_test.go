package clients

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

const threadMetadataResponse = `{
	"id": "thread-123",
	"messages": [
		{
			"id": "msg-001",
			"payload": {
				"headers": [
					{"name": "Message-Id", "value": "<AAA@mail.gmail.com>"},
					{"name": "Subject", "value": "TikTok Partnership Opportunity"}
				]
			}
		},
		{
			"id": "msg-002",
			"payload": {
				"headers": [
					{"name": "Message-Id", "value": "<BBB@mail.gmail.com>"},
					{"name": "Subject", "value": "Re: TikTok Partnership Opportunity"}
				]
			}
		}
	]
}`

func TestExtractReplyHeaders_MultiMessage(t *testing.T) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(threadMetadataResponse), &raw); err != nil {
		t.Fatal(err)
	}
	inReplyTo, refs := extractReplyHeaders(raw)

	if inReplyTo != "<BBB@mail.gmail.com>" {
		t.Errorf("inReplyTo = %q, want <BBB@mail.gmail.com>", inReplyTo)
	}
	if refs != "<AAA@mail.gmail.com> <BBB@mail.gmail.com>" {
		t.Errorf("references = %q, want both message IDs space-separated", refs)
	}
}

func TestExtractReplyHeaders_SingleMessage(t *testing.T) {
	raw := map[string]any{
		"messages": []any{
			map[string]any{
				"id": "msg-001",
				"payload": map[string]any{
					"headers": []any{
						map[string]any{"name": "Message-ID", "value": "<ONLY@mail.gmail.com>"},
					},
				},
			},
		},
	}
	inReplyTo, refs := extractReplyHeaders(raw)

	if inReplyTo != "<ONLY@mail.gmail.com>" {
		t.Errorf("inReplyTo = %q, want <ONLY@mail.gmail.com>", inReplyTo)
	}
	if refs != "<ONLY@mail.gmail.com>" {
		t.Errorf("references = %q, want <ONLY@mail.gmail.com>", refs)
	}
}

func TestExtractReplyHeaders_Empty(t *testing.T) {
	raw := map[string]any{"messages": []any{}}
	inReplyTo, refs := extractReplyHeaders(raw)
	if inReplyTo != "" || refs != "" {
		t.Errorf("expected empty, got inReplyTo=%q refs=%q", inReplyTo, refs)
	}
}

func TestBuildRawEmail_WithReplyHeaders(t *testing.T) {
	raw := buildRawEmail(
		"luke@beam.cloud",
		"Re: TikTok Partnership",
		"Hello!",
		"<AAA@mail.gmail.com>",
		"<AAA@mail.gmail.com> <BBB@mail.gmail.com>",
	)

	if !strings.Contains(raw, "In-Reply-To: <AAA@mail.gmail.com>\r\n") {
		t.Errorf("missing In-Reply-To header in:\n%s", raw)
	}
	if !strings.Contains(raw, "References: <AAA@mail.gmail.com> <BBB@mail.gmail.com>\r\n") {
		t.Errorf("missing References header in:\n%s", raw)
	}
	if !strings.Contains(raw, "Subject: Re: TikTok Partnership\r\n") {
		t.Errorf("subject should be plain text (not Q-encoded) for ASCII:\n%s", raw)
	}
	if !strings.Contains(raw, "Content-Type: text/plain; charset=UTF-8\r\n") {
		t.Errorf("expected text/plain content type in:\n%s", raw)
	}
}

func TestBuildRawEmail_NoReplyHeaders(t *testing.T) {
	raw := buildRawEmail("to@test.com", "Hello", "Body", "", "")
	if strings.Contains(raw, "In-Reply-To") || strings.Contains(raw, "References") {
		t.Errorf("should not have reply headers for new message:\n%s", raw)
	}
	if !strings.Contains(raw, "Content-Type: text/plain; charset=UTF-8\r\n") {
		t.Errorf("expected text/plain content type in:\n%s", raw)
	}
}

func TestBuildRawEmail_NonASCIISubject(t *testing.T) {
	raw := buildRawEmail("to@test.com", "Héllo Wörld", "Body", "", "")
	if !strings.Contains(raw, "=?utf-8?q?") {
		t.Errorf("non-ASCII subject should be Q-encoded:\n%s", raw)
	}
}

func gmailTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/threads/"):
			_, _ = io.WriteString(w, threadMetadataResponse)

		case r.Method == http.MethodPost && r.URL.Path == "/drafts":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode draft payload: %v", err)
				http.Error(w, "bad request", 400)
				return
			}
			message, _ := payload["message"].(map[string]any)
			if message == nil {
				t.Error("missing message in draft payload")
				http.Error(w, "bad request", 400)
				return
			}
			if _, ok := message["threadId"]; !ok {
				t.Error("missing threadId in draft message")
			}
			rawB64, _ := message["raw"].(string)
			if rawB64 == "" {
				t.Error("empty raw in draft")
			}
			decoded, err := base64.RawURLEncoding.DecodeString(rawB64)
			if err != nil {
				t.Errorf("decode raw: %v", err)
			}
			if !strings.Contains(string(decoded), "In-Reply-To:") {
				t.Error("draft raw email missing In-Reply-To header")
			}
			if !strings.Contains(string(decoded), "References:") {
				t.Error("draft raw email missing References header")
			}
			_, _ = io.WriteString(w, `{"id":"draft-1","message":{"id":"msg-1","threadId":"thread-123","labelIds":["DRAFT"]}}`)

		case r.Method == http.MethodPost && r.URL.Path == "/messages/send":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode send payload: %v", err)
				http.Error(w, "bad request", 400)
				return
			}
			rawB64, _ := payload["raw"].(string)
			if rawB64 == "" {
				t.Error("empty raw in send")
			}
			decoded, err := base64.RawURLEncoding.DecodeString(rawB64)
			if err != nil {
				t.Errorf("decode raw: %v", err)
			}
			rawStr := string(decoded)
			if !strings.Contains(rawStr, "In-Reply-To:") {
				t.Errorf("send raw email missing In-Reply-To header:\n%s", rawStr)
			}
			if !strings.Contains(rawStr, "References:") {
				t.Errorf("send raw email missing References header:\n%s", rawStr)
			}
			_, _ = io.WriteString(w, `{"id":"msg-003","threadId":"thread-123","labelIds":["SENT"]}`)

		case r.Method == http.MethodPost && r.URL.Path == "/drafts/send":
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Errorf("decode drafts/send payload: %v", err)
				http.Error(w, "bad request", 400)
				return
			}
			msg, _ := payload["message"].(map[string]any)
			if msg != nil {
				rawB64, _ := msg["raw"].(string)
				if rawB64 != "" {
					decoded, _ := base64.RawURLEncoding.DecodeString(rawB64)
					rawStr := string(decoded)
					if !strings.Contains(rawStr, "In-Reply-To:") {
						t.Errorf("drafts/send raw missing In-Reply-To:\n%s", rawStr)
					}
				}
			}
			_, _ = io.WriteString(w, `{"id":"msg-003","threadId":"thread-123","labelIds":["SENT"]}`)

		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "not found", 404)
		}
	}))
}

func newTestGmailClient(t *testing.T, server *httptest.Server) *GmailClient {
	t.Helper()
	c := NewGmailClient()
	c.api.baseURL = server.URL
	c.api.httpClient = server.Client()
	return c
}

func TestGmailCreateDraftSupportsThreadID(t *testing.T) {
	server := gmailTestServer(t)
	defer server.Close()

	client := newTestGmailClient(t, server)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gmailCmdCreateDraft, map[string]any{
		"to":        "luke@beam.cloud",
		"subject":   "Re: TikTok Partnership Opportunity",
		"body":      "Threaded reply draft.",
		"thread_id": "thread-123",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["thread_id"]; got != "thread-123" {
		t.Errorf("output thread_id = %v, want thread-123", got)
	}
	if got := out["draft_id"]; got != "draft-1" {
		t.Errorf("output draft_id = %v, want draft-1", got)
	}
}

func TestGmailSendEmailInThread(t *testing.T) {
	server := gmailTestServer(t)
	defer server.Close()

	client := newTestGmailClient(t, server)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gmailCmdSendEmail, map[string]any{
		"to":        "luke@beam.cloud",
		"subject":   "Re: TikTok Partnership Opportunity",
		"body":      "Great, let's do it!",
		"thread_id": "thread-123",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["thread_id"]; got != "thread-123" {
		t.Errorf("output thread_id = %v, want thread-123", got)
	}
}

func TestGmailSendDraftInThread(t *testing.T) {
	server := gmailTestServer(t)
	defer server.Close()

	client := newTestGmailClient(t, server)
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gmailCmdSendEmail, map[string]any{
		"to":        "luke@beam.cloud",
		"subject":   "Re: TikTok Partnership Opportunity",
		"body":      "Great, let's do it!",
		"thread_id": "thread-123",
		"draft_id":  "draft-1",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}

	var out map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &out); err != nil {
		t.Fatalf("decode output: %v", err)
	}
	if got := out["thread_id"]; got != "thread-123" {
		t.Errorf("output thread_id = %v, want thread-123", got)
	}
}

func TestUnwrapBody(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "single line unchanged",
			input: "Hello world",
			want:  "Hello world",
		},
		{
			name:  "hard wraps collapsed",
			input: "I am working on behalf of an experienced\nlaundromat operator who is\nlooking to open a new location.",
			want:  "I am working on behalf of an experienced laundromat operator who is looking to open a new location.",
		},
		{
			name:  "paragraph breaks preserved",
			input: "Hi Ryan,\n\nI am working on behalf of an experienced\nlaundromat operator.\n\nThank you,\nEli",
			want:  "Hi Ryan,\n\nI am working on behalf of an experienced laundromat operator.\n\nThank you, Eli",
		},
		{
			name:  "crlf normalized",
			input: "First paragraph.\r\n\r\nSecond paragraph.",
			want:  "First paragraph.\n\nSecond paragraph.",
		},
		{
			name:  "extra blank lines collapsed",
			input: "\n\nHello\n\n\n\nWorld\n\n",
			want:  "Hello\n\nWorld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unwrapBody(tt.input)
			if got != tt.want {
				t.Errorf("unwrapBody():\n got: %q\nwant: %q", got, tt.want)
			}
		})
	}
}
