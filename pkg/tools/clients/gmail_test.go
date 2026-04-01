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

func TestGmailCreateDraftSupportsThreadID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle the thread metadata fetch for reply headers.
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/threads/") {
			_, _ = io.WriteString(w, `{
				"id": "thread-123",
				"messages": [
					{"id":"msg-1","payload":{"headers":[{"name":"Message-ID","value":"<orig@example.com>"}]}}
				]
			}`)
			return
		}

		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/drafts" {
			t.Fatalf("path = %s, want /drafts", r.URL.Path)
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		message, ok := payload["message"].(map[string]any)
		if !ok {
			t.Fatalf("message payload missing: %#v", payload)
		}
		if got, want := message["threadId"], "thread-123"; got != want {
			t.Fatalf("message.threadId = %v, want %q", got, want)
		}
		rawB64, _ := message["raw"].(string)
		if rawB64 == "" {
			t.Fatal("message.raw was empty")
		}

		// Verify the raw email contains In-Reply-To and References headers.
		rawBytes, err := base64.RawURLEncoding.DecodeString(rawB64)
		if err != nil {
			t.Fatalf("decode raw: %v", err)
		}
		rawStr := string(rawBytes)
		if !strings.Contains(rawStr, "In-Reply-To: <orig@example.com>") {
			t.Fatalf("expected In-Reply-To header in raw email, got:\n%s", rawStr)
		}
		if !strings.Contains(rawStr, "References: <orig@example.com>") {
			t.Fatalf("expected References header in raw email, got:\n%s", rawStr)
		}

		_, _ = io.WriteString(w, `{"id":"draft-1","message":{"id":"msg-1","threadId":"thread-123","labelIds":["DRAFT"]}}`)
	}))
	defer server.Close()

	client := NewGmailClient()
	client.api.baseURL = server.URL
	client.api.httpClient = server.Client()

	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gmailCmdCreateDraft, map[string]any{
		"to":        "luke@beam.cloud",
		"subject":   "Re: Airstore probe",
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
	if got, want := out["thread_id"], "thread-123"; got != want {
		t.Fatalf("output thread_id = %v, want %q", got, want)
	}
	if got, want := out["draft_id"], "draft-1"; got != want {
		t.Fatalf("output draft_id = %v, want %q", got, want)
	}
}

func TestBuildRawEmailReplyHeaders(t *testing.T) {
	// With reply headers
	raw := buildRawEmail("test@example.com", "Re: Hello", "body text",
		"<abc@mail.gmail.com>", "<abc@mail.gmail.com> <def@mail.gmail.com>")

	if !strings.Contains(raw, "In-Reply-To: <abc@mail.gmail.com>\r\n") {
		t.Fatalf("expected In-Reply-To header, got:\n%s", raw)
	}
	if !strings.Contains(raw, "References: <abc@mail.gmail.com> <def@mail.gmail.com>\r\n") {
		t.Fatalf("expected References header, got:\n%s", raw)
	}

	// Without reply headers — should be identical to old behavior
	rawNoReply := buildRawEmail("test@example.com", "Hello", "body text", "", "")

	if strings.Contains(rawNoReply, "In-Reply-To") {
		t.Fatalf("unexpected In-Reply-To header without reply params:\n%s", rawNoReply)
	}
	if strings.Contains(rawNoReply, "References") {
		t.Fatalf("unexpected References header without reply params:\n%s", rawNoReply)
	}
}

func TestSendEmailThreadFetchFailureFallback(t *testing.T) {
	var postReceived bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Thread metadata fetch fails with 404.
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/threads/") {
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, `{"error":{"code":404,"message":"Not Found"}}`)
			return
		}

		// The email should still be sent successfully.
		if r.Method == http.MethodPost && r.URL.Path == "/messages/send" {
			postReceived = true

			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}

			// Verify threadId is still set in payload despite fetch failure.
			if got, want := payload["threadId"], "thread-404"; got != want {
				t.Fatalf("payload.threadId = %v, want %q", got, want)
			}

			_, _ = io.WriteString(w, `{"id":"msg-1","threadId":"thread-404","labelIds":["SENT"]}`)
			return
		}

		t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
	}))
	defer server.Close()

	client := NewGmailClient()
	client.api.baseURL = server.URL
	client.api.httpClient = server.Client()

	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gmailCmdSendEmail, map[string]any{
		"to":        "test@example.com",
		"subject":   "Re: Thread test",
		"body":      "Reply body.",
		"thread_id": "thread-404",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("Execute returned error: %v", err)
	}
	if !postReceived {
		t.Fatal("expected POST to /messages/send but it was never called")
	}
}
