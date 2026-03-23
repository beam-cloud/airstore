package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestGmailCreateDraftSupportsThreadID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if raw, _ := message["raw"].(string); raw == "" {
			t.Fatal("message.raw was empty")
		}

		w.Header().Set("Content-Type", "application/json")
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
