package services

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuildGmailFilter_AttachmentControls(t *testing.T) {
	filter := map[string]any{
		"from":                 "billing@example.com",
		"has_attachment":       true,
		"filename":             "pdf",
		"include_inline":       true,
		"include_message_body": false,
	}
	raw, _ := json.Marshal(filter)

	querySpec, err := buildGmailFilter(raw, 25)
	if err != nil {
		t.Fatalf("buildGmailFilter returned error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(querySpec), &decoded); err != nil {
		t.Fatalf("query spec is not valid JSON: %v", err)
	}

	query, _ := decoded["gmail_query"].(string)
	if !strings.Contains(query, "has:attachment") {
		t.Fatalf("expected has:attachment in query, got %q", query)
	}
	if !strings.Contains(query, "filename:pdf") {
		t.Fatalf("expected filename:pdf in query, got %q", query)
	}

	if includeAttachments, _ := decoded["include_attachments"].(bool); !includeAttachments {
		t.Fatalf("expected include_attachments=true, got %v", decoded["include_attachments"])
	}
	if includeInline, _ := decoded["include_inline"].(bool); !includeInline {
		t.Fatalf("expected include_inline=true, got %v", decoded["include_inline"])
	}
	if includeMessageBody, ok := decoded["include_message_body"].(bool); !ok || includeMessageBody {
		t.Fatalf("expected include_message_body=false, got %v", decoded["include_message_body"])
	}
}

func TestBuildGmailFilter_ExplicitIncludeAttachmentsOverride(t *testing.T) {
	filter := map[string]any{
		"has_attachment":      true,
		"include_attachments": false,
	}
	raw, _ := json.Marshal(filter)

	querySpec, err := buildGmailFilter(raw, 50)
	if err != nil {
		t.Fatalf("buildGmailFilter returned error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal([]byte(querySpec), &decoded); err != nil {
		t.Fatalf("query spec is not valid JSON: %v", err)
	}

	includeAttachments, ok := decoded["include_attachments"].(bool)
	if !ok {
		t.Fatalf("include_attachments missing from query spec")
	}
	if includeAttachments {
		t.Fatalf("expected include_attachments=false override, got true")
	}
}
