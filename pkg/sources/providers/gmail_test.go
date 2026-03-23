package providers

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func jsonHTTPResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestExecuteQuery_ExactThreadWatchIncludesAttachments(t *testing.T) {
	provider := NewGmailProvider()
	provider.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/gmail/v1/users/me/threads/thread-123" {
				t.Fatalf("unexpected request path: %s", req.URL.Path)
			}
			if got := req.URL.Query().Get("format"); got != "full" {
				t.Fatalf("expected full format request, got %q", got)
			}
			return jsonHTTPResponse(`{
				"messages": [
					{
						"id": "msg-1",
						"threadId": "thread-123",
						"snippet": "Quarterly report attached",
						"sizeEstimate": 128,
						"internalDate": "1710000000000",
						"payload": {
							"headers": [
								{"name":"From","value":"Alice Example <alice@example.com>"},
								{"name":"To","value":"me@example.com"},
								{"name":"Subject","value":"Quarterly report"},
								{"name":"Date","value":"Mon, 04 Mar 2024 12:00:00 +0000"}
							],
							"parts": [
								{"mimeType":"text/plain","body":{"data":"SGVsbG8="}},
								{"mimeType":"application/pdf","filename":"report.pdf","body":{"attachmentId":"att-1","size":1234}}
							]
						}
					}
				]
			}`), nil
		}),
	}

	resp, err := provider.ExecuteQuery(context.Background(), &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "token"},
	}, sources.QuerySpec{
		Query:          `subject:"Quarterly report"`,
		FilenameFormat: "{subject}_{id}.txt",
		Metadata: map[string]string{
			"thread_id":            "thread-123",
			"include_attachments":  "true",
			"include_message_body": "true",
		},
		Limit:      50,
		MaxResults: 500,
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("result count = %d, want 2", len(resp.Results))
	}
	if resp.Results[0].ID != "msg:msg-1" {
		t.Fatalf("first result id = %q, want message result", resp.Results[0].ID)
	}
	if resp.Results[1].ID != "att:msg-1:att-1" {
		t.Fatalf("second result id = %q, want attachment result", resp.Results[1].ID)
	}
	if got := resp.Results[1].Metadata["attachment_name"]; got != "report.pdf" {
		t.Fatalf("attachment_name = %q, want report.pdf", got)
	}
	if got := resp.Results[0].Metadata["thread_id"]; got != "thread-123" {
		t.Fatalf("thread_id = %q, want thread-123", got)
	}
}

func TestExecuteQuery_ExactMessageWatchFetchesSingleMessage(t *testing.T) {
	provider := NewGmailProvider()
	provider.httpClient = &http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path != "/gmail/v1/users/me/messages/msg-456" {
				t.Fatalf("unexpected request path: %s", req.URL.Path)
			}
			if got := req.URL.Query().Get("format"); got != "full" {
				t.Fatalf("expected full format request, got %q", got)
			}
			return jsonHTTPResponse(`{
				"id": "msg-456",
				"threadId": "thread-456",
				"snippet": "Single watched message",
				"sizeEstimate": 64,
				"internalDate": "1710000000001",
				"payload": {
					"headers": [
						{"name":"From","value":"Bob Example <bob@example.com>"},
						{"name":"To","value":"me@example.com"},
						{"name":"Subject","value":"Follow up"},
						{"name":"Date","value":"Tue, 05 Mar 2024 12:00:00 +0000"}
					],
					"body": {"data":"U2luZ2xlIG1lc3NhZ2U="}
				}
			}`), nil
		}),
	}

	resp, err := provider.ExecuteQuery(context.Background(), &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{AccessToken: "token"},
	}, sources.QuerySpec{
		FilenameFormat: "{subject}_{id}.txt",
		Metadata: map[string]string{
			"message_id":           "msg-456",
			"include_message_body": "true",
		},
		Limit:      50,
		MaxResults: 500,
	})
	if err != nil {
		t.Fatalf("ExecuteQuery returned error: %v", err)
	}
	if len(resp.Results) != 1 {
		t.Fatalf("result count = %d, want 1", len(resp.Results))
	}
	if got := resp.Results[0].ID; got != "msg:msg-456" {
		t.Fatalf("result id = %q, want msg:msg-456", got)
	}
	if got := resp.Results[0].Metadata["thread_id"]; got != "thread-456" {
		t.Fatalf("thread_id = %q, want thread-456", got)
	}
}

func TestExtractMimePartRecursive_PlainTextDirect(t *testing.T) {
	// Simple message with text/plain directly on payload
	payload := map[string]any{
		"mimeType": "text/plain",
		"body": map[string]any{
			"data": "SGVsbG8gV29ybGQh", // "Hello World!" in base64
		},
	}

	result := extractMimePartRecursive(payload, "text/plain")
	if result != "Hello World!" {
		t.Errorf("Expected 'Hello World!', got '%s'", result)
	}
}

func TestExtractMimePartRecursive_PlainTextInParts(t *testing.T) {
	// Multipart message with text/plain in parts
	payload := map[string]any{
		"mimeType": "multipart/alternative",
		"parts": []any{
			map[string]any{
				"mimeType": "text/plain",
				"body": map[string]any{
					"data": "UGxhaW4gdGV4dCBib2R5", // "Plain text body" in base64
				},
			},
			map[string]any{
				"mimeType": "text/html",
				"body": map[string]any{
					"data": "PGh0bWw+Ym9keTwvaHRtbD4=", // "<html>body</html>" in base64
				},
			},
		},
	}

	result := extractMimePartRecursive(payload, "text/plain")
	if result != "Plain text body" {
		t.Errorf("Expected 'Plain text body', got '%s'", result)
	}
}

func TestExtractMimePartRecursive_NestedMultipart(t *testing.T) {
	// Deeply nested multipart message
	payload := map[string]any{
		"mimeType": "multipart/mixed",
		"parts": []any{
			map[string]any{
				"mimeType": "multipart/alternative",
				"parts": []any{
					map[string]any{
						"mimeType": "text/plain",
						"body": map[string]any{
							"data": "TmVzdGVkIHBsYWluIHRleHQ=", // "Nested plain text" in base64
						},
					},
					map[string]any{
						"mimeType": "text/html",
						"body": map[string]any{
							"data": "PGh0bWw+TmVzdGVkPC9odG1sPg==",
						},
					},
				},
			},
			map[string]any{
				"mimeType": "application/pdf",
				"filename": "attachment.pdf",
				"body": map[string]any{
					"attachmentId": "some-id",
				},
			},
		},
	}

	result := extractMimePartRecursive(payload, "text/plain")
	if result != "Nested plain text" {
		t.Errorf("Expected 'Nested plain text', got '%s'", result)
	}
}

func TestExtractMimePartRecursive_HTMLOnly(t *testing.T) {
	// Message with only HTML, no plain text
	// Using URL-safe base64 without padding (Gmail format)
	payload := map[string]any{
		"mimeType": "text/html",
		"body": map[string]any{
			"data": "PHA-SFRNTCBUZXN0PC9wPg", // "<p>HTML Test</p>" in URL-safe base64 without padding
		},
	}

	// Should not find text/plain
	result := extractMimePartRecursive(payload, "text/plain")
	if result != "" {
		t.Errorf("Expected empty string for text/plain, got '%s'", result)
	}

	// Should find text/html
	result = extractMimePartRecursive(payload, "text/html")
	if result != "<p>HTML Test</p>" {
		t.Errorf("Expected HTML content, got '%s'", result)
	}
}

func TestDecodeBodyData_RawURLEncoding(t *testing.T) {
	// Base64url without padding
	body := map[string]any{
		"data": "SGVsbG8gV29ybGQh", // "Hello World!"
	}

	result := decodeBodyData(body)
	if result != "Hello World!" {
		t.Errorf("Expected 'Hello World!', got '%s'", result)
	}
}

func TestDecodeBodyData_WithPadding(t *testing.T) {
	// Base64url with padding
	body := map[string]any{
		"data": "SGVsbG8gV29ybGQh", // "Hello World!"
	}

	result := decodeBodyData(body)
	if result != "Hello World!" {
		t.Errorf("Expected 'Hello World!', got '%s'", result)
	}
}

func TestDecodeBodyData_Empty(t *testing.T) {
	body := map[string]any{
		"data": "",
	}

	result := decodeBodyData(body)
	if result != "" {
		t.Errorf("Expected empty string, got '%s'", result)
	}
}

func TestStripHTMLToText(t *testing.T) {
	tests := []struct {
		name     string
		html     string
		expected string
	}{
		{
			name:     "simple html",
			html:     "<p>Hello World</p>",
			expected: "Hello World",
		},
		{
			name:     "with entities",
			html:     "Hello &amp; Goodbye &lt;test&gt;",
			expected: "Hello & Goodbye <test>",
		},
		{
			name:     "with br tags",
			html:     "Line 1<br>Line 2<br/>Line 3",
			expected: "Line 1\nLine 2\nLine 3",
		},
		{
			name:     "with script and style",
			html:     "<style>body{color:red}</style><script>alert('x')</script><p>Content</p>",
			expected: "Content",
		},
		{
			name:     "nested divs",
			html:     "<div><div>Inner</div></div>",
			expected: "Inner",
		},
		{
			name:     "headings",
			html:     "<h1>Title</h1><p>Paragraph</p>",
			expected: "Title\nParagraph",
		},
		{
			name:     "excessive whitespace",
			html:     "<p>Line 1</p><p></p><p></p><p></p><p>Line 2</p>",
			expected: "Line 1\n\nLine 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := stripHTMLToText(tt.html)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestNormalizeWhitespace(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "single newlines preserved",
			input:    "Line 1\nLine 2\nLine 3",
			expected: "Line 1\nLine 2\nLine 3",
		},
		{
			name:     "double newlines preserved",
			input:    "Para 1\n\nPara 2",
			expected: "Para 1\n\nPara 2",
		},
		{
			name:     "triple+ newlines collapsed",
			input:    "Para 1\n\n\n\nPara 2",
			expected: "Para 1\n\nPara 2",
		},
		{
			name:     "trailing whitespace removed",
			input:    "Line 1   \nLine 2\t\nLine 3",
			expected: "Line 1\nLine 2\nLine 3",
		},
		{
			name:     "windows line endings",
			input:    "Line 1\r\nLine 2\r\nLine 3",
			expected: "Line 1\nLine 2\nLine 3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeWhitespace(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestExtractSenderName(t *testing.T) {
	tests := []struct {
		name     string
		from     string
		expected string
	}{
		{
			name:     "name with email",
			from:     "Raymond Xu <ray@example.com>",
			expected: "Raymond_Xu",
		},
		{
			name:     "quoted name",
			from:     "\"John Doe\" <john@example.com>",
			expected: "John_Doe",
		},
		{
			name:     "noreply address",
			from:     "noreply@calendly.com",
			expected: "Calendly",
		},
		{
			name:     "no-reply with domain",
			from:     "no-reply@notifications.github.com",
			expected: "Github",
		},
		{
			name:     "company name with email",
			from:     "KAYAK <kayak@msg.kayak.com>",
			expected: "KAYAK",
		},
		{
			name:     "email only with name-like local",
			from:     "john.smith@company.com",
			expected: "john.smith",
		},
		{
			name:     "automated sender",
			from:     "mailer-daemon@server.com",
			expected: "Server",
		},
		{
			name:     "generic sender name",
			from:     "Info <info@company.com>",
			expected: "info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractSenderName(tt.from)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestSanitizeFolderName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "spaces to underscores",
			input:    "Hello World",
			expected: "Hello_World",
		},
		{
			name:     "special chars",
			input:    "Test: Subject/Path",
			expected: "Test_Subject_Path",
		},
		{
			name:     "collapse underscores",
			input:    "Hello   World",
			expected: "Hello_World",
		},
		{
			name:     "trim underscores",
			input:    "_test_",
			expected: "test",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "_unknown_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sources.SanitizeFilename(tt.input)
			if result != tt.expected {
				t.Errorf("Expected '%s', got '%s'", tt.expected, result)
			}
		})
	}
}

func TestExtractHeaders(t *testing.T) {
	msg := map[string]any{
		"payload": map[string]any{
			"headers": []any{
				map[string]any{"name": "From", "value": "sender@example.com"},
				map[string]any{"name": "To", "value": "recipient@example.com"},
				map[string]any{"name": "Subject", "value": "Test Subject"},
				map[string]any{"name": "Date", "value": "Mon, 27 Jan 2026 10:00:00 -0500"},
				map[string]any{"name": "X-Custom", "value": "should be ignored"},
			},
		},
	}

	headers := extractHeaders(msg)

	if headers["From"] != "sender@example.com" {
		t.Errorf("Expected From to be 'sender@example.com', got '%s'", headers["From"])
	}
	if headers["To"] != "recipient@example.com" {
		t.Errorf("Expected To to be 'recipient@example.com', got '%s'", headers["To"])
	}
	if headers["Subject"] != "Test Subject" {
		t.Errorf("Expected Subject to be 'Test Subject', got '%s'", headers["Subject"])
	}
	if _, ok := headers["X-Custom"]; ok {
		t.Error("X-Custom header should not be included")
	}
}

func TestIsLikelyPersonName(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"john.smith", true},
		{"noreply", false},
		{"12345", false},
		{"info", true}, // Generic but has letters
		{"a", false},   // Too short
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := isLikelyPersonName(tt.input)
			if result != tt.expected {
				t.Errorf("isLikelyPersonName(%s) = %v, expected %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestParseGmailResultID(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantMessage  string
		wantAttachID string
		wantErr      bool
	}{
		{
			name:         "message result id",
			input:        "msg:abc123",
			wantMessage:  "abc123",
			wantAttachID: "",
		},
		{
			name:         "attachment result id",
			input:        "att:abc123:att456",
			wantMessage:  "abc123",
			wantAttachID: "att456",
		},
		{
			name:         "legacy message id",
			input:        "legacy-message-id",
			wantMessage:  "legacy-message-id",
			wantAttachID: "",
		},
		{
			name:    "invalid attachment id format",
			input:   "att:only-message-id",
			wantErr: true,
		},
		{
			name:    "empty id",
			input:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgID, attID, err := parseGmailResultID(tt.input)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if msgID != tt.wantMessage {
				t.Fatalf("message id mismatch: got %q want %q", msgID, tt.wantMessage)
			}
			if attID != tt.wantAttachID {
				t.Fatalf("attachment id mismatch: got %q want %q", attID, tt.wantAttachID)
			}
		})
	}
}

func TestExtractMessageAttachments_InlineFiltering(t *testing.T) {
	msg := map[string]any{
		"payload": map[string]any{
			"mimeType": "multipart/mixed",
			"parts": []any{
				map[string]any{
					"mimeType": "image/png",
					"filename": "logo.png",
					"headers": []any{
						map[string]any{"name": "Content-ID", "value": "<logo>"},
					},
					"body": map[string]any{
						"attachmentId": "inline-1",
						"size":         float64(12),
					},
				},
				map[string]any{
					"mimeType": "application/pdf",
					"filename": "invoice.pdf",
					"headers": []any{
						map[string]any{"name": "Content-Disposition", "value": "attachment; filename=invoice.pdf"},
					},
					"body": map[string]any{
						"attachmentId": "file-1",
						"size":         float64(64),
					},
				},
			},
		},
	}

	nonInline := extractMessageAttachments(msg, false)
	if len(nonInline) != 1 {
		t.Fatalf("expected 1 non-inline attachment, got %d", len(nonInline))
	}
	if nonInline[0].AttachmentID != "file-1" {
		t.Fatalf("expected file-1, got %s", nonInline[0].AttachmentID)
	}

	withInline := extractMessageAttachments(msg, true)
	if len(withInline) != 2 {
		t.Fatalf("expected 2 attachments when includeInline=true, got %d", len(withInline))
	}
}

func TestAttachmentOutputName_FallbackUsesMimeExtension(t *testing.T) {
	att := gmailAttachment{
		AttachmentID: "abcdef123456",
		MimeType:     "application/pdf",
	}

	name := attachmentOutputName(att)
	if !strings.HasPrefix(name, "attachment_abcdef12") {
		t.Fatalf("expected fallback attachment prefix, got %q", name)
	}
	if !strings.HasSuffix(name, ".pdf") {
		t.Fatalf("expected .pdf suffix, got %q", name)
	}
}

func TestShouldIncludeAttachments(t *testing.T) {
	tests := []struct {
		name string
		spec sources.QuerySpec
		want bool
	}{
		{
			name: "explicit metadata true",
			spec: sources.QuerySpec{
				Query:    "is:unread",
				Metadata: map[string]string{"include_attachments": "true"},
			},
			want: true,
		},
		{
			name: "explicit metadata false overrides query operators",
			spec: sources.QuerySpec{
				Query:    "has:attachment filename:pdf",
				Metadata: map[string]string{"include_attachments": "false"},
			},
			want: false,
		},
		{
			name: "inferred from query operator",
			spec: sources.QuerySpec{
				Query: "filename:pdf",
			},
			want: true,
		},
		{
			name: "default false for regular query",
			spec: sources.QuerySpec{
				Query: "is:unread",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldIncludeAttachments(tt.spec); got != tt.want {
				t.Fatalf("shouldIncludeAttachments() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeBodyBytes_PreservesBinaryData(t *testing.T) {
	original := []byte{0xff, 0x00, 0x01, 0x02, 0x7f}
	body := map[string]any{
		"data": base64.RawURLEncoding.EncodeToString(original),
	}

	decoded, err := decodeBodyBytes(body)
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	if !bytes.Equal(decoded, original) {
		t.Fatalf("decoded bytes mismatch: got %v want %v", decoded, original)
	}
}
