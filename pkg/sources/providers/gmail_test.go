package providers

import (
	"testing"
)

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
			result := sanitizeFolderName(tt.input)
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
