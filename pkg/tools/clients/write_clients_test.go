package clients

import (
	"bytes"
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/types"
)

func TestOAuthWriteClientsRequireConnection(t *testing.T) {
	tests := []struct {
		name   string
		execFn func(stdout *bytes.Buffer) error
	}{
		{
			name: "gmail",
			execFn: func(stdout *bytes.Buffer) error {
				return NewGmailClient().Execute(context.Background(), gmailCmdCreateDraft, map[string]any{
					"to":      "x@example.com",
					"subject": "hello",
					"body":    "world",
				}, nil, stdout, &bytes.Buffer{})
			},
		},
		{
			name: "gdrive",
			execFn: func(stdout *bytes.Buffer) error {
				return NewGDriveClient().Execute(context.Background(), gdriveCmdCreateFolder, map[string]any{
					"name": "docs",
				}, nil, stdout, &bytes.Buffer{})
			},
		},
		{
			name: "slack",
			execFn: func(stdout *bytes.Buffer) error {
				return NewSlackClient().Execute(context.Background(), slackCmdPostMessage, map[string]any{
					"channel": "C123",
					"text":    "hello",
				}, nil, stdout, &bytes.Buffer{})
			},
		},
		{
			name: "notion",
			execFn: func(stdout *bytes.Buffer) error {
				return NewNotionClient().Execute(context.Background(), notionCmdAppendParagraph, map[string]any{
					"block_id": "abc",
					"text":     "hello",
				}, nil, stdout, &bytes.Buffer{})
			},
		},
		{
			name: "linear",
			execFn: func(stdout *bytes.Buffer) error {
				return NewLinearClient().Execute(context.Background(), linearCmdCreateIssue, map[string]any{
					"title": "bug",
				}, nil, stdout, &bytes.Buffer{})
			},
		},
		{
			name: "outlook",
			execFn: func(stdout *bytes.Buffer) error {
				return NewOutlookToolClient().Execute(context.Background(), outlookCmdCreateDraft, map[string]any{
					"to":      "x@example.com",
					"subject": "hello",
					"body":    "world",
				}, nil, stdout, &bytes.Buffer{})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			if err := tc.execFn(&stdout); err != nil {
				t.Fatalf("execute: %v", err)
			}
			out := stdout.String()
			if out == "" || !bytes.Contains(stdout.Bytes(), []byte(`"error":true`)) {
				t.Fatalf("expected JSON error output, got %q", out)
			}
		})
	}
}

func TestGitHubCreateIssueRequiresArguments(t *testing.T) {
	client := NewGitHubClient()
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), githubCmdCreateIssue, map[string]any{
		"owner": "octocat",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected structured JSON error, got hard error: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
		t.Fatalf("expected JSON error output, got %q", stdout.String())
	}
}

func TestGitHubPRReviewCommandsRequireArguments(t *testing.T) {
	client := NewGitHubClient()
	tests := []struct {
		name    string
		command string
		args    map[string]any
	}{
		{
			name:    "comment-pr missing body",
			command: githubCmdCommentPR,
			args: map[string]any{
				"owner":  "octocat",
				"repo":   "hello-world",
				"number": 1,
			},
		},
		{
			name:    "review-pr missing body",
			command: githubCmdReviewPR,
			args: map[string]any{
				"owner":  "octocat",
				"repo":   "hello-world",
				"number": 1,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := client.Execute(context.Background(), tc.command, tc.args, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
			if err != nil {
				t.Fatalf("expected structured JSON error, got hard error: %v", err)
			}
			if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
				t.Fatalf("expected JSON error output, got %q", stdout.String())
			}
		})
	}
}

func TestGitHubReviewPRInvalidCommentsJSON(t *testing.T) {
	client := NewGitHubClient()
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), githubCmdReviewPR, map[string]any{
		"owner":    "octocat",
		"repo":     "hello-world",
		"number":   1,
		"body":     "review",
		"comments": "not-valid-json",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected structured JSON error, got hard error: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
		t.Fatalf("expected JSON error output, got %q", stdout.String())
	}
	if !bytes.Contains(stdout.Bytes(), []byte("invalid comments JSON")) {
		t.Fatalf("expected 'invalid comments JSON' in output, got %q", stdout.String())
	}
}

func TestGitHubReviewPRAcceptsCommentsArg(t *testing.T) {
	client := NewGitHubClient()
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), githubCmdReviewPR, map[string]any{
		"owner":    "octocat",
		"repo":     "hello-world",
		"number":   1,
		"body":     "review",
		"comments": `[{"path":"main.go","line":10,"body":"Fix this"}]`,
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("unexpected hard error: %v", err)
	}
	if bytes.Contains(stdout.Bytes(), []byte("invalid comments JSON")) {
		t.Fatalf("comments JSON should have parsed successfully, got %q", stdout.String())
	}
}

func TestGmailSendEmailRequiresArguments(t *testing.T) {
	client := NewGmailClient()
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gmailCmdSendEmail, map[string]any{
		"to": "x@example.com",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected structured JSON error, got hard error: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
		t.Fatalf("expected JSON error output, got %q", stdout.String())
	}
}

func TestBuildRawEmailSanitizesHeaderInjection(t *testing.T) {
	raw := buildRawEmail("victim@example.com\r\nBcc: attacker@example.com", "hello\r\nX-Test: injected", "body", "", "")
	if bytes.Contains([]byte(raw), []byte("\r\nBcc:")) {
		t.Fatalf("expected CRLF header injection to be sanitized, got %q", raw)
	}
	if bytes.Contains([]byte(raw), []byte("\r\nX-Test:")) {
		t.Fatalf("expected CRLF header injection to be sanitized, got %q", raw)
	}
}

func TestGDriveWriteFileRequiresArguments(t *testing.T) {
	client := NewGDriveClient()
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), gdriveCmdWriteFile, map[string]any{
		"name": "notes.txt",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected structured JSON error, got hard error: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
		t.Fatalf("expected JSON error output, got %q", stdout.String())
	}
}

func TestResolveContentAllowsZeroByteBase64(t *testing.T) {
	data, err := resolveContent(map[string]any{
		"content_base64": "",
	})
	if err != nil {
		t.Fatalf("resolveContent returned error: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("expected zero-byte payload, got %d bytes", len(data))
	}
}

func TestNotionCommandRequiresArguments(t *testing.T) {
	client := NewNotionClient()
	tests := []struct {
		name    string
		command string
		args    map[string]any
	}{
		{
			name:    "search missing query",
			command: notionCmdSearch,
			args:    map[string]any{},
		},
		{
			name:    "create-page missing title",
			command: notionCmdCreatePage,
			args: map[string]any{
				"parent_id": "abc123",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var stdout bytes.Buffer
			err := client.Execute(context.Background(), tc.command, tc.args, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
			if err != nil {
				t.Fatalf("expected structured JSON error, got hard error: %v", err)
			}
			if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
				t.Fatalf("expected JSON error output, got %q", stdout.String())
			}
		})
	}
}

func TestLinearCreateIssueRequiresTitle(t *testing.T) {
	client := NewLinearClient()
	var stdout bytes.Buffer
	err := client.Execute(context.Background(), linearCmdCreateIssue, map[string]any{
		"description": "missing title",
	}, &types.IntegrationCredentials{AccessToken: "token"}, &stdout, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("expected structured JSON error, got hard error: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte(`"error"`)) {
		t.Fatalf("expected JSON error output, got %q", stdout.String())
	}
}
