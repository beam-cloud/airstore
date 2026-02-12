package common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateTaskName(t *testing.T) {
	const externalId = "550e8400-e29b-41d4-a716-446655440000"
	const suffix = "550e8400" // first 8 of UUID without hyphens

	tests := []struct {
		name       string
		prompt     string
		image      string
		externalId string
		want       string
	}{
		{
			name:       "simple prompt",
			prompt:     "Fix the auth login bug",
			externalId: externalId,
			want:       "fix-auth-login-bug-" + suffix,
		},
		{
			name:       "prompt with stop words",
			prompt:     "Please can you fix the authentication bug in my app",
			externalId: externalId,
			want:       "fix-authentication-bug-app-" + suffix,
		},
		{
			name:       "image fallback",
			prompt:     "",
			image:      "ghcr.io/org/my-sandbox:latest",
			externalId: externalId,
			want:       "sandbox-" + suffix,
		},
		{
			name:       "empty inputs",
			prompt:     "",
			image:      "",
			externalId: externalId,
			want:       "task-" + suffix,
		},
		{
			name:       "long prompt truncated",
			prompt:     "implement a comprehensive error handling system with retry logic and exponential backoff for all API endpoints in the service layer",
			externalId: externalId,
			want:       "implement-comprehensive-error-handling-system-" + suffix,
		},
		{
			name:       "special characters",
			prompt:     "fix bug #123: handle @mention & <html> tags",
			externalId: externalId,
			want:       "fix-bug-123-handle-mention-" + suffix,
		},
		{
			name:       "stop words only",
			prompt:     "please can you do this for me",
			externalId: externalId,
			want:       "task-" + suffix,
		},
		{
			name:       "prompt preferred over image",
			prompt:     "deploy server",
			image:      "ubuntu:22.04",
			externalId: externalId,
			want:       "deploy-server-" + suffix,
		},
		{
			name:       "image without tag",
			prompt:     "",
			image:      "registry.com/team/app",
			externalId: externalId,
			want:       "app-" + suffix,
		},
		{
			name:       "image plain name",
			prompt:     "",
			image:      "ubuntu:22.04",
			externalId: externalId,
			want:       "ubuntu-" + suffix,
		},
		{
			name:       "short external id",
			prompt:     "test",
			externalId: "abc",
			want:       "test-abc",
		},
		{
			name:       "empty external id",
			prompt:     "test",
			externalId: "",
			want:       "test-00000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GenerateTaskName(tt.prompt, tt.image, tt.externalId)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGenerateTaskName_Format(t *testing.T) {
	// Verify the output format: only [a-z0-9-], no consecutive hyphens,
	// no leading/trailing hyphens
	inputs := []struct {
		prompt string
		image  string
	}{
		{"Fix the   auth--login   bug!!!", ""},
		{"", "ghcr.io/org/my_sandbox:latest"},
		{"---leading hyphens---", ""},
		{"UPPERCASE PROMPT", ""},
		{"日本語テスト", ""},
		{"  lots   of   spaces  ", ""},
	}

	for _, input := range inputs {
		name := GenerateTaskName(input.prompt, input.image, "550e8400-e29b-41d4-a716-446655440000")

		assert.False(t, strings.HasPrefix(name, "-"), "should not start with hyphen: %s", name)
		assert.False(t, strings.HasSuffix(name, "-"), "should not end with hyphen: %s", name)
		assert.NotContains(t, name, "--", "should not have consecutive hyphens: %s", name)

		for _, c := range name {
			assert.True(t, (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-',
				"invalid character '%c' in: %s", c, name)
		}

		// Should end with 8-char suffix
		assert.True(t, len(name) >= 8, "name too short: %s", name)
	}
}

func TestExtractImageName(t *testing.T) {
	tests := []struct {
		image string
		want  string
	}{
		{"ghcr.io/org/my-sandbox:latest", "my-sandbox"},
		{"ubuntu:22.04", "ubuntu"},
		{"registry.com/team/app", "app"},
		{"my-image", "my-image"},
		{"registry.com:5000/repo/img:v1", "img"},
	}

	for _, tt := range tests {
		t.Run(tt.image, func(t *testing.T) {
			got := extractImageName(tt.image)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestSlugMaxLength(t *testing.T) {
	// Very long prompt should be capped
	longPrompt := strings.Repeat("superlongword ", 20)
	name := GenerateTaskName(longPrompt, "", "550e8400-e29b-41d4-a716-446655440000")

	// Slug portion (before last hyphen-suffix) should be <= 50 chars
	// Total = slug + "-" + 8-char suffix
	assert.LessOrEqual(t, len(name), 50+1+8, "total name too long: %s (len=%d)", name, len(name))
}
