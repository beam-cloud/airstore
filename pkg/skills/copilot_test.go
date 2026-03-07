package skills

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCreateDraft(t *testing.T) {
	copilot := NewCopilot(nil, nil)

	draft := copilot.CreateDraft("ws-123")
	require.NotEmpty(t, draft.ID)
	require.Equal(t, "ws-123", draft.WorkspaceID)
	require.Equal(t, "active", draft.Status)
	require.Empty(t, draft.Messages)
	require.Empty(t, draft.SkillContent)
	require.NotZero(t, draft.CreatedAt)
	require.NotZero(t, draft.UpdatedAt)
}

func TestCreateDraftUniqueness(t *testing.T) {
	copilot := NewCopilot(nil, nil)

	d1 := copilot.CreateDraft("ws-123")
	d2 := copilot.CreateDraft("ws-123")
	require.NotEqual(t, d1.ID, d2.ID, "each draft should have a unique ID")
}

func TestFormatHistory(t *testing.T) {
	copilot := NewCopilot(nil, nil)

	tests := []struct {
		name     string
		messages []DraftMessage
		wantLen  int
	}{
		{
			name:     "empty messages",
			messages: []DraftMessage{},
			wantLen:  0,
		},
		{
			name: "single user message",
			messages: []DraftMessage{
				{Role: "user", Content: "hello", Timestamp: 1000000000000},
			},
			wantLen: 1,
		},
		{
			name: "user and assistant",
			messages: []DraftMessage{
				{Role: "user", Content: "create a skill", Timestamp: 1000000000000},
				{Role: "assistant", Content: "done", Timestamp: 1000000001000},
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := copilot.formatHistory(tt.messages)
			if tt.wantLen == 0 {
				require.Empty(t, result)
			} else {
				require.NotEmpty(t, result)
			}
		})
	}
}

func TestInstallDraftEmptyContent(t *testing.T) {
	copilot := NewCopilot(nil, nil)
	draft := copilot.CreateDraft("ws-123")

	_, err := copilot.InstallDraft(t.Context(), draft)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no skill content")
}

func TestInstallDraftInvalidSkill(t *testing.T) {
	copilot := NewCopilot(nil, nil)
	draft := copilot.CreateDraft("ws-123")
	draft.SkillContent = "not valid yaml frontmatter"

	_, err := copilot.InstallDraft(t.Context(), draft)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid skill")
}

func TestInstallDraftValidSkillNoStorage(t *testing.T) {
	copilot := NewCopilot(nil, nil)
	draft := copilot.CreateDraft("ws-123")
	draft.SkillContent = `---
name: test-skill
description: A test skill for validation.
version: 0.1.0
---

# Test Skill

Do the thing.
`

	_, err := copilot.InstallDraft(t.Context(), draft)
	require.Error(t, err)
	require.Contains(t, err.Error(), "storage not configured")
}

func TestDerefStr(t *testing.T) {
	require.Equal(t, "", derefStr(nil))

	s := "hello"
	require.Equal(t, "hello", derefStr(&s))

	empty := ""
	require.Equal(t, "", derefStr(&empty))
}

func TestLoadDraftNoS2(t *testing.T) {
	copilot := NewCopilot(nil, nil)
	_, err := copilot.LoadDraft(t.Context(), "nonexistent")
	require.Error(t, err)
	require.Contains(t, err.Error(), "S2 not configured")
}

func TestValidateContent(t *testing.T) {
	tests := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name:    "empty",
			content: "",
			wantErr: true,
		},
		{
			name:    "no frontmatter",
			content: "just plain text",
			wantErr: true,
		},
		{
			name:    "invalid frontmatter",
			content: "---\nbad: [yaml\n---\n",
			wantErr: true,
		},
		{
			name:    "missing name",
			content: "---\ndescription: test\n---\n# Hello\n",
			wantErr: true,
		},
		{
			name:    "missing description",
			content: "---\nname: test\n---\n# Hello\n",
			wantErr: true,
		},
		{
			name:    "valid skill",
			content: "---\nname: test-skill\ndescription: A valid test skill.\nversion: 0.1.0\n---\n\n# Instructions\n\nDo stuff.\n",
			wantErr: false,
		},
		{
			name:    "name too long",
			content: "---\nname: " + longName(65) + "\ndescription: test\n---\n",
			wantErr: true,
		},
		{
			name:    "consecutive hyphens",
			content: "---\nname: test--skill\ndescription: test\n---\n",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateContent(tt.content)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func longName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	return string(b)
}
