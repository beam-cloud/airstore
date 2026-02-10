package skills

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("valid skill with all fields", func(t *testing.T) {
		data := []byte(`---
name: email-triage
description: Categorizes emails by urgency and creates daily briefs. Use when processing Gmail inbox.
license: MIT
metadata:
  author: beam-cloud
  version: "1.0"
  airstore:
    needs:
      - gmail
    writes:
      - /memory/email-triage/
---

# Instructions

When triggered by new emails:
1. Read all files in /sources/gmail/unread/
2. Categorize each: urgent, needs-reply, FYI
`)

		manifest, err := Parse(data)
		require.NoError(t, err)

		assert.Equal(t, "email-triage", manifest.Name)
		assert.Equal(t, "Categorizes emails by urgency and creates daily briefs. Use when processing Gmail inbox.", manifest.Description)
		assert.Equal(t, "MIT", manifest.License)
		assert.Equal(t, "beam-cloud", manifest.Metadata["author"])

		am := manifest.AirstoreMetadata()
		assert.Equal(t, []string{"gmail"}, am.Needs)
		assert.Equal(t, []string{"/memory/email-triage/"}, am.Writes)
	})

	t.Run("minimal skill with name and description", func(t *testing.T) {
		data := []byte(`---
name: simple-skill
description: A simple skill for testing.
---

Do stuff.
`)
		manifest, err := Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "simple-skill", manifest.Name)
		assert.Equal(t, "A simple skill for testing.", manifest.Description)

		am := manifest.AirstoreMetadata()
		assert.Empty(t, am.Needs)
		assert.Empty(t, am.Writes)
	})

	t.Run("multiple needs and writes", func(t *testing.T) {
		data := []byte(`---
name: multi-source
description: Watches email and slack for action items.
metadata:
  airstore:
    needs:
      - gmail
      - slack
    writes:
      - /memory/multi-source/
      - /memory/urgent/
---
`)
		manifest, err := Parse(data)
		require.NoError(t, err)

		am := manifest.AirstoreMetadata()
		assert.Equal(t, []string{"gmail", "slack"}, am.Needs)
		assert.Equal(t, []string{"/memory/multi-source/", "/memory/urgent/"}, am.Writes)
	})

	t.Run("standard fields without airstore metadata", func(t *testing.T) {
		data := []byte(`---
name: code-review
description: Reviews code changes for common issues.
license: Apache-2.0
compatibility: Requires git
metadata:
  author: community
  version: "2.0"
allowed-tools: Bash(git:*) Read
---

# Code Review

Review the code.
`)
		manifest, err := Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "code-review", manifest.Name)
		assert.Equal(t, "Apache-2.0", manifest.License)
		assert.Equal(t, "Requires git", manifest.Compatibility)
		assert.Equal(t, "Bash(git:*) Read", manifest.AllowedTools)
		assert.Equal(t, "community", manifest.Metadata["author"])

		am := manifest.AirstoreMetadata()
		assert.Empty(t, am.Needs)
		assert.Empty(t, am.Writes)
	})

	t.Run("missing name", func(t *testing.T) {
		data := []byte(`---
description: No name provided.
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "name is required")
	})

	t.Run("missing description", func(t *testing.T) {
		data := []byte(`---
name: no-desc
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "description is required")
	})

	t.Run("invalid name - uppercase", func(t *testing.T) {
		data := []byte(`---
name: Email-Triage
description: Bad name with uppercase.
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lowercase")
	})

	t.Run("invalid name - leading hyphen", func(t *testing.T) {
		data := []byte(`---
name: -email
description: Bad name with leading hyphen.
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "lowercase")
	})

	t.Run("invalid name - consecutive hyphens", func(t *testing.T) {
		data := []byte(`---
name: email--triage
description: Bad name with consecutive hyphens.
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "consecutive hyphens")
	})

	t.Run("no frontmatter", func(t *testing.T) {
		data := []byte(`# Just instructions, no frontmatter`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no frontmatter found")
	})

	t.Run("unterminated frontmatter", func(t *testing.T) {
		data := []byte(`---
name: broken
description: Unterminated.
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unterminated frontmatter")
	})
}

func TestAirstoreMetadata(t *testing.T) {
	t.Run("nil metadata", func(t *testing.T) {
		m := &SkillManifest{}
		am := m.AirstoreMetadata()
		assert.Empty(t, am.Needs)
		assert.Empty(t, am.Writes)
	})

	t.Run("metadata without airstore key", func(t *testing.T) {
		m := &SkillManifest{
			Metadata: map[string]any{
				"author": "test",
			},
		}
		am := m.AirstoreMetadata()
		assert.Empty(t, am.Needs)
		assert.Empty(t, am.Writes)
	})
}

func TestExtractInstructions(t *testing.T) {
	t.Run("extracts content after frontmatter", func(t *testing.T) {
		data := []byte(`---
name: test
description: A test skill.
---

# Instructions

Do things.
Step 1.
Step 2.
`)
		instructions := ExtractInstructions(data)
		assert.Contains(t, instructions, "# Instructions")
		assert.Contains(t, instructions, "Step 1.")
		assert.Contains(t, instructions, "Step 2.")
	})

	t.Run("empty after frontmatter", func(t *testing.T) {
		data := []byte(`---
name: test
description: A test skill.
---
`)
		instructions := ExtractInstructions(data)
		assert.Equal(t, "", instructions)
	})
}
