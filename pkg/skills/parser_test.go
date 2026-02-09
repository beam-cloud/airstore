package skills

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Run("valid skill with all fields", func(t *testing.T) {
		data := []byte(`---
name: Email Triage
description: Categorizes emails by urgency and creates daily briefs
needs:
  - gmail
triggers:
  - on: source.change
    path: /sources/gmail
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

		assert.Equal(t, "Email Triage", manifest.Name)
		assert.Equal(t, "Categorizes emails by urgency and creates daily briefs", manifest.Description)
		assert.Equal(t, []string{"gmail"}, manifest.Needs)
		assert.Len(t, manifest.Triggers, 1)
		assert.Equal(t, "source.change", manifest.Triggers[0].On)
		assert.Equal(t, "/sources/gmail", manifest.Triggers[0].Path)
		assert.Equal(t, []string{"/memory/email-triage/"}, manifest.Writes)
	})

	t.Run("minimal skill with just name", func(t *testing.T) {
		data := []byte(`---
name: Simple Skill
---

Do stuff.
`)
		manifest, err := Parse(data)
		require.NoError(t, err)
		assert.Equal(t, "Simple Skill", manifest.Name)
		assert.Empty(t, manifest.Needs)
		assert.Empty(t, manifest.Triggers)
		assert.Empty(t, manifest.Writes)
	})

	t.Run("multiple triggers and needs", func(t *testing.T) {
		data := []byte(`---
name: Multi Source
description: Watches email and slack
needs:
  - gmail
  - slack
triggers:
  - on: source.change
    path: /sources/gmail
  - on: source.change
    path: /sources/slack
writes:
  - /memory/multi-source/
  - /memory/urgent/
---
`)
		manifest, err := Parse(data)
		require.NoError(t, err)
		assert.Equal(t, []string{"gmail", "slack"}, manifest.Needs)
		assert.Len(t, manifest.Triggers, 2)
		assert.Len(t, manifest.Writes, 2)
	})

	t.Run("missing name", func(t *testing.T) {
		data := []byte(`---
description: No name
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "name is required")
	})

	t.Run("invalid trigger event type", func(t *testing.T) {
		data := []byte(`---
name: Bad Trigger
triggers:
  - on: invalid.event
    path: /sources/gmail
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown event type")
	})

	t.Run("trigger missing on", func(t *testing.T) {
		data := []byte(`---
name: Bad Trigger
triggers:
  - path: /sources/gmail
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "'on' is required")
	})

	t.Run("trigger missing path", func(t *testing.T) {
		data := []byte(`---
name: Bad Trigger
triggers:
  - on: source.change
---
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "'path' is required")
	})

	t.Run("no frontmatter", func(t *testing.T) {
		data := []byte(`# Just instructions, no frontmatter`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no frontmatter found")
	})

	t.Run("unterminated frontmatter", func(t *testing.T) {
		data := []byte(`---
name: Broken
`)
		_, err := Parse(data)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unterminated frontmatter")
	})

	t.Run("all filesystem event types", func(t *testing.T) {
		for _, event := range []string{"source.change", "fs.create", "fs.write", "fs.delete"} {
			data := []byte("---\nname: Test\ntriggers:\n  - on: " + event + "\n    path: /test\n---\n")
			manifest, err := Parse(data)
			require.NoError(t, err, "event type %q should be valid", event)
			assert.Equal(t, event, manifest.Triggers[0].On)
		}
	})
}

func TestExtractInstructions(t *testing.T) {
	t.Run("extracts content after frontmatter", func(t *testing.T) {
		data := []byte(`---
name: Test
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
name: Test
---
`)
		instructions := ExtractInstructions(data)
		assert.Equal(t, "", instructions)
	})
}
