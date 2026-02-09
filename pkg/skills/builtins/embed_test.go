package builtins

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestList(t *testing.T) {
	skills, err := List()
	require.NoError(t, err)
	assert.Len(t, skills, 4)

	names := make(map[string]bool)
	for _, s := range skills {
		names[s.Name] = true
		assert.NotEmpty(t, s.Manifest.Name)
		assert.NotEmpty(t, s.Manifest.Description)
		assert.NotEmpty(t, s.Content)
	}

	assert.True(t, names["email-triage"])
	assert.True(t, names["slack-actions"])
	assert.True(t, names["pr-reviewer"])
	assert.True(t, names["issue-triage"])
}

func TestGet(t *testing.T) {
	t.Run("existing skill", func(t *testing.T) {
		bs, err := Get("email-triage")
		require.NoError(t, err)
		assert.Equal(t, "Email Triage", bs.Manifest.Name)
		assert.Equal(t, []string{"gmail"}, bs.Manifest.Needs)
		assert.Len(t, bs.Manifest.Triggers, 1)
		assert.Equal(t, "source.change", bs.Manifest.Triggers[0].On)
	})

	t.Run("non-existing skill", func(t *testing.T) {
		_, err := Get("does-not-exist")
		assert.Error(t, err)
	})
}

func TestExtractFiles(t *testing.T) {
	files, err := ExtractFiles("email-triage")
	require.NoError(t, err)
	assert.Contains(t, files, "SKILL.md")
	assert.NotEmpty(t, files["SKILL.md"])
}
