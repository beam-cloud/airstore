package providers

import (
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/sources/clients"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostHogProvider_Name(t *testing.T) {
	p := NewPostHogProvider()
	assert.Equal(t, "posthog", p.Name())
}

func TestPostHogProvider_IsNativeBrowsable(t *testing.T) {
	p := NewPostHogProvider()
	assert.True(t, p.IsNativeBrowsable())
}

func TestPostHogProvider_InterfaceCompliance(t *testing.T) {
	p := NewPostHogProvider()

	// Verify Provider interface
	var _ sources.Provider = p

	// Verify NativeBrowsable interface
	var _ sources.NativeBrowsable = p

	// Verify CredentialValidator interface
	var _ sources.CredentialValidator = p

	// Verify QueryExecutor interface
	var _ sources.QueryExecutor = p
}

func TestPostHogProvider_CheckAuth(t *testing.T) {
	p := NewPostHogProvider()
	ctx := context.Background()

	t.Run("no credentials", func(t *testing.T) {
		pctx := &sources.ProviderContext{}
		_, err := p.ReadDir(ctx, pctx, "")
		assert.ErrorIs(t, err, sources.ErrNotConnected)
	})

	t.Run("empty api key", func(t *testing.T) {
		pctx := &sources.ProviderContext{
			Credentials: &types.IntegrationCredentials{APIKey: ""},
		}
		_, err := p.ReadDir(ctx, pctx, "")
		assert.ErrorIs(t, err, sources.ErrNotConnected)
	})
}

func TestPostHogProvider_Stat(t *testing.T) {
	p := NewPostHogProvider()
	ctx := context.Background()

	// Root path should be a directory (no API call needed for root)
	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{APIKey: "phx_test"},
	}

	t.Run("root is dir", func(t *testing.T) {
		info, err := p.Stat(ctx, pctx, "")
		require.NoError(t, err)
		assert.True(t, info.IsDir)
	})

	t.Run("unknown deep path", func(t *testing.T) {
		_, err := p.Stat(ctx, pctx, "a/b/c/d")
		assert.ErrorIs(t, err, sources.ErrNotFound)
	})
}

func TestPostHogProvider_Search(t *testing.T) {
	p := NewPostHogProvider()
	ctx := context.Background()
	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{APIKey: "phx_test"},
	}

	_, err := p.Search(ctx, pctx, "test", 10)
	assert.ErrorIs(t, err, sources.ErrSearchNotSupported)
}

func TestPostHogProvider_Readlink(t *testing.T) {
	p := NewPostHogProvider()
	ctx := context.Background()
	pctx := &sources.ProviderContext{}

	_, err := p.Readlink(ctx, pctx, "anything")
	assert.ErrorIs(t, err, sources.ErrNotFound)
}

func TestPostHogProvider_ValidateCredentials_EmptyKey(t *testing.T) {
	p := NewPostHogProvider()
	ctx := context.Background()

	err := p.ValidateCredentials(ctx, &types.IntegrationCredentials{APIKey: ""})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "api_key is required")
}

func TestPostHogProjectDirName(t *testing.T) {
	tests := []struct {
		project  clients.PostHogProject
		expected string
	}{
		{clients.PostHogProject{ID: 1, Name: "My Project"}, "1_My_Project"},
		{clients.PostHogProject{ID: 42, Name: "test"}, "42_test"},
		{clients.PostHogProject{ID: 100, Name: ""}, "100__unknown_"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			got := posthogProjectDirName(tt.project)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPostHogParseProjectID(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		hasError bool
	}{
		{"1_My_Project", 1, false},
		{"42_test", 42, false},
		{"100", 100, false},
		{"abc", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := posthogParseProjectID(tt.input)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestPostHogProvider_FormatFilename(t *testing.T) {
	p := NewPostHogProvider()

	tests := []struct {
		name     string
		format   string
		metadata map[string]string
		expected string
	}{
		{
			name:   "feature flag format",
			format: "{key}_{id}.json",
			metadata: map[string]string{
				"id":  "42",
				"key": "beta-feature",
			},
			expected: "beta-feature_42.json",
		},
		{
			name:   "event format",
			format: "{date}_{event}_{id}.json",
			metadata: map[string]string{
				"id":    "abc123",
				"event": "$pageview",
				"date":  "2026-01-15",
			},
			expected: "2026-01-15_pageview_abc123.json",
		},
		{
			name:   "empty format defaults to id.json",
			format: "",
			metadata: map[string]string{
				"id": "99",
			},
			expected: "99.json",
		},
		{
			name:   "truncation of long name",
			format: "{name}_{id}.json",
			metadata: map[string]string{
				"id":   "1",
				"name": "This is a very long name that should be truncated to forty characters exactly here",
			},
			expected: "This_is_a_very_long_name_that_should_be__1.json",
		},
		{
			name:   "insight format",
			format: "{name}_{id}.json",
			metadata: map[string]string{
				"id":   "7",
				"name": "Weekly Retention",
			},
			expected: "Weekly_Retention_7.json",
		},
		{
			name:   "cohort format",
			format: "{name}_{id}.json",
			metadata: map[string]string{
				"id":   "5",
				"name": "Beta Users",
			},
			expected: "Beta_Users_5.json",
		},
		{
			name:   "no extension added when present",
			format: "{id}.json",
			metadata: map[string]string{
				"id": "123",
			},
			expected: "123.json",
		},
		{
			name:   "extension added when missing",
			format: "{id}",
			metadata: map[string]string{
				"id": "456",
			},
			expected: "456.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.FormatFilename(tt.format, tt.metadata)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestPostHogProvider_CompositeResultID(t *testing.T) {
	// Test that ReadResult correctly rejects invalid composite IDs
	p := NewPostHogProvider()
	ctx := context.Background()
	pctx := &sources.ProviderContext{
		Credentials: &types.IntegrationCredentials{APIKey: "phx_test"},
	}

	t.Run("invalid format - no colons", func(t *testing.T) {
		_, err := p.ReadResult(ctx, pctx, "invalid")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid result ID format")
	})

	t.Run("invalid format - one colon", func(t *testing.T) {
		_, err := p.ReadResult(ctx, pctx, "123:flags")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid result ID format")
	})

	t.Run("invalid project ID", func(t *testing.T) {
		_, err := p.ReadResult(ctx, pctx, "abc:feature-flags:42")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid project ID")
	})

	t.Run("unknown resource type", func(t *testing.T) {
		_, err := p.ReadResult(ctx, pctx, "123:unknown:42")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown resource type")
	})
}

func TestPostHogProvider_ExecuteQueryAuth(t *testing.T) {
	p := NewPostHogProvider()
	ctx := context.Background()

	t.Run("no credentials", func(t *testing.T) {
		pctx := &sources.ProviderContext{}
		_, err := p.ExecuteQuery(ctx, pctx, sources.QuerySpec{Query: "test"})
		assert.ErrorIs(t, err, sources.ErrNotConnected)
	})
}

func TestSliceBytes(t *testing.T) {
	data := []byte("hello world")

	t.Run("full read", func(t *testing.T) {
		result := sliceBytes(data, 0, 0)
		assert.Equal(t, data, result)
	})

	t.Run("offset", func(t *testing.T) {
		result := sliceBytes(data, 6, 0)
		assert.Equal(t, []byte("world"), result)
	})

	t.Run("offset and length", func(t *testing.T) {
		result := sliceBytes(data, 0, 5)
		assert.Equal(t, []byte("hello"), result)
	})

	t.Run("offset beyond length", func(t *testing.T) {
		result := sliceBytes(data, 100, 0)
		assert.Nil(t, result)
	})
}
