package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactSensitiveStringMasksSecrets(t *testing.T) {
	raw := `ANTHROPIC_API_KEY=sk-live-secret-value Authorization: Bearer super-secret-token {"api_key":"abc123"}`
	redacted := RedactSensitiveString(raw)

	require.NotContains(t, redacted, "sk-live-secret-value")
	require.NotContains(t, redacted, "super-secret-token")
	require.NotContains(t, redacted, "abc123")
	require.Contains(t, redacted, "[REDACTED]")
}

func TestRedactSensitiveMapMasksNestedValuesWithoutMutatingInput(t *testing.T) {
	payload := map[string]any{
		"api_key": "top-secret",
		"safe":    "value",
		"nested": map[string]any{
			"Authorization": "Bearer nested-secret",
			"note":          "normal",
		},
		"items": []any{
			"KERNEL_API_KEY=another-secret",
			map[string]any{"token": "nested-token"},
		},
	}

	redacted := RedactSensitiveMap(payload)

	require.Equal(t, "[REDACTED]", redacted["api_key"])
	require.Equal(t, "value", redacted["safe"])
	require.Equal(t, "top-secret", payload["api_key"]) // input is not mutated

	nested, ok := redacted["nested"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", nested["Authorization"])
	require.Equal(t, "normal", nested["note"])

	items, ok := redacted["items"].([]any)
	require.True(t, ok)
	require.NotContains(t, items[0], "another-secret")
	itemMap, ok := items[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "[REDACTED]", itemMap["token"])
}

func TestRedactTaskLogEntryMasksDataAndMetadata(t *testing.T) {
	entry := TaskLogEntry{
		TaskID: "task-1",
		Stream: "stdout",
		Data:   "KERNEL_API_KEY=12345",
		Metadata: map[string]any{
			"authorization": "Bearer abcde",
		},
	}

	redacted := RedactTaskLogEntry(entry)
	require.NotContains(t, redacted.Data, "12345")
	require.Equal(t, "[REDACTED]", redacted.Metadata["authorization"])
}
