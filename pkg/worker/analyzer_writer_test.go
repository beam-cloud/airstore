package worker

import "testing"

func TestFirstMatchingStringPrefersTopLevelNameOverNestedTitle(t *testing.T) {
	value := map[string]any{
		"name": "Top-level name",
		"child": map[string]any{
			"title": "Nested title",
		},
	}

	if got := firstMatchingString(value, "title", "name"); got != "Top-level name" {
		t.Fatalf("firstMatchingString = %q, want %q", got, "Top-level name")
	}
}

func TestFirstMatchingStringPrefersTopLevelURIOverNestedURL(t *testing.T) {
	value := map[string]any{
		"uri": "https://top-level.example.com",
		"child": map[string]any{
			"url": "https://nested.example.com",
		},
	}

	if got := firstMatchingString(value, "url", "uri"); got != "https://top-level.example.com" {
		t.Fatalf("firstMatchingString = %q, want %q", got, "https://top-level.example.com")
	}
}

func TestFirstMatchingStringFallsBackToNestedMatch(t *testing.T) {
	value := map[string]any{
		"child": map[string]any{
			"title": "Nested title",
		},
	}

	if got := firstMatchingString(value, "title", "name"); got != "Nested title" {
		t.Fatalf("firstMatchingString = %q, want %q", got, "Nested title")
	}
}
