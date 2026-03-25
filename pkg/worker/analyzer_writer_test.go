package worker

import (
	"encoding/json"
	"testing"
)

func TestPromoteToolResultFieldsRunsAlongsideDataFields(t *testing.T) {
	data := map[string]any{
		"address": "512 Lyell Ave",
		"market":  "Rochester, NY",
	}
	parsedResult := map[string]any{
		"address":    "512 Lyell Ave",
		"market":     "Rochester, NY",
		"sq_ft":      float64(2000),
		"rent":       float64(1200),
		"lease_type": "NNN",
		"broker":     "Jane Smith",
	}

	promoteToolResultFields(data, parsedResult)

	for _, key := range []string{"sq_ft", "rent", "lease_type", "broker"} {
		if _, ok := data[key]; !ok {
			t.Errorf("expected key %q to be promoted into Data, but it's missing", key)
		}
	}
	if data["address"] != "512 Lyell Ave" {
		t.Errorf("existing key should not be overwritten: address = %v", data["address"])
	}
}

func TestPromoteToolInputContentPromotesJSONObjectFields(t *testing.T) {
	data := map[string]any{}
	parsedInput := map[string]any{
		"file_path": "/workspace/agents/lead-outreach/properties.json",
		"content":   `{"address":"512 Lyell Ave","market":"Rochester, NY","sq_ft":2000,"rent":1200,"lease_type":"NNN","broker":"Jane Smith","broker_phone":"555-1234","broker_email":"jane@example.com"}`,
	}

	promoteToolInputContent(data, parsedInput)

	expected := map[string]any{
		"address":      "512 Lyell Ave",
		"market":       "Rochester, NY",
		"sq_ft":        float64(2000),
		"rent":         float64(1200),
		"lease_type":   "NNN",
		"broker":       "Jane Smith",
		"broker_phone": "555-1234",
		"broker_email": "jane@example.com",
	}
	for key, want := range expected {
		got, ok := data[key]
		if !ok {
			t.Errorf("expected key %q to be promoted, but it's missing", key)
			continue
		}
		if got != want {
			t.Errorf("data[%q] = %v, want %v", key, got, want)
		}
	}
}

func TestPromoteToolInputContentPromotesFirstArrayElement(t *testing.T) {
	data := map[string]any{}
	listing := []map[string]any{
		{"address": "512 Lyell Ave", "sq_ft": float64(2000), "rent": float64(1200)},
		{"address": "100 Main St", "sq_ft": float64(3000), "rent": float64(2000)},
	}
	contentBytes, _ := json.Marshal(listing)
	parsedInput := map[string]any{
		"file_path": "/workspace/properties.json",
		"content":   string(contentBytes),
	}

	promoteToolInputContent(data, parsedInput)

	if data["address"] != "512 Lyell Ave" {
		t.Errorf("expected address from first array element, got %v", data["address"])
	}
	if data["sq_ft"] != float64(2000) {
		t.Errorf("expected sq_ft from first array element, got %v", data["sq_ft"])
	}
}

func TestPromoteToolInputContentSkipsNonJSON(t *testing.T) {
	data := map[string]any{}
	parsedInput := map[string]any{
		"file_path": "/workspace/readme.md",
		"content":   "# Hello World\nThis is not JSON.",
	}

	promoteToolInputContent(data, parsedInput)

	if len(data) != 0 {
		t.Errorf("expected no promotion for non-JSON content, got %v", data)
	}
}

func TestPromoteToolInputContentDoesNotOverwriteExisting(t *testing.T) {
	data := map[string]any{"address": "KEEP THIS"}
	parsedInput := map[string]any{
		"file_path": "/workspace/props.json",
		"content":   `{"address":"OVERWRITE","rent":1200}`,
	}

	promoteToolInputContent(data, parsedInput)

	if data["address"] != "KEEP THIS" {
		t.Errorf("existing key was overwritten: address = %v", data["address"])
	}
	if data["rent"] != float64(1200) {
		t.Errorf("new key not promoted: rent = %v", data["rent"])
	}
}

func TestPromoteToolInputContentNoContentField(t *testing.T) {
	data := map[string]any{}
	parsedInput := map[string]any{
		"query": "SELECT * FROM users",
	}

	promoteToolInputContent(data, parsedInput)

	if len(data) != 0 {
		t.Errorf("expected no promotion without content field, got %v", data)
	}
}

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

func TestFirstMatchingStringPrefersHighPriorityKeyAcrossSiblings(t *testing.T) {
	value := map[string]any{
		"source":  map[string]any{"name": "Low-priority name"},
		"details": map[string]any{"title": "High-priority title"},
	}

	for range 20 {
		got := firstMatchingString(value, "title", "name")
		if got != "High-priority title" {
			t.Fatalf("firstMatchingString = %q, want %q (key priority violated)", got, "High-priority title")
		}
	}
}
