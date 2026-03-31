package worker

import (
	"encoding/json"
	"testing"

	"github.com/beam-cloud/airstore/pkg/tools"
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

func TestResolveToolCommand(t *testing.T) {
	tests := []struct {
		name      string
		toolName  string
		toolInput string
		wantTool  string
		wantCmd   string
	}{
		{
			name:      "direct call: gmail create-draft",
			toolName:  "gmail",
			toolInput: `create-draft "luke@beam.cloud" "Subject" "Body"`,
			wantTool:  "gmail",
			wantCmd:   "create-draft",
		},
		{
			name:      "direct call: gmail send-email",
			toolName:  "gmail",
			toolInput: `send-email "luke@beam.cloud" "Subject" "Body"`,
			wantTool:  "gmail",
			wantCmd:   "send-email",
		},
		{
			name:      "Bash JSON: gmail create-draft",
			toolName:  "Bash",
			toolInput: `{"command":"gmail create-draft \"luke@beam.cloud\" \"Subject\" \"Body\""}`,
			wantTool:  "gmail",
			wantCmd:   "create-draft",
		},
		{
			name:      "Bash JSON: gmail send-email",
			toolName:  "Bash",
			toolInput: `{"command":"gmail send-email \"luke@beam.cloud\" \"Subject\" \"Body\""}`,
			wantTool:  "gmail",
			wantCmd:   "send-email",
		},
		{
			name:      "Bash JSON: path-prefixed /workspace/tools/gmail create-draft",
			toolName:  "Bash",
			toolInput: `{"command":"/workspace/tools/gmail create-draft \"luke@beam.cloud\" \"Re: Subject\" \"Body\""}`,
			wantTool:  "gmail",
			wantCmd:   "create-draft",
		},
		{
			name:      "Bash JSON: path-prefixed /workspace/tools/gmail send-email",
			toolName:  "Bash",
			toolInput: `{"command":"/workspace/tools/gmail send-email \"luke@beam.cloud\" \"Subject\" \"Body\""}`,
			wantTool:  "gmail",
			wantCmd:   "send-email",
		},
		{
			name:      "Bash JSON: path-prefixed gmail search",
			toolName:  "Bash",
			toolInput: `{"command":"/workspace/tools/gmail search \"from:someone\""}`,
			wantTool:  "gmail",
			wantCmd:   "search",
		},
		{
			name:      "Bash plain text: gmail create-draft",
			toolName:  "Bash",
			toolInput: `gmail create-draft "luke@beam.cloud" "Subject" "Body"`,
			wantTool:  "gmail",
			wantCmd:   "create-draft",
		},
		{
			name:      "Bash plain text: path-prefixed",
			toolName:  "Bash",
			toolInput: `/workspace/tools/gmail create-draft "luke@beam.cloud" "Subject" "Body"`,
			wantTool:  "gmail",
			wantCmd:   "create-draft",
		},
		{
			name:      "unknown tool: no match",
			toolName:  "Bash",
			toolInput: `{"command":"echo hello"}`,
			wantTool:  "",
			wantCmd:   "",
		},
		{
			name:      "empty input",
			toolName:  "Bash",
			toolInput: "",
			wantTool:  "",
			wantCmd:   "",
		},
		{
			name:      "malformed JSON falls back to raw text",
			toolName:  "Bash",
			toolInput: `{bad json gmail create-draft "luke@beam.cloud"`,
			wantTool:  "",
			wantCmd:   "",
		},
		{
			name:      "Bash JSON with args field",
			toolName:  "Bash",
			toolInput: `{"args":"gmail send-email \"to\" \"subj\" \"body\""}`,
			wantTool:  "gmail",
			wantCmd:   "send-email",
		},
		{
			name:      "single token command: no match",
			toolName:  "Bash",
			toolInput: `{"command":"ls"}`,
			wantTool:  "",
			wantCmd:   "",
		},
		{
			name:      "Bash JSON: deeply nested path",
			toolName:  "Bash",
			toolInput: `{"command":"/usr/local/bin/gmail get-thread \"abc123\""}`,
			wantTool:  "gmail",
			wantCmd:   "get-thread",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTool, gotCmd := resolveToolCommand(tt.toolName, tt.toolInput)
			if gotTool != tt.wantTool || gotCmd != tt.wantCmd {
				t.Errorf("resolveToolCommand(%q, %q) = (%q, %q), want (%q, %q)",
					tt.toolName, tt.toolInput, gotTool, gotCmd, tt.wantTool, tt.wantCmd)
			}
		})
	}
}

func TestResolveToolCommandOutputTypeOverride(t *testing.T) {
	tool, cmd := resolveToolCommand("Bash", `{"command":"gmail create-draft \"luke@beam.cloud\" \"Subject\" \"Body\""}`)
	if tool != "gmail" || cmd != "create-draft" {
		t.Fatalf("resolve failed: (%q, %q)", tool, cmd)
	}

	// create-draft should have no output_type (schema has it blank)
	ot := tools.CommandOutputType(tool, cmd)
	if ot != "" {
		t.Errorf("CommandOutputType(gmail, create-draft) = %q, want empty string", ot)
	}

	// send-email should have output_type=email
	ot = tools.CommandOutputType("gmail", "send-email")
	if ot != "email" {
		t.Errorf("CommandOutputType(gmail, send-email) = %q, want %q", ot, "email")
	}
}
