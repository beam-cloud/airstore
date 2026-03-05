package sources

import (
	"strings"
	"testing"
)

func TestSanitizeFilename(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple text",
			input:    "hello world",
			expected: "hello_world",
		},
		{
			name:     "with emojis",
			input:    "📧 Important Email 🔥",
			expected: "Important_Email",
		},
		{
			name:     "mixed emojis and text",
			input:    "Weekly Update 📊 Report",
			expected: "Weekly_Update_Report",
		},
		{
			name:     "unicode characters",
			input:    "Café résumé naïve",
			expected: "Caf_r_sum_na_ve",
		},
		{
			name:     "special characters",
			input:    "File: Test/Path\\Name",
			expected: "File_Test_Path_Name",
		},
		{
			name:     "multiple spaces and underscores",
			input:    "Hello   World___Test",
			expected: "Hello_World_Test",
		},
		{
			name:     "keeps hyphens and dots",
			input:    "file-name.txt",
			expected: "file-name.txt",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "_unknown_",
		},
		{
			name:     "only emojis",
			input:    "🎉🎊🎈",
			expected: "_unknown_",
		},
		{
			name:     "leading and trailing special chars",
			input:    "___test___",
			expected: "test",
		},
		{
			name:     "japanese characters",
			input:    "日本語テスト",
			expected: "_unknown_",
		},
		{
			name:     "mixed valid and invalid",
			input:    "Report 2026 📈 Q1",
			expected: "Report_2026_Q1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SanitizeFilename(tt.input)
			if got != tt.expected {
				t.Errorf("SanitizeFilename(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestGenerateSourceReadmeConnectedUsesReadOnlyGuidance(t *testing.T) {
	content := string(GenerateSourceReadme("linear", true, "shared", ""))
	if strings.Contains(content, "mkdir /sources/") {
		t.Fatalf("connected README should not suggest mkdir on sources: %s", content)
	}
	if !strings.Contains(content, "read-only") {
		t.Fatalf("connected README should mention read-only sources: %s", content)
	}
	if !strings.Contains(content, "tools/linear --help") {
		t.Fatalf("connected README should suggest tool usage: %s", content)
	}
}
