package sources

import (
	"testing"
)

func TestQuerySpec_Defaults(t *testing.T) {
	spec := QuerySpec{}

	// Default values should be zero
	if spec.Limit != 0 {
		t.Errorf("Expected Limit to be 0 (unset), got %d", spec.Limit)
	}
	if spec.MaxResults != 0 {
		t.Errorf("Expected MaxResults to be 0 (unset), got %d", spec.MaxResults)
	}
	if spec.PageToken != "" {
		t.Errorf("Expected PageToken to be empty, got %s", spec.PageToken)
	}
}

func TestQuerySpec_WithPagination(t *testing.T) {
	spec := QuerySpec{
		Query:          "is:unread",
		Limit:          50,
		MaxResults:     500,
		FilenameFormat: "{date}_{subject}_{id}.txt",
		PageToken:      "abc123",
	}

	if spec.Query != "is:unread" {
		t.Errorf("Expected Query to be 'is:unread', got %s", spec.Query)
	}
	if spec.Limit != 50 {
		t.Errorf("Expected Limit to be 50, got %d", spec.Limit)
	}
	if spec.MaxResults != 500 {
		t.Errorf("Expected MaxResults to be 500, got %d", spec.MaxResults)
	}
	if spec.PageToken != "abc123" {
		t.Errorf("Expected PageToken to be 'abc123', got %s", spec.PageToken)
	}
}

func TestQueryResponse_Empty(t *testing.T) {
	resp := QueryResponse{
		Results:       []QueryResult{},
		NextPageToken: "",
		HasMore:       false,
	}

	if len(resp.Results) != 0 {
		t.Errorf("Expected empty Results, got %d items", len(resp.Results))
	}
	if resp.HasMore {
		t.Error("Expected HasMore to be false")
	}
	if resp.NextPageToken != "" {
		t.Errorf("Expected empty NextPageToken, got %s", resp.NextPageToken)
	}
}

func TestQueryResponse_WithResults(t *testing.T) {
	results := []QueryResult{
		{
			ID:       "msg1",
			Filename: "2026-01-30_sender_subject_msg1.txt",
			Metadata: map[string]string{
				"from":    "sender@example.com",
				"subject": "Test Subject",
			},
			Size:  1024,
			Mtime: 1706600000,
		},
		{
			ID:       "msg2",
			Filename: "2026-01-29_sender_another_msg2.txt",
			Metadata: map[string]string{
				"from":    "sender@example.com",
				"subject": "Another Subject",
			},
			Size:  2048,
			Mtime: 1706500000,
		},
	}

	resp := QueryResponse{
		Results:       results,
		NextPageToken: "next_page_token_xyz",
		HasMore:       true,
	}

	if len(resp.Results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(resp.Results))
	}
	if !resp.HasMore {
		t.Error("Expected HasMore to be true")
	}
	if resp.NextPageToken != "next_page_token_xyz" {
		t.Errorf("Expected NextPageToken to be 'next_page_token_xyz', got %s", resp.NextPageToken)
	}

	// Verify first result
	if resp.Results[0].ID != "msg1" {
		t.Errorf("Expected first result ID to be 'msg1', got %s", resp.Results[0].ID)
	}
	if resp.Results[0].Size != 1024 {
		t.Errorf("Expected first result Size to be 1024, got %d", resp.Results[0].Size)
	}
}

func TestQueryResponse_LastPage(t *testing.T) {
	// When there are no more pages
	resp := QueryResponse{
		Results: []QueryResult{
			{ID: "final_msg", Filename: "final.txt"},
		},
		NextPageToken: "",
		HasMore:       false,
	}

	if resp.HasMore {
		t.Error("Expected HasMore to be false for last page")
	}
	if resp.NextPageToken != "" {
		t.Errorf("Expected NextPageToken to be empty for last page, got %s", resp.NextPageToken)
	}
}

func TestDefaultFilenameFormat(t *testing.T) {
	tests := []struct {
		integration string
		expected    string
	}{
		{"gmail", "{date}_{from}_{subject}_{id}.txt"},
		{"gdrive", "{name}_{id}"},
		{"notion", "{title}_{id}.md"},
		{"unknown", "{id}"},
	}

	for _, tt := range tests {
		t.Run(tt.integration, func(t *testing.T) {
			result := DefaultFilenameFormat(tt.integration)
			if result != tt.expected {
				t.Errorf("DefaultFilenameFormat(%s) = %s, expected %s", tt.integration, result, tt.expected)
			}
		})
	}
}
