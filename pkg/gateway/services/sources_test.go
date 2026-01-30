package services

import (
	"testing"
)

func TestParseQuerySpec_Defaults(t *testing.T) {
	// Empty query spec should use defaults
	spec := parseQuerySpec("gmail", "{}")

	if spec.Limit != defaultPageSize {
		t.Errorf("Expected Limit to be %d, got %d", defaultPageSize, spec.Limit)
	}
	if spec.MaxResults != defaultMaxResults {
		t.Errorf("Expected MaxResults to be %d, got %d", defaultMaxResults, spec.MaxResults)
	}
}

func TestParseQuerySpec_Gmail(t *testing.T) {
	queryJSON := `{"gmail_query": "is:unread", "limit": 100, "max_results": 300}`
	spec := parseQuerySpec("gmail", queryJSON)

	if spec.Query != "is:unread" {
		t.Errorf("Expected Query to be 'is:unread', got '%s'", spec.Query)
	}
	if spec.Limit != 100 {
		t.Errorf("Expected Limit to be 100, got %d", spec.Limit)
	}
	if spec.MaxResults != 300 {
		t.Errorf("Expected MaxResults to be 300, got %d", spec.MaxResults)
	}
}

func TestParseQuerySpec_GDrive(t *testing.T) {
	queryJSON := `{"gdrive_query": "mimeType='application/pdf'", "limit": 50}`
	spec := parseQuerySpec("gdrive", queryJSON)

	if spec.Query != "mimeType='application/pdf'" {
		t.Errorf("Expected Query to be gdrive query, got '%s'", spec.Query)
	}
	if spec.Limit != 50 {
		t.Errorf("Expected Limit to be 50, got %d", spec.Limit)
	}
	// MaxResults should default to 500
	if spec.MaxResults != defaultMaxResults {
		t.Errorf("Expected MaxResults to be %d (default), got %d", defaultMaxResults, spec.MaxResults)
	}
}

func TestParseQuerySpec_Notion(t *testing.T) {
	queryJSON := `{"notion_query": "meeting notes", "limit": 25, "max_results": 200}`
	spec := parseQuerySpec("notion", queryJSON)

	if spec.Query != "meeting notes" {
		t.Errorf("Expected Query to be 'meeting notes', got '%s'", spec.Query)
	}
	if spec.Limit != 25 {
		t.Errorf("Expected Limit to be 25, got %d", spec.Limit)
	}
	if spec.MaxResults != 200 {
		t.Errorf("Expected MaxResults to be 200, got %d", spec.MaxResults)
	}
}

func TestParseQuerySpec_MaxResultsCapped(t *testing.T) {
	// MaxResults should be capped at defaultMaxResults (500)
	queryJSON := `{"gmail_query": "is:starred", "max_results": 10000}`
	spec := parseQuerySpec("gmail", queryJSON)

	if spec.MaxResults != defaultMaxResults {
		t.Errorf("Expected MaxResults to be capped at %d, got %d", defaultMaxResults, spec.MaxResults)
	}
}

func TestParseQuerySpec_FilenameFormat(t *testing.T) {
	queryJSON := `{"gmail_query": "from:test@example.com", "filename_format": "{date}_{from}_{id}.txt"}`
	spec := parseQuerySpec("gmail", queryJSON)

	if spec.FilenameFormat != "{date}_{from}_{id}.txt" {
		t.Errorf("Expected FilenameFormat to be custom format, got '%s'", spec.FilenameFormat)
	}
}

func TestParseQuerySpec_DefaultFilenameFormat(t *testing.T) {
	// When no filename_format is provided, should use default for integration
	queryJSON := `{"gmail_query": "is:important"}`
	spec := parseQuerySpec("gmail", queryJSON)

	expected := "{date}_{from}_{subject}_{id}.txt"
	if spec.FilenameFormat != expected {
		t.Errorf("Expected FilenameFormat to be '%s', got '%s'", expected, spec.FilenameFormat)
	}
}

func TestParseQuerySpec_InvalidJSON(t *testing.T) {
	// Invalid JSON should use all defaults
	spec := parseQuerySpec("gmail", "not valid json")

	if spec.Limit != defaultPageSize {
		t.Errorf("Expected Limit to be %d for invalid JSON, got %d", defaultPageSize, spec.Limit)
	}
	if spec.MaxResults != defaultMaxResults {
		t.Errorf("Expected MaxResults to be %d for invalid JSON, got %d", defaultMaxResults, spec.MaxResults)
	}
	if spec.Query != "" {
		t.Errorf("Expected Query to be empty for invalid JSON, got '%s'", spec.Query)
	}
}

func TestParseQuerySpec_ZeroLimit(t *testing.T) {
	// Zero limit should use default
	queryJSON := `{"gmail_query": "is:unread", "limit": 0}`
	spec := parseQuerySpec("gmail", queryJSON)

	if spec.Limit != defaultPageSize {
		t.Errorf("Expected Limit to be %d for zero limit, got %d", defaultPageSize, spec.Limit)
	}
}

func TestParseQuerySpec_NegativeLimit(t *testing.T) {
	// Negative limit should use default
	queryJSON := `{"gmail_query": "is:unread", "limit": -10}`
	spec := parseQuerySpec("gmail", queryJSON)

	if spec.Limit != defaultPageSize {
		t.Errorf("Expected Limit to be %d for negative limit, got %d", defaultPageSize, spec.Limit)
	}
}

func TestDefaultPaginationConstants(t *testing.T) {
	// Verify the default constants are set correctly
	if defaultPageSize != 50 {
		t.Errorf("Expected defaultPageSize to be 50, got %d", defaultPageSize)
	}
	if defaultMaxResults != 500 {
		t.Errorf("Expected defaultMaxResults to be 500, got %d", defaultMaxResults)
	}
}
