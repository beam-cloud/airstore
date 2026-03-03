package services

import (
	"context"
	"testing"

	hookspkg "github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type testHookEmitter struct {
	events []map[string]any
}

func (e *testHookEmitter) Emit(_ context.Context, data map[string]any) error {
	copied := make(map[string]any, len(data))
	for k, v := range data {
		copied[k] = v
	}
	e.events = append(e.events, copied)
	return nil
}

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

func TestParseQuerySpec_GmailAttachmentMetadata(t *testing.T) {
	queryJSON := `{"gmail_query":"has:attachment","include_attachments":true,"include_inline":false,"include_message_body":false}`
	spec := parseQuerySpec("gmail", queryJSON)

	if spec.Query != "has:attachment" {
		t.Errorf("Expected Query to be 'has:attachment', got %q", spec.Query)
	}
	if got := spec.Metadata["include_attachments"]; got != "true" {
		t.Errorf("Expected include_attachments=true metadata, got %q", got)
	}
	if got := spec.Metadata["include_inline"]; got != "false" {
		t.Errorf("Expected include_inline=false metadata, got %q", got)
	}
	if got := spec.Metadata["include_message_body"]; got != "false" {
		t.Errorf("Expected include_message_body=false metadata, got %q", got)
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

func TestEmitSourceHookEvents_FirstObservationEmits(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	emitter := &testHookEmitter{}
	svc := &SourceService{
		seenTracker: hookspkg.NewSeenTracker(rdb),
		hookStream:  emitter,
	}

	query := &types.FilesystemQuery{
		WorkspaceId: 124,
		Integration: "github",
		Path:        "/sources/github/test-prs",
	}
	results := []repository.QueryResult{
		{ID: "pr-1"},
		{ID: "pr-2"},
	}

	newCount := svc.emitSourceHookEvents(context.Background(), 124, query, results)
	if newCount != 2 {
		t.Fatalf("expected 2 new results on first observation, got %d", newCount)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(emitter.events))
	}
	if gotEvent, _ := emitter.events[0]["event"].(string); gotEvent != hookspkg.EventFsCreate {
		t.Fatalf("expected fs.create event, got %q", gotEvent)
	}
	if gotPath, _ := emitter.events[0]["path"].(string); gotPath != "/sources/github/test-prs" {
		t.Fatalf("unexpected emitted path: %q", gotPath)
	}
	if gotHash, _ := emitter.events[0]["new_items_hash"].(string); gotHash == "" {
		t.Fatalf("expected non-empty new_items_hash on emitted event")
	}

	// Same snapshot should not emit again.
	newCount = svc.emitSourceHookEvents(context.Background(), 124, query, results)
	if newCount != 0 {
		t.Fatalf("expected 0 new results for unchanged snapshot, got %d", newCount)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected still 1 emitted event, got %d", len(emitter.events))
	}
}

func TestEmitSourceHookEvents_EmptyThenReappearEmits(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	emitter := &testHookEmitter{}
	svc := &SourceService{
		seenTracker: hookspkg.NewSeenTracker(rdb),
		hookStream:  emitter,
	}

	query := &types.FilesystemQuery{
		WorkspaceId: 125,
		Integration: "github",
		// Deliberately include trailing slash to verify normalization before emit.
		Path: "/sources/github/reappear/",
	}

	// First poll empty: no event, but tracker is initialized.
	newCount := svc.emitSourceHookEvents(context.Background(), 125, query, nil)
	if newCount != 0 {
		t.Fatalf("expected 0 new results for empty snapshot, got %d", newCount)
	}
	if len(emitter.events) != 0 {
		t.Fatalf("expected no emitted events for empty snapshot, got %d", len(emitter.events))
	}

	// Results appear later: should emit immediately.
	newCount = svc.emitSourceHookEvents(context.Background(), 125, query, []repository.QueryResult{
		{ID: "pr-99"},
	})
	if newCount != 1 {
		t.Fatalf("expected 1 new result after reappearance, got %d", newCount)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event after reappearance, got %d", len(emitter.events))
	}
	if gotEvent, _ := emitter.events[0]["event"].(string); gotEvent != hookspkg.EventFsCreate {
		t.Fatalf("expected fs.create event, got %q", gotEvent)
	}
	if gotPath, _ := emitter.events[0]["path"].(string); gotPath != "/sources/github/reappear" {
		t.Fatalf("expected normalized emitted path, got %q", gotPath)
	}
	if gotHash, _ := emitter.events[0]["new_items_hash"].(string); gotHash == "" {
		t.Fatalf("expected non-empty new_items_hash on emitted event")
	}
}

func TestEmitSourceHookEvents_RemovedItemsEmitFsDelete(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	emitter := &testHookEmitter{}
	svc := &SourceService{
		seenTracker: hookspkg.NewSeenTracker(rdb),
		hookStream:  emitter,
	}

	query := &types.FilesystemQuery{
		WorkspaceId: 126,
		Integration: "linear",
		Path:        "/sources/linear/issues",
	}

	// Bootstrap with {a, b, c}
	svc.emitSourceHookEvents(context.Background(), 126, query, []repository.QueryResult{
		{ID: "a"}, {ID: "b"}, {ID: "c"},
	})
	emitter.events = nil

	// Now only {a} remains → b,c removed
	newCount := svc.emitSourceHookEvents(context.Background(), 126, query, []repository.QueryResult{
		{ID: "a"},
	})
	if newCount != 0 {
		t.Fatalf("expected 0 new results, got %d", newCount)
	}
	if len(emitter.events) != 1 {
		t.Fatalf("expected 1 emitted event (fs.delete), got %d", len(emitter.events))
	}
	if gotEvent, _ := emitter.events[0]["event"].(string); gotEvent != hookspkg.EventFsDelete {
		t.Fatalf("expected fs.delete event, got %q", gotEvent)
	}
	if gotCount, _ := emitter.events[0]["removed_count"].(string); gotCount != "2" {
		t.Fatalf("expected removed_count=2, got %q", gotCount)
	}
}
