package services

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	hookspkg "github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/types"
)

type fakeSourceWatchProvider struct {
	name          string
	results       []sources.QueryResult
	executeErr    error
	executeCalls  int
	executedSpecs []sources.QuerySpec
}

func (p *fakeSourceWatchProvider) Name() string {
	if p.name != "" {
		return p.name
	}
	return string(types.SourceWeb)
}

func (p *fakeSourceWatchProvider) Stat(context.Context, *sources.ProviderContext, string) (*sources.FileInfo, error) {
	return &sources.FileInfo{IsDir: true}, nil
}

func (p *fakeSourceWatchProvider) ReadDir(context.Context, *sources.ProviderContext, string) ([]sources.DirEntry, error) {
	return nil, nil
}

func (p *fakeSourceWatchProvider) Read(context.Context, *sources.ProviderContext, string, int64, int64) ([]byte, error) {
	return nil, nil
}

func (p *fakeSourceWatchProvider) Readlink(context.Context, *sources.ProviderContext, string) (string, error) {
	return "", nil
}

func (p *fakeSourceWatchProvider) Search(context.Context, *sources.ProviderContext, string, int) ([]sources.SearchResult, error) {
	return nil, sources.ErrSearchNotSupported
}

func (p *fakeSourceWatchProvider) ExecuteQuery(_ context.Context, _ *sources.ProviderContext, spec sources.QuerySpec) (*sources.QueryResponse, error) {
	p.executeCalls++
	p.executedSpecs = append(p.executedSpecs, spec)
	if p.executeErr != nil {
		return nil, p.executeErr
	}
	results := make([]sources.QueryResult, len(p.results))
	copy(results, p.results)
	return &sources.QueryResponse{Results: results}, nil
}

func (p *fakeSourceWatchProvider) ReadResult(context.Context, *sources.ProviderContext, string) ([]byte, error) {
	return []byte("ok"), nil
}

func (p *fakeSourceWatchProvider) FormatFilename(format string, metadata map[string]string) string {
	if title := metadata["title"]; title != "" {
		return title + ".md"
	}
	if id := metadata["id"]; id != "" {
		return id + ".md"
	}
	return "result.md"
}

type fakeSourceWatchConnectionBackend struct {
	repository.BackendRepository
	connections []types.IntegrationConnection
	runs        map[string]*types.AgentRun
	getCalls    []struct {
		workspaceID uint
		memberID    uint
		integration string
	}
}

func (b *fakeSourceWatchConnectionBackend) GetConnection(
	_ context.Context,
	workspaceID uint,
	memberID uint,
	integration string,
) (*types.IntegrationConnection, error) {
	b.getCalls = append(b.getCalls, struct {
		workspaceID uint
		memberID    uint
		integration string
	}{
		workspaceID: workspaceID,
		memberID:    memberID,
		integration: integration,
	})
	var shared *types.IntegrationConnection
	for _, conn := range b.connections {
		if conn.WorkspaceId != workspaceID || conn.IntegrationType != integration {
			continue
		}
		connCopy := conn
		if conn.MemberId != nil && *conn.MemberId == memberID {
			return &connCopy, nil
		}
		if conn.MemberId == nil {
			shared = &connCopy
		}
	}
	return shared, nil
}

func (b *fakeSourceWatchConnectionBackend) ListConnections(
	_ context.Context,
	workspaceID uint,
) ([]types.IntegrationConnection, error) {
	out := make([]types.IntegrationConnection, 0, len(b.connections))
	for _, conn := range b.connections {
		if conn.WorkspaceId != workspaceID {
			continue
		}
		out = append(out, conn)
	}
	return out, nil
}

func (b *fakeSourceWatchConnectionBackend) SaveConnection(
	_ context.Context,
	workspaceID uint,
	memberID *uint,
	integration string,
	creds *types.IntegrationCredentials,
	scope string,
) (*types.IntegrationConnection, error) {
	body, err := json.Marshal(creds)
	if err != nil {
		return nil, err
	}
	conn := &types.IntegrationConnection{
		WorkspaceId:     workspaceID,
		MemberId:        memberID,
		IntegrationType: integration,
		Credentials:     body,
		Scope:           scope,
	}
	return conn, nil
}

func (b *fakeSourceWatchConnectionBackend) GetAgentRunByID(_ context.Context, runID string) (*types.AgentRun, error) {
	if b.runs == nil {
		return nil, nil
	}
	return b.runs[runID], nil
}

func TestBuildSourceWatchQuerySpec_GmailExactIdentifiers(t *testing.T) {
	memberID := uint(42)
	querySpec, filenameFormat, err := buildSourceWatchQuerySpec(&types.SourceWatchRequest{
		Integration:        string(types.SourceGmail),
		Query:              `subject:"Quarterly report"`,
		ThreadID:           "thread-123",
		MessageID:          "msg-456",
		IncludeAttachments: true,
		IncludeMessageBody: true,
	}, &memberID, true)
	if err != nil {
		t.Fatalf("buildSourceWatchQuerySpec returned error: %v", err)
	}
	if filenameFormat != sources.DefaultFilenameFormat(string(types.SourceGmail)) {
		t.Fatalf("filename format = %q, want default gmail format", filenameFormat)
	}

	parsed := parseQuerySpec(string(types.SourceGmail), querySpec)
	if parsed.Query != `subject:"Quarterly report"` {
		t.Fatalf("query = %q, want exact gmail query", parsed.Query)
	}
	if got := parsed.Metadata["thread_id"]; got != "thread-123" {
		t.Fatalf("thread_id = %q, want thread-123", got)
	}
	if got := parsed.Metadata["message_id"]; got != "msg-456" {
		t.Fatalf("message_id = %q, want msg-456", got)
	}
	if got := parsed.Metadata["include_attachments"]; got != "true" {
		t.Fatalf("include_attachments = %q, want true", got)
	}
	if got := parsed.Metadata[legacyQueryCredentialMemberIDKey]; got != "42" {
		t.Fatalf("credential_member_id = %q, want 42", got)
	}
	var rawSpec struct {
		BaselinePending bool `json:"source_watch_baseline_pending"`
	}
	if err := json.Unmarshal([]byte(querySpec), &rawSpec); err != nil {
		t.Fatalf("failed to decode raw query spec: %v", err)
	}
	if !rawSpec.BaselinePending {
		t.Fatal("expected source watch baseline to start pending")
	}
}

func TestRegisterTaskSourceWatchesSeedsBaselineAndDedupes(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		results: []sources.QueryResult{
			{ID: "doc-1", Filename: "alpha.md", Metadata: map[string]string{"title": "alpha"}},
			{ID: "doc-2", Filename: "beta.md", Metadata: map[string]string{"title": "beta"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	svc := &SourceService{
		registry:    registry,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-1", WorkspaceID: 7}

	req := &types.SourceWatchRequest{
		Integration: string(types.SourceWeb),
		Query:       "site:example.com quarterly report",
		EntityLabel: "Quarterly report",
	}
	blocker, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{
		req,
		{
			Integration: string(types.SourceWeb),
			Query:       " site:example.com quarterly report ",
			EntityLabel: "Quarterly report",
		},
	})
	if err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}
	if blocker == nil {
		t.Fatal("expected blocker spec")
	}
	if got, _ := blocker.PayloadJSON["source_watch_count"].(int); got != 1 {
		t.Fatalf("source_watch_count = %d, want 1", got)
	}
	if provider.executeCalls != 0 {
		t.Fatalf("execute calls = %d, want 0 (baseline is lazy)", provider.executeCalls)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceWeb))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	if !queries[0].SystemManaged {
		t.Fatal("expected source watch query to be system managed")
	}
	if queries[0].Lifecycle != types.FilesystemQueryLifecycleTaskFollowUp {
		t.Fatalf("query lifecycle = %q, want %q", queries[0].Lifecycle, types.FilesystemQueryLifecycleTaskFollowUp)
	}
	if queries[0].OwnerTaskID == nil || *queries[0].OwnerTaskID != task.ID {
		t.Fatalf("query owner task id = %#v, want %q", queries[0].OwnerTaskID, task.ID)
	}
	if !sourceWatchBaselinePending(queries[0]) {
		t.Fatal("expected baseline_pending=true (baseline is established lazily by poller)")
	}
	sourceWatches, ok := blocker.PayloadJSON["source_watches"].([]map[string]any)
	if !ok || len(sourceWatches) != 1 {
		t.Fatalf("source_watches payload = %#v, want one entry", blocker.PayloadJSON["source_watches"])
	}
	if gotPath, _ := sourceWatches[0]["path"].(string); gotPath != queries[0].Path {
		t.Fatalf("blocker path = %q, want %q", gotPath, queries[0].Path)
	}

	hooks, err := fsStore.ListHooks(context.Background(), task.WorkspaceID)
	if err != nil {
		t.Fatalf("ListHooks returned error: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("hook count = %d, want 1", len(hooks))
	}
	if hooks[0].DeliveryMode != types.HookDeliveryModeTaskInput {
		t.Fatalf("hook delivery mode = %q, want %q", hooks[0].DeliveryMode, types.HookDeliveryModeTaskInput)
	}
	if hooks[0].TargetTaskID == nil || *hooks[0].TargetTaskID != task.ID {
		t.Fatalf("hook target task id = %#v, want %q", hooks[0].TargetTaskID, task.ID)
	}
}

func TestRegisterTaskSourceWatchesBuildsMultiWatchBlocker(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		results: []sources.QueryResult{
			{ID: "doc-1", Filename: "alpha.md", Metadata: map[string]string{"title": "alpha"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	svc := &SourceService{
		registry:    registry,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-2", WorkspaceID: 8}

	blocker, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{
		{Integration: string(types.SourceWeb), Query: "site:example.com alpha", EntityLabel: "alpha"},
		{Integration: string(types.SourceWeb), Query: "site:example.com beta", EntityLabel: "beta"},
	})
	if err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}
	if blocker == nil {
		t.Fatal("expected blocker spec")
	}
	if got, _ := blocker.PayloadJSON["source_watch_count"].(int); got != 2 {
		t.Fatalf("source_watch_count = %d, want 2", got)
	}
	sourceWatches, ok := blocker.PayloadJSON["source_watches"].([]map[string]any)
	if !ok || len(sourceWatches) != 2 {
		t.Fatalf("source_watches payload = %#v, want 2 entries", blocker.PayloadJSON["source_watches"])
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceWeb))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 2 {
		t.Fatalf("query count = %d, want 2", len(queries))
	}
}

func TestCleanupTaskSourceWatchesDeletesSystemManagedResources(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		results: []sources.QueryResult{
			{ID: "doc-1", Filename: "alpha.md", Metadata: map[string]string{"title": "alpha"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	svc := &SourceService{
		registry:    registry,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-3", WorkspaceID: 9}

	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{
		{Integration: string(types.SourceWeb), Query: "site:example.com cleanup", EntityLabel: "cleanup"},
	}); err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}
	if err := svc.CleanupTaskSourceWatches(context.Background(), task); err != nil {
		t.Fatalf("CleanupTaskSourceWatches returned error: %v", err)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceWeb))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 0 {
		t.Fatalf("query count after cleanup = %d, want 0", len(queries))
	}
	hooks, err := fsStore.ListHooks(context.Background(), task.WorkspaceID)
	if err != nil {
		t.Fatalf("ListHooks returned error: %v", err)
	}
	if len(hooks) != 0 {
		t.Fatalf("hook count after cleanup = %d, want 0", len(hooks))
	}
}

func TestCleanupTaskSourceWatchesLeavesManualViewsIntact(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		results: []sources.QueryResult{
			{ID: "doc-1", Filename: "alpha.md", Metadata: map[string]string{"title": "alpha"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	svc := &SourceService{
		registry:    registry,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-manual-cleanup", WorkspaceID: 13}
	manualPath := types.PathSources + "/" + string(types.SourceWeb) + "/manual-view"
	if _, err := fsStore.CreateQuery(context.Background(), &types.FilesystemQuery{
		WorkspaceId:    task.WorkspaceID,
		Integration:    string(types.SourceWeb),
		Path:           manualPath,
		Name:           "manual-view",
		QuerySpec:      `{"web_query":"site:example.com manual","limit":50}`,
		OutputFormat:   types.ViewOutputFolder,
		FilenameFormat: sources.DefaultFilenameFormat(string(types.SourceWeb)),
		Mode:           types.ViewModeQuery,
	}); err != nil {
		t.Fatalf("CreateQuery returned error: %v", err)
	}
	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceWeb),
		Query:       "site:example.com cleanup",
		EntityLabel: "cleanup",
	}}); err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}

	if err := svc.CleanupTaskSourceWatches(context.Background(), task); err != nil {
		t.Fatalf("CleanupTaskSourceWatches returned error: %v", err)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceWeb))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count after cleanup = %d, want 1 manual view", len(queries))
	}
	if queries[0].Path != manualPath {
		t.Fatalf("remaining query path = %q, want %q", queries[0].Path, manualPath)
	}
	if queries[0].SystemManaged {
		t.Fatal("expected remaining manual view to stay user managed")
	}
}

func TestRegisterTaskSourceWatchesPersistsMemberScopedCredentialSelector(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	memberID := uint(42)
	credsJSON, err := json.Marshal(&types.IntegrationCredentials{AccessToken: "token"})
	if err != nil {
		t.Fatalf("failed to marshal test credentials: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		name: string(types.SourceGmail),
		results: []sources.QueryResult{
			{ID: "msg-1", Filename: "reply.txt", Metadata: map[string]string{"id": "msg-1"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	backend := &fakeSourceWatchConnectionBackend{
		connections: []types.IntegrationConnection{{
			WorkspaceId:     10,
			MemberId:        &memberID,
			IntegrationType: string(types.SourceGmail),
			Credentials:     credsJSON,
		}},
	}
	svc := &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-gmail", WorkspaceID: 10}

	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Query:       "label:inbox",
		EntityLabel: "Inbox replies",
		ThreadID:    "thread-123",
	}}); err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceGmail))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	if queries[0].CredentialMemberID == nil || *queries[0].CredentialMemberID != 42 {
		t.Fatalf("credential member id = %v, want 42", queries[0].CredentialMemberID)
	}

	hooks, err := fsStore.ListHooks(context.Background(), task.WorkspaceID)
	if err != nil {
		t.Fatalf("ListHooks returned error: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("hook count = %d, want 1", len(hooks))
	}
	if hooks[0].CreatedByMemberId == nil || *hooks[0].CreatedByMemberId != 42 {
		t.Fatalf("hook created_by_member_id = %v, want 42", hooks[0].CreatedByMemberId)
	}

	svc.credCache = sync.Map{}
	backend.getCalls = nil
	if err := svc.RefreshQuery(context.Background(), &types.FilesystemQuery{
		ExternalId:  queries[0].ExternalId,
		WorkspaceId: queries[0].WorkspaceId,
		Integration: queries[0].Integration,
		Path:        queries[0].Path,
	}); err != nil {
		t.Fatalf("RefreshQuery returned error: %v", err)
	}
	if len(backend.getCalls) == 0 {
		t.Fatal("expected credential lookup during refresh")
	}
	if got := backend.getCalls[0].memberID; got != memberID {
		t.Fatalf("refresh credential member id = %d, want %d", got, memberID)
	}
}

func TestRegisterTaskSourceWatchesUsesOriginRunMemberToDisambiguateConnections(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	memberA := uint(42)
	memberB := uint(84)
	credsA, err := json.Marshal(&types.IntegrationCredentials{AccessToken: "token-a"})
	if err != nil {
		t.Fatalf("failed to marshal member A credentials: %v", err)
	}
	credsB, err := json.Marshal(&types.IntegrationCredentials{AccessToken: "token-b"})
	if err != nil {
		t.Fatalf("failed to marshal member B credentials: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		name: string(types.SourceGmail),
		results: []sources.QueryResult{
			{ID: "msg-1", Filename: "reply.txt", Metadata: map[string]string{"id": "msg-1"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	runID := "run-disambiguated"
	backend := &fakeSourceWatchConnectionBackend{
		connections: []types.IntegrationConnection{
			{
				WorkspaceId:     11,
				MemberId:        &memberA,
				IntegrationType: string(types.SourceGmail),
				Credentials:     credsA,
			},
			{
				WorkspaceId:     11,
				MemberId:        &memberB,
				IntegrationType: string(types.SourceGmail),
				Credentials:     credsB,
			},
		},
		runs: map[string]*types.AgentRun{
			runID: {
				ID:                runID,
				WorkspaceID:       11,
				CreatedByMemberID: &memberA,
			},
		},
	}
	svc := &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-gmail-origin", WorkspaceID: 11, TargetRunID: &runID}

	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Query:       "label:inbox",
		EntityLabel: "Inbox replies",
		ThreadID:    "thread-123",
	}}); err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceGmail))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	if queries[0].CredentialMemberID == nil || *queries[0].CredentialMemberID != memberA {
		t.Fatalf("credential member id = %v, want %d", queries[0].CredentialMemberID, memberA)
	}
	if queries[0].OwnerRunID == nil || *queries[0].OwnerRunID != runID {
		t.Fatalf("query owner run id = %#v, want %q", queries[0].OwnerRunID, runID)
	}
}

func TestRegisterTaskSourceWatchesSucceedsWithFailingProvider(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		executeErr: fmt.Errorf("transient provider failure"),
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	emitter := &testHookEmitter{}
	svc := &SourceService{
		registry:    registry,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
		hookStream:  emitter,
	}
	task := &types.AgentTask{ID: "task-bootstrap-failure", WorkspaceID: 12}

	blocker, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceWeb),
		Query:       "site:example.com status",
		EntityLabel: "status page",
	}})
	if err != nil {
		t.Fatalf("registration should succeed (bootstrap is lazy): %v", err)
	}
	if blocker == nil {
		t.Fatal("expected blocker spec")
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceWeb))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	if !sourceWatchBaselinePending(queries[0]) {
		t.Fatal("expected baseline_pending=true (poller will handle bootstrap)")
	}
	if provider.executeCalls != 0 {
		t.Fatalf("execute calls = %d, want 0 (no inline bootstrap)", provider.executeCalls)
	}

	hooks, err := fsStore.ListHooks(context.Background(), task.WorkspaceID)
	if err != nil {
		t.Fatalf("ListHooks returned error: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("hook count = %d, want 1", len(hooks))
	}
}

func TestRegisterTaskSourceWatchesAllowsExactGmailQueryWithoutFallback(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	memberID := uint(42)
	creds, err := json.Marshal(&types.IntegrationCredentials{AccessToken: "token-a"})
	if err != nil {
		t.Fatalf("failed to marshal credentials: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		name: string(types.SourceGmail),
		results: []sources.QueryResult{
			{ID: "msg-1", Filename: "reply.txt", Metadata: map[string]string{"id": "msg-1"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	backend := &fakeSourceWatchConnectionBackend{
		connections: []types.IntegrationConnection{{
			WorkspaceId:     13,
			MemberId:        &memberID,
			IntegrationType: string(types.SourceGmail),
			Credentials:     creds,
		}},
	}
	svc := &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-exact-gmail-watch", WorkspaceID: 13}

	blocker, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		ThreadID:    "thread-123",
	}})
	if err != nil {
		t.Fatalf("RegisterTaskSourceWatches returned error: %v", err)
	}
	if blocker == nil {
		t.Fatal("expected blocker spec")
	}
	if provider.executeCalls != 0 {
		t.Fatalf("executeCalls = %d, want 0 (baseline is lazy)", provider.executeCalls)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceGmail))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	if !sourceWatchBaselinePending(queries[0]) {
		t.Fatal("expected baseline_pending=true (poller will establish baseline)")
	}

	parsed := parseQuerySpec(string(types.SourceGmail), queries[0].QuerySpec)
	if got := parsed.Query; got != "" {
		t.Fatalf("query = %q, want empty thread-targeted query", got)
	}
	if got := parsed.Metadata["thread_id"]; got != "thread-123" {
		t.Fatalf("thread_id metadata = %q, want %q", got, "thread-123")
	}
}

func TestRegisterTaskSourceWatchesReusesExistingThreadContext(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	memberID := uint(42)
	creds, err := json.Marshal(&types.IntegrationCredentials{AccessToken: "token-a"})
	if err != nil {
		t.Fatalf("failed to marshal credentials: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		name: string(types.SourceGmail),
		results: []sources.QueryResult{
			{ID: "msg-1", Filename: "reply.txt", Metadata: map[string]string{"id": "msg-1"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	backend := &fakeSourceWatchConnectionBackend{
		connections: []types.IntegrationConnection{{
			WorkspaceId:     15,
			MemberId:        &memberID,
			IntegrationType: string(types.SourceGmail),
			Credentials:     creds,
		}},
	}
	svc := &SourceService{
		registry:    registry,
		backend:     backend,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-reuse-thread-context", WorkspaceID: 15}

	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		ThreadID:    "thread-123",
		EntityLabel: "Live soak thread",
	}}); err != nil {
		t.Fatalf("initial RegisterTaskSourceWatches returned error: %v", err)
	}

	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceGmail),
		Query:       `subject:"Live soak 9b8c239b"`,
		EntityLabel: "Live soak thread",
	}}); err != nil {
		t.Fatalf("second RegisterTaskSourceWatches returned error: %v", err)
	}

	if provider.executeCalls != 0 {
		t.Fatalf("executeCalls = %d, want 0 (baseline is lazy)", provider.executeCalls)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceGmail))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	parsed := parseQuerySpec(string(types.SourceGmail), queries[0].QuerySpec)
	if got := parsed.Metadata["thread_id"]; got != "thread-123" {
		t.Fatalf("persisted thread_id = %q, want %q", got, "thread-123")
	}
	if got := parsed.Query; got != `subject:"Live soak 9b8c239b"` {
		t.Fatalf("query = %q, want subject-scoped follow-up query", got)
	}
}

func TestRegisterTaskSourceWatchesReplacesExistingTaskWatches(t *testing.T) {
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("failed to create test redis: %v", err)
	}
	provider := &fakeSourceWatchProvider{
		results: []sources.QueryResult{
			{ID: "doc-1", Filename: "alpha.md", Metadata: map[string]string{"title": "alpha"}},
		},
	}
	registry := sources.NewRegistry()
	registry.Register(provider)
	fsStore := repository.NewMemoryFilesystemStore()
	svc := &SourceService{
		registry:    registry,
		fsStore:     fsStore,
		seenTracker: hookspkg.NewSeenTracker(rdb),
	}
	task := &types.AgentTask{ID: "task-replace-watches", WorkspaceID: 14}

	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceWeb),
		Query:       "site:example.com first",
		EntityLabel: "first watch",
	}}); err != nil {
		t.Fatalf("first RegisterTaskSourceWatches returned error: %v", err)
	}
	if _, err := svc.RegisterTaskSourceWatches(context.Background(), task, nil, []*types.SourceWatchRequest{{
		Integration: string(types.SourceWeb),
		Query:       "site:example.com second",
		EntityLabel: "second watch",
	}}); err != nil {
		t.Fatalf("second RegisterTaskSourceWatches returned error: %v", err)
	}

	queries, err := fsStore.ListQueries(context.Background(), task.WorkspaceID, types.PathSources+"/"+string(types.SourceWeb))
	if err != nil {
		t.Fatalf("ListQueries returned error: %v", err)
	}
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	spec := parseQuerySpec(string(types.SourceWeb), queries[0].QuerySpec)
	if got := spec.Query; got != "site:example.com second" {
		t.Fatalf("query = %q, want %q", got, "site:example.com second")
	}

	hooks, err := fsStore.ListHooks(context.Background(), task.WorkspaceID)
	if err != nil {
		t.Fatalf("ListHooks returned error: %v", err)
	}
	if len(hooks) != 1 {
		t.Fatalf("hook count = %d, want 1", len(hooks))
	}
	if hooks[0].Path != queries[0].Path {
		t.Fatalf("hook path = %q, want %q", hooks[0].Path, queries[0].Path)
	}
}
