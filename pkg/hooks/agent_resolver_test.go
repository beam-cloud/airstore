package hooks

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

type fakeAgentBackend struct {
	repository.BackendRepository
	mu        sync.Mutex
	nextID    int
	byID      map[string]*types.AgentProfile
	byWSAgent map[string]string
	createErr error
	updateErr error
}

func newFakeAgentBackend() *fakeAgentBackend {
	return &fakeAgentBackend{
		byID:      map[string]*types.AgentProfile{},
		byWSAgent: map[string]string{},
	}
}

func (f *fakeAgentBackend) CreateAgentProfile(_ context.Context, profile *types.AgentProfile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.createErr != nil {
		return f.createErr
	}
	key := wsAgentKey(profile.WorkspaceID, profile.AgentKey)
	if _, exists := f.byWSAgent[key]; exists {
		return fmt.Errorf("duplicate key")
	}
	f.nextID++
	id := fmt.Sprintf("agent-%d", f.nextID)
	stored := cloneProfile(profile)
	stored.ID = id
	f.byID[id] = stored
	f.byWSAgent[key] = id
	profile.ID = id
	return nil
}

func (f *fakeAgentBackend) GetAgentProfile(_ context.Context, workspaceID uint, agentID string) (*types.AgentProfile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	profile, ok := f.byID[agentID]
	if !ok || profile.WorkspaceID != workspaceID {
		return nil, &types.ErrAgentProfileNotFound{ID: agentID}
	}
	return cloneProfile(profile), nil
}

func (f *fakeAgentBackend) GetAgentProfileByKey(_ context.Context, workspaceID uint, agentKey string) (*types.AgentProfile, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.byWSAgent[wsAgentKey(workspaceID, agentKey)]
	if !ok {
		return nil, &types.ErrAgentProfileNotFound{ID: agentKey}
	}
	return cloneProfile(f.byID[id]), nil
}

func (f *fakeAgentBackend) UpdateAgentProfile(_ context.Context, profile *types.AgentProfile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.updateErr != nil {
		return f.updateErr
	}
	existing, ok := f.byID[profile.ID]
	if !ok {
		return &types.ErrAgentProfileNotFound{ID: profile.ID}
	}
	key := wsAgentKey(existing.WorkspaceID, existing.AgentKey)
	delete(f.byWSAgent, key)

	stored := cloneProfile(profile)
	f.byID[profile.ID] = stored
	f.byWSAgent[wsAgentKey(stored.WorkspaceID, stored.AgentKey)] = profile.ID
	return nil
}

func (f *fakeAgentBackend) DeleteAgentProfile(_ context.Context, workspaceID uint, agentID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	profile, ok := f.byID[agentID]
	if !ok || profile.WorkspaceID != workspaceID {
		return &types.ErrAgentProfileNotFound{ID: agentID}
	}
	delete(f.byID, agentID)
	delete(f.byWSAgent, wsAgentKey(profile.WorkspaceID, profile.AgentKey))
	return nil
}

func (f *fakeAgentBackend) ListRunExecutionsByHook(_ context.Context, _ uint) ([]*types.RunExecution, error) {
	return []*types.RunExecution{}, nil
}

func wsAgentKey(workspaceID uint, agentKey string) string {
	return fmt.Sprintf("%d:%s", workspaceID, agentKey)
}

func cloneProfile(profile *types.AgentProfile) *types.AgentProfile {
	if profile == nil {
		return nil
	}
	cfg := map[string]any{}
	for k, v := range profile.ConfigJSON {
		cfg[k] = v
	}
	cp := *profile
	cp.ConfigJSON = cfg
	return &cp
}

func strPtr(v string) *string {
	return &v
}

type failOnSecondCreateStore struct {
	repository.FilesystemStore
	createCalls int
}

func (s *failOnSecondCreateStore) CreateHook(ctx context.Context, hook *types.Hook) (*types.Hook, error) {
	s.createCalls++
	if s.createCalls > 1 {
		return nil, fmt.Errorf("forced create failure")
	}
	return s.FilesystemStore.CreateHook(ctx, hook)
}

func TestResolveHookAgent_GetOrCreateAndUpdate(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()

	created, err := ResolveHookAgent(ctx, backend, 11, "/sources/github/prs", nil, &AgentConfigPatch{
		Name:  strPtr("PR Review Hook"),
		Model: strPtr("claude-sonnet-4-6"),
	})
	if err != nil {
		t.Fatalf("resolve create: %v", err)
	}
	if created.ID == "" {
		t.Fatal("expected created agent id")
	}
	if created.AgentKey != HookAgentKey("/sources/github/prs") {
		t.Fatalf("unexpected agent key: %s", created.AgentKey)
	}
	if created.Name != "PR Review Hook" {
		t.Fatalf("unexpected agent name: %s", created.Name)
	}
	if got := fmt.Sprintf("%v", created.ConfigJSON["model"]); got != "claude-sonnet-4-6" {
		t.Fatalf("expected model override, got %s", got)
	}

	updated, err := ResolveHookAgent(ctx, backend, 11, "/sources/github/prs", &created.ID, &AgentConfigPatch{
		SystemPromptMode: strPtr("replace"),
	})
	if err != nil {
		t.Fatalf("resolve update: %v", err)
	}
	if updated.ID != created.ID {
		t.Fatalf("expected same agent id, got %s vs %s", updated.ID, created.ID)
	}
	if got := fmt.Sprintf("%v", updated.ConfigJSON["system_prompt_mode"]); got != "replace" {
		t.Fatalf("expected system_prompt_mode=replace, got %s", got)
	}
}

func TestDefaultHookAgentName_StripsSeparators(t *testing.T) {
	got := defaultHookAgentName("/sources/github/airstore-prs_updates")
	if got != "Airstore Prs Updates" {
		t.Fatalf("unexpected default hook name: %q", got)
	}
}

func TestResolveHookAgent_CreateNameConflictReturnsFriendlyError(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	backend.createErr = fmt.Errorf(
		`create agent profile: pq: duplicate key value violates unique constraint "uq_agent_profile_workspace_name"`,
	)

	_, err := ResolveHookAgent(ctx, backend, 11, "/sources/github/prs", nil, &AgentConfigPatch{
		Name: strPtr("PR Reviewer"),
	})
	if err == nil {
		t.Fatal("expected conflict error")
	}
	if !strings.Contains(err.Error(), `agent name "PR Reviewer" is already in use`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestResolveHookAgent_UpdateNameConflictReturnsFriendlyError(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	created, err := ResolveHookAgent(ctx, backend, 11, "/sources/github/prs", nil, nil)
	if err != nil {
		t.Fatalf("seed hook agent: %v", err)
	}

	backend.updateErr = fmt.Errorf(
		`update agent profile: pq: duplicate key value violates unique constraint "uq_agent_profile_workspace_name"`,
	)
	_, err = ResolveHookAgent(ctx, backend, 11, "/sources/github/prs", &created.ID, &AgentConfigPatch{
		Name: strPtr("Conflicting Name"),
	})
	if err == nil {
		t.Fatal("expected update conflict error")
	}
	if !strings.Contains(err.Error(), `agent name "Conflicting Name" is already in use`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServiceCreate_AttachesAgentAndSkillPaths(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	store := repository.NewMemoryFilesystemStore()
	svc := &Service{Store: store, Backend: backend}

	hook, err := svc.Create(
		ctx,
		99,
		nil,
		nil,
		"token",
		"/sources/gmail/inbox",
		"triage new emails",
		[]string{"/skills/email-triage", "/skills/email-triage", "  "},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create hook: %v", err)
	}
	if hook.AgentId == nil || *hook.AgentId == "" {
		t.Fatal("expected hook agent id to be set")
	}
	if len(hook.SkillPaths) != 1 || hook.SkillPaths[0] != "/skills/email-triage" {
		t.Fatalf("unexpected skill paths: %#v", hook.SkillPaths)
	}
	if hook.SkillPath != "/skills/email-triage" {
		t.Fatalf("unexpected legacy skill path: %s", hook.SkillPath)
	}
}

func TestServiceDelete_DeletesAssociatedAgent(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	store := repository.NewMemoryFilesystemStore()
	svc := &Service{Store: store, Backend: backend}

	created, err := svc.Create(
		ctx,
		7,
		nil,
		nil,
		"token",
		"/sources/notion/updates",
		"review updates",
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create hook: %v", err)
	}
	if created.AgentId == nil || *created.AgentId == "" {
		t.Fatal("expected hook to include agent")
	}

	if err := svc.Delete(ctx, created.ExternalId); err != nil {
		t.Fatalf("delete hook: %v", err)
	}

	deletedHook, err := store.GetHook(ctx, created.ExternalId)
	if err != nil {
		t.Fatalf("get deleted hook: %v", err)
	}
	if deletedHook != nil {
		t.Fatal("expected hook to be deleted")
	}

	_, err = backend.GetAgentProfile(ctx, created.WorkspaceId, *created.AgentId)
	if err == nil {
		t.Fatal("expected associated agent to be deleted")
	}
	if _, ok := err.(*types.ErrAgentProfileNotFound); !ok {
		t.Fatalf("expected ErrAgentProfileNotFound, got %T (%v)", err, err)
	}
}

func TestServiceDelete_DoesNotDeleteSharedAgent(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	store := repository.NewMemoryFilesystemStore()
	svc := &Service{Store: store, Backend: backend}

	first, err := svc.Create(
		ctx,
		8,
		nil,
		nil,
		"token",
		"/sources/github/repo-a",
		"review repo a",
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create first hook: %v", err)
	}
	if first.AgentId == nil || *first.AgentId == "" {
		t.Fatal("expected first hook to include agent")
	}

	second := &types.Hook{
		WorkspaceId: first.WorkspaceId,
		Path:        "/sources/github/repo-b",
		Prompt:      "review repo b",
		AgentId:     first.AgentId,
		Active:      true,
	}
	if _, err := store.CreateHook(ctx, second); err != nil {
		t.Fatalf("create second hook: %v", err)
	}

	if err := svc.Delete(ctx, first.ExternalId); err != nil {
		t.Fatalf("delete first hook: %v", err)
	}

	profile, err := backend.GetAgentProfile(ctx, first.WorkspaceId, *first.AgentId)
	if err != nil {
		t.Fatalf("expected shared agent to remain, got error: %v", err)
	}
	if profile == nil || profile.ID != *first.AgentId {
		t.Fatalf("expected shared agent %s to remain", *first.AgentId)
	}
}

func TestServiceCreate_FailedCreateDoesNotMutateExistingAgent(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	store := &failOnSecondCreateStore{FilesystemStore: repository.NewMemoryFilesystemStore()}
	svc := &Service{Store: store, Backend: backend}

	first, err := svc.Create(
		ctx,
		13,
		nil,
		nil,
		"token",
		"/sources/github/repo-prs",
		"review prs",
		nil,
		nil,
		&AgentConfigPatch{Name: strPtr("Original Hook Agent")},
	)
	if err != nil {
		t.Fatalf("create initial hook: %v", err)
	}
	if first.AgentId == nil || *first.AgentId == "" {
		t.Fatal("expected initial hook to include agent")
	}

	_, err = svc.Create(
		ctx,
		13,
		nil,
		nil,
		"token",
		"/sources/github/repo-prs",
		"review prs again",
		nil,
		nil,
		&AgentConfigPatch{Name: strPtr("Mutated Hook Agent")},
	)
	if err == nil {
		t.Fatal("expected second hook create to fail")
	}

	profile, err := backend.GetAgentProfile(ctx, 13, *first.AgentId)
	if err != nil {
		t.Fatalf("get existing hook agent: %v", err)
	}
	if profile.Name != "Original Hook Agent" {
		t.Fatalf("expected existing hook agent name to remain unchanged, got %q", profile.Name)
	}
}

func TestServiceDeleteThenRecreate_RefiresSeenSnapshot(t *testing.T) {
	ctx := context.Background()
	backend := newFakeAgentBackend()
	store := repository.NewMemoryFilesystemStore()
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	tracker := NewSeenTracker(rdb)
	svc := &Service{Store: store, Backend: backend, Seen: tracker}

	path := "/sources/github/recreate-prs"
	workspaceID := uint(88)
	first, err := svc.Create(
		ctx,
		workspaceID,
		nil,
		nil,
		"token",
		path,
		"review prs",
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("create first hook: %v", err)
	}

	seenKey := common.Keys.HookSeen(workspaceID, types.GeneratePathID(NormalizePath(path)))
	ids := []string{"pr-1", "pr-2"}
	_, _ = tracker.Compare(ctx, seenKey, ids)
	if err := tracker.Commit(ctx, seenKey, ids); err != nil {
		t.Fatalf("commit baseline: %v", err)
	}
	result, err := tracker.Compare(ctx, seenKey, ids)
	if err != nil {
		t.Fatalf("compare baseline: %v", err)
	}
	if result != nil && len(result.Added) != 0 {
		t.Fatalf("expected no new IDs before delete/recreate, got %v", result.Added)
	}

	if err := svc.Delete(ctx, first.ExternalId); err != nil {
		t.Fatalf("delete first hook: %v", err)
	}
	if _, err := svc.Create(
		ctx,
		workspaceID,
		nil,
		nil,
		"token",
		path,
		"review prs again",
		nil,
		nil,
		nil,
	); err != nil {
		t.Fatalf("recreate hook: %v", err)
	}

	result, err = tracker.Compare(ctx, seenKey, ids)
	if err != nil {
		t.Fatalf("compare after recreate: %v", err)
	}
	if len(result.Added) != len(ids) {
		t.Fatalf("expected %d new IDs after recreate, got %d (%v)", len(ids), len(result.Added), result.Added)
	}
}
