package hooks

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/skills"
	"github.com/beam-cloud/airstore/pkg/types"
)

// --- Mock TaskCreator ---

type mockTask struct {
	WorkspaceId uint
	Prompt      string
	HookId      uint
	EventID     string
	Event       string
}

type mockCreator struct {
	mu    sync.Mutex
	tasks []mockTask
	err   error // if set, CreateTask returns this error
}

func (m *mockCreator) CreateTask(_ context.Context, hook *types.Hook, eventID, event, prompt string, _ map[string]any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.tasks = append(m.tasks, mockTask{
		WorkspaceId: hook.WorkspaceId,
		Prompt:      prompt,
		HookId:      hook.Id,
		EventID:     eventID,
		Event:       event,
	})
	return nil
}

func (m *mockCreator) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.tasks)
}

func (m *mockCreator) last() mockTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasks[len(m.tasks)-1]
}

func (m *mockCreator) all() []mockTask {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]mockTask, len(m.tasks))
	copy(out, m.tasks)
	return out
}

// --- Mock Backend ---

type mockBackend struct {
	repository.BackendRepository // embed to satisfy interface
	mu                           sync.Mutex
	retryableTasks               []*types.RunExecution
	tasksByHook                  []*types.RunExecution
}

func (m *mockBackend) GetRetryableRunExecutions(_ context.Context) ([]*types.RunExecution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retryableTasks, nil
}

func (m *mockBackend) ListRunExecutionsByHook(_ context.Context, hookId uint) ([]*types.RunExecution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasksByHook, nil
}

func (m *mockBackend) GetStuckHookRunExecutions(_ context.Context, _ time.Duration) ([]*types.RunExecution, error) {
	return nil, nil
}

func (m *mockBackend) SetRunExecutionResult(_ context.Context, _ string, _ int, _ string) error {
	return nil
}

func (m *mockBackend) MarkRunExecutionRetried(_ context.Context, _ string) error {
	return nil
}

func (m *mockBackend) setRetryable(tasks []*types.RunExecution) {
	m.mu.Lock()
	m.retryableTasks = tasks
	m.mu.Unlock()
}

// --- Mock FilesystemStore ---

type mockStore struct {
	repository.FilesystemStore // embed
	hooks                      []*types.Hook
	updatedHooks               []*types.Hook
	existingPaths              map[string]bool
	statErr                    error
	listErrCount               int
}

func (m *mockStore) ListHooks(_ context.Context, wsId uint) ([]*types.Hook, error) {
	if m.listErrCount > 0 {
		m.listErrCount--
		return nil, fmt.Errorf("transient list hooks failure")
	}
	var out []*types.Hook
	for _, h := range m.hooks {
		if h.WorkspaceId == wsId {
			out = append(out, h)
		}
	}
	return out, nil
}

func (m *mockStore) GetHookById(_ context.Context, id uint) (*types.Hook, error) {
	for _, h := range m.hooks {
		if h.Id == id {
			return h, nil
		}
	}
	return nil, nil
}

func (m *mockStore) StatPath(_ context.Context, path string) (*types.DirMeta, *types.FileMeta, string, error) {
	if m.statErr != nil {
		return nil, nil, "", m.statErr
	}
	if m.existingPaths != nil && m.existingPaths[path] {
		return nil, &types.FileMeta{Path: path}, "", nil
	}
	return nil, nil, "", nil
}

func (m *mockStore) UpdateHook(_ context.Context, hook *types.Hook) error {
	if hook == nil {
		return nil
	}
	copied := *hook
	m.updatedHooks = append(m.updatedHooks, &copied)
	for idx, existing := range m.hooks {
		if existing != nil && existing.ExternalId == hook.ExternalId {
			m.hooks[idx] = &copied
			break
		}
	}
	return nil
}

// --- Helpers ---

func makeHook(id uint, wsId uint, path, prompt string) *types.Hook {
	tokenId := uint(1)
	token, _ := EncodeToken("test-token")
	agentID := fmt.Sprintf("agent-%d", id)
	return &types.Hook{
		Id:             id,
		ExternalId:     fmt.Sprintf("hook-%d", id),
		WorkspaceId:    wsId,
		Path:           path,
		Prompt:         prompt,
		AgentId:        &agentID,
		Active:         true,
		TokenId:        &tokenId,
		EncryptedToken: token,
	}
}

func makeEvent(event, path string, wsId uint) map[string]any {
	return map[string]any{
		"event":        event,
		"workspace_id": fmt.Sprintf("%d", wsId),
		"path":         path,
	}
}

func setShortDebounce(eng *Engine) time.Duration {
	delay := 40 * time.Millisecond
	eng.debounce.delay = delay
	return delay
}

func waitForDebounce(delay time.Duration) {
	time.Sleep(delay + 40*time.Millisecond)
}

// --- Tests ---

func TestEngine_Submit_CreatesTask(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze files")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task, got %d", creator.count())
	}
	task := creator.last()
	if task.HookId != 1 {
		t.Errorf("expected hook_id=1, got %d", task.HookId)
	}
	if task.Event != EventFsCreate {
		t.Errorf("expected event=%s, got %s", EventFsCreate, task.Event)
	}
}

func TestEngine_Submit_TaskInputOneShotDisablesHookAfterFire(t *testing.T) {
	targetTaskID := "task-123"
	hook := makeHook(1, 10, "/sources/gmail/thread-watch", "resume this task")
	hook.AgentId = nil
	hook.DeliveryMode = types.HookDeliveryModeTaskInput
	hook.TargetTaskID = &targetTaskID
	hook.OneShot = true

	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	event := map[string]any{
		"event":        EventFsCreate,
		"workspace_id": "10",
		"path":         "/sources/gmail/thread-watch",
		"integration":  "gmail",
		"new_count":    "2",
		"new_items":    "/sources/gmail/thread-watch/message.txt, /sources/gmail/thread-watch/report.pdf",
	}
	eng.Handle("1", event)
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 delivered task input, got %d", creator.count())
	}
	if hook.Active {
		t.Fatal("expected one-shot hook to be deactivated after firing")
	}
	if got := len(store.updatedHooks); got != 1 {
		t.Fatalf("expected 1 hook update, got %d", got)
	}
	if store.updatedHooks[0].Active {
		t.Fatal("expected persisted hook update to mark hook inactive")
	}
}

func TestEngine_Submit_ConstraintRejectsDuplicate(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	// Simulate the DB unique constraint rejecting the insert
	creator := &mockCreator{err: fmt.Errorf("pq: duplicate key value violates unique constraint")}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	waitForDebounce(delay)

	// CreateTask was called but rejected by constraint -- no task created
	if creator.count() != 0 {
		t.Fatalf("expected 0 tasks (constraint rejected), got %d", creator.count())
	}
}

func TestEngine_Submit_DoesNotRequireToken(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	hook.TokenId = nil
	hook.EncryptedToken = nil
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task without token requirement, got %d", creator.count())
	}
}

func TestEngine_Submit_PathMatching(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	// Should match: file under /skills
	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	waitForDebounce(delay)
	if creator.count() != 1 {
		t.Fatalf("expected match for /skills/test.txt, got %d tasks", creator.count())
	}

	// Should NOT match: different path
	eng.Handle("2", makeEvent(EventFsCreate, "/inbox/doc.pdf", 10))
	waitForDebounce(delay)
	if creator.count() != 1 {
		t.Fatalf("expected no match for /inbox/doc.pdf, got %d tasks", creator.count())
	}

	// Should NOT match: prefix collision (e.g. /skillset != /skills)
	eng.Handle("3", makeEvent(EventFsCreate, "/skillset/foo.txt", 10))
	waitForDebounce(delay)
	if creator.count() != 1 {
		t.Fatalf("expected no match for /skillset/foo.txt, got %d tasks", creator.count())
	}
}

func TestEngine_Submit_PathMatching_NormalizesTrailingSlashHookPath(t *testing.T) {
	hook := makeHook(1, 10, "/skills/", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	waitForDebounce(delay)
	if creator.count() != 1 {
		t.Fatalf("expected trailing-slash hook path to match, got %d tasks", creator.count())
	}
}

func TestEngine_Submit_PathMatching_RootHookMatchesAllPaths(t *testing.T) {
	hook := makeHook(1, 10, "/", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsWrite, "/pdfs/file.pdf", 10))
	waitForDebounce(delay)
	if creator.count() != 1 {
		t.Fatalf("expected root hook to match any path, got %d tasks", creator.count())
	}
}

func TestEngine_CacheLoad_RetriesAfterTransientListFailure(t *testing.T) {
	hook := makeHook(1, 10, "/pdfs", "analyze")
	store := &mockStore{
		hooks:        []*types.Hook{hook},
		listErrCount: 1, // fail first load, then succeed
	}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsWrite, "/pdfs/file-a.pdf", 10))
	waitForDebounce(delay)
	if creator.count() != 0 {
		t.Fatalf("expected first event to miss due transient load error, got %d tasks", creator.count())
	}

	eng.Handle("2", makeEvent(EventFsWrite, "/pdfs/file-b.pdf", 10))
	waitForDebounce(delay)
	if creator.count() != 1 {
		t.Fatalf("expected second event to load hooks and create task, got %d tasks", creator.count())
	}
}

func TestEngine_Submit_PromptEnrichment(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "do stuff")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsWrite, "/skills/report.md", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task after debounce, got %d", creator.count())
	}
	task := creator.last()
	// New structured prompt: trigger first, then user prompt
	expected := "A file was modified at `/workspace/skills/report.md`.\n\nTask: do stuff"
	if task.Prompt != expected {
		t.Errorf("unexpected prompt:\ngot:  %q\nwant: %q", task.Prompt, expected)
	}
}

func TestEngine_Debounce_CoalescesWrites(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	// Rapid writes to same path
	for i := 0; i < 10; i++ {
		eng.Handle(fmt.Sprintf("%d", i), makeEvent(EventFsWrite, "/skills/file.txt", 10))
		time.Sleep(5 * time.Millisecond)
	}

	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task (debounced), got %d", creator.count())
	}
}

func TestEngine_Debounce_CoalescesDeleteThenWriteAsWrite(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsDelete, "/skills/file.txt", 10))
	eng.Handle("2", makeEvent(EventFsWrite, "/skills/file.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 coalesced task, got %d", creator.count())
	}
	if task := creator.last(); task.Event != EventFsWrite {
		t.Fatalf("expected final event %s, got %s", EventFsWrite, task.Event)
	}
}

func TestEngine_Debounce_CoalescesCreateThenWriteAsCreate(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/file.txt", 10))
	eng.Handle("2", makeEvent(EventFsWrite, "/skills/file.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 coalesced task, got %d", creator.count())
	}
	if task := creator.last(); task.Event != EventFsCreate {
		t.Fatalf("expected final event %s, got %s", EventFsCreate, task.Event)
	}
}

func TestEngine_Debounce_CoalescesWriteThenDeleteAsWrite(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsWrite, "/skills/file.txt", 10))
	eng.Handle("2", makeEvent(EventFsDelete, "/skills/file.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 coalesced task, got %d", creator.count())
	}
	if task := creator.last(); task.Event != EventFsWrite {
		t.Fatalf("expected final event %s, got %s", EventFsWrite, task.Event)
	}
}

func TestEngine_Debounce_DeleteOnlyRemainsDelete(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsDelete, "/skills/file.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 coalesced task, got %d", creator.count())
	}
	if task := creator.last(); task.Event != EventFsDelete {
		t.Fatalf("expected final event %s, got %s", EventFsDelete, task.Event)
	}
}

func TestEngine_Debounce_DeleteOnlyWithExistingPathResolvesAsWrite(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{
		hooks:         []*types.Hook{hook},
		existingPaths: map[string]bool{"/skills/file.txt": true},
	}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	eng.Handle("1", makeEvent(EventFsDelete, "/skills/file.txt", 10))
	waitForDebounce(delay)

	if creator.count() != 1 {
		t.Fatalf("expected 1 coalesced task, got %d", creator.count())
	}
	if task := creator.last(); task.Event != EventFsWrite {
		t.Fatalf("expected final event %s, got %s", EventFsWrite, task.Event)
	}
}

func TestEngine_MoveEvents_FireSourceAndDestinationHooks(t *testing.T) {
	sourceHook := makeHook(1, 10, "/inbox", "handle source delete")
	destHook := makeHook(2, 10, "/pdfs", "handle destination write")
	store := &mockStore{hooks: []*types.Hook{sourceHook, destHook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	delay := setShortDebounce(eng)

	deleteEvent := makeEvent(EventFsDelete, "/inbox/UHC_letter_jan12026.pdf", 10)
	deleteEvent["old_path"] = "/inbox/UHC_letter_jan12026.pdf"
	deleteEvent["new_path"] = "/pdfs/UHC_letter_jan12026.pdf"
	deleteEvent["move_op_id"] = "mv-test-1"
	writeEvent := makeEvent(EventFsWrite, "/pdfs/UHC_letter_jan12026.pdf", 10)
	writeEvent["old_path"] = "/inbox/UHC_letter_jan12026.pdf"
	writeEvent["new_path"] = "/pdfs/UHC_letter_jan12026.pdf"
	writeEvent["move_op_id"] = "mv-test-1"

	eng.Handle("1", deleteEvent)
	eng.Handle("2", writeEvent)
	waitForDebounce(delay)

	if creator.count() != 2 {
		t.Fatalf("expected 2 tasks (source+destination), got %d", creator.count())
	}

	var sourceDelete, destWrite bool
	for _, task := range creator.all() {
		if task.HookId == sourceHook.Id && task.Event == EventFsDelete {
			sourceDelete = true
		}
		if task.HookId == destHook.Id && task.Event == EventFsWrite {
			destWrite = true
		}
	}
	if !sourceDelete {
		t.Fatalf("expected source hook %d to receive %s event", sourceHook.Id, EventFsDelete)
	}
	if !destWrite {
		t.Fatalf("expected destination hook %d to receive %s event", destHook.Id, EventFsWrite)
	}
}

func TestEngine_Poll_NoOp(t *testing.T) {
	store := &mockStore{}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)
	eng.Poll(context.Background())
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{types.PathSkills, types.PathSkills},
		{types.DirNameSkills, types.PathSkills},
		{"/skills/", "/skills"}, // NormalizePath only trims slash, doesn't change case
		{"skills/", "/skills"},
		{"/", "/"},
		{"", "/"},
	}
	for _, tt := range tests {
		got := NormalizePath(tt.in)
		if got != tt.want {
			t.Errorf("NormalizePath(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParseUint(t *testing.T) {
	tests := []struct {
		in   any
		want uint
	}{
		{float64(42), 42},
		{int(10), 10},
		{int64(99), 99},
		{uint(7), 7},
		{"123", 123},
		{"", 0},
		{nil, 0},
		{true, 0},
	}
	for _, tt := range tests {
		got := ParseUint(tt.in)
		if got != tt.want {
			t.Errorf("ParseUint(%v) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestEncodeDecodeToken(t *testing.T) {
	raw := "my-secret-token"
	encoded, err := EncodeToken(raw)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeToken(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if decoded != raw {
		t.Errorf("roundtrip failed: got %q, want %q", decoded, raw)
	}
}

func TestDecodeToken_Empty(t *testing.T) {
	_, err := DecodeToken(nil)
	if err == nil {
		t.Error("expected error for empty token")
	}
	_, err = DecodeToken([]byte{})
	if err == nil {
		t.Error("expected error for empty token")
	}
}

// --- Mock SkillReader ---

type mockSkillReader struct {
	content string
	err     error
}

func (m *mockSkillReader) ReadSkillContent(_ context.Context, _ uint, _ string) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	return m.content, nil
}

// --- Prompt construction tests ---

func TestBuildTriggerContext_FsCreate(t *testing.T) {
	data := makeEvent(EventFsCreate, "/inbox/doc.pdf", 10)
	got := buildTriggerContext(EventFsCreate, data)
	want := "A new file was created at `/workspace/inbox/doc.pdf`.\nRead it from: `/workspace/inbox/doc.pdf`"
	if got != want {
		t.Errorf("buildTriggerContext(FsCreate):\ngot:  %q\nwant: %q", got, want)
	}
}

func TestBuildTriggerContext_SourceCreate(t *testing.T) {
	data := map[string]any{
		"event":        EventFsCreate,
		"workspace_id": "10",
		"path":         "/sources/gmail/inbox",
		"integration":  "gmail",
		"new_count":    "3",
		"new_items":    "msg-1, msg-2, msg-3",
	}
	got := buildTriggerContext(EventFsCreate, data)

	if !strings.Contains(got, "Source: **gmail**") {
		t.Errorf("expected integration in trigger, got: %s", got)
	}
	if !strings.Contains(got, "3 new item(s)") {
		t.Errorf("expected new count in trigger, got: %s", got)
	}
	if !strings.Contains(got, "New items: msg-1, msg-2, msg-3") {
		t.Errorf("expected new items in trigger, got: %s", got)
	}
}

func TestBuildTriggerContext_SourceCreateWithoutItems(t *testing.T) {
	data := map[string]any{
		"event":        EventFsCreate,
		"workspace_id": "10",
		"path":         "/sources/gmail/inbox",
		"integration":  "gmail",
		"new_count":    "5",
	}
	got := buildTriggerContext(EventFsCreate, data)

	if strings.Contains(got, "New items:") {
		t.Errorf("should not contain 'New items:' when no items, got: %s", got)
	}
}

func TestBuildTriggerContext_SourceDelete(t *testing.T) {
	data := map[string]any{
		"event":         EventFsDelete,
		"workspace_id":  "10",
		"path":          "/sources/linear/issues",
		"integration":   "linear",
		"removed_count": "2",
		"removed_items": "issue-1, issue-2",
	}
	got := buildTriggerContext(EventFsDelete, data)

	if !strings.Contains(got, "Source: **linear**") {
		t.Errorf("expected integration in trigger, got: %s", got)
	}
	if !strings.Contains(got, "2 item(s) were removed") {
		t.Errorf("expected removed count in trigger, got: %s", got)
	}
	if !strings.Contains(got, "Removed items: issue-1, issue-2") {
		t.Errorf("expected removed items in trigger, got: %s", got)
	}
}

func TestBuildTriggerContext_UnknownEvent(t *testing.T) {
	data := makeEvent("unknown.event", "/some/path", 10)
	got := buildTriggerContext("unknown.event", data)
	if got != "" {
		t.Errorf("expected empty string for unknown event, got: %q", got)
	}
}

func TestBuildSkillContext_NilMeta(t *testing.T) {
	got := buildSkillContext(nil, nil)
	if got != "" {
		t.Errorf("expected empty for nil meta, got: %q", got)
	}
}

func TestBuildSkillContext_MismatchedIntegration(t *testing.T) {
	meta := &skills.AirstoreSkillMeta{Needs: []string{"gmail"}}
	data := map[string]any{"integration": "gdrive"}
	got := buildSkillContext(meta, data)

	if !strings.Contains(got, "designed for gmail") {
		t.Errorf("expected mismatch warning, got: %q", got)
	}
	if !strings.Contains(got, "triggered by gdrive") {
		t.Errorf("expected triggered-by info, got: %q", got)
	}
}

func TestBuildSkillContext_MatchedIntegration(t *testing.T) {
	meta := &skills.AirstoreSkillMeta{Needs: []string{"gmail"}}
	data := map[string]any{"integration": "gmail"}
	got := buildSkillContext(meta, data)

	if strings.Contains(got, "designed for") {
		t.Errorf("should not warn when integration matches, got: %q", got)
	}
}

func TestBuildSkillContext_WritePaths(t *testing.T) {
	meta := &skills.AirstoreSkillMeta{Writes: []string{"/reports/email-triage/", "/reports/"}}
	data := map[string]any{}
	got := buildSkillContext(meta, data)

	if !strings.Contains(got, "Write output to:") {
		t.Errorf("expected write paths, got: %q", got)
	}
	if !strings.Contains(got, "`reports/email-triage/`") {
		t.Errorf("expected relative path, got: %q", got)
	}
}

func TestBuildPrompt_FullStructure(t *testing.T) {
	skillContent := `---
name: email-triage
description: Categorize emails by urgency.
metadata:
  airstore:
    needs:
      - gmail
    writes:
      - /reports/email-triage/
---

Read all new emails and categorize them by urgency.
`
	reader := &mockSkillReader{content: skillContent}
	hook := makeHook(1, 10, "/sources/gmail/inbox", "Also flag anything from VIPs.")
	hook.SkillPath = "/skills/email-triage"
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, reader)

	data := map[string]any{
		"event":        EventFsCreate,
		"workspace_id": "10",
		"path":         "/sources/gmail/inbox",
		"integration":  "gmail",
		"new_count":    "2",
		"new_items":    "msg-a, msg-b",
	}

	ctx := context.Background()
	prompt := eng.buildPrompt(ctx, hook, EventFsCreate, data)

	// Section 1: Trigger context
	if !strings.Contains(prompt, "2 new item(s)") {
		t.Error("prompt missing trigger section")
	}
	if !strings.Contains(prompt, "New items: msg-a, msg-b") {
		t.Error("prompt missing new items")
	}

	// Section 2: Skill references (name + path, not full content)
	if !strings.Contains(prompt, "## Skills") {
		t.Error("prompt missing skills section")
	}
	if !strings.Contains(prompt, "**email-triage**") {
		t.Error("prompt missing skill name reference")
	}
	if !strings.Contains(prompt, "/workspace/skills/email-triage/SKILL.md") {
		t.Error("prompt missing skill path reference")
	}
	if strings.Contains(prompt, "Read all new emails and categorize them by urgency.") {
		t.Error("prompt should NOT contain full skill instructions — only a reference")
	}

	// Section 3: Additional user prompt
	if !strings.Contains(prompt, "Task: Also flag anything from VIPs.") {
		t.Error("prompt missing additional user prompt")
	}

	// Verify order: trigger comes before skill references
	triggerIdx := strings.Index(prompt, "2 new item(s)")
	skillIdx := strings.Index(prompt, "## Skills")
	userIdx := strings.Index(prompt, "Task: Also flag anything")
	if triggerIdx >= skillIdx {
		t.Error("trigger should come before skill references")
	}
	if skillIdx >= userIdx {
		t.Error("skill references should come before user prompt")
	}
}

func TestBuildPrompt_NoSkill(t *testing.T) {
	hook := makeHook(1, 10, "/inbox", "process these files")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, nil)

	data := makeEvent(EventFsCreate, "/inbox/report.pdf", 10)
	ctx := context.Background()
	prompt := eng.buildPrompt(ctx, hook, EventFsCreate, data)

	if !strings.Contains(prompt, "A new file was created at `/workspace/inbox/report.pdf`.") {
		t.Error("prompt missing trigger section")
	}
	if !strings.Contains(prompt, "Task: process these files") {
		t.Error("prompt missing user prompt")
	}
}

func TestBuildPrompt_SkillOnly_NoAdditionalPrompt(t *testing.T) {
	skillContent := `---
name: summarizer
description: Summarize documents.
---

Summarize the document concisely.
`
	reader := &mockSkillReader{content: skillContent}
	hook := makeHook(1, 10, "/inbox", "")
	hook.SkillPath = types.PathSkills + "/summarizer"
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend, reader)

	data := makeEvent(EventFsCreate, "/inbox/report.pdf", 10)
	ctx := context.Background()
	prompt := eng.buildPrompt(ctx, hook, EventFsCreate, data)

	if !strings.Contains(prompt, "**summarizer**") {
		t.Error("prompt missing skill name reference")
	}
	if !strings.Contains(prompt, "/workspace/skills/summarizer/SKILL.md") {
		t.Error("prompt missing skill path reference")
	}
	if strings.Contains(prompt, "Summarize the document concisely.") {
		t.Error("prompt should NOT contain full skill instructions")
	}
	parts := strings.Split(prompt, "\n\n")
	lastPart := parts[len(parts)-1]
	if lastPart == "" {
		t.Error("prompt should not end with empty section")
	}
}

func TestValidateHookPath(t *testing.T) {
	tests := []struct {
		path    string
		wantErr bool
	}{
		// Blocked system root directories (testing both cases)
		{types.PathTasks, true},
		{"/tasks/", true}, // lowercase also blocked (case-insensitive)
		{types.PathTools, true},
		{types.PathSkills, true},
		{types.PathSources, true},
		{"/sources/", true}, // lowercase also blocked (case-insensitive)
		// Root-level source folders - blocked (testing case-insensitive)
		{types.PathSources + "/gdrive", true},
		{"/sources/gdrive/", true},
		{types.PathSources + "/github", true},
		{types.PathSources + "/gmail", true},
		{"/sources/gmail/", true},

		// Source view folders under sources - allowed
		{types.PathSources + "/gdrive/invoices", false},
		{"/sources/gdrive/invoices/", false},
		{types.PathSources + "/gmail/new unread emails", false},
		{"/sources/gmail/my-query", false},

		// Top-level query paths - allowed
		{"/emails", false},
		{"/my-query", false},
		{"/invoices", false},
	}

	for _, tt := range tests {
		err := ValidateHookPath(tt.path)
		if (err != nil) != tt.wantErr {
			t.Errorf("ValidateHookPath(%q) error = %v, wantErr = %v", tt.path, err, tt.wantErr)
		}
	}
}
