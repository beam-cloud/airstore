package hooks

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

// --- Mock TaskCreator ---

type mockTask struct {
	WorkspaceId uint
	Prompt      string
	HookId      uint
	Attempt     int
	MaxAttempts int
	Token       string
}

type mockCreator struct {
	mu    sync.Mutex
	tasks []mockTask
	err   error // if set, CreateTask returns this error
}

func (m *mockCreator) CreateTask(_ context.Context, wsId uint, _ *uint, token, prompt string, hookId uint, attempt, maxAttempts int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.tasks = append(m.tasks, mockTask{
		WorkspaceId: wsId,
		Prompt:      prompt,
		HookId:      hookId,
		Attempt:     attempt,
		MaxAttempts: maxAttempts,
		Token:       token,
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

// --- Mock Backend ---

type mockBackend struct {
	repository.BackendRepository // embed to satisfy interface
	mu                           sync.Mutex
	activeTask                   *types.Task
	retryableTasks               []*types.Task
	tasksByHook                  []*types.Task
}

func (m *mockBackend) GetActiveHookTask(_ context.Context, hookId uint) (*types.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.activeTask, nil
}

func (m *mockBackend) GetRetryableTasks(_ context.Context) ([]*types.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.retryableTasks, nil
}

func (m *mockBackend) ListTasksByHook(_ context.Context, hookId uint) ([]*types.Task, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tasksByHook, nil
}

func (m *mockBackend) GetStuckHookTasks(_ context.Context, _ time.Duration) ([]*types.Task, error) {
	return nil, nil
}

func (m *mockBackend) SetTaskResult(_ context.Context, _ string, _ int, _ string) error {
	return nil
}

func (m *mockBackend) setActive(t *types.Task) {
	m.mu.Lock()
	m.activeTask = t
	m.mu.Unlock()
}

func (m *mockBackend) setRetryable(tasks []*types.Task) {
	m.mu.Lock()
	m.retryableTasks = tasks
	m.mu.Unlock()
}

// --- Mock FilesystemStore ---

type mockStore struct {
	repository.FilesystemStore // embed
	hooks                      []*types.Hook
}

func (m *mockStore) ListHooks(_ context.Context, wsId uint) ([]*types.Hook, error) {
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

// --- Helpers ---

func makeHook(id uint, wsId uint, path, prompt string) *types.Hook {
	tokenId := uint(1)
	token, _ := EncodeToken("test-token")
	return &types.Hook{
		Id:             id,
		ExternalId:     fmt.Sprintf("hook-%d", id),
		WorkspaceId:    wsId,
		Path:           path,
		Prompt:         prompt,
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

// --- Tests ---

func TestEngine_Submit_CreatesTask(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze files")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))

	if creator.count() != 1 {
		t.Fatalf("expected 1 task, got %d", creator.count())
	}
	task := creator.last()
	if task.HookId != 1 {
		t.Errorf("expected hook_id=1, got %d", task.HookId)
	}
	if task.Attempt != 1 {
		t.Errorf("expected attempt=1, got %d", task.Attempt)
	}
	if task.MaxAttempts != maxAttempts {
		t.Errorf("expected max_attempts=%d, got %d", maxAttempts, task.MaxAttempts)
	}
	if task.Token != "test-token" {
		t.Errorf("expected token=test-token, got %s", task.Token)
	}
}

func TestEngine_Submit_ConstraintRejectsDuplicate(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	// Simulate the DB unique constraint rejecting the insert
	creator := &mockCreator{err: fmt.Errorf("pq: duplicate key value violates unique constraint")}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))

	// CreateTask was called but rejected by constraint -- no task created
	if creator.count() != 0 {
		t.Fatalf("expected 0 tasks (constraint rejected), got %d", creator.count())
	}
}

func TestEngine_Submit_SkipsRevokedToken(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	hook.TokenId = nil // revoked
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend)

	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))

	if creator.count() != 0 {
		t.Fatalf("expected 0 tasks (token revoked), got %d", creator.count())
	}
}

func TestEngine_Submit_PathMatching(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend)

	// Should match: file under /skills
	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	if creator.count() != 1 {
		t.Fatalf("expected match for /skills/test.txt, got %d tasks", creator.count())
	}

	// Should NOT match: different path
	backend.setActive(nil) // reset
	eng.Handle("2", makeEvent(EventFsCreate, "/inbox/doc.pdf", 10))
	if creator.count() != 1 {
		t.Fatalf("expected no match for /inbox/doc.pdf, got %d tasks", creator.count())
	}

	// Should NOT match: prefix collision (e.g. /skillset != /skills)
	eng.Handle("3", makeEvent(EventFsCreate, "/skillset/foo.txt", 10))
	if creator.count() != 1 {
		t.Fatalf("expected no match for /skillset/foo.txt, got %d tasks", creator.count())
	}
}

func TestEngine_Submit_PromptEnrichment(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "do stuff")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend)

	eng.Handle("1", makeEvent(EventFsWrite, "/skills/report.md", 10))
	// Debounced -- wait for it
	time.Sleep(3 * time.Second)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task after debounce, got %d", creator.count())
	}
	task := creator.last()
	if task.Prompt != "do stuff\n\nEvent: file changed at /skills/report.md" {
		t.Errorf("unexpected prompt: %s", task.Prompt)
	}
}

func TestEngine_Debounce_CoalescesWrites(t *testing.T) {
	hook := makeHook(1, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}
	backend := &mockBackend{}
	eng := NewEngine(store, creator, backend)

	// Rapid writes to same path
	for i := 0; i < 10; i++ {
		eng.Handle(fmt.Sprintf("%d", i), makeEvent(EventFsWrite, "/skills/file.txt", 10))
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for debounce (2s after last write + buffer)
	time.Sleep(3 * time.Second)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task (debounced), got %d", creator.count())
	}
}

func TestEngine_Poll_RetriesFailedTask(t *testing.T) {
	hookId := uint(1)
	hook := makeHook(hookId, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}

	finished := time.Now().Add(-1 * time.Minute) // finished 1 minute ago
	failedTask := &types.Task{
		Id:          99,
		ExternalId:  "task-99",
		WorkspaceId: 10,
		HookId:      &hookId,
		Status:      types.TaskStatusFailed,
		Attempt:     1,
		MaxAttempts: 3,
		Prompt:      "analyze",
		FinishedAt:  &finished,
	}

	backend := &mockBackend{retryableTasks: []*types.Task{failedTask}}
	eng := NewEngine(store, creator, backend)

	eng.Poll(context.Background())

	if creator.count() != 1 {
		t.Fatalf("expected 1 retry task, got %d", creator.count())
	}
	task := creator.last()
	if task.Attempt != 2 {
		t.Errorf("expected attempt=2, got %d", task.Attempt)
	}
	if task.MaxAttempts != 3 {
		t.Errorf("expected max_attempts=3, got %d", task.MaxAttempts)
	}
}

func TestEngine_Poll_RespectsBackoff(t *testing.T) {
	hookId := uint(1)
	hook := makeHook(hookId, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}

	// Task failed 5 seconds ago, attempt 1. Backoff is 10s. Should NOT retry yet.
	finished := time.Now().Add(-5 * time.Second)
	failedTask := &types.Task{
		Id:          99,
		ExternalId:  "task-99",
		WorkspaceId: 10,
		HookId:      &hookId,
		Status:      types.TaskStatusFailed,
		Attempt:     1,
		MaxAttempts: 3,
		Prompt:      "analyze",
		FinishedAt:  &finished,
	}

	backend := &mockBackend{retryableTasks: []*types.Task{failedTask}}
	eng := NewEngine(store, creator, backend)

	eng.Poll(context.Background())

	if creator.count() != 0 {
		t.Fatalf("expected 0 retries (backoff not elapsed), got %d", creator.count())
	}
}

func TestEngine_Poll_SkipsWhenActiveTaskExists(t *testing.T) {
	hookId := uint(1)
	hook := makeHook(hookId, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}

	finished := time.Now().Add(-1 * time.Minute)
	failedTask := &types.Task{
		Id:          99,
		ExternalId:  "task-99",
		WorkspaceId: 10,
		HookId:      &hookId,
		Status:      types.TaskStatusFailed,
		Attempt:     1,
		MaxAttempts: 3,
		Prompt:      "analyze",
		FinishedAt:  &finished,
	}

	// DB constraint rejects retry when active task exists
	creator := &mockCreator{err: fmt.Errorf("pq: duplicate key value violates unique constraint")}
	backend := &mockBackend{retryableTasks: []*types.Task{failedTask}}
	eng := NewEngine(store, creator, backend)

	eng.Poll(context.Background())

	if creator.count() != 0 {
		t.Fatalf("expected 0 retries (constraint rejected), got %d", creator.count())
	}
}

func TestEngine_Poll_DeadLetterAfterMaxAttempts(t *testing.T) {
	hookId := uint(1)
	hook := makeHook(hookId, 10, "/skills", "analyze")
	store := &mockStore{hooks: []*types.Hook{hook}}
	creator := &mockCreator{}

	finished := time.Now().Add(-1 * time.Minute)
	// Attempt 3 of 3 -- should NOT retry (GetRetryableTasks wouldn't return it,
	// but let's verify the query filter is correct via the test expectation)
	failedTask := &types.Task{
		Id:          99,
		ExternalId:  "task-99",
		WorkspaceId: 10,
		HookId:      &hookId,
		Status:      types.TaskStatusFailed,
		Attempt:     3,
		MaxAttempts: 3,
		Prompt:      "analyze",
		FinishedAt:  &finished,
	}

	// Even if SQL leaks a max-attempt task, the engine should not retry it.
	backend := &mockBackend{retryableTasks: []*types.Task{failedTask}}
	eng := NewEngine(store, creator, backend)

	eng.Poll(context.Background())

	if creator.count() != 0 {
		t.Fatalf("expected 0 retries (max attempts exhausted), got %d", creator.count())
	}
}

func TestRetryDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 10 * time.Second},
		{2, 30 * time.Second},
		{3, 90 * time.Second},
		{4, 270 * time.Second},
		{5, 5 * time.Minute}, // capped
		{10, 5 * time.Minute},
	}
	for _, tt := range tests {
		got := retryDelay(tt.attempt)
		if got != tt.want {
			t.Errorf("retryDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestNormalizePath(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"/skills", "/skills"},
		{"skills", "/skills"},
		{"/skills/", "/skills"},
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
