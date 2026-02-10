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
	retryableTasks               []*types.Task
	tasksByHook                  []*types.Task
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

func (m *mockBackend) MarkTaskRetried(_ context.Context, _ string) error {
	return nil
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
	eng := NewEngine(store, creator, backend, nil)

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
	eng := NewEngine(store, creator, backend, nil)

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
	eng := NewEngine(store, creator, backend, nil)

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
	eng := NewEngine(store, creator, backend, nil)

	// Should match: file under /skills
	eng.Handle("1", makeEvent(EventFsCreate, "/skills/test.txt", 10))
	if creator.count() != 1 {
		t.Fatalf("expected match for /skills/test.txt, got %d tasks", creator.count())
	}

	// Should NOT match: different path
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
	eng := NewEngine(store, creator, backend, nil)

	eng.Handle("1", makeEvent(EventFsWrite, "/skills/report.md", 10))
	// Debounced -- wait for it
	time.Sleep(3 * time.Second)

	if creator.count() != 1 {
		t.Fatalf("expected 1 task after debounce, got %d", creator.count())
	}
	task := creator.last()
	// New structured prompt: trigger first, then user prompt
	expected := "## Trigger\n\nA file was modified at `skills/report.md`.\nRead the updated content from: `skills/report.md`\n\ndo stuff"
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
	eng := NewEngine(store, creator, backend, nil)

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
	eng := NewEngine(store, creator, backend, nil)

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
	eng := NewEngine(store, creator, backend, nil)

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
	eng := NewEngine(store, creator, backend, nil)

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
	want := "## Trigger\n\nA new file was created at `inbox/doc.pdf`.\nRead it from your working directory: `inbox/doc.pdf`"
	if got != want {
		t.Errorf("buildTriggerContext(FsCreate):\ngot:  %q\nwant: %q", got, want)
	}
}

func TestBuildTriggerContext_SourceChange(t *testing.T) {
	data := map[string]any{
		"event":        EventSourceChange,
		"workspace_id": "10",
		"path":         "/sources/gmail/inbox",
		"integration":  "gmail",
		"new_count":    "3",
		"new_items":    "msg-1, msg-2, msg-3",
	}
	got := buildTriggerContext(EventSourceChange, data)

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

func TestBuildTriggerContext_SourceChangeWithoutItems(t *testing.T) {
	data := map[string]any{
		"event":        EventSourceChange,
		"workspace_id": "10",
		"path":         "/sources/gmail/inbox",
		"integration":  "gmail",
		"new_count":    "5",
	}
	got := buildTriggerContext(EventSourceChange, data)

	if strings.Contains(got, "New items:") {
		t.Errorf("should not contain 'New items:' when no items, got: %s", got)
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
	meta := &skills.AirstoreSkillMeta{Writes: []string{"/memory/email-triage/", "/reports/"}}
	data := map[string]any{}
	got := buildSkillContext(meta, data)

	if !strings.Contains(got, "Write output to:") {
		t.Errorf("expected write paths, got: %q", got)
	}
	if !strings.Contains(got, "`memory/email-triage/`") {
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
      - /memory/email-triage/
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
		"event":        EventSourceChange,
		"workspace_id": "10",
		"path":         "/sources/gmail/inbox",
		"integration":  "gmail",
		"new_count":    "2",
		"new_items":    "msg-a, msg-b",
	}

	ctx := context.Background()
	prompt := eng.buildPrompt(ctx, hook, EventSourceChange, data)

	// Section 1: Trigger context
	if !strings.Contains(prompt, "## Trigger") {
		t.Error("prompt missing trigger section")
	}
	if !strings.Contains(prompt, "2 new item(s)") {
		t.Error("prompt missing new count")
	}
	if !strings.Contains(prompt, "New items: msg-a, msg-b") {
		t.Error("prompt missing new items")
	}

	// Section 2: Skill instructions
	if !strings.Contains(prompt, "Read all new emails and categorize them by urgency.") {
		t.Error("prompt missing skill instructions")
	}

	// Section 3: Skill context (write paths)
	if !strings.Contains(prompt, "Write output to:") {
		t.Error("prompt missing write paths from skill metadata")
	}

	// Section 4: Additional user prompt
	if !strings.Contains(prompt, "Also flag anything from VIPs.") {
		t.Error("prompt missing additional user prompt")
	}

	// Verify order: trigger comes before skill instructions
	triggerIdx := strings.Index(prompt, "## Trigger")
	skillIdx := strings.Index(prompt, "Read all new emails")
	userIdx := strings.Index(prompt, "Also flag anything")
	if triggerIdx >= skillIdx {
		t.Error("trigger should come before skill instructions")
	}
	if skillIdx >= userIdx {
		t.Error("skill instructions should come before user prompt")
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

	if !strings.Contains(prompt, "## Trigger") {
		t.Error("prompt missing trigger section")
	}
	if !strings.Contains(prompt, "process these files") {
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

	if !strings.Contains(prompt, "Summarize the document concisely.") {
		t.Error("prompt missing skill instructions")
	}
	// With empty user prompt, it should still have trigger + skill but no trailing empty section
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
		{types.PathMemory, true},

		// Root-level source folders - blocked (testing case-insensitive)
		{types.PathSources + "/gdrive", true},
		{"/sources/gdrive/", true},
		{types.PathSources + "/github", true},
		{types.PathSources + "/gmail", true},
		{"/sources/gmail/", true},

		// Smart query folders under sources - allowed
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
