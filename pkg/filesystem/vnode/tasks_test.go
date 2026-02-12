package vnode

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

func newTestTask(id string) *types.Task {
	return &types.Task{
		ExternalId: id,
		Status:     types.TaskStatusRunning,
		Prompt:     "test prompt",
		CreatedAt:  time.Unix(1739332800, 0).UTC(),
	}
}

func newTestTasksVNode(task *types.Task, logs string) *TasksVNode {
	return &TasksVNode{
		cachedTasks:  []*types.Task{task},
		cacheExpiry:  time.Now().Add(time.Minute),
		contentCache: make(map[string]*cachedTaskContent),
		logsFetcher: func(ctx context.Context, taskId string) string {
			return logs
		},
	}
}

func TestTasksVNode_GetattrUsesPlaceholderUntilContentCached(t *testing.T) {
	task := newTestTask("task-large")
	logs := strings.Repeat("x", (10<<20)+8192)
	logFetches := 0
	tv := &TasksVNode{
		cachedTasks:  []*types.Task{task},
		cacheExpiry:  time.Now().Add(time.Minute),
		contentCache: make(map[string]*cachedTaskContent),
		logsFetcher: func(ctx context.Context, taskId string) string {
			logFetches++
			return logs
		},
	}

	path := TasksPath + "/task-large.task"
	info, err := tv.Getattr(path)
	if err != nil {
		t.Fatalf("Getattr failed: %v", err)
	}
	if info.Size != 0 {
		t.Fatalf("expected uncached getattr size placeholder 0, got %d", info.Size)
	}
	if logFetches != 0 {
		t.Fatalf("expected getattr to avoid log fetches, got %d fetches", logFetches)
	}

	buf := make([]byte, len(logs)+4096)
	n, err := tv.Read(path, buf, 0, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if n <= 10<<20 {
		t.Fatalf("expected read length >10MB, got %d", n)
	}
	if logFetches != 1 {
		t.Fatalf("expected one log fetch after read, got %d", logFetches)
	}

	info, err = tv.Getattr(path)
	if err != nil {
		t.Fatalf("second getattr failed: %v", err)
	}
	if int64(n) != info.Size {
		t.Fatalf("expected cached getattr size %d, got %d", n, info.Size)
	}

	n, err = tv.Read(path, buf[:1], info.Size, 0)
	if err != nil {
		t.Fatalf("Read at EOF failed: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected EOF read to return 0 bytes, got %d", n)
	}
}

func TestTasksVNode_ReaddirSizeMatchesReadableContent(t *testing.T) {
	task := newTestTask("task-readdir")
	logs := strings.Repeat("y", (10<<20)+1024)
	tv := newTestTasksVNode(task, logs)

	entries, err := tv.Readdir(TasksPath)
	if err != nil {
		t.Fatalf("Readdir failed: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 task entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Name != "task-readdir.task" {
		t.Fatalf("unexpected entry name %q", entry.Name)
	}
	if entry.Size <= 10<<20 {
		t.Fatalf("expected readdir size >10MB, got %d", entry.Size)
	}

	path := TasksPath + "/" + entry.Name
	buf := make([]byte, int(entry.Size)+1)
	n, err := tv.Read(path, buf, 0, 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if int64(n) != entry.Size {
		t.Fatalf("expected read length %d to match readdir size, got %d", entry.Size, n)
	}
}

func TestTasksVNode_TaskContentCacheReusedAcrossOps(t *testing.T) {
	task := newTestTask("task-cache")
	logFetches := 0
	tv := &TasksVNode{
		cachedTasks:  []*types.Task{task},
		cacheExpiry:  time.Now().Add(time.Minute),
		contentCache: make(map[string]*cachedTaskContent),
		logsFetcher: func(ctx context.Context, taskId string) string {
			logFetches++
			return "cached logs\n"
		},
	}

	path := TasksPath + "/task-cache.task"
	if _, err := tv.Getattr(path); err != nil {
		t.Fatalf("Getattr failed: %v", err)
	}
	if _, err := tv.Readdir(TasksPath); err != nil {
		t.Fatalf("Readdir failed: %v", err)
	}
	buf := make([]byte, 4096)
	if _, err := tv.Read(path, buf, 0, 0); err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if logFetches != 1 {
		t.Fatalf("expected one log fetch due to content cache reuse, got %d", logFetches)
	}
}
