package index

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSQLiteIndexStore_BasicOperations(t *testing.T) {
	store, err := NewSQLiteIndexStore(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Test Upsert
	entry := &IndexEntry{
		Integration: "gmail",
		EntityID:    "msg123",
		Path:        "messages/inbox/test/body.txt",
		ParentPath:  "messages/inbox/test",
		Name:        "body.txt",
		Type:        EntryTypeFile,
		Size:        100,
		ModTime:     time.Now(),
		Title:       "Test Email Subject",
		Body:        "This is the email body content with some searchable text.",
		Metadata:    `{"from":"test@example.com"}`,
	}

	if err := store.Upsert(ctx, entry); err != nil {
		t.Fatalf("failed to upsert: %v", err)
	}

	// Test Get
	got, err := store.Get(ctx, "gmail", "msg123")
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}
	if got == nil {
		t.Fatal("expected entry, got nil")
	}
	if got.Title != "Test Email Subject" {
		t.Errorf("expected title 'Test Email Subject', got '%s'", got.Title)
	}

	// Test GetByPath
	got, err = store.GetByPath(ctx, "messages/inbox/test/body.txt")
	if err != nil {
		t.Fatalf("failed to get by path: %v", err)
	}
	if got == nil {
		t.Fatal("expected entry, got nil")
	}

	// Test Search
	results, err := store.Search(ctx, "gmail", "searchable")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}

	// Test Delete
	if err := store.Delete(ctx, "gmail", "msg123"); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	got, err = store.Get(ctx, "gmail", "msg123")
	if err != nil {
		t.Fatalf("failed to get after delete: %v", err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestSQLiteIndexStore_List(t *testing.T) {
	store, err := NewSQLiteIndexStore(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert multiple entries
	entries := []*IndexEntry{
		{Integration: "gmail", EntityID: "msg1", Path: "messages/inbox/email1", ParentPath: "messages/inbox", Name: "email1", Type: EntryTypeDir, Title: "Email 1"},
		{Integration: "gmail", EntityID: "msg2", Path: "messages/inbox/email2", ParentPath: "messages/inbox", Name: "email2", Type: EntryTypeDir, Title: "Email 2"},
		{Integration: "gmail", EntityID: "msg3", Path: "messages/sent/email3", ParentPath: "messages/sent", Name: "email3", Type: EntryTypeDir, Title: "Email 3"},
	}

	for _, e := range entries {
		e.ModTime = time.Now()
		if err := store.Upsert(ctx, e); err != nil {
			t.Fatalf("failed to upsert: %v", err)
		}
	}

	// Test List
	results, err := store.List(ctx, "gmail", "messages/inbox")
	if err != nil {
		t.Fatalf("failed to list: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
}

func TestSQLiteIndexStore_FullTextSearch(t *testing.T) {
	store, err := NewSQLiteIndexStore(":memory:")
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert entries with searchable content
	entries := []*IndexEntry{
		{Integration: "gmail", EntityID: "msg1", Path: "e1", ParentPath: "", Name: "e1", Type: EntryTypeFile, Title: "Meeting tomorrow", Body: "Let's discuss the quarterly budget report."},
		{Integration: "gmail", EntityID: "msg2", Path: "e2", ParentPath: "", Name: "e2", Type: EntryTypeFile, Title: "Project Update", Body: "The software deployment is complete."},
		{Integration: "gmail", EntityID: "msg3", Path: "e3", ParentPath: "", Name: "e3", Type: EntryTypeFile, Title: "Budget Review", Body: "Please review the attached quarterly report."},
	}

	for _, e := range entries {
		e.ModTime = time.Now()
		if err := store.Upsert(ctx, e); err != nil {
			t.Fatalf("failed to upsert: %v", err)
		}
	}

	// Search for "quarterly"
	results, err := store.Search(ctx, "gmail", "quarterly")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 2 {
		t.Errorf("expected 2 results for 'quarterly', got %d", len(results))
	}

	// Search for "deployment"
	results, err = store.Search(ctx, "gmail", "deployment")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'deployment', got %d", len(results))
	}

	// Search for "meeting"
	results, err = store.Search(ctx, "gmail", "meeting")
	if err != nil {
		t.Fatalf("failed to search: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result for 'meeting', got %d", len(results))
	}
}

func BenchmarkSQLiteIndexStore_Search(b *testing.B) {
	store, err := NewSQLiteIndexStore(":memory:")
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert 1000 entries
	for i := 0; i < 1000; i++ {
		entry := &IndexEntry{
			Integration: "gmail",
			EntityID:    fmt.Sprintf("msg%d", i),
			Path:        fmt.Sprintf("messages/inbox/email%d/body.txt", i),
			ParentPath:  fmt.Sprintf("messages/inbox/email%d", i),
			Name:        "body.txt",
			Type:        EntryTypeFile,
			Size:        int64(100 + i%500),
			ModTime:     time.Now(),
			Title:       fmt.Sprintf("Email Subject %d", i),
			Body:        fmt.Sprintf("This is email %d content. Some emails mention quarterly reports, others discuss deployments.", i),
		}
		if i%10 == 0 {
			entry.Body = "This email contains information about the quarterly budget review."
		}
		if err := store.Upsert(ctx, entry); err != nil {
			b.Fatalf("failed to upsert: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.Search(ctx, "gmail", "quarterly")
		if err != nil {
			b.Fatalf("failed to search: %v", err)
		}
	}
}

func BenchmarkSQLiteIndexStore_List(b *testing.B) {
	store, err := NewSQLiteIndexStore(":memory:")
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert 1000 entries
	for i := 0; i < 1000; i++ {
		entry := &IndexEntry{
			Integration: "gmail",
			EntityID:    fmt.Sprintf("msg%d", i),
			Path:        fmt.Sprintf("messages/inbox/email%d", i),
			ParentPath:  "messages/inbox",
			Name:        fmt.Sprintf("email%d", i),
			Type:        EntryTypeDir,
			ModTime:     time.Now(),
			Title:       fmt.Sprintf("Email %d", i),
		}
		if err := store.Upsert(ctx, entry); err != nil {
			b.Fatalf("failed to upsert: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := store.List(ctx, "gmail", "messages/inbox")
		if err != nil {
			b.Fatalf("failed to list: %v", err)
		}
	}
}

func BenchmarkSQLiteIndexStore_Read(b *testing.B) {
	store, err := NewSQLiteIndexStore(":memory:")
	if err != nil {
		b.Fatalf("failed to create store: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Insert entries
	for i := 0; i < 100; i++ {
		entry := &IndexEntry{
			Integration: "gmail",
			EntityID:    fmt.Sprintf("msg%d", i),
			Path:        fmt.Sprintf("messages/inbox/email%d/body.txt", i),
			ParentPath:  fmt.Sprintf("messages/inbox/email%d", i),
			Name:        "body.txt",
			Type:        EntryTypeFile,
			Size:        5000,
			ModTime:     time.Now(),
			Title:       fmt.Sprintf("Email %d", i),
			Body:        fmt.Sprintf("This is the body content of email %d. It contains some longer text to simulate real email content that would be read during a grep operation.", i),
		}
		if err := store.Upsert(ctx, entry); err != nil {
			b.Fatalf("failed to upsert: %v", err)
		}
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("messages/inbox/email%d/body.txt", i%100)
		_, err := store.GetByPath(ctx, path)
		if err != nil {
			b.Fatalf("failed to get: %v", err)
		}
	}
}
