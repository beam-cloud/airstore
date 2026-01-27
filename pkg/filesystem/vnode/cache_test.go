package vnode

import (
	"testing"
	"time"
)

func TestMetadataCache_SetGet(t *testing.T) {
	c := NewMetadataCache()

	info := &FileInfo{Ino: 123, Size: 100, Mode: 0644}
	c.Set("/test/file", info)

	got := c.GetInfo("/test/file")
	if got == nil {
		t.Fatal("expected cached info, got nil")
	}
	if got.Ino != 123 {
		t.Errorf("expected ino 123, got %d", got.Ino)
	}
}

func TestMetadataCache_NegativeCache(t *testing.T) {
	c := NewMetadataCache()

	c.SetNegative("/nonexistent")

	if !c.IsNegative("/nonexistent") {
		t.Error("expected path to be in negative cache")
	}
	if c.IsNegative("/other") {
		t.Error("expected /other to not be in negative cache")
	}
}

func TestMetadataCache_SetWithChildren(t *testing.T) {
	c := NewMetadataCache()

	children := []DirEntry{
		{Name: "file1", Mode: 0644, Ino: 1},
		{Name: "file2", Mode: 0644, Ino: 2},
	}
	childMeta := map[string]*FileInfo{
		"file1": {Ino: 1, Size: 10},
		"file2": {Ino: 2, Size: 20},
	}

	c.SetWithChildren("/dir", children, childMeta)

	// Check parent entry
	entry := c.Get("/dir")
	if entry == nil {
		t.Fatal("expected dir entry")
	}
	if len(entry.Children) != 2 {
		t.Errorf("expected 2 children, got %d", len(entry.Children))
	}

	// Check child info lookup from parent
	info := c.GetInfo("/dir/file1")
	if info == nil {
		t.Fatal("expected child info from parent cache")
	}
	if info.Size != 10 {
		t.Errorf("expected size 10, got %d", info.Size)
	}
}

func TestMetadataCache_Invalidate(t *testing.T) {
	c := NewMetadataCache()

	info := &FileInfo{Ino: 123, Size: 100}
	c.Set("/dir/file", info)

	// Invalidate should remove entry
	c.Invalidate("/dir/file")

	if c.GetInfo("/dir/file") != nil {
		t.Error("expected entry to be invalidated")
	}
}

func TestMetadataCache_InvalidateClearsParentChildren(t *testing.T) {
	c := NewMetadataCache()

	children := []DirEntry{{Name: "file", Mode: 0644, Ino: 1}}
	childMeta := map[string]*FileInfo{"file": {Ino: 1, Size: 10}}
	c.SetWithChildren("/dir", children, childMeta)

	// Invalidating child should clear parent's children cache
	c.Invalidate("/dir/file")

	entry := c.Get("/dir")
	if entry != nil && entry.Children != nil {
		t.Error("expected parent children to be cleared")
	}
}

func TestPathIno_Deterministic(t *testing.T) {
	path := "/tools/wikipedia"
	ino1 := PathIno(path)
	ino2 := PathIno(path)

	if ino1 != ino2 {
		t.Errorf("PathIno not deterministic: %d != %d", ino1, ino2)
	}

	// Different paths should have different inodes
	ino3 := PathIno("/tools/github")
	if ino1 == ino3 {
		t.Error("different paths should have different inodes")
	}
}

func TestFileInfoConstructors(t *testing.T) {
	tests := []struct {
		name     string
		fn       func() *FileInfo
		checkIno uint64
		checkMode uint32
	}{
		{
			name:     "NewDirInfo",
			fn:       func() *FileInfo { return NewDirInfo(100) },
			checkIno: 100,
			checkMode: 0040755, // S_IFDIR | 0755
		},
		{
			name:     "NewExecFileInfo",
			fn:       func() *FileInfo { return NewExecFileInfo(200, 1024) },
			checkIno: 200,
			checkMode: 0100755, // S_IFREG | 0755
		},
		{
			name:     "NewSymlinkInfo",
			fn:       func() *FileInfo { return NewSymlinkInfo(300, 10) },
			checkIno: 300,
			checkMode: 0120777, // S_IFLNK | 0777
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := tt.fn()
			if info.Ino != tt.checkIno {
				t.Errorf("expected ino %d, got %d", tt.checkIno, info.Ino)
			}
			if info.Mode != tt.checkMode {
				t.Errorf("expected mode %o, got %o", tt.checkMode, info.Mode)
			}
			// Check timestamps are set
			if info.Atime.IsZero() || info.Mtime.IsZero() || info.Ctime.IsZero() {
				t.Error("expected timestamps to be set")
			}
			// Check timestamps are recent (within last second)
			if time.Since(info.Atime) > time.Second {
				t.Error("expected recent timestamp")
			}
		})
	}
}
