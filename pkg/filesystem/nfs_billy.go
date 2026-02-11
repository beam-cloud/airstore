package filesystem

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5"
)

// billyFS wraps a Filesystem to implement billy.Filesystem for use with go-nfs.
type billyFS struct {
	fs *Filesystem
}

// Compile-time interface checks.
var (
	_ billy.Filesystem = (*billyFS)(nil)
	_ billy.Change     = (*billyFS)(nil)
)

func newBillyFS(fs *Filesystem) *billyFS {
	return &billyFS{fs: fs}
}

// ---------------------------------------------------------------------------
// billy.Basic
// ---------------------------------------------------------------------------

func (b *billyFS) Create(filename string) (billy.File, error) {
	filename = b.cleanPath(filename)
	fh, err := b.fs.Create(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return newBillyFile(b.fs, filename, fh, os.O_RDWR|os.O_CREATE|os.O_TRUNC), nil
}

func (b *billyFS) Open(filename string) (billy.File, error) {
	filename = b.cleanPath(filename)
	fh, err := b.fs.Open(filename, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	return newBillyFile(b.fs, filename, fh, os.O_RDONLY), nil
}

func (b *billyFS) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	filename = b.cleanPath(filename)

	if flag&os.O_CREATE != 0 {
		_, err := b.fs.Getattr(filename)
		if err != nil {
			// File doesn't exist — create it.
			fh, err := b.fs.Create(filename, flag, uint32(perm))
			if err != nil {
				return nil, err
			}
			return newBillyFile(b.fs, filename, fh, flag), nil
		}
	}

	fh, err := b.fs.Open(filename, flag)
	if err != nil {
		return nil, err
	}

	// Truncate existing file when O_TRUNC is requested (e.g., overwrite via mv/cp).
	if flag&os.O_TRUNC != 0 {
		if tErr := b.fs.Truncate(filename, 0, fh); tErr != nil {
			b.fs.Release(filename, fh)
			return nil, tErr
		}
	}

	return newBillyFile(b.fs, filename, fh, flag), nil
}

func (b *billyFS) Stat(filename string) (os.FileInfo, error) {
	filename = b.cleanPath(filename)
	info, err := b.fs.Getattr(filename)
	if err != nil {
		return nil, err
	}
	return newBillyFileInfo(path.Base(filename), info), nil
}

func (b *billyFS) Rename(oldpath, newpath string) error {
	return b.fs.Rename(b.cleanPath(oldpath), b.cleanPath(newpath))
}

func (b *billyFS) Remove(filename string) error {
	filename = b.cleanPath(filename)
	// Try unlink first (files). If it fails because the target is a directory,
	// fall back to rmdir. All other errors (permission, I/O) are returned as-is.
	err := b.fs.Unlink(filename)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrIsDir) {
		return b.fs.Rmdir(filename)
	}
	return err
}

func (b *billyFS) Join(elem ...string) string {
	return path.Join(elem...)
}

// ---------------------------------------------------------------------------
// billy.TempFile
// ---------------------------------------------------------------------------

func (b *billyFS) TempFile(dir, prefix string) (billy.File, error) {
	return nil, fmt.Errorf("TempFile not supported")
}

// ---------------------------------------------------------------------------
// billy.Dir
// ---------------------------------------------------------------------------

func (b *billyFS) ReadDir(dirname string) ([]os.FileInfo, error) {
	dirname = b.cleanPath(dirname)
	entries, err := b.fs.Readdir(dirname)
	if err != nil {
		return nil, err
	}

	infos := make([]os.FileInfo, len(entries))
	for i, e := range entries {
		infos[i] = newBillyFileInfoFromDirEntry(&e)
	}
	return infos, nil
}

func (b *billyFS) MkdirAll(filename string, perm os.FileMode) error {
	filename = b.cleanPath(filename)
	parts := strings.Split(strings.Trim(filename, "/"), "/")
	current := ""
	for _, part := range parts {
		current = current + "/" + part
		if _, err := b.fs.Getattr(current); err != nil {
			if mkErr := b.fs.Mkdir(current, uint32(perm)); mkErr != nil {
				return mkErr
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// billy.Symlink
// ---------------------------------------------------------------------------

func (b *billyFS) Lstat(filename string) (os.FileInfo, error) {
	// We don't distinguish lstat from stat (no real symlinks at the VFS level).
	return b.Stat(filename)
}

func (b *billyFS) Symlink(target, link string) error {
	return b.fs.Symlink(target, b.cleanPath(link))
}

func (b *billyFS) Readlink(link string) (string, error) {
	return b.fs.Readlink(b.cleanPath(link))
}

// ---------------------------------------------------------------------------
// billy.Chroot
// ---------------------------------------------------------------------------

func (b *billyFS) Chroot(p string) (billy.Filesystem, error) {
	return nil, fmt.Errorf("Chroot not supported")
}

func (b *billyFS) Root() string { return "/" }

// ---------------------------------------------------------------------------
// billy.Change — allows NFS setattr operations
// ---------------------------------------------------------------------------

func (b *billyFS) Chmod(name string, mode os.FileMode) error {
	return b.fs.Chmod(b.cleanPath(name), uint32(mode))
}

func (b *billyFS) Lchown(name string, uid, gid int) error {
	return b.fs.Chown(b.cleanPath(name), uint32(uid), uint32(gid))
}

func (b *billyFS) Chown(name string, uid, gid int) error {
	return b.fs.Chown(b.cleanPath(name), uint32(uid), uint32(gid))
}

func (b *billyFS) Chtimes(name string, atime time.Time, mtime time.Time) error {
	aNs := atime.UnixNano()
	mNs := mtime.UnixNano()
	return b.fs.Utimens(b.cleanPath(name), &aNs, &mNs)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func (b *billyFS) cleanPath(p string) string {
	p = path.Clean(p)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

// =========================================================================
// billyFile — implements billy.File wrapping path-based Filesystem ops
// =========================================================================

type billyFile struct {
	fs   *Filesystem
	path string
	fh   FileHandle
	flag int
	pos  int64
	mu   sync.Mutex
}

func newBillyFile(fs *Filesystem, path string, fh FileHandle, flag int) *billyFile {
	return &billyFile{fs: fs, path: path, fh: fh, flag: flag}
}

func (f *billyFile) Name() string { return f.path }

func (f *billyFile) Read(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := f.fs.Read(f.path, p, f.pos, f.fh)
	if err != nil {
		return 0, err
	}
	if n == 0 && len(p) > 0 {
		return 0, io.EOF
	}
	f.pos += int64(n)
	return n, nil
}

// ReadAt implements io.ReaderAt — reads without changing the file position.
func (f *billyFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.fs.Read(f.path, p, off, f.fh)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *billyFile) Write(p []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	writePos := f.pos
	// O_APPEND: always write at the current end of file.
	if f.flag&os.O_APPEND != 0 {
		if info, err := f.fs.Getattr(f.path); err == nil {
			writePos = info.Size
		}
	}

	n, err := f.fs.Write(f.path, p, writePos, f.fh)
	if err != nil {
		return 0, err
	}
	f.pos = writePos + int64(n)
	return n, nil
}

func (f *billyFile) Seek(offset int64, whence int) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		info, err := f.fs.Getattr(f.path)
		if err != nil {
			return 0, err
		}
		f.pos = info.Size + offset
	}
	return f.pos, nil
}

func (f *billyFile) Close() error {
	return f.fs.Release(f.path, f.fh)
}

func (f *billyFile) Lock() error   { return nil }
func (f *billyFile) Unlock() error { return nil }

func (f *billyFile) Truncate(size int64) error {
	return f.fs.Truncate(f.path, size, f.fh)
}

// =========================================================================
// billyFileInfo — implements os.FileInfo
// =========================================================================

type billyFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	dir     bool
}

func newBillyFileInfo(name string, info *FileInfo) *billyFileInfo {
	return &billyFileInfo{
		name:    name,
		size:    info.Size,
		mode:    unixModeToGoFileMode(info.Mode),
		modTime: info.Mtime,
		dir:     info.IsDir(),
	}
}

func newBillyFileInfoFromDirEntry(e *DirEntry) *billyFileInfo {
	mtime := time.Now()
	if e.Mtime > 0 {
		mtime = time.Unix(e.Mtime, 0)
	}
	return &billyFileInfo{
		name:    e.Name,
		size:    e.Size,
		mode:    unixModeToGoFileMode(e.Mode),
		modTime: mtime,
		dir:     e.Mode&syscall.S_IFDIR != 0,
	}
}

func (fi *billyFileInfo) Name() string      { return fi.name }
func (fi *billyFileInfo) Size() int64        { return fi.size }
func (fi *billyFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *billyFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *billyFileInfo) IsDir() bool        { return fi.dir }
func (fi *billyFileInfo) Sys() interface{}   { return nil }

// unixModeToGoFileMode converts raw Unix mode bits (as stored in our FileInfo
// and DirEntry) to Go's os.FileMode encoding. This is critical because go-nfs
// checks os.ModeSymlink, os.ModeDir, etc. which use different bit positions
// than the raw Unix S_IFLNK, S_IFDIR values.
func unixModeToGoFileMode(mode uint32) os.FileMode {
	fm := os.FileMode(mode & 0777) // permission bits (rwxrwxrwx)
	switch mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fm |= os.ModeDevice
	case syscall.S_IFCHR:
		fm |= os.ModeDevice | os.ModeCharDevice
	case syscall.S_IFDIR:
		fm |= os.ModeDir
	case syscall.S_IFIFO:
		fm |= os.ModeNamedPipe
	case syscall.S_IFLNK:
		fm |= os.ModeSymlink
	case syscall.S_IFSOCK:
		fm |= os.ModeSocket
	}
	if mode&syscall.S_ISGID != 0 {
		fm |= os.ModeSetgid
	}
	if mode&syscall.S_ISUID != 0 {
		fm |= os.ModeSetuid
	}
	if mode&syscall.S_ISVTX != 0 {
		fm |= os.ModeSticky
	}
	return fm
}
