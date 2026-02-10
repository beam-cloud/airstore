package filesystem

import (
	"errors"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/winfsp/cgofuse/fuse"
)

type adapter struct {
	fuse.FileSystemBase
	fs *Filesystem
}

func newAdapter(fs *Filesystem) *adapter {
	return &adapter{
		fs: fs,
	}
}

func (a *adapter) Init()    { a.fs.Init() }
func (a *adapter) Destroy() { a.fs.Destroy() }

func (a *adapter) Statfs(path string, stat *fuse.Statfs_t) int {
	info, err := a.fs.Statfs()
	if err != nil {
		return toErrno(err)
	}
	stat.Bsize = info.Bsize
	stat.Frsize = info.Bsize
	stat.Blocks = info.Blocks
	stat.Bfree = info.Bfree
	stat.Bavail = info.Bavail
	stat.Files = info.Files
	stat.Ffree = info.Ffree
	stat.Favail = info.Ffree
	stat.Namemax = info.Namemax
	return 0
}

func (a *adapter) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	info, err := a.fs.Getattr(path)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordGetattr(path, time.Since(start), err)
		}
		return toErrno(err)
	}
	fillStat(stat, info)
	if a.fs.trace != nil {
		a.fs.trace.recordGetattr(path, time.Since(start), nil)
	}
	return 0
}

func (a *adapter) Readlink(path string) (int, string) {
	target, err := a.fs.Readlink(path)
	if err != nil {
		return toErrno(err), ""
	}
	return 0, target
}

func (a *adapter) Mkdir(path string, mode uint32) int {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	err := a.fs.Mkdir(path, mode)
	if a.fs.trace != nil {
		a.fs.trace.recordMkdir(path, time.Since(start), err)
	}
	return toErrno(err)
}

func (a *adapter) Rmdir(path string) int {
	return toErrno(a.fs.Rmdir(path))
}

func (a *adapter) Unlink(path string) int {
	return toErrno(a.fs.Unlink(path))
}

func (a *adapter) Rename(oldpath, newpath string) int {
	// FUSE-T SMB backend uses rename-to-hidden for delete operations.
	// When we see a rename to .fuse_hidden*, treat it as a delete.
	if strings.Contains(newpath, ".fuse_hidden") {
		return toErrno(a.fs.Unlink(oldpath))
	}
	return toErrno(a.fs.Rename(oldpath, newpath))
}

func (a *adapter) Link(oldpath, newpath string) int {
	return toErrno(a.fs.Link(oldpath, newpath))
}

func (a *adapter) Symlink(target, newpath string) int {
	return toErrno(a.fs.Symlink(target, newpath))
}

func (a *adapter) Chmod(path string, mode uint32) int {
	return toErrno(a.fs.Chmod(path, mode))
}

func (a *adapter) Chown(path string, uid, gid uint32) int {
	return toErrno(a.fs.Chown(path, uid, gid))
}

// Access checks file access permissions. We always return success to allow
// operations like `cp` on macOS to work properly with fcopyfile.
func (a *adapter) Access(path string, mask uint32) int {
	// Check if file exists first
	if _, err := a.fs.Getattr(path); err != nil {
		return toErrno(err)
	}
	return 0 // Allow all access to existing files
}

// Chflags handles BSD file flags (macOS). We silently accept and ignore them.
func (a *adapter) Chflags(path string, flags uint32) int {
	return 0 // Accept and discard
}

// Setcrtime sets file creation time (macOS). We silently accept and ignore.
func (a *adapter) Setcrtime(path string, tmsp fuse.Timespec) int {
	return 0 // Accept and discard
}

// Setchgtime sets file change time (macOS). We silently accept and ignore.
func (a *adapter) Setchgtime(path string, tmsp fuse.Timespec) int {
	return 0 // Accept and discard
}

func (a *adapter) Utimens(path string, tmsp []fuse.Timespec) int {
	var atime, mtime *int64
	if len(tmsp) >= 1 {
		t := tmsp[0].Sec*1e9 + tmsp[0].Nsec
		atime = &t
	}
	if len(tmsp) >= 2 {
		t := tmsp[1].Sec*1e9 + tmsp[1].Nsec
		mtime = &t
	}
	return toErrno(a.fs.Utimens(path, atime, mtime))
}

func (a *adapter) Open(path string, flags int) (int, uint64) {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	fh, err := a.fs.Open(path, flags)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordOpen(path, time.Since(start), err)
		}
		return toErrno(err), 0
	}
	if a.fs.trace != nil {
		a.fs.trace.recordOpen(path, time.Since(start), nil)
	}
	return 0, uint64(fh)
}

func (a *adapter) Create(path string, flags int, mode uint32) (int, uint64) {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	fh, err := a.fs.Create(path, flags, mode)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordCreate(path, time.Since(start), err)
		}
		return toErrno(err), 0
	}
	if a.fs.trace != nil {
		a.fs.trace.recordCreate(path, time.Since(start), nil)
	}
	return 0, uint64(fh)
}

func (a *adapter) Read(path string, buf []byte, off int64, fh uint64) int {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	n, err := a.fs.Read(path, buf, off, FileHandle(fh))
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordRead(path, time.Since(start), err)
		}
		return toErrno(err)
	}
	if a.fs.trace != nil {
		a.fs.trace.recordRead(path, time.Since(start), nil)
	}
	return n
}

func (a *adapter) Write(path string, buf []byte, off int64, fh uint64) int {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	n, err := a.fs.Write(path, buf, off, FileHandle(fh))
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordWrite(path, time.Since(start), err)
		}
		return toErrno(err)
	}
	if a.fs.trace != nil {
		a.fs.trace.recordWrite(path, time.Since(start), nil)
	}
	return n
}

func (a *adapter) Truncate(path string, size int64, fh uint64) int {
	return toErrno(a.fs.Truncate(path, size, FileHandle(fh)))
}

func (a *adapter) Flush(path string, fh uint64) int {
	return toErrno(a.fs.Flush(path, FileHandle(fh)))
}

func (a *adapter) Release(path string, fh uint64) int {
	return toErrno(a.fs.Release(path, FileHandle(fh)))
}

func (a *adapter) Fsync(path string, datasync bool, fh uint64) int {
	return toErrno(a.fs.Fsync(path, datasync, FileHandle(fh)))
}

func (a *adapter) Opendir(path string) (int, uint64) {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	fh, err := a.fs.Opendir(path)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordOpendir(path, time.Since(start), err)
		}
		return toErrno(err), 0
	}
	if a.fs.trace != nil {
		a.fs.trace.recordOpendir(path, time.Since(start), nil)
	}
	return 0, uint64(fh)
}

func (a *adapter) Readdir(path string, fill func(string, *fuse.Stat_t, int64) bool, off int64, fh uint64) int {
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	entries, err := a.fs.Readdir(path)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordReaddir(path, time.Since(start), err)
		}
		return toErrno(err)
	}

	// Batch-compute values used per entry to avoid repeated syscalls/atomics.
	uid, gid := vnode.GetOwner()
	now := time.Now()
	nowSpec := fuse.NewTimespec(now)

	// "." — use a synthetic dir stat instead of a full Getattr roundtrip.
	var dotStat fuse.Stat_t
	fillDirStat(&dotStat, vnode.PathIno(path), uid, gid, nowSpec)
	fill(".", &dotStat, 0)

	// ".." — use parent path inode; avoids a full Getattr.
	var dotdotStat fuse.Stat_t
	fillDirStat(&dotdotStat, vnode.PathIno(parentPath(path)), uid, gid, nowSpec)
	fill("..", &dotdotStat, 0)

	// Fill entry stats. When Mode is set (the common case for all vnodes),
	// we use the embedded metadata directly — zero Getattr calls.
	for i := range entries {
		e := &entries[i]
		var stat fuse.Stat_t

		if e.Mode != 0 {
			fillStatFromDirEntry(&stat, e, uid, gid, nowSpec)
		} else {
			// Fallback: no metadata — must call Getattr (rare/legacy path).
			p := path + "/" + e.Name
			if path == "/" {
				p = "/" + e.Name
			}
			if info, err := a.fs.Getattr(p); err == nil {
				fillStat(&stat, info)
			} else {
				fillStatFromDirEntry(&stat, e, uid, gid, nowSpec)
			}
		}
		if !fill(e.Name, &stat, 0) {
			break
		}
	}
	if a.fs.trace != nil {
		a.fs.trace.recordReaddir(path, time.Since(start), nil)
	}
	return 0
}

func parentPath(path string) string {
	if path == "/" || path == "" {
		return "/"
	}
	i := len(path) - 1
	for i > 0 && path[i] != '/' {
		i--
	}
	if i == 0 {
		return "/"
	}
	return path[:i]
}

func (a *adapter) Releasedir(path string, fh uint64) int {
	return toErrno(a.fs.Releasedir(path, FileHandle(fh)))
}

func (a *adapter) Getxattr(path, name string) (int, []byte) {
	data, err := a.fs.Getxattr(path, name)
	if err != nil {
		return toErrno(err), nil
	}
	return 0, data
}

func (a *adapter) Setxattr(path, name string, value []byte, flags int) int {
	return toErrno(a.fs.Setxattr(path, name, value, flags))
}

func (a *adapter) Removexattr(path, name string) int {
	return toErrno(a.fs.Removexattr(path, name))
}

func (a *adapter) Listxattr(path string, fill func(string) bool) int {
	names, err := a.fs.Listxattr(path)
	if err != nil {
		return toErrno(err)
	}
	for _, name := range names {
		if !fill(name) {
			break
		}
	}
	return 0
}

func fillStat(stat *fuse.Stat_t, info *FileInfo) {
	*stat = fuse.Stat_t{}
	stat.Dev = 1
	stat.Ino = info.Ino
	stat.Mode = info.Mode
	stat.Nlink = info.Nlink
	stat.Uid = info.Uid
	stat.Gid = info.Gid
	stat.Size = info.Size
	stat.Blksize = 4096
	stat.Blocks = (info.Size + 511) / 512
	atim := fuse.NewTimespec(info.Atime)
	mtim := fuse.NewTimespec(info.Mtime)
	ctim := fuse.NewTimespec(info.Ctime)
	stat.Atim = atim
	stat.Mtim = mtim
	stat.Ctim = ctim
	stat.Birthtim = ctim // macOS
}

// fillStatFromDirEntry fills a fuse.Stat_t from a DirEntry using pre-computed
// uid/gid and time. Called in a tight loop during Readdir — zero allocations,
// zero syscalls, zero atomic loads per entry.
func fillStatFromDirEntry(stat *fuse.Stat_t, e *DirEntry, uid, gid uint32, fallbackTime fuse.Timespec) {
	*stat = fuse.Stat_t{}
	stat.Dev = 1
	stat.Ino = e.Ino
	stat.Mode = e.Mode
	stat.Nlink = 1
	stat.Uid = uid
	stat.Gid = gid
	stat.Size = e.Size
	stat.Blksize = 4096
	stat.Blocks = (e.Size + 511) / 512

	var ts fuse.Timespec
	if e.Mtime > 0 {
		ts = fuse.Timespec{Sec: e.Mtime}
	} else {
		ts = fallbackTime
	}
	stat.Atim = ts
	stat.Mtim = ts
	stat.Ctim = ts
	stat.Birthtim = ts // macOS
}

// fillDirStat fills a minimal directory stat for "." and ".." entries.
// Avoids a full Getattr roundtrip.
func fillDirStat(stat *fuse.Stat_t, ino uint64, uid, gid uint32, ts fuse.Timespec) {
	*stat = fuse.Stat_t{}
	stat.Dev = 1
	stat.Ino = ino
	stat.Mode = syscall.S_IFDIR | 0755
	stat.Nlink = 2
	stat.Uid = uid
	stat.Gid = gid
	stat.Blksize = 4096
	stat.Atim = ts
	stat.Mtim = ts
	stat.Ctim = ts
	stat.Birthtim = ts
}

func toErrno(err error) int {
	if err == nil {
		return 0
	}

	var errno syscall.Errno
	if errors.As(err, &errno) {
		return -int(errno)
	}

	switch {
	case errors.Is(err, ErrNotFound):
		return -int(syscall.ENOENT)
	case errors.Is(err, ErrPermission):
		return -int(syscall.EACCES)
	case errors.Is(err, ErrExist):
		return -int(syscall.EEXIST)
	case errors.Is(err, ErrNotDir):
		return -int(syscall.ENOTDIR)
	case errors.Is(err, ErrIsDir):
		return -int(syscall.EISDIR)
	case errors.Is(err, ErrNotEmpty):
		return -int(syscall.ENOTEMPTY)
	case errors.Is(err, ErrReadOnly):
		return -int(syscall.EROFS)
	case errors.Is(err, ErrInvalid):
		return -int(syscall.EINVAL)
	case errors.Is(err, ErrIO):
		return -int(syscall.EIO)
	case errors.Is(err, ErrNoSpace):
		return -int(syscall.ENOSPC)
	case errors.Is(err, ErrNotSupported):
		return -int(syscall.ENOTSUP)
	case errors.Is(err, ErrNoAttr):
		return -int(syscall.ENODATA)
	default:
		return -int(syscall.EIO)
	}
}
