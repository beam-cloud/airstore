package filesystem

import (
	"errors"
	"syscall"
	"time"

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
	start := time.Now()
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
	start := time.Now()
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
	start := time.Now()
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
	start := time.Now()
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
	start := time.Now()
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
	start := time.Now()
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
	start := time.Now()
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
	start := time.Now()
	entries, err := a.fs.Readdir(path)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordReaddir(path, time.Since(start), err)
		}
		return toErrno(err)
	}

	// Get . stat
	var dotStat fuse.Stat_t
	if info, err := a.fs.Getattr(path); err == nil {
		fillStat(&dotStat, info)
	}
	fill(".", &dotStat, 0)

	// Get .. stat
	var dotdotStat fuse.Stat_t
	if info, err := a.fs.Getattr(parentPath(path)); err == nil {
		fillStat(&dotdotStat, info)
	}
	fill("..", &dotdotStat, 0)

	// Get entry stats - use embedded metadata when available to avoid N Getattr calls
	for _, e := range entries {
		var stat fuse.Stat_t

		// If entry has Size or Mtime, use it directly (avoids expensive Getattr call)
		if e.Size > 0 || e.Mtime > 0 {
			fillStatFromEntry(&stat, &e)
		} else {
			// Fall back to Getattr for entries without metadata
			p := path + "/" + e.Name
			if path == "/" {
				p = "/" + e.Name
			}
			if info, err := a.fs.Getattr(p); err == nil {
				fillStat(&stat, info)
			} else {
				// Getattr failed, use entry mode at least
				fillStatFromEntry(&stat, &e)
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
	*stat = fuse.Stat_t{} // Zero all fields first
	stat.Dev = 1
	stat.Ino = info.Ino
	stat.Mode = info.Mode
	stat.Nlink = info.Nlink
	stat.Uid = info.Uid
	stat.Gid = info.Gid
	stat.Rdev = 0
	stat.Size = info.Size
	stat.Atim = fuse.NewTimespec(info.Atime)
	stat.Mtim = fuse.NewTimespec(info.Mtime)
	stat.Ctim = fuse.NewTimespec(info.Ctime)
	stat.Blksize = 4096
	stat.Blocks = (info.Size + 511) / 512
	stat.Birthtim = fuse.NewTimespec(info.Ctime) // macOS
	stat.Flags = 0                               // macOS
}

// fillStatFromEntry fills stat from a DirEntry, using embedded Size/Mtime when available.
// This avoids expensive Getattr calls for each entry during directory listings.
func fillStatFromEntry(stat *fuse.Stat_t, e *DirEntry) {
	*stat = fuse.Stat_t{} // Zero all fields first
	stat.Dev = 1
	stat.Ino = e.Ino
	stat.Mode = e.Mode
	stat.Nlink = 1
	stat.Uid = uint32(syscall.Getuid())
	stat.Gid = uint32(syscall.Getgid())
	stat.Rdev = 0
	stat.Size = e.Size
	stat.Blksize = 4096
	stat.Blocks = (e.Size + 511) / 512
	stat.Flags = 0 // macOS

	// Use entry mtime if available, otherwise use current time
	var mtime time.Time
	if e.Mtime > 0 {
		mtime = time.Unix(e.Mtime, 0)
	} else {
		mtime = time.Now()
	}
	stat.Atim = fuse.NewTimespec(mtime)
	stat.Mtim = fuse.NewTimespec(mtime)
	stat.Ctim = fuse.NewTimespec(mtime)
	stat.Birthtim = fuse.NewTimespec(mtime) // macOS
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
