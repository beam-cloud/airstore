package filesystem

import (
	"errors"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"
)

type adapter struct {
	fuse.FileSystemBase
	fs *Filesystem
}

func newAdapter(fs *Filesystem) *adapter {
	return &adapter{fs: fs}
}

// recoverEIO catches panics in FUSE operations so a single bad source
// provider can't crash the mount. Sets *rc to -EIO and logs the stack.
func recoverEIO(rc *int) {
	if r := recover(); r != nil {
		log.Error().Interface("recovered", r).Str("stack", string(debug.Stack())).Msg("fuse: panic in operation")
		*rc = -int(syscall.EIO)
	}
}

func (a *adapter) Init()    { a.fs.Init() }
func (a *adapter) Destroy() { a.fs.Destroy() }

func (a *adapter) Statfs(path string, stat *fuse.Statfs_t) (rc int) {
	defer recoverEIO(&rc)
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

func (a *adapter) Getattr(path string, stat *fuse.Stat_t, fh uint64) (rc int) {
	defer recoverEIO(&rc)
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

func (a *adapter) Readlink(path string) (rc int, target string) {
	defer recoverEIO(&rc)
	t, err := a.fs.Readlink(path)
	if err != nil {
		return toErrno(err), ""
	}
	return 0, t
}

func (a *adapter) Mkdir(path string, mode uint32) (rc int) {
	defer recoverEIO(&rc)
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

func (a *adapter) Rmdir(path string) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Rmdir(path))
}

func (a *adapter) Unlink(path string) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Unlink(path))
}

func (a *adapter) Rename(oldpath, newpath string) (rc int) {
	defer recoverEIO(&rc)
	// FUSE-T SMB backend uses rename-to-hidden for delete operations.
	if strings.Contains(newpath, ".fuse_hidden") {
		return toErrno(a.fs.Unlink(oldpath))
	}
	return toErrno(a.fs.Rename(oldpath, newpath))
}

func (a *adapter) Link(oldpath, newpath string) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Link(oldpath, newpath))
}

func (a *adapter) Symlink(target, newpath string) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Symlink(target, newpath))
}

func (a *adapter) Chmod(path string, mode uint32) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Chmod(path, mode))
}

func (a *adapter) Chown(path string, uid, gid uint32) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Chown(path, uid, gid))
}

func (a *adapter) Access(path string, mask uint32) (rc int) {
	defer recoverEIO(&rc)
	if _, err := a.fs.Getattr(path); err != nil {
		return toErrno(err)
	}
	return 0
}

func (a *adapter) Chflags(path string, flags uint32) int   { return 0 }
func (a *adapter) Setcrtime(path string, tmsp fuse.Timespec) int  { return 0 }
func (a *adapter) Setchgtime(path string, tmsp fuse.Timespec) int { return 0 }

func (a *adapter) Utimens(path string, tmsp []fuse.Timespec) (rc int) {
	defer recoverEIO(&rc)
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

// OpenEx uses direct I/O so the kernel reads until EOF instead of trusting
// the file size from Getattr. Essential for a remote FS with compression.
func (a *adapter) OpenEx(path string, fi *fuse.FileInfo_t) (rc int) {
	defer recoverEIO(&rc)
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	fh, err := a.fs.Open(path, fi.Flags)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordOpen(path, time.Since(start), err)
		}
		return toErrno(err)
	}
	if a.fs.trace != nil {
		a.fs.trace.recordOpen(path, time.Since(start), nil)
	}
	fi.Fh = uint64(fh)
	fi.DirectIo = true
	return 0
}

func (a *adapter) CreateEx(path string, mode uint32, fi *fuse.FileInfo_t) (rc int) {
	defer recoverEIO(&rc)
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	fh, err := a.fs.Create(path, fi.Flags, mode)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordCreate(path, time.Since(start), err)
		}
		return toErrno(err)
	}
	if a.fs.trace != nil {
		a.fs.trace.recordCreate(path, time.Since(start), nil)
	}
	fi.Fh = uint64(fh)
	fi.DirectIo = true
	return 0
}

func (a *adapter) Read(path string, buf []byte, off int64, fh uint64) (rc int) {
	defer recoverEIO(&rc)
	start := time.Now()
	n, attr, err := a.fs.Read(path, buf, off, FileHandle(fh))
	elapsed := time.Since(start)
	a.fs.recordLogicalRead(path, off, len(buf), n, elapsed, err, attr)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordRead(path, elapsed, err)
		}
		return toErrno(err)
	}
	if a.fs.trace != nil {
		a.fs.trace.recordRead(path, elapsed, nil)
	}
	return n
}

func (a *adapter) Write(path string, buf []byte, off int64, fh uint64) (rc int) {
	defer recoverEIO(&rc)
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

func (a *adapter) Truncate(path string, size int64, fh uint64) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Truncate(path, size, FileHandle(fh)))
}

func (a *adapter) Flush(path string, fh uint64) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Flush(path, FileHandle(fh)))
}

func (a *adapter) Release(path string, fh uint64) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Release(path, FileHandle(fh)))
}

func (a *adapter) Fsync(path string, datasync bool, fh uint64) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Fsync(path, datasync, FileHandle(fh)))
}

func (a *adapter) Opendir(path string) (rc int, fh uint64) {
	defer recoverEIO(&rc)
	var start time.Time
	if a.fs.trace != nil {
		start = time.Now()
	}
	h, err := a.fs.Opendir(path)
	if err != nil {
		if a.fs.trace != nil {
			a.fs.trace.recordOpendir(path, time.Since(start), err)
		}
		return toErrno(err), 0
	}
	if a.fs.trace != nil {
		a.fs.trace.recordOpendir(path, time.Since(start), nil)
	}
	return 0, uint64(h)
}

func (a *adapter) Readdir(path string, fill func(string, *fuse.Stat_t, int64) bool, off int64, fh uint64) (rc int) {
	defer recoverEIO(&rc)
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

	uid, gid := vnode.GetOwner()
	now := time.Now()
	nowSpec := fuse.NewTimespec(now)

	var dotStat fuse.Stat_t
	fillDirStat(&dotStat, vnode.PathIno(path), uid, gid, nowSpec)
	fill(".", &dotStat, 0)

	var dotdotStat fuse.Stat_t
	fillDirStat(&dotdotStat, vnode.PathIno(parentPath(path)), uid, gid, nowSpec)
	fill("..", &dotdotStat, 0)

	for i := range entries {
		e := &entries[i]
		var stat fuse.Stat_t
		if e.Mode != 0 {
			fillStatFromDirEntry(&stat, e, uid, gid, nowSpec)
		} else {
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

func (a *adapter) Releasedir(path string, fh uint64) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Releasedir(path, FileHandle(fh)))
}

func (a *adapter) Getxattr(path, name string) (rc int, data []byte) {
	defer recoverEIO(&rc)
	d, err := a.fs.Getxattr(path, name)
	if err != nil {
		return toErrno(err), nil
	}
	return 0, d
}

func (a *adapter) Setxattr(path, name string, value []byte, flags int) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Setxattr(path, name, value, flags))
}

func (a *adapter) Removexattr(path, name string) (rc int) {
	defer recoverEIO(&rc)
	return toErrno(a.fs.Removexattr(path, name))
}

func (a *adapter) Listxattr(path string, fill func(string) bool) (rc int) {
	defer recoverEIO(&rc)
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

// ---------------------------------------------------------------------------
// Stat helpers
// ---------------------------------------------------------------------------

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
	stat.Birthtim = ctim
}

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
	stat.Birthtim = ts
}

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

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

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
