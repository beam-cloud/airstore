package filesystem

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"syscall"
	"time"
)

type FileInfo struct {
	Ino   uint64
	Size  int64
	Mode  uint32
	Nlink uint32
	Uid   uint32
	Gid   uint32
	Atime time.Time
	Mtime time.Time
	Ctime time.Time
}

func (fi *FileInfo) IsDir() bool     { return fi.Mode&syscall.S_IFDIR != 0 }
func (fi *FileInfo) IsRegular() bool { return fi.Mode&syscall.S_IFREG != 0 }
func (fi *FileInfo) IsSymlink() bool { return fi.Mode&syscall.S_IFLNK != 0 }

type DirEntry struct {
	Name string
	Mode uint32
	Ino  uint64
}

type StatInfo struct {
	Bsize   uint64
	Blocks  uint64
	Bfree   uint64
	Bavail  uint64
	Files   uint64
	Ffree   uint64
	Namemax uint64
}

type FileHandle uint64

type NodeInfo struct {
	Path string
	ID   string
	PID  string
}

var (
	ErrNotFound     = fs.ErrNotExist
	ErrPermission   = fs.ErrPermission
	ErrExist        = fs.ErrExist
	ErrNotDir       = syscall.ENOTDIR
	ErrIsDir        = syscall.EISDIR
	ErrNotEmpty     = syscall.ENOTEMPTY
	ErrReadOnly     = syscall.EROFS
	ErrInvalid      = fs.ErrInvalid
	ErrIO           = syscall.EIO
	ErrNoSpace      = syscall.ENOSPC
	ErrNotSupported = syscall.ENOTSUP
	ErrNoAttr       = syscall.ENODATA // ENOATTR on macOS maps to ENODATA
)

type DirectoryAccessMetadata struct {
	PID         string            `json:"pid"`
	ID          string            `json:"id"`
	Permission  uint32            `json:"permission"`
	RenameList  map[string]string `json:"renameList,omitempty"`
	BackPointer *BackPointer      `json:"backPointer,omitempty"`
}

type BackPointer struct {
	BirthParentID string `json:"birthParentId"`
	NameVersion   int    `json:"nameVersion"`
}

type DirectoryContentMetadata struct {
	Id         string               `json:"id"`
	EntryList  []string             `json:"entryList"`
	Timestamps map[string]time.Time `json:"timestamps"`
}

type FileMetadata struct {
	ID       string `json:"id"`
	PID      string `json:"pid"`
	Name     string `json:"name"`
	FileData []byte `json:"fileData"`
}

func GenerateDirectoryID(parentID, name string, version int) string {
	triple := fmt.Sprintf("%s:%s:%d", parentID, name, version)
	hash := sha256.Sum256([]byte(triple))
	return hex.EncodeToString(hash[:])
}
