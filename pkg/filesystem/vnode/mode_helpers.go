package vnode

import "syscall"

// ChmodNode is an optional vnode capability for chmod support.
// Filesystem delegates chmod calls to nodes implementing this interface.
type ChmodNode interface {
	Chmod(path string, mode uint32) error
}

func sanitizeNodeMode(mode uint32, fileType uint32, defaultPerm uint32) uint32 {
	if mode == 0 {
		return fileType | defaultPerm
	}
	if requestedType := mode & syscall.S_IFMT; requestedType != 0 {
		fileType = requestedType
	}
	return fileType | (mode & 07777)
}

func withNodeFileType(mode uint32, fileType uint32) uint32 {
	return fileType | (mode & 07777)
}

// normalizeRuntimeWritableMode makes writable filesystems permissive enough for
// sandbox/gofer default_permissions checks while preserving execute bits.
func normalizeRuntimeWritableMode(mode uint32) uint32 {
	fileType := mode & syscall.S_IFMT
	switch fileType {
	case syscall.S_IFDIR:
		return syscall.S_IFDIR | 0777
	case syscall.S_IFLNK:
		return syscall.S_IFLNK | 0777
	case 0:
		fileType = syscall.S_IFREG
	}
	return fileType | 0666 | (mode & 0111)
}
