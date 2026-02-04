package filesystem

import (
	"strings"
	"syscall"
	"time"
)

func isMacSystemName(name string) bool {
	if strings.HasPrefix(name, ".metadata_") {
		return true
	}
	switch name {
	case ".DS_Store", ".Spotlight-V100", ".Trashes", ".fseventsd", ".TemporaryItems", ".VolumeIcon.icns",
		".hidden", "DCIM", ".metadata_never_index_unless_rootfs", ".metadata_never_index", ".metadata_direct_scope_only",
		".git", ".gitignore", ".gitmodules", ".gitattributes", ".hg", ".svn",
		".tool-versions", ".node-version", ".ruby-version", ".python-version", ".nvmrc",
		".envrc", ".env":
		return true
	default:
		return false
	}
}

func isAppleDoubleName(name string) bool {
	return strings.HasPrefix(name, "._")
}

// isMacResourceName catches macOS resource files like "Icon\r".
func isMacResourceName(name string) bool {
	for _, r := range name {
		if r == '\r' || r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

func macPlaceholderInfo(path string) *FileInfo {
	now := time.Now()
	return &FileInfo{
		Ino:   hashToIno(path),
		Size:  0,
		Mode:  syscall.S_IFREG | 0644,
		Nlink: 1,
		Uid:   uint32(syscall.Getuid()),
		Gid:   uint32(syscall.Getgid()),
		Atime: now,
		Mtime: now,
		Ctime: now,
	}
}
