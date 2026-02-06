package filesystem

import (
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
)

func isMacSystemName(name string) bool {
	if strings.HasPrefix(name, ".metadata_") {
		return true
	}
	// FUSE-T SMB backend uses .fuse_hidden* for deferred deletes
	if strings.HasPrefix(name, ".fuse_hidden") {
		return true
	}
	switch name {
	// macOS system files
	case ".DS_Store", ".Spotlight-V100", ".Trashes", ".fseventsd", ".TemporaryItems", ".VolumeIcon.icns",
		".hidden", "DCIM", ".metadata_never_index_unless_rootfs", ".metadata_never_index", ".metadata_direct_scope_only":
		return true
	// VCS directories (these are never in a virtual filesystem)
	case ".git", ".gitignore", ".gitmodules", ".gitattributes", ".hg", ".svn":
		return true
	// Version manager / env files
	case ".tool-versions", ".node-version", ".ruby-version", ".python-version", ".nvmrc",
		".envrc", ".env":
		return true
	// Tool config files that ripgrep/editors probe at every directory level.
	// These will never exist in a virtual filesystem. User-content files like
	// CLAUDE.md are NOT listed here — those are handled by the dirChildren cache.
	case ".rgignore", ".ignore", "libinfo.dylib",
		".eslintrc", ".eslintrc.json", ".eslintrc.js",
		".prettierrc", ".prettierrc.json",
		".editorconfig", ".clang-format", ".clang-tidy":
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
	uid, gid := vnode.GetOwner()
	now := time.Now()
	return &FileInfo{
		Ino:   hashToIno(path),
		Size:  0,
		Mode:  syscall.S_IFREG | 0644,
		Nlink: 1,
		Uid:   uid,
		Gid:   gid,
		Atime: now,
		Mtime: now,
		Ctime: now,
	}
}
