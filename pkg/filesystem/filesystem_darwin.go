//go:build darwin

package filesystem

import (
	"os"
	"strings"
)

func init() {
	if os.Getenv("CGOFUSE_LIBFUSE_PATH") != "" {
		return
	}

	// Search common FUSE-T installation locations
	// Prefer FUSE-T over macFUSE (no kernel extension required)
	fuseTLibPaths := []string{
		"/opt/homebrew/lib/libfuse-t.dylib",           // Homebrew on Apple Silicon
		"/usr/local/lib/libfuse-t.dylib",              // Homebrew on Intel / standard install
		"/Library/Frameworks/fuse-t.framework/fuse-t", // Framework installation
	}

	for _, path := range fuseTLibPaths {
		if _, err := os.Stat(path); err == nil {
			os.Setenv("CGOFUSE_LIBFUSE_PATH", path)
			return
		}
	}
}

func (f *Filesystem) mountOptions() []string {
	opts := []string{
		"-o", "volname=Airstore",
		"-o", "local",
		"-o", "allow_other",
		"-o", "iosize=1048576",     // 1MB I/O size (default is much smaller)
		"-o", "daemon_timeout=120", // 2 minute timeout for slow API calls
	}

	// FUSE-T: Use SMB backend to avoid NFS "not responding" disconnects.
	isFuseT := strings.Contains(os.Getenv("CGOFUSE_LIBFUSE_PATH"), "libfuse-t") ||
		strings.Contains(os.Getenv("CGOFUSE_LIBFUSE_PATH"), "fuse-t.framework")
	if isFuseT {
		opts = append(opts, "-o", "backend=smb")
		// FUSE-T supports rwsize (power-of-two). Increase to 1MB for fewer round trips.
		opts = append(opts, "-o", "rwsize=1048576")
	}

	return opts
}
