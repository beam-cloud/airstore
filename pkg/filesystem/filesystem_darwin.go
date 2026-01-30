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
	// Prefer FUSE-T over macFUSE (no kernel extension required)
	if _, err := os.Stat("/usr/local/lib/libfuse-t.dylib"); err == nil {
		os.Setenv("CGOFUSE_LIBFUSE_PATH", "/usr/local/lib/libfuse-t.dylib")
	}
}

func (f *Filesystem) mountOptions() []string {
	opts := []string{
		"-o", "volname=Airstore",
		"-o", "local",
		"-o", "allow_other",
		"-o", "iosize=1048576", // 1MB I/O size (default is much smaller)
		"-o", "noapplexattr",   // Disable Apple extended attributes
		"-o", "noappledouble",  // Disable AppleDouble files (._*)
		"-o", "daemon_timeout=120", // 2 minute timeout for slow API calls
	}

	// FUSE-T supports selecting the transport backend. Default is NFS; use SMB to
	// avoid macOS NFS client "not responding / alive again" disconnects.
	// Guarded so we don't pass an unknown option to macFUSE.
	if strings.Contains(os.Getenv("CGOFUSE_LIBFUSE_PATH"), "libfuse-t") {
		opts = append(opts, "-o", "backend=smb")
	}

	return opts
}
