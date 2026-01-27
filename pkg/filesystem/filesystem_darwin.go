//go:build darwin

package filesystem

import "os"

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
	return []string{
		"-o", "volname=Airstore",
		"-o", "local",
		"-o", "allow_other",
		"-o", "iosize=1048576", // 1MB I/O size (default is much smaller)
		"-o", "noapplexattr", // Disable Apple extended attributes
		"-o", "noappledouble", // Disable AppleDouble files (._*)
		"-o", "daemon_timeout=60", // Increase timeout
	}
}
