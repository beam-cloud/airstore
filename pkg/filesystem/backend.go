package filesystem

import (
	"errors"
	"os"
	"runtime"
)

// MountBackend abstracts how the filesystem is presented to the OS.
// Supported backends: "fuse" (default) and "nfs" (Linux fallback when FUSE is unavailable).
type MountBackend interface {
	// Mount presents the filesystem at the given mount point.
	// This call blocks until the filesystem is unmounted.
	Mount(fs *Filesystem, mountPoint string) error

	// Unmount requests the filesystem to be unmounted.
	Unmount() error
}

// Backend name constants.
const (
	BackendFUSE = "fuse"
	BackendNFS  = "nfs"
)

var (
	// ErrFUSEUnavailable indicates that the runtime cannot mount via FUSE.
	ErrFUSEUnavailable = errors.New("fuse unavailable")
	// ErrNFSHelperMissing indicates mount(8) could not find mount.nfs helper.
	ErrNFSHelperMissing = errors.New("nfs helper missing")
)

// NewBackend creates a MountBackend by name.
// Callers should resolve "" to a concrete name via defaultBackend() before calling.
func NewBackend(name string) MountBackend {
	switch name {
	case BackendNFS:
		return NewNFSBackend()
	default:
		return NewFuseBackend()
	}
}

// defaultBackend returns the platform-appropriate mount backend.
// On Linux, checks for /dev/fuse and falls back to NFS if unavailable.
// On all other platforms, defaults to FUSE.
// The AIRSTORE_MOUNT_BACKEND env var overrides auto-detection.
func defaultBackend() string {
	if v := os.Getenv("AIRSTORE_MOUNT_BACKEND"); v != "" {
		return v
	}
	if runtime.GOOS == "linux" {
		if _, err := os.Stat("/dev/fuse"); err != nil {
			return BackendNFS
		}
	}
	return BackendFUSE
}
