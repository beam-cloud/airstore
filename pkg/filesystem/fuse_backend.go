package filesystem

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/winfsp/cgofuse/fuse"
)

// FuseBackend mounts the filesystem using FUSE (via cgofuse).
// This is the default backend on all platforms where FUSE is available.
// On macOS it uses fuse-t; on Linux it uses libfuse.
type FuseBackend struct {
	host *fuse.FileSystemHost
}

// NewFuseBackend creates a new FUSE mount backend.
func NewFuseBackend() *FuseBackend {
	return &FuseBackend{}
}

func (b *FuseBackend) Mount(fs *Filesystem, mountPoint string) error {
	b.host = fuse.NewFileSystemHost(newAdapter(fs))
	opts := fs.mountOptions()

	if fs.verbose {
		log.Debug().Str("path", mountPoint).Msg("mounting filesystem via FUSE")
	}

	// Start FUSE trace reporting if enabled
	stopTrace := make(chan struct{})
	if fs.trace != nil {
		log.Info().Str("mount", mountPoint).Msg("fuse trace enabled (AIRSTORE_FUSE_TRACE=1)")
		go fs.trace.reportLoop(stopTrace, mountPoint)
	}

	ok := b.host.Mount(mountPoint, opts)

	if fs.trace != nil {
		close(stopTrace)
	}

	if !ok {
		return fmt.Errorf("FUSE mount failed")
	}
	return nil
}

func (b *FuseBackend) Unmount() error {
	if b.host == nil {
		return nil
	}
	_ = b.host.Unmount()
	return nil
}
