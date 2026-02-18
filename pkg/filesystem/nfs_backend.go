package filesystem

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	nfs "github.com/willscott/go-nfs"
	nfshelper "github.com/willscott/go-nfs/helpers"
)

// NFSBackend mounts the filesystem by running a userspace NFSv3 server on
// loopback and mounting it via the kernel NFS client. This is the fallback
// for Linux environments where FUSE (/dev/fuse) is not available.
type NFSBackend struct {
	listener   net.Listener
	mountPoint string
}

// NewNFSBackend creates a new NFS mount backend.
func NewNFSBackend() *NFSBackend {
	return &NFSBackend{}
}

func (b *NFSBackend) Mount(fs *Filesystem, mountPoint string) error {
	// Start NFS server on loopback with a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("NFS listen: %w", err)
	}
	b.listener = listener
	b.mountPoint = mountPoint

	port := listener.Addr().(*net.TCPAddr).Port
	log.Info().Int("port", port).Str("mount", mountPoint).Msg("starting NFS server")

	// Create billy filesystem adapter wrapping our Filesystem
	bfs := newBillyFS(fs)

	// Create NFS handler (no auth — loopback only)
	handler := nfshelper.NewNullAuthHandler(bfs)
	cacheHandler := nfshelper.NewCachingHandler(handler, 1024)

	// Start NFS server in background
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- nfs.Serve(listener, cacheHandler)
	}()

	// Wait until the NFS server is accepting connections before mounting.
	addr := listener.Addr().String()
	for i := 0; i < 20; i++ {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Mount the NFS share via the kernel NFS client
	if err := b.execMount(port, mountPoint); err != nil {
		listener.Close()
		return fmt.Errorf("NFS mount: %w", err)
	}

	log.Info().Int("port", port).Str("mount", mountPoint).Msg("NFS filesystem mounted")

	// Block until the NFS server exits (triggered by Unmount closing the listener)
	err = <-serverDone

	// "use of closed network connection" is expected during normal shutdown
	if err != nil && !isClosedConnError(err) {
		return err
	}
	return nil
}

func (b *NFSBackend) Unmount() error {
	var umountErr error

	// Unmount the OS-level NFS mount first (stops kernel from sending requests).
	if b.mountPoint != "" {
		cmds := [][]string{
			{"umount", b.mountPoint},
			{"umount", "-f", b.mountPoint},
			{"umount", "-l", b.mountPoint}, // lazy unmount as last resort
		}
		umountErr = fmt.Errorf("all umount attempts failed for %s", b.mountPoint)
		for _, args := range cmds {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if exec.CommandContext(ctx, args[0], args[1:]...).Run() == nil {
				cancel()
				umountErr = nil
				break
			}
			cancel()
		}
		if umountErr != nil {
			log.Warn().Str("mount", b.mountPoint).Msg("all umount attempts failed; mount may still be active")
		}
	}

	// Close the listener, which causes nfs.Serve to return.
	// Done unconditionally — even if umount failed, killing the server
	// ensures the mount becomes stale rather than serving bad data.
	if b.listener != nil {
		b.listener.Close()
	}
	return umountErr
}

// execMount runs the mount(8) command to attach the NFS share.
func (b *NFSBackend) execMount(port int, mountPoint string) error {
	opts := fmt.Sprintf("port=%d,mountport=%d,nfsvers=3,noacl,tcp,nolock", port, port)
	source := "127.0.0.1:/"

	// Prefer mount(2) first. This avoids relying on distro-provided mount.nfs
	// helper binaries that are often missing on minimal Linux images.
	mountSysErr := mountNFS(source, mountPoint, opts)
	if mountSysErr == nil {
		return nil
	}

	cmd := exec.Command("mount", "-t", "nfs", "-o", opts, source, mountPoint)
	out, err := cmd.CombinedOutput()
	if err != nil {
		msg := strings.TrimSpace(string(out))
		if msg == "" {
			msg = err.Error()
		}
		if isNFSHelperMissingMessage(msg) {
			return fmt.Errorf(
				"%w: %s (install nfs mount helpers, e.g. `apt-get install nfs-common`)",
				ErrNFSHelperMissing,
				msg,
			)
		}
		return fmt.Errorf("%s: %s (mount(2) error: %v)", err, msg, mountSysErr)
	}
	return nil
}

func isClosedConnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}

func isNFSHelperMissingMessage(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "mount.nfs") ||
		(strings.Contains(lower, "helper program") && strings.Contains(lower, "mount"))
}
