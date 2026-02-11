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

	// Give the server a moment to accept connections
	time.Sleep(100 * time.Millisecond)

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
	// Unmount the OS-level NFS mount first (stops kernel from sending requests).
	if b.mountPoint != "" {
		cmds := [][]string{
			{"umount", b.mountPoint},
			{"umount", "-f", b.mountPoint},
			{"umount", "-l", b.mountPoint}, // lazy unmount as last resort
		}
		for _, args := range cmds {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if exec.CommandContext(ctx, args[0], args[1:]...).Run() == nil {
				cancel()
				break
			}
			cancel()
		}
	}

	// Close the listener, which causes nfs.Serve to return.
	if b.listener != nil {
		b.listener.Close()
	}
	return nil
}

// execMount runs the mount(8) command to attach the NFS share.
func (b *NFSBackend) execMount(port int, mountPoint string) error {
	opts := fmt.Sprintf("port=%d,mountport=%d,nfsvers=3,noacl,tcp,nolock", port, port)
	cmd := exec.Command("mount", "-t", "nfs", "-o", opts, "127.0.0.1:/", mountPoint)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func isClosedConnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "use of closed network connection")
}
