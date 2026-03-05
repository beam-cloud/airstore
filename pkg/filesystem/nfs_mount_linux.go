//go:build linux

package filesystem

import "golang.org/x/sys/unix"

func mountNFS(source, target, options string) error {
	return unix.Mount(source, target, "nfs", 0, options)
}
