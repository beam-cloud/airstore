//go:build !linux

package filesystem

import "errors"

var errNFSMountSyscallUnsupported = errors.New("mount(2) nfs unsupported on this platform")

func mountNFS(source, target, options string) error {
	return errNFSMountSyscallUnsupported
}
