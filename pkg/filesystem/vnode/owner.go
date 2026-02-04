package vnode

import "syscall"

// Owner holds the uid/gid for all filesystem entries.
// Set via InitOwner() during mount initialization.
var Owner struct{ Uid, Gid uint32 }

// InitOwner sets file ownership for this mount.
// If uid/gid are 0, defaults to the current user.
func InitOwner(uid, gid uint32) {
	if uid != 0 {
		Owner.Uid = uid
	} else {
		Owner.Uid = uint32(syscall.Getuid())
	}
	if gid != 0 {
		Owner.Gid = gid
	} else {
		Owner.Gid = uint32(syscall.Getgid())
	}
}
