package vnode

import (
	"sync"
	"syscall"
)

var ownerMu sync.RWMutex

// Owner holds the uid/gid for all filesystem entries.
// Access through GetOwner() for thread-safe reads.
// Initialized to current user by default; override with InitOwner().
var Owner struct{ Uid, Gid uint32 }

func init() {
	// Default to current user so tests and code that doesn't call
	// InitOwner() still get sensible ownership (not root).
	Owner.Uid = uint32(syscall.Getuid())
	Owner.Gid = uint32(syscall.Getgid())
}

// InitOwner sets file ownership for this mount (thread-safe).
// If uid/gid are nil, defaults to the current user.
// To explicitly set root ownership, pass pointers to 0.
func InitOwner(uid, gid *uint32) {
	ownerMu.Lock()
	defer ownerMu.Unlock()
	if uid != nil {
		Owner.Uid = *uid
	} else {
		Owner.Uid = uint32(syscall.Getuid())
	}
	if gid != nil {
		Owner.Gid = *gid
	} else {
		Owner.Gid = uint32(syscall.Getgid())
	}
}

// GetOwner returns the current owner uid/gid (thread-safe).
func GetOwner() (uid, gid uint32) {
	ownerMu.RLock()
	defer ownerMu.RUnlock()
	return Owner.Uid, Owner.Gid
}
