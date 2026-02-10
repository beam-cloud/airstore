package vnode

import (
	"sync/atomic"
	"syscall"
)

// ownerPacked stores uid in high 32 bits, gid in low 32 bits.
// Single atomic load/store — zero lock overhead on the hot path.
var ownerPacked atomic.Uint64

func init() {
	// Default to current user so tests and code that doesn't call
	// InitOwner() still get sensible ownership (not root).
	storeOwner(uint32(syscall.Getuid()), uint32(syscall.Getgid()))
}

func storeOwner(uid, gid uint32) {
	ownerPacked.Store(uint64(uid)<<32 | uint64(gid))
}

// InitOwner sets file ownership for this mount (thread-safe).
// If uid/gid are nil, defaults to the current user.
// To explicitly set root ownership, pass pointers to 0.
func InitOwner(uid, gid *uint32) {
	u := uint32(syscall.Getuid())
	g := uint32(syscall.Getgid())
	if uid != nil {
		u = *uid
	}
	if gid != nil {
		g = *gid
	}
	storeOwner(u, g)
}

// GetOwner returns the current owner uid/gid.
// Uses a single atomic load — no locks, no contention.
func GetOwner() (uid, gid uint32) {
	v := ownerPacked.Load()
	return uint32(v >> 32), uint32(v)
}
