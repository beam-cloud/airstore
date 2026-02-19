package filesystem

import (
	"errors"
	"fmt"
	"testing"
)

func TestShouldAttemptFallback(t *testing.T) {
	fs := &Filesystem{backendAuto: true}

	fuseUnavailable := fmt.Errorf("%w: cannot find FUSE", ErrFUSEUnavailable)
	if !fs.shouldAttemptFallback(BackendFUSE, fuseUnavailable) {
		t.Fatalf("expected fallback for auto FUSE unavailable error")
	}
	if fs.shouldAttemptFallback(BackendFUSE, errors.New("other mount error")) {
		t.Fatalf("did not expect fallback for non-FUSE-unavailable error")
	}
	if fs.shouldAttemptFallback(BackendNFS, fuseUnavailable) {
		t.Fatalf("did not expect fallback when primary backend is not FUSE")
	}

	fs.backendAuto = false
	if fs.shouldAttemptFallback(BackendFUSE, fuseUnavailable) {
		t.Fatalf("did not expect fallback when backend is explicitly configured")
	}
}
