package worker

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	gatewayRetryAttempts = 3
	gatewayRetryTimeout  = 10 * time.Second
)

// isTransientGRPCError returns true for gRPC status codes that indicate
// a transient failure — typically during gateway rollouts.
func isTransientGRPCError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Unauthenticated, codes.ResourceExhausted:
		return true
	}
	return false
}

// isNonRetriableGatewayError returns true for errors that should never be
// retried — the task/run is gone or the request is fundamentally invalid.
func isNonRetriableGatewayError(err error) bool {
	if err == nil {
		return false
	}
	switch status.Code(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.InvalidArgument, codes.PermissionDenied:
		return true
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "task not found") ||
		strings.Contains(lower, "run execution not found") ||
		strings.Contains(lower, "already finished")
}

// retryOnTransient executes fn up to gatewayRetryAttempts times with
// exponential backoff (1s, 2s, ...), retrying only when fn returns a
// transient gRPC error. Returns nil on success, the non-transient error
// immediately, or the last transient error after exhausting retries.
func retryOnTransient(ctx context.Context, fn func() error) error {
	var lastErr error
	for attempt := range gatewayRetryAttempts {
		if attempt > 0 {
			contextSleep(ctx, time.Duration(1<<(attempt-1))*time.Second)
		}
		if ctx.Err() != nil {
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return lastErr
		}
		lastErr = fn()
		if lastErr == nil || !isTransientGRPCError(lastErr) {
			return lastErr
		}
	}
	return lastErr
}

// contextSleep sleeps for d or until ctx is cancelled, whichever comes first.
func contextSleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}
