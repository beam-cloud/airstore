package instrumentation

import (
	"context"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"
)

type collectingEventRecorder struct {
	mu     sync.Mutex
	events []Event
}

func (r *collectingEventRecorder) Record(_ context.Context, event Event) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *collectingEventRecorder) snapshot() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

func newTestRedis(t *testing.T) *common.RedisClient {
	t.Helper()

	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(s.Close)

	rdb, err := common.NewRedisClient(types.RedisConfig{
		Addrs: []string{s.Addr()},
		Mode:  types.RedisModeSingle,
	})
	if err != nil {
		t.Fatalf("new redis client: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	return rdb
}

func contextWithAuthAndSession(workspaceExtID, session string, includeHeader bool) context.Context {
	ctx := context.Background()
	if includeHeader {
		ctx = grpcmd.NewIncomingContext(ctx, grpcmd.Pairs("x-airstore-session", session))
	}
	return auth.WithAuthInfo(ctx, &types.AuthInfo{
		TokenType: types.TokenTypeWorkspaceMember,
		Workspace: &types.WorkspaceInfo{ExternalId: workspaceExtID},
	})
}

func invokeSessionInterceptor(t *testing.T, interceptor grpc.UnaryServerInterceptor, ctx context.Context) {
	t.Helper()
	_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test.Service/Call"}, func(ctx context.Context, req any) (any, error) {
		return &struct{}{}, nil
	})
	if err != nil {
		t.Fatalf("invoke interceptor: %v", err)
	}
}

func TestSessionInterceptorEmptySessionFallsBackToWorkspace(t *testing.T) {
	recorder := &collectingEventRecorder{}
	interceptor := NewSessionInterceptor(newTestRedis(t), recorder).Unary()

	invokeSessionInterceptor(t, interceptor, contextWithAuthAndSession("ws-1", "", true))

	events := recorder.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Type != "mount.started" {
		t.Fatalf("expected event type mount.started, got %q", events[0].Type)
	}
	if got := events[0].Properties["session_id"]; got != "ws-1" {
		t.Fatalf("expected session_id ws-1, got %#v", got)
	}
	if got := events[0].Properties["workspace_id"]; got != "ws-1" {
		t.Fatalf("expected workspace_id ws-1, got %#v", got)
	}
}

func TestSessionInterceptorDedupesWithinWorkspace(t *testing.T) {
	recorder := &collectingEventRecorder{}
	interceptor := NewSessionInterceptor(newTestRedis(t), recorder).Unary()
	ctx := contextWithAuthAndSession("ws-1", "session-abc", true)

	invokeSessionInterceptor(t, interceptor, ctx)
	invokeSessionInterceptor(t, interceptor, ctx)

	events := recorder.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 deduped event, got %d", len(events))
	}
}

func TestSessionInterceptorDoesNotCollideAcrossWorkspaces(t *testing.T) {
	recorder := &collectingEventRecorder{}
	interceptor := NewSessionInterceptor(newTestRedis(t), recorder).Unary()

	invokeSessionInterceptor(t, interceptor, contextWithAuthAndSession("ws-1", "shared-session", true))
	invokeSessionInterceptor(t, interceptor, contextWithAuthAndSession("ws-2", "shared-session", true))

	events := recorder.snapshot()
	if len(events) != 2 {
		t.Fatalf("expected 2 events across workspaces, got %d", len(events))
	}
	if got := events[0].Properties["workspace_id"]; got != "ws-1" {
		t.Fatalf("expected first workspace ws-1, got %#v", got)
	}
	if got := events[1].Properties["workspace_id"]; got != "ws-2" {
		t.Fatalf("expected second workspace ws-2, got %#v", got)
	}
}

func TestSessionInterceptorSkipsWhenHeaderMissing(t *testing.T) {
	recorder := &collectingEventRecorder{}
	interceptor := NewSessionInterceptor(newTestRedis(t), recorder).Unary()

	invokeSessionInterceptor(t, interceptor, contextWithAuthAndSession("ws-1", "", false))

	events := recorder.snapshot()
	if len(events) != 0 {
		t.Fatalf("expected 0 events, got %d", len(events))
	}
}
