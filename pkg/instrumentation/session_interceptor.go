package instrumentation

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"
)

const sessionSeenTTL = 24 * time.Hour

// SessionInterceptor is a gRPC unary interceptor that emits a "mount.started"
// event the first time a new session ID is seen. It uses Redis SETNX for dedup.
type SessionInterceptor struct {
	redis    *common.RedisClient
	recorder EventRecorder
}

func NewSessionInterceptor(redis *common.RedisClient, recorder EventRecorder) *SessionInterceptor {
	return &SessionInterceptor{redis: redis, recorder: recorder}
}

func (s *SessionInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if s.redis == nil || s.recorder == nil {
			return handler(ctx, req)
		}

		md, ok := grpcmd.FromIncomingContext(ctx)
		if !ok {
			return handler(ctx, req)
		}

		vals := md.Get("x-airstore-session")
		if len(vals) == 0 || vals[0] == "" {
			return handler(ctx, req)
		}
		sessionID := vals[0]

		key := "session:seen:" + sessionID
		set, err := s.redis.SetNX(ctx, key, "1", sessionSeenTTL).Result()
		if err != nil || !set {
			// Either error or already seen — skip.
			return handler(ctx, req)
		}

		wsExtId := auth.WorkspaceExtId(ctx)
		s.recorder.Record(ctx, NewEvent("mount.started", map[string]any{
			"session_id":   sessionID,
			"workspace_id": wsExtId,
		}))

		return handler(ctx, req)
	}
}
