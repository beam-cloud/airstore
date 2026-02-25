package instrumentation

import (
	"context"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/types"
	"google.golang.org/grpc"
	grpcmd "google.golang.org/grpc/metadata"
)

// pathExtractor is implemented by gRPC request types that carry a file path.
type pathExtractor interface {
	GetPath() string
}

// AccessLogInterceptor is a gRPC server-side unary interceptor that records
// access events for every successful Read RPC. It is intentionally generic:
// any service whose Read method accepts a request with a `path` field will
// be recorded. The interceptor only fires when the caller supplies the
// x-airstore-session gRPC metadata header.
//
// This is the single instrumentation point for all file reads regardless of
// which service handles them (SourceService, ContextService, etc.).
type AccessLogInterceptor struct {
	recorder AccessRecorder
}

func NewAccessLogInterceptor(recorder AccessRecorder) *AccessLogInterceptor {
	return &AccessLogInterceptor{recorder: recorder}
}

// Unary returns a grpc.UnaryServerInterceptor suitable for use with
// grpc.ChainUnaryInterceptor. It should be placed AFTER the auth interceptor
// so that auth context (workspace, member) is available.
func (a *AccessLogInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Only intercept Read methods.
		if !isReadMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		// Only record when the session header is present (mount passed --access-log).
		// The header may be empty, meaning "use workspace ID as session".
		if !hasMetaKey(ctx, "x-airstore-session") {
			return handler(ctx, req)
		}

		// Mount-originated reads are recorded by the mount collector and flushed
		// via AccessLogService. Skip interceptor logging to avoid duplicates.
		if metaVal(ctx, "x-airstore-access-origin") == "fuse" {
			return handler(ctx, req)
		}

		// Let the compression middleware record its own richer events.
		if metaVal(ctx, "x-airstore-compression") != "" {
			return handler(ctx, req)
		}

		resp, err := handler(ctx, req)
		if err != nil {
			return resp, err
		}

		// Extract path from request (all Read RPCs have a path field).
		var filePath string
		if pe, ok := req.(pathExtractor); ok {
			filePath = pe.GetPath()
		}

		// Derive the service prefix for the path (e.g. "sources", "skills").
		svcPrefix := servicePrefix(info.FullMethod)

		wsExtId := auth.WorkspaceExtId(ctx)
		session := metaVal(ctx, "x-airstore-session")
		if session == "" {
			session = wsExtId
		}

		fullPath := filePath
		if svcPrefix != "" {
			fullPath = svcPrefix + "/" + filePath
		}
		if types.IsHiddenDotPath(fullPath) {
			return resp, err
		}

		a.recorder.Record(ctx, AccessEvent{
			Timestamp:   time.Now().UnixMilli(),
			WorkspaceID: wsExtId,
			SessionID:   session,
			Path:        fullPath,
			Outcome:     "passthrough",
		})

		return resp, err
	}
}

// isReadMethod returns true for gRPC methods named "Read" (not ReadDir, Readlink).
func isReadMethod(fullMethod string) bool {
	// fullMethod is e.g. "/sources.SourceService/Read"
	i := strings.LastIndex(fullMethod, "/")
	if i < 0 {
		return false
	}
	return fullMethod[i+1:] == "Read"
}

// servicePrefix maps a gRPC full method to a filesystem path prefix.
func servicePrefix(fullMethod string) string {
	switch {
	case strings.Contains(fullMethod, "SourceService"):
		return "sources"
	case strings.Contains(fullMethod, "ContextService"):
		// The ContextService is used for /skills and /memory; the path
		// itself already carries the prefix (e.g. "skills/AGENTS.md").
		return ""
	default:
		return ""
	}
}

// hasMetaKey returns true if the gRPC metadata key exists (even if empty).
func hasMetaKey(ctx context.Context, key string) bool {
	md, ok := grpcmd.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	return len(md.Get(key)) > 0
}

// metaVal reads the first value of a gRPC metadata key.
func metaVal(ctx context.Context, key string) string {
	md, ok := grpcmd.FromIncomingContext(ctx)
	if !ok {
		return ""
	}
	if vals := md.Get(key); len(vals) > 0 {
		return vals[0]
	}
	return ""
}
