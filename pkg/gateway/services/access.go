package services

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	pb "github.com/beam-cloud/airstore/proto"
)

const accessEventDedupeTTL = 15 * time.Minute

// AccessService ingests batched logical-read events emitted by mounts.
// Events are normalized with auth-derived workspace/session fields and persisted
// via the shared recorder.
type AccessService struct {
	pb.UnimplementedAccessLogServiceServer
	recorder instrumentation.AccessRecorder
	rdb      *common.RedisClient
}

func NewAccessService(recorder instrumentation.AccessRecorder, rdb *common.RedisClient) *AccessService {
	return &AccessService{recorder: recorder, rdb: rdb}
}

func (s *AccessService) IngestAccessEvents(ctx context.Context, req *pb.IngestAccessEventsRequest) (*pb.IngestAccessEventsResponse, error) {
	workspaceID := auth.WorkspaceExtId(ctx)
	if workspaceID == "" {
		return &pb.IngestAccessEventsResponse{Ok: false, Error: "workspace not found in auth context"}, nil
	}
	if req == nil || len(req.Events) == 0 {
		return &pb.IngestAccessEventsResponse{Ok: true, Accepted: 0}, nil
	}

	var accepted int32
	for _, in := range req.Events {
		if in == nil || !s.acceptEvent(ctx, in) {
			continue
		}
		if s.recorder == nil {
			continue
		}
		_ = s.recorder.Record(ctx, normalizeAccessEvent(workspaceID, in))
		accepted++
	}

	return &pb.IngestAccessEventsResponse{Ok: true, Accepted: accepted}, nil
}

func (s *AccessService) acceptEvent(ctx context.Context, in *pb.AccessLogEvent) bool {
	if s.rdb == nil || in.EventId == "" {
		return true
	}
	key := "access:dedupe:" + in.EventId
	ok, err := s.rdb.SetNX(ctx, key, "1", accessEventDedupeTTL).Result()
	return err != nil || ok
}

func normalizeAccessEvent(workspaceID string, in *pb.AccessLogEvent) instrumentation.AccessEvent {
	sessionID := in.SessionId
	if sessionID == "" {
		sessionID = workspaceID
	}

	ev := instrumentation.AccessEvent{
		EventID:          in.EventId,
		Timestamp:        in.Ts,
		WorkspaceID:      workspaceID,
		SessionID:        sessionID,
		Path:             in.Path,
		CacheSource:      in.CacheSource,
		Offset:           in.Offset,
		RequestedBytes:   int(in.RequestedBytes),
		ReadBytes:        int(in.ReadBytes),
		LatencyMs:        in.LatencyMs,
		Integration:      in.Integration,
		SourceURI:        in.SourceUri,
		QueryPath:        in.QueryPath,
		ResultID:         in.ResultId,
		OriginalBytes:    int(in.OriginalBytes),
		CompressedBytes:  int(in.CompressedBytes),
		OriginalTokens:   int(in.OriginalTokens),
		CompressedTokens: int(in.CompressedTokens),
		Strategy:         in.Strategy,
		Outcome:          in.Outcome,
		CompressionMs:    in.CompressionMs,
		ErrorMsg:         in.ErrorMsg,
		MountID:          in.MountId,
		AccessOrigin:     in.AccessOrigin,
	}

	if ev.AccessOrigin == "" {
		ev.AccessOrigin = "fuse"
	}
	if ev.Outcome == "" {
		if ev.ErrorMsg != "" {
			ev.Outcome = "error"
		} else {
			ev.Outcome = "passthrough"
		}
	}
	return ev
}
