package services

import (
	"context"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	pb "github.com/beam-cloud/airstore/proto"
)

const accessIngestDedupeTTL = 15 * time.Minute

// AccessIngestService accepts batched logical-read events emitted by mounts.
// Events are normalized with auth-derived workspace/session fields and persisted
// via the shared recorder.
type AccessIngestService struct {
	pb.UnimplementedAccessLogServiceServer
	recorder instrumentation.AccessRecorder
	rdb      *common.RedisClient
}

func NewAccessIngestService(recorder instrumentation.AccessRecorder, rdb *common.RedisClient) *AccessIngestService {
	return &AccessIngestService{recorder: recorder, rdb: rdb}
}

func (s *AccessIngestService) IngestAccessEvents(ctx context.Context, req *pb.IngestAccessEventsRequest) (*pb.IngestAccessEventsResponse, error) {
	wsExtID := auth.WorkspaceExtId(ctx)
	if wsExtID == "" {
		return &pb.IngestAccessEventsResponse{Ok: false, Error: "workspace not found in auth context"}, nil
	}
	if req == nil || len(req.Events) == 0 {
		return &pb.IngestAccessEventsResponse{Ok: true, Accepted: 0}, nil
	}

	var accepted int32
	for _, in := range req.Events {
		if in == nil {
			continue
		}
		if s.rdb != nil && in.EventId != "" {
			key := "access:dedupe:" + in.EventId
			ok, err := s.rdb.SetNX(ctx, key, "1", accessIngestDedupeTTL).Result()
			if err == nil && !ok {
				continue
			}
		}

		session := in.SessionId
		if session == "" {
			session = wsExtID
		}

		ev := instrumentation.AccessEvent{
			EventID:          in.EventId,
			Timestamp:        in.Ts,
			WorkspaceID:      wsExtID,
			SessionID:        session,
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

		if s.recorder != nil {
			_ = s.recorder.Record(ctx, ev)
			accepted++
		}
	}

	return &pb.IngestAccessEventsResponse{Ok: true, Accepted: accepted}, nil
}
