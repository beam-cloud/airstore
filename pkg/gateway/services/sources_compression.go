package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/compression"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/sources"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

// readWithCompression checks the Redis cache, fetches raw content on miss,
// compresses with a timeout, records the access event, and dispatches an
// async cache write. The querySpec hash is folded into cache keys so that
// changing a smart query automatically invalidates stale content.
func (s *SourceService) readWithCompression(
	ctx context.Context,
	pctx *sources.ProviderContext,
	executor sources.QueryExecutor,
	integration, queryPath, filename, resultID, querySpec string,
	offset, length int64,
	strategyStr, session string,
) (*pb.SourceReadResponse, error) {
	wsExtId := auth.WorkspaceExtId(ctx)
	if session == "" {
		session = wsExtId
	}

	// Fold query-spec hash into the cache key so that editing a smart query
	// invalidates stale compressed content without an explicit flush.
	cacheResultID := resultID
	if querySpec != "" {
		h := sha256.Sum256([]byte(querySpec))
		cacheResultID = resultID + ":" + hex.EncodeToString(h[:8])
	}

	meta := compression.ContentMeta{
		Integration: integration,
		QueryPath:   queryPath,
		ResultID:    resultID,
		Filename:    filename,
	}

	buildEvent := func(content []byte, result *compression.CompressionResult, outcome compression.Outcome, errMsg string) instrumentation.AccessEvent {
		// Build canonical source reference: {integration}://{resultID}
		sourceURI := integration + "://" + resultID

		ev := instrumentation.AccessEvent{
			Timestamp:   time.Now().UnixMilli(),
			WorkspaceID: wsExtId,
			SessionID:   session,
			Path:        queryPath + "/" + filename,
			Integration: integration,
			SourceURI:   sourceURI,
			QueryPath:   queryPath,
			ResultID:    resultID,
			Strategy:    strategyStr,
			Outcome:     string(outcome),
			ErrorMsg:    errMsg,
		}
		if content != nil {
			ev.OriginalBytes = len(content)
		}
		if result != nil {
			ev.OriginalTokens = result.OriginalTokens
			ev.CompressedTokens = result.CompressedTokens
			ev.CompressedBytes = len(result.Data)
			ev.CompressionMs = result.DurationMs
		}
		return ev
	}

	// Check content cache
	if s.compressedStore != nil {
		if ptr := s.compressedStore.GetPointer(ctx, pctx.WorkspaceId, queryPath, cacheResultID, strategyStr); ptr != nil {
			if cached := s.compressedStore.GetContent(ctx, pctx.WorkspaceId, queryPath, cacheResultID, strategyStr); cached != nil {
				log.Debug().
					Str("strategy", strategyStr).Str("file", filename).
					Int("original_tokens", ptr.OriginalTokens).Int("compressed_tokens", ptr.CompressedTokens).
					Int("cached_bytes", len(cached)).
					Msg("compression: cache hit")

				// Populate context-carried stats from cached pointer.
				if st := compression.GetCompressionStats(ctx); st != nil {
					st.OriginalBytes = ptr.Size
					st.CompressedBytes = len(cached)
					st.OriginalTokens = ptr.OriginalTokens
					st.CompressedTokens = ptr.CompressedTokens
					st.Strategy = strategyStr
				}

				if s.recorder != nil {
					ev := buildEvent(nil, nil, compression.OutcomeCacheHit, "")
					ev.OriginalTokens = ptr.OriginalTokens
					ev.CompressedTokens = ptr.CompressedTokens
					ev.CompressedBytes = len(cached)
					ev.OriginalBytes = ptr.Size
					s.recorder.Record(ctx, ev)
				}
				return readSlice(cached, offset, length), nil
			}
			log.Debug().Str("strategy", strategyStr).Str("file", filename).
				Msg("compression: pointer hit but content expired, re-compressing")
		}
	}

	// Fetch raw content from cache or provider
	var rawContent []byte
	if content, err := s.fsStore.GetResultContent(ctx, pctx.WorkspaceId, queryPath, resultID); err == nil && len(content) > 0 {
		rawContent = content
	} else {
		content, err := executor.ReadResult(ctx, pctx, resultID)
		if err != nil {
			return &pb.SourceReadResponse{Ok: false, Error: err.Error()}, nil
		}
		rawContent = content
		if err := s.fsStore.StoreResultContent(ctx, pctx.WorkspaceId, queryPath, resultID, content); err != nil {
			log.Warn().Err(err).Str("path", queryPath).Str("result", resultID).Msg("failed to cache result content")
		}
	}

	// Compress.
	compressor := s.compressor
	if reqStrategy := compression.CompressionStrategy(strategyStr); reqStrategy.Valid() && reqStrategy != s.compressor.Name() {
		if perReq, err := compression.NewCompressor(reqStrategy, s.compressionCfg); err == nil {
			compressor = perReq
		}
	}

	timeout := s.compressionCfg.DefaultTimeout()
	compCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, compErr := compressor.Compress(compCtx, rawContent, meta)

	// Determine outcome and response data.
	var returnData []byte
	var outcome compression.Outcome
	var errMsg string

	switch {
	case compErr != nil && compCtx.Err() != nil:
		outcome, errMsg, returnData = compression.OutcomeTimeout, compCtx.Err().Error(), rawContent
	case compErr != nil:
		outcome, errMsg, returnData = compression.OutcomeError, compErr.Error(), rawContent
	case result.Outcome == compression.OutcomeSkipped:
		outcome, returnData = compression.OutcomeSkipped, rawContent
	default:
		outcome, returnData = compression.OutcomeCompressed, result.Data
	}

	// Populate context-carried stats so the HTTP handler can emit headers.
	compression.SetCompressionStats(ctx, len(rawContent), result, strategyStr)

	logEvent := log.Debug().
		Str("strategy", strategyStr).Str("file", filename).
		Str("outcome", string(outcome)).
		Int("original_bytes", len(rawContent))

	if result != nil {
		logEvent = logEvent.
			Int("original_tokens", result.OriginalTokens).
			Int("compressed_tokens", result.CompressedTokens).
			Int("compressed_bytes", len(result.Data)).
			Int64("duration_ms", result.DurationMs)
		if result.OriginalTokens > 0 {
			logEvent = logEvent.Float64("token_ratio_pct", 100.0*float64(result.CompressedTokens)/float64(result.OriginalTokens))
		}
	}
	if errMsg != "" {
		logEvent = logEvent.Str("error", errMsg)
	}
	logEvent.Msg("compression: result")

	if s.recorder != nil {
		s.recorder.Record(ctx, buildEvent(rawContent, result, outcome, errMsg))
	}

	// Async write to cache
	if s.compressedStore != nil && outcome == compression.OutcomeCompressed {
		data := make([]byte, len(result.Data))
		copy(data, result.Data)
		store := s.compressedStore
		wsID, qp, rID, strat := pctx.WorkspaceId, queryPath, cacheResultID, strategyStr
		ptr := &compression.CompressedPointer{
			OriginalTokens:   result.OriginalTokens,
			CompressedTokens: result.CompressedTokens,
			Strategy:         strategyStr,
			CreatedAt:        time.Now().Unix(),
			Size:             len(rawContent),
		}

		go func() {
			bgCtx := context.Background()
			if err := store.SetPointer(bgCtx, wsID, qp, rID, strat, ptr); err != nil {
				log.Warn().Err(err).Msg("compression: failed to cache pointer")
			}
			if err := store.SetContent(bgCtx, wsID, qp, rID, strat, data); err != nil {
				log.Warn().Err(err).Msg("compression: failed to cache content")
			}
		}()
	}

	return readSlice(returnData, offset, length), nil
}
