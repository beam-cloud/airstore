package auth

import (
	"context"
	"errors"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TokenValidator validates tokens and returns auth info.
type TokenValidator interface {
	ValidateClusterToken(token string) bool
	ValidateToken(ctx context.Context, token string) (*types.AuthInfo, error)
}

// GRPCInterceptor provides authentication interceptors for gRPC.
type GRPCInterceptor struct {
	validator     TokenValidator
	publicMethods map[string]bool
}

func NewGRPCInterceptor(validator TokenValidator) *GRPCInterceptor {
	return &GRPCInterceptor{
		validator: validator,
		publicMethods: map[string]bool{
			"/grpc.health.v1.Health/Check": true,
		},
	}
}

func (i *GRPCInterceptor) authenticate(ctx context.Context) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx, nil
	}

	authHeaders := md.Get("authorization")
	if len(authHeaders) == 0 {
		return ctx, nil
	}

	token := strings.TrimPrefix(authHeaders[0], "Bearer ")
	if token == "" {
		return ctx, nil
	}

	if i.validator.ValidateClusterToken(token) {
		log.Debug().Msg("auth: cluster admin")
		return WithAuthInfo(ctx, &types.AuthInfo{TokenType: types.TokenTypeClusterAdmin}), nil
	}

	info, err := i.validator.ValidateToken(ctx, token)
	if err != nil {
		statusErr := mapValidationError(err)
		if status.Code(statusErr) == codes.Unavailable {
			log.Warn().Err(err).Msg("auth backend unavailable during token validation")
		} else {
			log.Debug().Err(err).Msg("auth: invalid token")
		}
		return ctx, statusErr
	}

	if info != nil {
		return WithAuthInfo(ctx, info), nil
	}

	return ctx, status.Errorf(codes.Unauthenticated, "invalid token")
}

func mapValidationError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "auth validation timeout")
	case isCredentialValidationError(err):
		return status.Error(codes.Unauthenticated, "invalid token")
	default:
		return status.Error(codes.Unavailable, "authentication backend unavailable")
	}
}

func (i *GRPCInterceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if i.publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}
		ctx, err := i.authenticate(ctx)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (i *GRPCInterceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if i.publicMethods[info.FullMethod] {
			return handler(srv, stream)
		}
		ctx, err := i.authenticate(stream.Context())
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: stream, ctx: ctx})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context { return w.ctx }
