package auth

import (
	"context"
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
		log.Debug().Err(err).Msg("auth: invalid token")
		return ctx, status.Errorf(codes.Unauthenticated, "invalid token")
	}

	if info != nil {
		log.Debug().Str("type", string(info.TokenType)).Msg("auth: validated")
		return WithAuthInfo(ctx, info), nil
	}

	return ctx, status.Errorf(codes.Unauthenticated, "invalid token")
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
