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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

var legacyAuthKey = "auth"

// AuthInfo holds authentication information for a request (legacy)
type AuthInfo struct {
	Token string
}

// AuthInfoFromContext extracts AuthInfo from context (legacy)
func AuthInfoFromContext(ctx context.Context) (*AuthInfo, bool) {
	authInfo, ok := ctx.Value(legacyAuthKey).(*AuthInfo)
	return authInfo, ok
}

// TokenValidator validates tokens and returns identity information
type TokenValidator interface {
	// ValidateToken checks if a token is valid. Returns (valid, isGatewayToken)
	ValidateToken(token string) (bool, bool)
	// ValidateWorkspaceToken validates a workspace token and returns identity
	ValidateWorkspaceToken(ctx context.Context, token string) *types.TokenValidationResult
}

// StaticValidator validates against a static gateway token only
type StaticValidator struct {
	token string
}

func NewStaticValidator(token string) *StaticValidator {
	return &StaticValidator{token: token}
}

func (v *StaticValidator) ValidateToken(token string) (bool, bool) {
	if v.token == "" {
		return true, true // No token configured = auth disabled
	}
	valid := token == v.token
	return valid, valid
}

func (v *StaticValidator) ValidateWorkspaceToken(ctx context.Context, token string) *types.TokenValidationResult {
	return nil
}

// WorkspaceValidator validates workspace tokens from the database
type WorkspaceValidator interface {
	ValidateToken(ctx context.Context, token string) (*types.TokenValidationResult, error)
}

// CompositeValidator validates both gateway and workspace tokens
type CompositeValidator struct {
	gatewayToken string
	workspace    WorkspaceValidator
}

func NewCompositeValidator(gatewayToken string, workspace WorkspaceValidator) *CompositeValidator {
	return &CompositeValidator{gatewayToken: gatewayToken, workspace: workspace}
}

func (v *CompositeValidator) ValidateToken(token string) (bool, bool) {
	// If gateway token is configured and matches, it's a gateway auth
	if v.gatewayToken != "" && token == v.gatewayToken {
		return true, true
	}
	// Otherwise, treat as a potential workspace token (will be validated in ValidateWorkspaceToken)
	// If no gateway token configured, we still allow workspace tokens through
	return true, false
}

func (v *CompositeValidator) ValidateWorkspaceToken(ctx context.Context, token string) *types.TokenValidationResult {
	if v.workspace == nil {
		return nil
	}
	result, err := v.workspace.ValidateToken(ctx, token)
	if err != nil {
		return nil
	}
	return result
}

// Interceptor provides gRPC authentication
type Interceptor struct {
	validator     TokenValidator
	publicMethods map[string]bool
}

func NewInterceptor(validator TokenValidator) *Interceptor {
	return &Interceptor{
		validator: validator,
		publicMethods: map[string]bool{
			"/grpc.health.v1.Health/Check": true,
		},
	}
}

func (i *Interceptor) authenticate(ctx context.Context, md metadata.MD) (context.Context, bool) {
	// No authorization header - try empty token (gateway auth)
	if len(md["authorization"]) == 0 {
		if valid, isGateway := i.validator.ValidateToken(""); valid {
			rc := &RequestContext{IsGatewayAuth: isGateway}
			ctx = WithContext(ctx, rc)
			ctx = context.WithValue(ctx, legacyAuthKey, &AuthInfo{Token: ""})
			return ctx, true
		}
		return ctx, false
	}

	token := strings.TrimPrefix(md["authorization"][0], "Bearer ")
	valid, isGateway := i.validator.ValidateToken(token)
	if !valid {
		log.Debug().Msg("auth: invalid token")
		return ctx, false
	}

	var rc *RequestContext
	if isGateway {
		rc = &RequestContext{IsGatewayAuth: true}
	} else {
		result := i.validator.ValidateWorkspaceToken(ctx, token)
		if result == nil {
			// Token didn't match gateway and workspace validation failed
			return ctx, false
		}

		rc = &RequestContext{
			WorkspaceId:   result.WorkspaceId,
			WorkspaceExt:  result.WorkspaceExt,
			WorkspaceName: result.WorkspaceName,
			MemberId:      result.MemberId,
			MemberExt:     result.MemberExt,
			MemberEmail:   result.MemberEmail,
			MemberRole:    result.MemberRole,
			IsGatewayAuth: false,
		}
	}

	ctx = WithContext(ctx, rc)
	ctx = context.WithValue(ctx, legacyAuthKey, &AuthInfo{Token: token})
	return ctx, true
}

// Unary returns a unary server interceptor for authentication
func (i *Interceptor) Unary() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if i.publicMethods[info.FullMethod] {
			return handler(ctx, req)
		}

		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		ctx, valid := i.authenticate(ctx, md)
		if !valid {
			return nil, status.Errorf(codes.Unauthenticated, "invalid token")
		}

		return handler(ctx, req)
	}
}

// Stream returns a stream server interceptor for authentication
func (i *Interceptor) Stream() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if i.publicMethods[info.FullMethod] {
			return handler(srv, stream)
		}

		md, ok := metadata.FromIncomingContext(stream.Context())
		if !ok {
			return status.Errorf(codes.Unauthenticated, "missing metadata")
		}

		ctx, valid := i.authenticate(stream.Context(), md)
		if !valid {
			return status.Errorf(codes.Unauthenticated, "invalid token")
		}

		return handler(srv, &wrappedStream{ServerStream: stream, ctx: ctx})
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (w *wrappedStream) Context() context.Context {
	return w.ctx
}
