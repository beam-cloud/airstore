package auth

import (
	"context"

	"github.com/beam-cloud/airstore/pkg/types"
)

// TokenAuthorizer is implemented by repositories that can authorize tokens.
type TokenAuthorizer interface {
	AuthorizeToken(ctx context.Context, rawToken string) (*types.AuthInfo, error)
}

// CompositeValidator checks cluster admin token first, then database tokens.
type CompositeValidator struct {
	clusterToken string
	authorizer   TokenAuthorizer
}

func NewCompositeValidator(clusterToken string, authorizer TokenAuthorizer) *CompositeValidator {
	return &CompositeValidator{clusterToken: clusterToken, authorizer: authorizer}
}

func (v *CompositeValidator) ValidateClusterToken(token string) bool {
	return v.clusterToken != "" && token == v.clusterToken
}

func (v *CompositeValidator) ValidateToken(ctx context.Context, token string) (*types.AuthInfo, error) {
	if v.authorizer == nil {
		return nil, nil
	}
	return v.authorizer.AuthorizeToken(ctx, token)
}

// StaticValidator only checks cluster admin token (no database).
type StaticValidator struct {
	clusterToken string
}

func NewStaticValidator(clusterToken string) *StaticValidator {
	return &StaticValidator{clusterToken: clusterToken}
}

func (v *StaticValidator) ValidateClusterToken(token string) bool {
	return v.clusterToken == "" || token == v.clusterToken
}

func (v *StaticValidator) ValidateToken(ctx context.Context, token string) (*types.AuthInfo, error) {
	return nil, nil
}
