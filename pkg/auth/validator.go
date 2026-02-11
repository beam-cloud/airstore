package auth

import (
	"context"
	"crypto/subtle"
	"time"

	expirable "github.com/hashicorp/golang-lru/v2/expirable"

	"github.com/beam-cloud/airstore/pkg/types"
)

// TokenAuthorizer is implemented by repositories that can authorize tokens.
type TokenAuthorizer interface {
	AuthorizeToken(ctx context.Context, rawToken string) (*types.AuthInfo, error)
}

const (
	tokenCacheSize = 4096            // max cached tokens
	tokenCacheTTL  = 5 * time.Minute // cache entries expire after 5 minutes
)

// CompositeValidator checks cluster admin token first, then database tokens.
// Includes an in-memory LRU cache to avoid hitting Postgres + bcrypt on every request.
type CompositeValidator struct {
	clusterToken string
	authorizer   TokenAuthorizer
	cache        *expirable.LRU[string, *types.AuthInfo]
}

func NewCompositeValidator(clusterToken string, authorizer TokenAuthorizer) *CompositeValidator {
	return &CompositeValidator{
		clusterToken: clusterToken,
		authorizer:   authorizer,
		cache:        expirable.NewLRU[string, *types.AuthInfo](tokenCacheSize, nil, tokenCacheTTL),
	}
}

func (v *CompositeValidator) ValidateClusterToken(token string) bool {
	return v.clusterToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(v.clusterToken)) == 1
}

func (v *CompositeValidator) ValidateToken(ctx context.Context, token string) (*types.AuthInfo, error) {
	if v.authorizer == nil {
		return nil, nil
	}

	// Check cache first — avoids Postgres query + O(n) bcrypt comparisons.
	// Return a shallow copy so callers can't corrupt shared cached state.
	if info, ok := v.cache.Get(token); ok {
		cp := *info
		return &cp, nil
	}

	info, err := v.authorizer.AuthorizeToken(ctx, token)
	if err != nil {
		return nil, err
	}

	if info != nil {
		v.cache.Add(token, info)
	}

	return info, nil
}

// StaticValidator only checks cluster admin token (no database).
type StaticValidator struct {
	clusterToken string
}

func NewStaticValidator(clusterToken string) *StaticValidator {
	return &StaticValidator{clusterToken: clusterToken}
}

func (v *StaticValidator) ValidateClusterToken(token string) bool {
	return v.clusterToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(v.clusterToken)) == 1
}

func (v *StaticValidator) ValidateToken(ctx context.Context, token string) (*types.AuthInfo, error) {
	return nil, nil
}
