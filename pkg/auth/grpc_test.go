package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMapValidationError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{
			name: "invalid token maps to unauthenticated",
			err:  errors.New("invalid token"),
			code: codes.Unauthenticated,
		},
		{
			name: "expired token maps to unauthenticated",
			err:  errors.New("token expired"),
			code: codes.Unauthenticated,
		},
		{
			name: "context canceled maps to canceled",
			err:  context.Canceled,
			code: codes.Canceled,
		},
		{
			name: "deadline exceeded maps to deadline exceeded",
			err:  context.DeadlineExceeded,
			code: codes.DeadlineExceeded,
		},
		{
			name: "backend failure maps to unavailable",
			err:  errors.New("query tokens: dial tcp: connection refused"),
			code: codes.Unavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.code, status.Code(mapValidationError(tc.err)))
		})
	}
}

func TestIsCredentialValidationError(t *testing.T) {
	require.True(t, isCredentialValidationError(errors.New("invalid token")))
	require.True(t, isCredentialValidationError(errors.New("token expired")))
	require.False(t, isCredentialValidationError(errors.New("dial tcp: connection refused")))
	require.False(t, isCredentialValidationError(context.DeadlineExceeded))
}
