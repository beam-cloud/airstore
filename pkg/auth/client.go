package auth

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ClientAuthInterceptor adds an authorization header to outgoing unary calls
func ClientAuthInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(newCtx, method, req, reply, cc, opts...)
	}
}

// ClientAuthStreamInterceptor adds an authorization header to outgoing streaming calls
func ClientAuthStreamInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		newCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(newCtx, desc, cc, method, opts...)
	}
}

// ClientRetryInterceptor retries calls on transient errors
func ClientRetryInterceptor(maxRetries int, delay time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error
		currentDelay := delay

		for i := 0; i < maxRetries; i++ {
			err := invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}

			lastErr = err
			st, ok := status.FromError(err)
			if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.ResourceExhausted) {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(currentDelay):
				currentDelay *= 2
			}
		}

		if lastErr != nil {
			return lastErr
		}
		return errors.New("max retries reached")
	}
}
