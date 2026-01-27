package cli

import (
	"context"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// Client wraps gRPC connections to the gateway
type Client struct {
	conn    *grpc.ClientConn
	Gateway pb.GatewayServiceClient
	Tools   pb.ToolServiceClient
}

// NewClient creates a new gRPC client
func NewClient(addr, token string) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if token != "" {
		opts = append(opts, grpc.WithUnaryInterceptor(authInterceptor(token)))
		opts = append(opts, grpc.WithStreamInterceptor(streamAuthInterceptor(token)))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:    conn,
		Gateway: pb.NewGatewayServiceClient(conn),
		Tools:   pb.NewToolServiceClient(conn),
	}, nil
}

// Close closes the gRPC connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// authInterceptor adds bearer token to unary calls
func authInterceptor(token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// streamAuthInterceptor adds bearer token to stream calls
func streamAuthInterceptor(token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
