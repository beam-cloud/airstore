package cli

import (
	"context"
	"crypto/tls"
	"strings"

	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
		grpc.WithTransportCredentials(TransportCredentials(addr)),
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

// TransportCredentials returns TLS or insecure credentials based on the address.
// Uses TLS for port 443 or airstore.ai domains, insecure otherwise.
func TransportCredentials(addr string) credentials.TransportCredentials {
	if NeedsTLS(addr) {
		return credentials.NewTLS(&tls.Config{})
	}
	return insecure.NewCredentials()
}

// NeedsTLS returns true if the address requires TLS (port 443 or airstore.ai domain)
func NeedsTLS(addr string) bool {
	return strings.HasSuffix(addr, ":443") || strings.Contains(addr, ".airstore.ai")
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
