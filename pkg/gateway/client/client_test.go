package gatewayclient

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/beam-cloud/airstore/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type testWorkerServiceServer struct {
	pb.UnimplementedWorkerServiceServer
}

func TestNewGatewayClientConnectsToLocalGateway(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	server := grpc.NewServer()
	pb.RegisterWorkerServiceServer(server, &testWorkerServiceServer{})
	defer server.Stop()

	go func() {
		_ = server.Serve(listener)
	}()

	client, err := NewGatewayClient(listener.Addr().String(), "")
	require.NoError(t, err)
	require.NoError(t, client.Close())
}

func TestNewGatewayClientDefersConnectionFailureUntilFirstRPC(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	client, err := NewGatewayClient(addr, "")
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	_, err = client.RegisterWorker(ctx, &RegisterWorkerRequest{
		Hostname: "worker-1",
		PoolName: "default",
		Cpu:      1000,
		Memory:   1024,
		Version:  "test",
	})
	require.Error(t, err)
}

func TestResolveAddressFamiliesForLocalhost(t *testing.T) {
	family := resolveAddressFamilies("127.0.0.1:1993")
	require.Equal(t, "ipv4", family)
}
