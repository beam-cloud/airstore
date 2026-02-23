package gateway

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/tools"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestStartAsyncFailsWhenHTTPBindFails(t *testing.T) {
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer httpListener.Close()
	httpPort := httpListener.Addr().(*net.TCPAddr).Port

	gateway := newGatewayForBindTest(t, httpPort, freeTCPPort(t))
	err = gateway.StartAsync()
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to bind http listener")
}

func TestStartAsyncClosesHTTPListenerIfGRPCBindFails(t *testing.T) {
	grpcListener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	defer grpcListener.Close()
	grpcPort := grpcListener.Addr().(*net.TCPAddr).Port

	httpPort := freeTCPPort(t)
	gateway := newGatewayForBindTest(t, httpPort, grpcPort)
	err = gateway.StartAsync()
	if err == nil {
		gateway.Shutdown()
	}
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to bind grpc listener")

	// StartAsync should close the already-bound HTTP listener on gRPC bind failure.
	rebind, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", httpPort))
	require.NoError(t, err)
	_ = rebind.Close()
}

func newGatewayForBindTest(t *testing.T, httpPort, grpcPort int) *Gateway {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	return &Gateway{
		Config: types.AppConfig{
			Mode: types.ModeLocal,
			Gateway: types.GatewayConfig{
				AuthToken:       "test-token",
				ShutdownTimeout: 2 * time.Second,
				HTTP: types.HTTPConfig{
					Host: "127.0.0.1",
					Port: httpPort,
					CORS: types.CORSConfig{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET", "POST"},
						AllowedHeaders: []string{"*"},
					},
				},
				GRPC: types.GRPCConfig{
					Port:           grpcPort,
					MaxRecvMsgSize: 16,
					MaxSendMsgSize: 16,
				},
			},
			Scheduler: types.SchedulerConfig{Enabled: false},
		},
		ctx:            ctx,
		cancelFunc:     cancel,
		toolRegistry:   tools.NewRegistry(),
		sourceRegistry: sources.NewRegistry(),
		mcpManager:     tools.NewMCPManager(),
		oauthStore:     oauth.NewStore(nil, 0),
		oauthRegistry:  oauth.NewRegistry(),
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
