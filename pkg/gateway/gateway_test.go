package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/tools"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/labstack/echo/v4"
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

type fakeSourceWatchRegistrarTarget struct {
	registrar orchestration.SourceWatchRegistrar
}

func (f *fakeSourceWatchRegistrarTarget) SetSourceWatchRegistrar(registrar orchestration.SourceWatchRegistrar) {
	f.registrar = registrar
}

type fakeSourceWatchRegistrar struct{}

func (f *fakeSourceWatchRegistrar) RegisterTaskSourceWatches(context.Context, *types.AgentTask, *types.RunExecutionWakeSignal, []*types.SourceWatchRequest) (*types.TaskBlockerSpec, error) {
	return nil, nil
}

func (f *fakeSourceWatchRegistrar) CleanupTaskSourceWatches(context.Context, *types.AgentTask) error {
	return nil
}

func TestWireSourceWatchRegistrar(t *testing.T) {
	target := &fakeSourceWatchRegistrarTarget{}
	registrar := &fakeSourceWatchRegistrar{}

	wireSourceWatchRegistrar(target, registrar)

	require.Same(t, registrar, target.registrar)
}

func TestWireSourceWatchRegistrarSkipsNil(t *testing.T) {
	target := &fakeSourceWatchRegistrarTarget{}

	wireSourceWatchRegistrar(target, nil)
	wireSourceWatchRegistrar(nil, &fakeSourceWatchRegistrar{})

	require.Nil(t, target.registrar)
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

func TestShouldSkipHTTPRequestLogHealthEndpoints(t *testing.T) {
	cLive := newEchoContext(http.MethodGet, "/api/v1/health/live", "/api/v1/health/live")
	require.True(t, shouldSkipHTTPRequestLog(cLive))

	cReady := newEchoContext(http.MethodGet, "/api/v1/health/ready", "/api/v1/health/ready")
	require.True(t, shouldSkipHTTPRequestLog(cReady))
}

func TestShouldSkipHTTPRequestLogAccessLogPolling(t *testing.T) {
	cTemplate := newEchoContext(
		http.MethodGet,
		"/api/v1/workspaces/ws-123/access-log?cursor=0",
		"/api/v1/workspaces/:workspace_id/access-log",
	)
	require.True(t, shouldSkipHTTPRequestLog(cTemplate))

	// Ensure URL-path fallback also works when route template is unavailable.
	cURLFallback := newEchoContext(
		http.MethodGet,
		"/api/v1/workspaces/ws-123/access-log?cursor=0",
		"",
	)
	require.True(t, shouldSkipHTTPRequestLog(cURLFallback))
}

func TestShouldSkipHTTPRequestLogDoesNotSkipNonPollingRequests(t *testing.T) {
	cNoCursor := newEchoContext(
		http.MethodGet,
		"/api/v1/workspaces/ws-123/access-log",
		"/api/v1/workspaces/:workspace_id/access-log",
	)
	require.False(t, shouldSkipHTTPRequestLog(cNoCursor))

	cOther := newEchoContext(http.MethodGet, "/api/v1/workspaces/ws-123/tasks", "/api/v1/workspaces/:workspace_id/tasks")
	require.False(t, shouldSkipHTTPRequestLog(cOther))

	cPost := newEchoContext(http.MethodPost, "/api/v1/workspaces/ws-123/access-log?cursor=0", "/api/v1/workspaces/:workspace_id/access-log")
	require.False(t, shouldSkipHTTPRequestLog(cPost))
}

func newEchoContext(method, target, routePath string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(method, target, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	if routePath != "" {
		c.SetPath(routePath)
	}
	return c
}
