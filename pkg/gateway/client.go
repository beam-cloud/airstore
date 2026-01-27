package gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultDialTimeout   = 10 * time.Second
	defaultRequestTimout = 30 * time.Second
)

// GatewayClient is a gRPC client for communicating with the gateway
type GatewayClient struct {
	conn      *grpc.ClientConn
	client    pb.WorkerServiceClient
	authToken string
}

// NewGatewayClient creates a new gateway gRPC client
func NewGatewayClient(addr string, authToken string) (*GatewayClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gateway: %w", err)
	}

	return &GatewayClient{
		conn:      conn,
		client:    pb.NewWorkerServiceClient(conn),
		authToken: authToken,
	}, nil
}

// Close closes the gRPC connection
func (c *GatewayClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// withAuth adds authentication metadata to context
func (c *GatewayClient) withAuth(ctx context.Context) context.Context {
	if c.authToken != "" {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.authToken)
	}
	return ctx
}

// withTimeout creates a context with default timeout and auth
func (c *GatewayClient) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = c.withAuth(ctx)
	return context.WithTimeout(ctx, defaultRequestTimout)
}

// RegisterWorkerRequest is the request for registering a worker
type RegisterWorkerRequest struct {
	Hostname string
	PoolName string
	Cpu      int64
	Memory   int64
	Version  string
}

// RegisterWorkerResponse is the response from registering a worker
type RegisterWorkerResponse struct {
	WorkerID string
}

// RegisterWorker registers a worker with the gateway
func (c *GatewayClient) RegisterWorker(ctx context.Context, req *RegisterWorkerRequest) (*RegisterWorkerResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.RegisterWorker(ctx, &pb.RegisterWorkerRequest{
		Hostname: req.Hostname,
		PoolName: req.PoolName,
		Cpu:      req.Cpu,
		Memory:   req.Memory,
		Version:  req.Version,
	})
	if err != nil {
		return nil, fmt.Errorf("register worker failed: %w", err)
	}

	return &RegisterWorkerResponse{WorkerID: resp.WorkerId}, nil
}

// Heartbeat sends a heartbeat for the worker
func (c *GatewayClient) Heartbeat(ctx context.Context, workerId string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.Heartbeat(ctx, &pb.HeartbeatRequest{
		WorkerId: workerId,
	})
	if err != nil {
		if isNotFound(err) {
			return &types.ErrWorkerNotFound{WorkerId: workerId}
		}
		return fmt.Errorf("heartbeat failed: %w", err)
	}

	return nil
}

// UpdateStatus updates the worker's status
func (c *GatewayClient) UpdateStatus(ctx context.Context, workerId string, workerStatus types.WorkerStatus) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.UpdateStatus(ctx, &pb.UpdateStatusRequest{
		WorkerId: workerId,
		Status:   string(workerStatus),
	})
	if err != nil {
		if isNotFound(err) {
			return &types.ErrWorkerNotFound{WorkerId: workerId}
		}
		return fmt.Errorf("update status failed: %w", err)
	}

	return nil
}

// Deregister removes the worker from the gateway
func (c *GatewayClient) Deregister(ctx context.Context, workerId string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.Deregister(ctx, &pb.DeregisterRequest{
		WorkerId: workerId,
	})
	if err != nil {
		return fmt.Errorf("deregister failed: %w", err)
	}

	return nil
}

// GetWorker retrieves worker information
func (c *GatewayClient) GetWorker(ctx context.Context, workerId string) (*types.Worker, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.GetWorker(ctx, &pb.GetWorkerRequest{
		WorkerId: workerId,
	})
	if err != nil {
		if isNotFound(err) {
			return nil, &types.ErrWorkerNotFound{WorkerId: workerId}
		}
		return nil, fmt.Errorf("get worker failed: %w", err)
	}

	return &types.Worker{
		ID:           resp.Id,
		Status:       types.WorkerStatus(resp.Status),
		PoolName:     resp.PoolName,
		Hostname:     resp.Hostname,
		Cpu:          resp.Cpu,
		Memory:       resp.Memory,
		LastSeenAt:   time.Unix(resp.LastSeenAt, 0),
		RegisteredAt: time.Unix(resp.RegisteredAt, 0),
		Version:      resp.Version,
	}, nil
}

// SetTaskResult reports the result of a task to the gateway
func (c *GatewayClient) SetTaskResult(ctx context.Context, taskID string, exitCode int, errorMsg string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.SetTaskResult(ctx, &pb.SetTaskResultRequest{
		TaskId:   taskID,
		ExitCode: int32(exitCode),
		Error:    errorMsg,
	})
	if err != nil {
		return fmt.Errorf("set task result failed: %w", err)
	}

	return nil
}

// isNotFound checks if the error is a gRPC NotFound status
func isNotFound(err error) bool {
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.NotFound
	}
	return false
}
