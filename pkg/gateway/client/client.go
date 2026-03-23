package gatewayclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultDialTimeout    = 10 * time.Second
	defaultRequestTimeout = 30 * time.Second
)

// GatewayClient is a gRPC client for communicating with the gateway.
type GatewayClient struct {
	conn      *grpc.ClientConn
	client    pb.WorkerServiceClient
	authToken string
}

// NewGatewayClient creates a new gateway gRPC client.
func NewGatewayClient(addr string, authToken string) (*GatewayClient, error) {
	ctx, cancel := context.WithTimeout(context.Background(), defaultDialTimeout)
	defer cancel()

	tlsEnabled := common.NeedsTLS(addr)
	keepaliveParams := keepalive.ClientParameters{
		Time:                60 * time.Second,
		Timeout:             10 * time.Second,
		PermitWithoutStream: true,
	}

	dialer := &net.Dialer{
		Timeout:   defaultDialTimeout,
		KeepAlive: 30 * time.Second,
	}

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(common.TransportCredentials(addr)),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", target)
		}),
	)
	if err != nil {
		familySummary := resolveAddressFamilies(addr)
		mode := "plaintext"
		if tlsEnabled {
			mode = "tls"
		}
		return nil, fmt.Errorf(
			"failed to connect to gateway (target=%s mode=%s timeout=%s resolved=%s): %w",
			addr,
			mode,
			defaultDialTimeout,
			familySummary,
			err,
		)
	}

	return &GatewayClient{
		conn:      conn,
		client:    pb.NewWorkerServiceClient(conn),
		authToken: authToken,
	}, nil
}

func resolveAddressFamilies(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "unresolved"
	}

	hasV4 := false
	hasV6 := false
	for _, candidate := range addrs {
		if candidate.IP.To4() != nil {
			hasV4 = true
			continue
		}
		if candidate.IP.To16() != nil {
			hasV6 = true
		}
	}

	switch {
	case hasV4 && hasV6:
		return "ipv4+ipv6"
	case hasV4:
		return "ipv4"
	case hasV6:
		return "ipv6"
	default:
		return "none"
	}
}

// Close closes the gRPC connection.
func (c *GatewayClient) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// withAuth adds authentication metadata to context.
func (c *GatewayClient) withAuth(ctx context.Context) context.Context {
	if c.authToken != "" {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.authToken)
	}
	return ctx
}

// withTimeout creates a context with default timeout and auth.
func (c *GatewayClient) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.withAuth(ctx), defaultRequestTimeout)
}

// RegisterWorkerRequest is the request for registering a worker.
type RegisterWorkerRequest struct {
	Hostname string
	PoolName string
	Cpu      int64
	Memory   int64
	Version  string
}

// RegisterWorkerResponse is the response from registering a worker.
type RegisterWorkerResponse struct {
	WorkerID string
}

// RegisterWorker registers a worker with the gateway.
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

// Heartbeat sends a heartbeat for the worker.
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

// UpdateStatus updates the worker's status.
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

// Deregister removes the worker from the gateway.
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

// GetWorker retrieves worker information.
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

// SetTaskStarted marks a task as running in Postgres.
func (c *GatewayClient) SetTaskStarted(ctx context.Context, taskID string, attemptID string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.client.SetTaskStarted(ctx, &pb.SetTaskStartedRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	})
	return err
}

// SetTaskResult reports the result of a task to the gateway.
func (c *GatewayClient) SetTaskResult(ctx context.Context, taskID string, attemptID string, result *types.RunExecutionResult) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.SetTaskResult(ctx, buildSetTaskResultRequest(taskID, attemptID, result))
	if err != nil {
		return fmt.Errorf("set task result failed: %w", err)
	}

	return nil
}

func buildSetTaskResultRequest(taskID string, attemptID string, result *types.RunExecutionResult) *pb.SetTaskResultRequest {
	req := &pb.SetTaskResultRequest{
		TaskId:    taskID,
		AttemptId: attemptID,
	}
	if result == nil {
		return req
	}

	postRun := result.NormalizedPostRun()
	req.ExitCode = int32(result.ExitCode)
	req.Error = result.Error
	if postRun != nil {
		req.WaitingForInput = postRun.WaitingForInput
		req.WakeSignal = wakeSignalToProto(postRun.WakeSignal)
		req.SubtaskRequests = subtaskRequestsToProto(postRun.SubtaskRequests)
		req.SourceWatchRequests = sourceWatchRequestsToProto(postRun.SourceWatchRequests)
	}
	return req
}

func wakeSignalToProto(signal *types.RunExecutionWakeSignal) *pb.WakeSignal {
	signal = types.NormalizeRunExecutionWakeSignal(signal)
	if signal == nil {
		return nil
	}

	agenda := make([]*pb.WakeAgendaItem, 0, len(signal.WakeAgenda))
	for _, item := range signal.WakeAgenda {
		if item == nil {
			continue
		}
		agenda = append(agenda, &pb.WakeAgendaItem{
			Type:   item.Type,
			Title:  item.Title,
			Reason: item.Reason,
		})
	}

	return &pb.WakeSignal{
		DelayMinutes:   int32(signal.DelayMinutes),
		Reason:         signal.Reason,
		FollowUpPrompt: signal.FollowUpPrompt,
		WakeAgenda:     agenda,
	}
}

func subtaskRequestsToProto(requests []*types.SubtaskRequest) []*pb.SubtaskRequest {
	requests = types.NormalizeSubtaskRequests(requests)
	if len(requests) == 0 {
		return nil
	}

	out := make([]*pb.SubtaskRequest, 0, len(requests))
	for _, req := range requests {
		if req == nil {
			continue
		}
		out = append(out, &pb.SubtaskRequest{
			SourceOutputId:   req.SourceOutputID,
			EntityLabel:      req.EntityLabel,
			Prompt:           req.Prompt,
			WakeDelayMinutes: int32(req.WakeDelayMinutes),
		})
	}
	return out
}

func sourceWatchRequestsToProto(requests []*types.SourceWatchRequest) []*pb.SourceWatchRequest {
	requests = types.NormalizeSourceWatchRequestList(requests)
	if len(requests) == 0 {
		return nil
	}

	out := make([]*pb.SourceWatchRequest, 0, len(requests))
	for _, req := range requests {
		if req == nil {
			continue
		}
		out = append(out, &pb.SourceWatchRequest{
			Integration:        req.Integration,
			Reason:             req.Reason,
			Query:              req.Query,
			FilenameFormat:     req.FilenameFormat,
			EventTypes:         append([]string{}, req.EventTypes...),
			EntityKey:          req.EntityKey,
			EntityLabel:        req.EntityLabel,
			SourceOutputId:     req.SourceOutputID,
			ThreadId:           req.ThreadID,
			MessageId:          req.MessageID,
			IncludeAttachments: req.IncludeAttachments,
			IncludeInline:      req.IncludeInline,
			IncludeMessageBody: req.IncludeMessageBody,
		})
	}
	return out
}

func (c *GatewayClient) UpdateTaskState(ctx context.Context, update types.TaskLiveUpdate) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	req := &pb.UpdateTaskStateRequest{
		TaskId: update.TaskID,
		State:  string(update.State),
		RunId:  update.RunID,
	}
	if update.State == types.AgentTaskStateWaiting {
		if update.Blocker == nil {
			return fmt.Errorf("update task state: waiting update requires blocker")
		}
	}
	if blocker := update.Blocker; blocker != nil {
		req.InputKind = string(blocker.InputKind)
		req.BlockerKind = string(blocker.Kind)
		if blocker.WaitGroupID != nil {
			req.BlockerWaitGroupId = *blocker.WaitGroupID
		}
		req.BlockerOutputIds = append(req.BlockerOutputIds, blocker.OutputIDs...)
		payload := blocker.PayloadJSON
		if payload == nil {
			payload = map[string]any{}
		}
		payloadJSON, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal blocker payload: %w", err)
		}
		req.BlockerPayloadJson = string(payloadJSON)
	}
	_, err := c.client.UpdateTaskState(ctx, req)
	if err != nil {
		return fmt.Errorf("update task state failed: %w", err)
	}
	return nil
}

// ClaimTaskInput claims the next pending durable input for a task.
func (c *GatewayClient) ClaimTaskInput(ctx context.Context, taskID, runID, executionID string) (*pb.ClaimTaskInputResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.ClaimTaskInput(ctx, &pb.ClaimTaskInputRequest{
		TaskId:      taskID,
		RunId:       runID,
		ExecutionId: executionID,
	})
	if err != nil {
		return nil, fmt.Errorf("claim task input failed: %w", err)
	}
	return resp, nil
}

// AckTaskInput acknowledges that a claimed input has been consumed.
func (c *GatewayClient) AckTaskInput(ctx context.Context, inputID string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.AckTaskInput(ctx, &pb.AckTaskInputRequest{
		InputId: inputID,
	})
	if err != nil {
		return fmt.Errorf("ack task input failed: %w", err)
	}
	return nil
}

// AllocateIP requests an IP allocation for a sandbox from the gateway.
func (c *GatewayClient) AllocateIP(ctx context.Context, sandboxID, workerID string) (*types.IPAllocation, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.client.AllocateIP(ctx, &pb.AllocateIPRequest{
		SandboxId: sandboxID,
		WorkerId:  workerID,
	})
	if err != nil {
		return nil, fmt.Errorf("allocate IP failed: %w", err)
	}

	return &types.IPAllocation{
		IP:            resp.Ip,
		Gateway:       resp.Gateway,
		PrefixLen:     int(resp.PrefixLen),
		IPv6:          resp.Ipv6,
		GatewayIPv6:   resp.GatewayIpv6,
		PrefixLenIPv6: int(resp.PrefixLenIpv6),
	}, nil
}

// ReleaseIP releases an IP allocation for a sandbox.
func (c *GatewayClient) ReleaseIP(ctx context.Context, sandboxID string) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.client.ReleaseIP(ctx, &pb.ReleaseIPRequest{
		SandboxId: sandboxID,
	})
	if err != nil {
		return fmt.Errorf("release IP failed: %w", err)
	}

	return nil
}

// CreateTaskOutput creates a structured output for a task via gRPC.
func (c *GatewayClient) CreateTaskOutput(ctx context.Context, req *pb.CreateTaskOutputRequest) (string, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	resp, err := c.client.CreateTaskOutput(ctx, req)
	if err != nil {
		return "", fmt.Errorf("create task output: %w", err)
	}
	return resp.Id, nil
}

// AppendTaskOutputRows appends rows to a streaming table output via gRPC.
func (c *GatewayClient) AppendTaskOutputRows(ctx context.Context, req *pb.AppendTaskOutputRowsRequest) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.client.AppendTaskOutputRows(ctx, req)
	if err != nil {
		return fmt.Errorf("append task output rows: %w", err)
	}
	return nil
}

// FinalizeTaskOutput sets the summary on an output via gRPC.
func (c *GatewayClient) FinalizeTaskOutput(ctx context.Context, req *pb.FinalizeTaskOutputRequest) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.client.FinalizeTaskOutput(ctx, req)
	if err != nil {
		return fmt.Errorf("finalize task output: %w", err)
	}
	return nil
}

// UpdateTaskOutputStatus updates the lifecycle status of a task output via gRPC.
func (c *GatewayClient) UpdateTaskOutputStatus(ctx context.Context, req *pb.UpdateTaskOutputStatusRequest) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()
	_, err := c.client.UpdateTaskOutputStatus(ctx, req)
	if err != nil {
		return fmt.Errorf("update task output status: %w", err)
	}
	return nil
}

// isNotFound checks if the error is a gRPC NotFound status.
func isNotFound(err error) bool {
	if st, ok := status.FromError(err); ok {
		return st.Code() == codes.NotFound
	}
	return false
}
