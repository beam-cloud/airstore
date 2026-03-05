package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

// WorkerRepository manages worker state in Redis
type WorkerRepository interface {
	AddWorker(ctx context.Context, worker *types.Worker) error
	GetWorker(ctx context.Context, workerId string) (*types.Worker, error)
	GetAllWorkers(ctx context.Context) ([]*types.Worker, error)
	GetAvailableWorkers(ctx context.Context) ([]*types.Worker, error)
	RemoveWorker(ctx context.Context, workerId string) error
	SetWorkerKeepAlive(ctx context.Context, workerId string) error
	UpdateWorkerStatus(ctx context.Context, workerId string, status types.WorkerStatus) error
	AllocateIP(ctx context.Context, sandboxID, workerID string) (*types.IPAllocation, error)
	ReleaseIP(ctx context.Context, sandboxID string) error
	GetSandboxIP(ctx context.Context, sandboxID string) (string, bool)
}

// WorkerPoolRepository manages worker pool state in Redis
type WorkerPoolRepository interface {
	SetPoolState(ctx context.Context, poolName string, state *types.WorkerPoolState) error
	GetPoolState(ctx context.Context, poolName string) (*types.WorkerPoolState, error)
}

// TaskQueue manages task queuing and distribution via Redis
type TaskQueue interface {
	Push(ctx context.Context, task *types.RunExecution) error
	Pop(ctx context.Context, workerID string) (*types.RunExecution, error)
	Complete(ctx context.Context, taskID string, result *types.RunExecutionResult) error
	Fail(ctx context.Context, taskID string, err error) error
	GetState(ctx context.Context, taskID string) (*types.RunExecutionState, error)
	GetResult(ctx context.Context, taskID string) (*types.RunExecutionResult, error)
	Len(ctx context.Context) (int64, error)
	InFlightCount(ctx context.Context) (int64, error)

	// Log streaming
	PublishLog(ctx context.Context, taskID string, stream string, data string) error
	PublishStatus(ctx context.Context, taskID string, status types.RunExecutionStatus, exitCode *int, errorMsg string) error
	SubscribeLogs(ctx context.Context, taskID string) (<-chan []byte, func(), error)
	GetLogBuffer(ctx context.Context, taskID string) ([][]byte, error)
}

// TerminalIORepository manages interactive terminal I/O transport.
// Implementations encapsulate broker/channel details (e.g., Redis pub/sub).
type TerminalIORepository interface {
	PublishInput(ctx context.Context, taskID string, data []byte) error
	SubscribeInput(ctx context.Context, taskID string) (<-chan []byte, func(), error)
	ListPendingInputs(ctx context.Context, taskID string) ([]types.PendingInput, error)

	PublishOutput(ctx context.Context, taskID string, data []byte) error
	SubscribeOutput(ctx context.Context, taskID string) (<-chan []byte, func(), error)

	PublishCancel(ctx context.Context, taskID string) error
	SubscribeCancel(ctx context.Context, taskID string) (<-chan struct{}, func(), error)

	// Session lease: exclusive ownership of an interactive session.
	AcquireSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string, ttl time.Duration) (bool, error)
	RenewSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string, ttl time.Duration) (bool, error)
	ReleaseSessionLease(ctx context.Context, workspaceID uint, sessionID, ownerID string) error
	GetSessionLeaseOwner(ctx context.Context, workspaceID uint, sessionID string) (string, error)

	// Run interaction state: backend-owned state for working/waiting/closed.
	SetRunInteraction(ctx context.Context, workspaceID uint, runID string, state types.RunInteractionState, activeExecutionID string, ttl time.Duration) error
	GetRunInteraction(ctx context.Context, workspaceID uint, runID string) (*types.RunInteraction, error)
	ClearRunInteraction(ctx context.Context, workspaceID uint, runID string) error
}

// MemberRepository manages workspace members
type MemberRepository interface {
	CreateMember(ctx context.Context, workspaceId uint, email, name string, role types.MemberRole) (*types.WorkspaceMember, error)
	GetMember(ctx context.Context, externalId string) (*types.WorkspaceMember, error)
	GetMemberByEmail(ctx context.Context, workspaceId uint, email string) (*types.WorkspaceMember, error)
	ListMembers(ctx context.Context, workspaceId uint) ([]types.WorkspaceMember, error)
	UpdateMember(ctx context.Context, externalId string, name string, role types.MemberRole) (*types.WorkspaceMember, error)
	DeleteMember(ctx context.Context, externalId string) error
}

// TokenRepository manages authentication tokens
type TokenRepository interface {
	// Workspace member tokens
	CreateToken(ctx context.Context, workspaceId, memberId uint, name string, expiresAt *time.Time, tokenType types.TokenType) (*types.Token, string, error)
	GetToken(ctx context.Context, externalId string) (*types.Token, error)
	ListTokens(ctx context.Context, workspaceId uint) ([]types.Token, error)
	RevokeToken(ctx context.Context, externalId string) error

	// Worker tokens (cluster-level)
	CreateWorkerToken(ctx context.Context, name string, poolName *string, expiresAt *time.Time) (*types.Token, string, error)
	ListWorkerTokens(ctx context.Context) ([]types.Token, error)

	// Organization tokens (tenant-scoped)
	CreateOrgToken(ctx context.Context, name string, tenantId string, expiresAt *time.Time) (*types.Token, string, error)
	ListOrgTokens(ctx context.Context, tenantId string) ([]types.Token, error)
	RevokeOrgToken(ctx context.Context, externalId string) error

	// Workspace service tokens (workspace-scoped, no member)
	CreateWorkspaceServiceToken(ctx context.Context, workspaceId uint, name string) (*types.Token, string, error)
	EnsureWorkspaceServiceToken(ctx context.Context, workspaceId uint) (*types.Token, string, error)

	// Validation
	ValidateToken(ctx context.Context, rawToken string) (*types.TokenValidationResult, error)
	AuthorizeToken(ctx context.Context, rawToken string) (*types.AuthInfo, error)
}

// IntegrationRepository manages integration connections
type IntegrationRepository interface {
	SaveConnection(ctx context.Context, workspaceId uint, memberId *uint, integrationType string, creds *types.IntegrationCredentials, scope string) (*types.IntegrationConnection, error)
	GetConnection(ctx context.Context, workspaceId uint, memberId uint, integrationType string) (*types.IntegrationConnection, error)
	GetConnectionByExternalId(ctx context.Context, externalId string) (*types.IntegrationConnection, error)
	ListConnections(ctx context.Context, workspaceId uint) ([]types.IntegrationConnection, error)
	DeleteConnection(ctx context.Context, externalId string) error
}

// WorkspaceToolRepository manages workspace-scoped tool providers
type WorkspaceToolRepository interface {
	CreateWorkspaceTool(ctx context.Context, workspaceId uint, createdByMemberId *uint, name string, providerType types.WorkspaceToolProviderType, config json.RawMessage, manifest json.RawMessage) (*types.WorkspaceTool, error)
	GetWorkspaceTool(ctx context.Context, id uint) (*types.WorkspaceTool, error)
	GetWorkspaceToolByExternalId(ctx context.Context, externalId string) (*types.WorkspaceTool, error)
	GetWorkspaceToolByName(ctx context.Context, workspaceId uint, name string) (*types.WorkspaceTool, error)
	ListWorkspaceTools(ctx context.Context, workspaceId uint) ([]*types.WorkspaceTool, error)
	UpdateWorkspaceToolManifest(ctx context.Context, id uint, manifest json.RawMessage) error
	UpdateWorkspaceToolConfig(ctx context.Context, id uint, config json.RawMessage) error
	DeleteWorkspaceTool(ctx context.Context, id uint) error
	DeleteWorkspaceToolByName(ctx context.Context, workspaceId uint, name string) error
}

// BackendRepository is the main Postgres repository for persistent data.
// For filesystem queries and metadata, use FilesystemStore instead.
type BackendRepository interface {
	// Workspaces
	CreateWorkspace(ctx context.Context, name string, tenantId *string) (*types.Workspace, error)
	GetWorkspace(ctx context.Context, id uint) (*types.Workspace, error)
	GetWorkspaceByExternalId(ctx context.Context, externalId string) (*types.Workspace, error)
	GetWorkspaceByName(ctx context.Context, name string) (*types.Workspace, error)
	ListWorkspaces(ctx context.Context) ([]*types.Workspace, error)
	ListWorkspacesByTenantId(ctx context.Context, tenantId string) ([]*types.Workspace, error)
	DeleteWorkspace(ctx context.Context, id uint) error

	// Workspace Tool Settings
	GetWorkspaceToolSettings(ctx context.Context, workspaceId uint) (*types.WorkspaceToolSettings, error)
	GetWorkspaceToolSetting(ctx context.Context, workspaceId uint, toolName string) (*types.WorkspaceToolSetting, error)
	SetWorkspaceToolSetting(ctx context.Context, workspaceId uint, toolName string, enabled bool) error
	ListWorkspaceToolSettings(ctx context.Context, workspaceId uint) ([]types.WorkspaceToolSetting, error)

	// Members
	MemberRepository

	// Tokens
	TokenRepository

	// Integrations
	IntegrationRepository

	// Workspace Tools
	WorkspaceToolRepository

	// Run execution payloads
	CreateRunExecution(ctx context.Context, task *types.RunExecution) error
	GetRunExecution(ctx context.Context, externalId string) (*types.RunExecution, error)
	GetRunExecutionByID(ctx context.Context, id uint) (*types.RunExecution, error)
	ListRunExecutions(ctx context.Context, workspaceId uint) ([]*types.RunExecution, error)
	UpdateRunExecutionStatus(ctx context.Context, externalId string, status types.RunExecutionStatus) error
	SetRunExecutionStarted(ctx context.Context, externalId string) error
	SetRunExecutionResult(ctx context.Context, externalId string, exitCode int, errorMsg string) error
	SetRunExecutionStartedForAttempt(ctx context.Context, externalId string, attemptID string) (bool, error)
	SetRunExecutionResultForAttempt(ctx context.Context, externalId string, attemptID string, exitCode int, errorMsg string) (bool, error)
	CancelRunExecution(ctx context.Context, externalId string) error
	DeleteRunExecution(ctx context.Context, externalId string) error
	MarkRunExecutionRetried(ctx context.Context, externalId string) error
	GetRetryableRunExecutions(ctx context.Context) ([]*types.RunExecution, error)
	GetStuckHookRunExecutions(ctx context.Context, timeout time.Duration) ([]*types.RunExecution, error)
	ListRunExecutionsByHook(ctx context.Context, hookId uint) ([]*types.RunExecution, error)

	// Agents
	CreateAgentProfile(ctx context.Context, profile *types.AgentProfile) error
	GetAgentProfile(ctx context.Context, workspaceId uint, agentId string) (*types.AgentProfile, error)
	GetAgentProfileByKey(ctx context.Context, workspaceId uint, agentKey string) (*types.AgentProfile, error)
	ListAgentProfiles(ctx context.Context, workspaceId uint) ([]*types.AgentProfile, error)
	UpdateAgentProfile(ctx context.Context, profile *types.AgentProfile) error
	DeleteAgentProfile(ctx context.Context, workspaceId uint, agentId string) error

	// Channel Bindings
	// When agentID is nil, operates on workspace-level bindings (agent_id IS NULL).
	// When agentID is non-nil, operates on that specific agent's bindings.
	ListChannelBindings(ctx context.Context, workspaceId uint, agentID *string) ([]*types.ChannelBinding, error)
	UpsertChannelBinding(ctx context.Context, binding *types.ChannelBinding) error
	DeleteChannelBinding(ctx context.Context, workspaceId uint, agentID *string, channelType string) error
	GetChannelBindingByAddress(ctx context.Context, channelType string, address string) (*types.ChannelBinding, error)

	// Tasks
	CreateTask(ctx context.Context, task *types.AgentTask) error
	CreateTaskWithOutbox(ctx context.Context, task *types.AgentTask, event *types.OrchestrationOutboxEvent) error
	ListTasks(ctx context.Context, workspaceId uint, limit int) ([]*types.AgentTask, error)
	ListTasksFiltered(ctx context.Context, workspaceId uint, filter types.AgentTaskListFilter) ([]*types.AgentTask, error)
	GetTaskByID(ctx context.Context, taskId string) (*types.AgentTask, error)
	GetTask(ctx context.Context, workspaceId uint, taskId string) (*types.AgentTask, error)
	GetTaskByIdempotency(ctx context.Context, workspaceId uint, agentId *string, idempotencyKey string) (*types.AgentTask, error)
	ClaimQueuedTaskForDispatch(ctx context.Context, taskID string, staleAfter time.Duration) (*types.AgentTask, bool, error)
	UpdateTaskState(ctx context.Context, taskId string, state types.AgentTaskState, droppedReason *string, targetRunID *string) error
	UpdateTaskStateIfCurrentRun(ctx context.Context, taskID string, expectedRunID string, state types.AgentTaskState, droppedReason *string, targetRunID *string) (bool, error)
	ArchiveTask(ctx context.Context, taskId string) error
	CreateScheduledTask(ctx context.Context, st *types.ScheduledTask) error
	GetScheduledTask(ctx context.Context, workspaceID uint, externalID string) (*types.ScheduledTask, error)
	ListScheduledTasks(ctx context.Context, workspaceID uint) ([]*types.ScheduledTask, error)
	UpdateScheduledTask(ctx context.Context, st *types.ScheduledTask) error
	DeleteScheduledTask(ctx context.Context, workspaceID uint, externalID string) error
	ListDueScheduledTasks(ctx context.Context, now time.Time, limit int) ([]*types.ScheduledTask, error)
	AdvanceScheduledTask(ctx context.Context, id string, oldNextRunAt, newNextRunAt time.Time) (bool, error)
	RevertScheduledTaskAdvance(ctx context.Context, id string, currentNextRunAt, revertTo time.Time) (bool, error)

	// Orchestration outbox/inbox/retry guard
	EnqueueOrchestrationOutboxEvent(ctx context.Context, event *types.OrchestrationOutboxEvent) error
	ClaimPendingOrchestrationOutboxEvents(ctx context.Context, limit int) ([]*types.OrchestrationOutboxEvent, error)
	MarkOrchestrationOutboxEventPublished(ctx context.Context, eventID int64) error
	MarkOrchestrationOutboxEventError(ctx context.Context, eventID int64, lastError string) error
	AcquireOrchestrationResultInbox(ctx context.Context, resultKey string, streamID string) (bool, error)
	AcquireOrchestrationRetryGuard(ctx context.Context, guardKey string) (bool, error)

	// Runs
	CreateAgentRun(ctx context.Context, run *types.AgentRun) error
	GetAgentRunByID(ctx context.Context, runId string) (*types.AgentRun, error)
	GetAgentRun(ctx context.Context, workspaceId uint, runId string) (*types.AgentRun, error)
	ListAgentRuns(ctx context.Context, workspaceId uint, limit int) ([]*types.AgentRun, error)
	ListAgentRunsFiltered(ctx context.Context, workspaceId uint, filter types.AgentRunListFilter) ([]*types.AgentRun, error)
	ListActiveRunsBySession(ctx context.Context, workspaceId uint, sessionID string, excludeRunIDs []string, limit int) ([]*types.AgentRun, error)
	UpdateAgentRunLifecycle(ctx context.Context, runId string, status types.AgentRunStatus, startedAt, endedAt *time.Time, errorMsg *string) error
	SetAgentRunClaim(ctx context.Context, runId string, workerId string, heartbeatAt time.Time, expiresAt time.Time) error
	ClearAgentRunClaim(ctx context.Context, runId string) error
	ClearExpiredAgentRunClaim(ctx context.Context, runId string, workerId string, expiresAt time.Time) (bool, error)
	RefreshAgentRunClaims(ctx context.Context, workerId string, heartbeatAt time.Time, expiresAt time.Time) (int64, error)
	ListClaimedAgentRuns(ctx context.Context, limit int) ([]*types.AgentRun, error)
	ListExpiredClaimedAgentRuns(ctx context.Context, now time.Time, limit int) ([]*types.AgentRun, error)
	ListStaleUnclaimedAgentRuns(ctx context.Context, cutoff time.Time, limit int) ([]*types.AgentRun, error)
	IncrementAgentRunSnapshotSeq(ctx context.Context, runId string) (int64, error)

	// Run attempts
	CreateAgentRunAttempt(ctx context.Context, attempt *types.AgentRunAttempt) error
	GetAgentRunAttempt(ctx context.Context, attemptId string) (*types.AgentRunAttempt, error)
	ListAgentRunAttempts(ctx context.Context, runId string) ([]*types.AgentRunAttempt, error)
	GetRunAttemptByExecutionID(ctx context.Context, executionID string) (*types.AgentRunAttempt, error)
	UpdateAgentRunAttemptStart(ctx context.Context, attemptId string, startedAt time.Time) error
	UpdateAgentRunAttemptResult(ctx context.Context, attemptId string, status types.AgentAttemptStatus, exitCode *int, endedAt time.Time, errorMsg *string) error
	BindAttemptExecutionTask(ctx context.Context, attemptId, taskExternalID string) error

	// Run snapshots
	AppendAgentRunSnapshot(ctx context.Context, snap *types.AgentRunSnapshot) error
	ListAgentRunSnapshots(ctx context.Context, runId string, limit int) ([]*types.AgentRunSnapshot, error)

	// Execution instances
	GetOrCreateExecutionInstance(ctx context.Context, inst *types.AgentExecutionInstance) (*types.AgentExecutionInstance, error)
	GetExecutionInstanceByKey(ctx context.Context, instanceKey string) (*types.AgentExecutionInstance, error)
	UpdateExecutionInstanceState(ctx context.Context, instanceKey string, running, pending, stopping, desired int, status types.AgentExecutionInstanceStatus, lastEventAt *time.Time) error
	AdjustExecutionInstanceRunningAttempts(ctx context.Context, instanceKey string, runningDelta int, lastEventAt *time.Time) error

	// Agent stats
	GetAgentStats(ctx context.Context, workspaceId uint, agentID string) (*types.AgentStats, error)

	// Task outputs
	ListTaskOutputs(ctx context.Context, workspaceId uint, taskID string) ([]*types.TaskOutput, error)
	CreateTaskOutput(ctx context.Context, output *types.TaskOutput) error
	GetTaskOutput(ctx context.Context, workspaceId uint, outputID string) (*types.TaskOutput, error)
	AppendTaskOutputRows(ctx context.Context, workspaceId uint, outputID string, rows []byte) error
	UpdateTaskOutputSummary(ctx context.Context, workspaceId uint, outputID string, summary string) error
	DeleteTaskOutput(ctx context.Context, workspaceId uint, outputID string) error

	// Database access
	DB() *sql.DB

	// Utilities
	Ping(ctx context.Context) error
	Close() error
	RunMigrations() error
}
