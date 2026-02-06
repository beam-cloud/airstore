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
	Push(ctx context.Context, task *types.Task) error
	Pop(ctx context.Context, workerID string) (*types.Task, error)
	Complete(ctx context.Context, taskID string, result *types.TaskResult) error
	Fail(ctx context.Context, taskID string, err error) error
	GetState(ctx context.Context, taskID string) (*types.TaskState, error)
	GetResult(ctx context.Context, taskID string) (*types.TaskResult, error)
	Len(ctx context.Context) (int64, error)
	InFlightCount(ctx context.Context) (int64, error)

	// Log streaming
	PublishLog(ctx context.Context, taskID string, stream string, data string) error
	PublishStatus(ctx context.Context, taskID string, status types.TaskStatus, exitCode *int, errorMsg string) error
	SubscribeLogs(ctx context.Context, taskID string) (<-chan []byte, func(), error)
	GetLogBuffer(ctx context.Context, taskID string) ([][]byte, error)
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
	CreateWorkspace(ctx context.Context, name string) (*types.Workspace, error)
	GetWorkspace(ctx context.Context, id uint) (*types.Workspace, error)
	GetWorkspaceByExternalId(ctx context.Context, externalId string) (*types.Workspace, error)
	GetWorkspaceByName(ctx context.Context, name string) (*types.Workspace, error)
	ListWorkspaces(ctx context.Context) ([]*types.Workspace, error)
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

	// Tasks
	CreateTask(ctx context.Context, task *types.Task) error
	GetTask(ctx context.Context, externalId string) (*types.Task, error)
	GetTaskById(ctx context.Context, id uint) (*types.Task, error)
	ListTasks(ctx context.Context, workspaceId uint) ([]*types.Task, error)
	UpdateTaskStatus(ctx context.Context, externalId string, status types.TaskStatus) error
	SetTaskStarted(ctx context.Context, externalId string) error
	SetTaskResult(ctx context.Context, externalId string, exitCode int, errorMsg string) error
	CancelTask(ctx context.Context, externalId string) error
	DeleteTask(ctx context.Context, externalId string) error
	MarkTaskRetried(ctx context.Context, externalId string) error
	GetRetryableTasks(ctx context.Context) ([]*types.Task, error)
	GetStuckHookTasks(ctx context.Context, timeout time.Duration) ([]*types.Task, error)
	ListTasksByHook(ctx context.Context, hookId uint) ([]*types.Task, error)

	// Database access
	DB() *sql.DB

	// Utilities
	Ping(ctx context.Context) error
	Close() error
	RunMigrations() error
}
