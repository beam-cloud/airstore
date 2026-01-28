package repository

import (
	"context"
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

// TokenRepository manages workspace authentication tokens
type TokenRepository interface {
	CreateToken(ctx context.Context, workspaceId, memberId uint, name string, expiresAt *time.Time) (*types.WorkspaceToken, string, error)
	ValidateToken(ctx context.Context, rawToken string) (*types.TokenValidationResult, error)
	GetToken(ctx context.Context, externalId string) (*types.WorkspaceToken, error)
	ListTokens(ctx context.Context, workspaceId uint) ([]types.WorkspaceToken, error)
	RevokeToken(ctx context.Context, externalId string) error
}

// IntegrationRepository manages integration connections
type IntegrationRepository interface {
	SaveConnection(ctx context.Context, workspaceId uint, memberId *uint, integrationType string, creds *types.IntegrationCredentials, scope string) (*types.IntegrationConnection, error)
	GetConnection(ctx context.Context, workspaceId uint, memberId uint, integrationType string) (*types.IntegrationConnection, error)
	ListConnections(ctx context.Context, workspaceId uint) ([]types.IntegrationConnection, error)
	DeleteConnection(ctx context.Context, externalId string) error
}

// FilesystemRepository manages filesystem metadata in Redis (cache layer)
// S3 is the source of truth; this provides fast access for stat/listing operations
type FilesystemRepository interface {
	// Directory operations
	GetDirMeta(ctx context.Context, path string) (*types.DirMeta, error)
	SaveDirMeta(ctx context.Context, meta *types.DirMeta) error
	DeleteDirMeta(ctx context.Context, path string) error
	ListDir(ctx context.Context, path string) ([]types.DirEntry, error)

	// File operations
	GetFileMeta(ctx context.Context, path string) (*types.FileMeta, error)
	SaveFileMeta(ctx context.Context, meta *types.FileMeta) error
	DeleteFileMeta(ctx context.Context, path string) error

	// Symlink operations
	GetSymlink(ctx context.Context, path string) (string, error)
	SaveSymlink(ctx context.Context, path, target string) error
	DeleteSymlink(ctx context.Context, path string) error

	// Cache management
	Invalidate(ctx context.Context, path string) error
	InvalidatePrefix(ctx context.Context, prefix string) error
}

// BackendRepository is the main Postgres repository for persistent data
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

	// Tasks
	CreateTask(ctx context.Context, task *types.Task) error
	GetTask(ctx context.Context, externalId string) (*types.Task, error)
	GetTaskById(ctx context.Context, id uint) (*types.Task, error)
	ListTasks(ctx context.Context, workspaceId uint) ([]*types.Task, error)
	UpdateTaskStatus(ctx context.Context, externalId string, status types.TaskStatus) error
	SetTaskStarted(ctx context.Context, externalId string) error
	SetTaskResult(ctx context.Context, externalId string, exitCode int, errorMsg string) error
	DeleteTask(ctx context.Context, externalId string) error

	// Utilities
	Ping(ctx context.Context) error
	Close() error
	RunMigrations() error
}
