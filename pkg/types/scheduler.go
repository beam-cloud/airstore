package types

import "time"

// WorkerStatus represents the status of a worker
type WorkerStatus string

const (
	WorkerStatusPending   WorkerStatus = "pending"
	WorkerStatusAvailable WorkerStatus = "available"
	WorkerStatusBusy      WorkerStatus = "busy"
	WorkerStatusDraining  WorkerStatus = "draining"
	WorkerStatusOffline   WorkerStatus = "offline"
)

// Worker represents a worker that can execute workloads
type Worker struct {
	ID           string       `json:"id" redis:"id"`
	Status       WorkerStatus `json:"status" redis:"status"`
	PoolName     string       `json:"pool_name" redis:"pool_name"`
	Hostname     string       `json:"hostname" redis:"hostname"`
	Cpu          int64        `json:"cpu" redis:"cpu"`
	Memory       int64        `json:"memory" redis:"memory"`
	LastSeenAt   time.Time    `json:"last_seen_at" redis:"last_seen_at"`
	RegisteredAt time.Time    `json:"registered_at" redis:"registered_at"`
	Version      string       `json:"version" redis:"version"`
}

// WorkerJobOpts contains options for creating a worker Kubernetes Job
type WorkerJobOpts struct {
	PoolName  string
	Cpu       int64
	Memory    int64
	Image     string
	Namespace string
	Labels    map[string]string
	Env       map[string]string
}

// SchedulerConfig holds scheduler-specific configuration
type SchedulerConfig struct {
	Enabled             bool                        `key:"enabled" json:"enabled"`
	GatewayServiceName  string                      `key:"gatewayServiceName" json:"gateway_service_name"`
	WorkerImage         string                      `key:"workerImage" json:"worker_image"`
	WorkerNamespace     string                      `key:"workerNamespace" json:"worker_namespace"`
	WorkerTTL           time.Duration               `key:"workerTTL" json:"worker_ttl"`
	CleanupInterval     time.Duration               `key:"cleanupInterval" json:"cleanup_interval"`
	HeartbeatInterval   time.Duration               `key:"heartbeatInterval" json:"heartbeat_interval"`
	HeartbeatTimeout    time.Duration               `key:"heartbeatTimeout" json:"heartbeat_timeout"`
	DefaultWorkerCpu    int64                       `key:"defaultWorkerCpu" json:"default_worker_cpu"`
	DefaultWorkerMemory int64                       `key:"defaultWorkerMemory" json:"default_worker_memory"`
	Pools               map[string]WorkerPoolConfig `key:"pools" json:"pools"`
}

// Worker TTL constant
const (
	WorkerStateTTL = 5 * time.Minute // TTL for worker state (refreshed by heartbeat)
)
