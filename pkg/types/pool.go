package types

import "time"

// WorkerPoolConfig defines the configuration for a worker pool
type WorkerPoolConfig struct {
	// DeploymentName is the K8s Deployment to scale (auto-generated if empty)
	DeploymentName string `key:"deploymentName" json:"deployment_name"`

	// Namespace is the K8s namespace
	Namespace string `key:"namespace" json:"namespace"`

	// MinReplicas is the minimum number of workers
	MinReplicas int32 `key:"minReplicas" json:"min_replicas"`

	// MaxReplicas is the maximum number of workers
	MaxReplicas int32 `key:"maxReplicas" json:"max_replicas"`

	// ScaleDownDelay is how long queue must be empty before scaling down
	ScaleDownDelay time.Duration `key:"scaleDownDelay" json:"scale_down_delay"`

	// Cpu is the CPU request/limit for workers (e.g., "500m", "1")
	Cpu string `key:"cpu" json:"cpu"`

	// Memory is the memory request/limit for workers (e.g., "512Mi", "1Gi")
	Memory string `key:"memory" json:"memory"`
}

// NewWorkerPoolConfig creates a new WorkerPoolConfig with default values
func NewWorkerPoolConfig() *WorkerPoolConfig {
	return &WorkerPoolConfig{
		DeploymentName: "airstore-worker",
		Namespace:      "airstore",
		MinReplicas:    1,
		MaxReplicas:    10,
		ScaleDownDelay: 5 * time.Minute,
	}
}

// WorkerPoolStatus represents the health status of a worker pool
type WorkerPoolStatus string

const (
	WorkerPoolStatusHealthy  WorkerPoolStatus = "healthy"
	WorkerPoolStatusDegraded WorkerPoolStatus = "degraded"
)

// WorkerPoolState represents the current state of a worker pool
type WorkerPoolState struct {
	Name             string           `json:"name"`
	Status           WorkerPoolStatus `json:"status"`
	TotalWorkers     int              `json:"total_workers"`
	AvailableWorkers int              `json:"available_workers"`
}

// PoolScalerStatus represents the current scaling status of a pool
type PoolScalerStatus struct {
	PoolName        string `json:"pool_name"`
	QueueDepth      int64  `json:"queue_depth"`
	InFlightTasks   int64  `json:"in_flight_tasks"`
	CurrentReplicas int32  `json:"current_replicas"`
	MinReplicas     int32  `json:"min_replicas"`
	MaxReplicas     int32  `json:"max_replicas"`
}
