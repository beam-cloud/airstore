package types

import "time"

// SandboxConfig defines the configuration for creating a sandbox
type SandboxConfig struct {
	// ID is the unique identifier for this sandbox
	ID string `json:"id"`

	// WorkspaceID is the workspace this sandbox belongs to
	WorkspaceID string `json:"workspace_id"`

	// Image is the container image to use for the sandbox
	Image string `json:"image"`

	// Runtime is the container runtime to use (runc, gvisor)
	Runtime ContainerRuntime `json:"runtime"`

	// Entrypoint is the command to run in the sandbox
	Entrypoint []string `json:"entrypoint"`

	// Env is the environment variables to set
	Env map[string]string `json:"env"`

	// WorkingDir is the working directory inside the sandbox
	WorkingDir string `json:"working_dir"`

	// Resources specifies resource limits for the sandbox
	Resources SandboxResources `json:"resources"`

	// Mounts specifies additional mounts for the sandbox
	Mounts []SandboxMount `json:"mounts"`

	// Network specifies network configuration
	Network SandboxNetwork `json:"network"`
}

// SandboxResources specifies resource limits for a sandbox
type SandboxResources struct {
	// CPU limit in millicores (e.g., 1000 = 1 CPU)
	CPU int64 `json:"cpu"`

	// Memory limit in bytes
	Memory int64 `json:"memory"`

	// GPU count (0 = no GPU)
	GPU int `json:"gpu"`
}

// SandboxMount specifies a mount point for a sandbox
type SandboxMount struct {
	// Source is the host path or volume name
	Source string `json:"source"`

	// Destination is the path inside the sandbox
	Destination string `json:"destination"`

	// ReadOnly specifies if the mount is read-only
	ReadOnly bool `json:"read_only"`

	// Type is the mount type (bind, volume, tmpfs)
	Type string `json:"type"`
}

// SandboxNetwork specifies network configuration for a sandbox
type SandboxNetwork struct {
	// Mode is the network mode (none, host, bridge)
	Mode string `json:"mode"`

	// ExposedPorts is a list of ports to expose
	ExposedPorts []int `json:"exposed_ports"`
}

// SandboxStatus represents the current status of a sandbox
type SandboxStatus string

const (
	SandboxStatusPending  SandboxStatus = "pending"
	SandboxStatusCreating SandboxStatus = "creating"
	SandboxStatusRunning  SandboxStatus = "running"
	SandboxStatusStopped  SandboxStatus = "stopped"
	SandboxStatusFailed   SandboxStatus = "failed"
)

// SandboxState represents the current state of a sandbox
type SandboxState struct {
	// ID is the sandbox identifier
	ID string `json:"id"`

	// Status is the current status
	Status SandboxStatus `json:"status"`

	// PID is the main process ID (0 if not running)
	PID int `json:"pid"`

	// ExitCode is the exit code if stopped (-1 if still running)
	ExitCode int `json:"exit_code"`

	// Error contains error message if failed
	Error string `json:"error,omitempty"`

	// ContainerIP is the container's IP address (for routing)
	ContainerIP string `json:"container_ip,omitempty"`

	// CreatedAt is when the sandbox was created
	CreatedAt time.Time `json:"created_at"`

	// StartedAt is when the sandbox started running
	StartedAt time.Time `json:"started_at,omitempty"`

	// FinishedAt is when the sandbox stopped
	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// IPAllocation represents an allocated IP for a sandbox
type IPAllocation struct {
	IP        string `json:"ip"`
	Gateway   string `json:"gateway"`
	PrefixLen int    `json:"prefix_len"`
}

// Default network settings
const (
	DefaultSubnet       = "10.88.0.0/24"
	DefaultSubnetPrefix = "10.88.0"
	DefaultGateway      = "10.88.0.1"
	DefaultPrefixLen    = 24
)
