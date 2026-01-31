package types

// Worker filesystem path constants
const (
	// DefaultBundleDir is where sandbox bundles are created
	DefaultBundleDir = "/var/lib/airstore/bundles"

	// DefaultStateDir is where overlay state is stored (should be tmpfs)
	DefaultStateDir = "/mnt/overlay"

	// DefaultMountDir is where per-task FUSE mounts are created
	DefaultMountDir = "/tmp/airstore-mounts"

	// DefaultCLIBinary is the path to the CLI binary for mounting
	DefaultCLIBinary = "/usr/local/bin/cli"

	// DefaultWorkerMount is where the worker's global FUSE mount lives
	DefaultWorkerMount = "/var/lib/airstore/fs"

	// ContainerWorkDir is the working directory inside containers
	ContainerWorkDir = "/workspace"
)

// WorkerPaths configures filesystem paths for the worker
type WorkerPaths struct {
	// BundleDir is where sandbox bundles are created
	BundleDir string `key:"bundleDir" json:"bundle_dir"`

	// StateDir is where overlay state is stored (should be tmpfs)
	StateDir string `key:"stateDir" json:"state_dir"`

	// MountDir is where per-task FUSE mounts are created
	MountDir string `key:"mountDir" json:"mount_dir"`

	// CLIBinary is the path to the CLI binary for mounting
	CLIBinary string `key:"cliBinary" json:"cli_binary"`

	// WorkerMount is the global worker FUSE mount path
	WorkerMount string `key:"workerMount" json:"worker_mount"`
}

// DefaultWorkerPaths returns sensible defaults for worker paths
func DefaultWorkerPaths() WorkerPaths {
	return WorkerPaths{
		BundleDir:   DefaultBundleDir,
		StateDir:    DefaultStateDir,
		MountDir:    DefaultMountDir,
		CLIBinary:   DefaultCLIBinary,
		WorkerMount: DefaultWorkerMount,
	}
}
