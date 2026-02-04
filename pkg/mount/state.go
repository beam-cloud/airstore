package mount

// State represents the current state of the mount lifecycle.
type State int

const (
	Idle       State = iota // Not mounted, no activity
	Mounting                // Mount in progress
	Mounted                 // Filesystem is mounted and operational
	Unmounting              // Unmount in progress
	Error                   // An error occurred
)

func (s State) String() string {
	switch s {
	case Idle:
		return "idle"
	case Mounting:
		return "mounting"
	case Mounted:
		return "mounted"
	case Unmounting:
		return "unmounting"
	case Error:
		return "error"
	default:
		return "unknown"
	}
}
