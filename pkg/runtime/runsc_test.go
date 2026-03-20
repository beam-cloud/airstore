package runtime

import (
	"syscall"
	"testing"
)

func TestShouldLogRunscExitStderr(t *testing.T) {
	tests := []struct {
		name     string
		exitCode int
		stderr   string
		want     bool
	}{
		{
			name:     "logs regular non-zero exit with stderr",
			exitCode: 1,
			stderr:   "boom",
			want:     true,
		},
		{
			name:     "skips empty stderr",
			exitCode: 1,
			stderr:   "   ",
			want:     false,
		},
		{
			name:     "skips sigkill encoded exit",
			exitCode: 128 + int(syscall.SIGKILL),
			stderr:   "runtime teardown noise",
			want:     false,
		},
		{
			name:     "skips sigterm encoded exit",
			exitCode: 128 + int(syscall.SIGTERM),
			stderr:   "runtime teardown noise",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldLogRunscExitStderr(tt.exitCode, tt.stderr); got != tt.want {
				t.Fatalf("shouldLogRunscExitStderr(%d, %q) = %v, want %v", tt.exitCode, tt.stderr, got, tt.want)
			}
		})
	}
}
