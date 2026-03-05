//go:build linux

package filesystem

import (
	"os"
	"strings"
)

func (f *Filesystem) mountOptions() []string {
	opts := []string{
		"-o", "default_permissions",
		"-o", "entry_timeout=3", // Cache directory entries for 3s (conservative for freshness)
		"-o", "attr_timeout=3", // Cache attributes for 3s
		"-o", "negative_timeout=1", // Cache negative lookups for only 1s (new files appear fast)
		"-o", "max_read=1048576", // 1MB max read size
		"-o", "max_write=1048576", // 1MB max write size
	}

	if shouldUseAllowOther() {
		return append([]string{"-o", "allow_other"}, opts...)
	}
	return opts
}

// shouldUseAllowOther decides whether we should pass `-o allow_other` to FUSE.
//
// In rootless environments, allow_other requires `user_allow_other` in
// /etc/fuse.conf. If absent, FUSE fails hard with:
// "option allow_other only allowed if 'user_allow_other' is set in /etc/fuse.conf".
func shouldUseAllowOther() bool {
	if v, ok := envBoolWithSet("AIRSTORE_FUSE_ALLOW_OTHER"); ok {
		return v
	}
	if os.Geteuid() == 0 {
		return true
	}
	data, err := os.ReadFile("/etc/fuse.conf")
	if err != nil {
		return false
	}
	return fuseConfigAllowsOther(string(data))
}

func envBoolWithSet(name string) (bool, bool) {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(name)))
	switch v {
	case "":
		return false, false
	case "1", "true", "yes", "on":
		return true, true
	case "0", "false", "no", "off":
		return false, true
	default:
		return false, false
	}
}

func fuseConfigAllowsOther(contents string) bool {
	for _, line := range strings.Split(contents, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "user_allow_other") {
			return true
		}
	}
	return false
}
