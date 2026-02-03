//go:build linux

package filesystem

func (f *Filesystem) mountOptions() []string {
	return []string{
		"-o", "allow_other",
		"-o", "default_permissions",
		"-o", "entry_timeout=3",    // Cache directory entries for 3s (conservative for freshness)
		"-o", "attr_timeout=3",     // Cache attributes for 3s
		"-o", "negative_timeout=1", // Cache negative lookups for only 1s (new files appear fast)
		"-o", "max_read=1048576",   // 1MB max read size
		"-o", "max_write=1048576",  // 1MB max write size
	}
}
