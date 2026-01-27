//go:build linux

package filesystem

func (f *Filesystem) mountOptions() []string {
	return []string{"-o", "allow_other", "-o", "default_permissions"}
}
