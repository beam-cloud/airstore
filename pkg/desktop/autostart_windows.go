//go:build windows

package desktop

func EnableAutostart() error    { return nil }
func DisableAutostart() error   { return nil }
func IsAutostartEnabled() bool  { return false }
