package embed

import (
	"embed"
	"fmt"
	"runtime"
)

//go:embed shims/*
var shimsFS embed.FS

// Platform represents a target OS/architecture combination
type Platform struct {
	OS   string
	Arch string
}

// Current returns the platform of the running system
func Current() Platform {
	return Platform{OS: runtime.GOOS, Arch: runtime.GOARCH}
}

func (p Platform) String() string {
	return fmt.Sprintf("%s_%s", p.OS, p.Arch)
}

// SupportedPlatforms lists all platforms we build shims for
var SupportedPlatforms = []Platform{
	{OS: "darwin", Arch: "amd64"},
	{OS: "darwin", Arch: "arm64"},
	{OS: "linux", Arch: "amd64"},
	{OS: "linux", Arch: "arm64"},
}

// GetShim returns the shim binary for the current platform
func GetShim() ([]byte, error) {
	return GetShimForPlatform(Current())
}

// GetShimForPlatform returns the shim binary for a specific platform
func GetShimForPlatform(p Platform) ([]byte, error) {
	path := fmt.Sprintf("shims/%s_%s", p.OS, p.Arch)
	data, err := shimsFS.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("shim not available for %s: %w", p, err)
	}
	return data, nil
}

// HasShim checks if a shim exists for the given platform
func HasShim(p Platform) bool {
	path := fmt.Sprintf("shims/%s_%s", p.OS, p.Arch)
	_, err := shimsFS.ReadFile(path)
	return err == nil
}
