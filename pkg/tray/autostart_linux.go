//go:build linux

package tray

import (
	"fmt"
	"os"
	"path/filepath"
)

const desktopFileName = "airstore.desktop"

func autostartDir() string {
	// Check XDG_CONFIG_HOME first
	if xdgConfig := os.Getenv("XDG_CONFIG_HOME"); xdgConfig != "" {
		return filepath.Join(xdgConfig, "autostart")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "autostart")
}

func autostartPath() string {
	return filepath.Join(autostartDir(), desktopFileName)
}

// getBinaryPath returns the resolved path to the current executable.
func getBinaryPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot determine executable path: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return exe, nil
	}
	return resolved, nil
}

const desktopTemplate = `[Desktop Entry]
Type=Application
Name=Airstore
Comment=Virtual filesystem for AI agents
Exec=%s start
Icon=airstore
Terminal=false
Categories=Utility;
StartupNotify=false
X-GNOME-Autostart-enabled=true
`

// EnableAutostart creates an XDG autostart desktop entry.
func EnableAutostart() error {
	binaryPath, err := getBinaryPath()
	if err != nil {
		return err
	}

	path := autostartPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	return os.WriteFile(path, []byte(fmt.Sprintf(desktopTemplate, binaryPath)), 0644)
}

// DisableAutostart removes the XDG autostart desktop entry.
func DisableAutostart() error {
	err := os.Remove(autostartPath())
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// IsAutostartEnabled checks if the autostart desktop entry exists.
func IsAutostartEnabled() bool {
	_, err := os.Stat(autostartPath())
	return err == nil
}
