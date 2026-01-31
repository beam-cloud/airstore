//go:build darwin

package tray

import (
	"fmt"
	"os"
	"path/filepath"
)

const launchAgentLabel = "com.beam-cloud.airstore"

func launchAgentPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents", launchAgentLabel+".plist")
}

// getBinaryPath returns the resolved path to the current executable.
func getBinaryPath() (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot determine executable path: %w", err)
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		return exe, nil // symlink resolution failed, use original
	}
	return resolved, nil
}

const plistTemplate = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
	<key>Label</key>
	<string>%s</string>
	<key>ProgramArguments</key>
	<array>
		<string>%s</string>
		<string>start</string>
	</array>
	<key>RunAtLoad</key>
	<true/>
	<key>KeepAlive</key>
	<false/>
	<key>StandardOutPath</key>
	<string>%s</string>
	<key>StandardErrorPath</key>
	<string>%s</string>
</dict>
</plist>
`

// EnableAutostart creates a LaunchAgent plist so the app starts at login.
func EnableAutostart() error {
	binaryPath, err := getBinaryPath()
	if err != nil {
		return err
	}

	home, _ := os.UserHomeDir()
	plistPath := launchAgentPath()
	logFile := filepath.Join(home, "Library", "Logs", "Airstore", "airstore.log")

	if err := os.MkdirAll(filepath.Dir(plistPath), 0755); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(logFile), 0755); err != nil {
		return err
	}

	plist := fmt.Sprintf(plistTemplate, launchAgentLabel, binaryPath, logFile, logFile)
	return os.WriteFile(plistPath, []byte(plist), 0644)
}

// DisableAutostart removes the LaunchAgent plist.
func DisableAutostart() error {
	return os.Remove(launchAgentPath())
}

// IsAutostartEnabled checks if the LaunchAgent plist exists.
func IsAutostartEnabled() bool {
	_, err := os.Stat(launchAgentPath())
	return err == nil
}
