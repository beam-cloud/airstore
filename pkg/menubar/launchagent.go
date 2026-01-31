package menubar

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

func appBinaryPath() string {
	return "/Applications/Airstore.app/Contents/MacOS/airstore-menubar"
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

// InstallLaunchAgent creates a LaunchAgent plist so the app starts at login.
func InstallLaunchAgent() error {
	path := launchAgentPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create LaunchAgents directory: %w", err)
	}

	home, _ := os.UserHomeDir()
	logDir := filepath.Join(home, "Library", "Logs", "Airstore")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	stdout := filepath.Join(logDir, "airstore.log")
	stderr := filepath.Join(logDir, "airstore.log")

	plist := fmt.Sprintf(plistTemplate, launchAgentLabel, appBinaryPath(), stdout, stderr)
	return os.WriteFile(path, []byte(plist), 0644)
}

// UninstallLaunchAgent removes the LaunchAgent plist.
func UninstallLaunchAgent() error {
	return os.Remove(launchAgentPath())
}

// IsLaunchAgentInstalled checks if the LaunchAgent plist exists.
func IsLaunchAgentInstalled() bool {
	_, err := os.Stat(launchAgentPath())
	return err == nil
}
