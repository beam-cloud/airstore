package menubar

import (
	"fmt"
	"os"
	"path/filepath"
)

const launchAgentLabel = "com.beam-cloud.airstore"

func launchAgentPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("failed to get home directory: %w", err)
	}
	return filepath.Join(home, "Library", "LaunchAgents", launchAgentLabel+".plist"), nil
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
	path, err := launchAgentPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("failed to create LaunchAgents directory: %w", err)
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}
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
	path, err := launchAgentPath()
	if err != nil {
		return err
	}
	return os.Remove(path)
}

// IsLaunchAgentInstalled checks if the LaunchAgent plist exists.
func IsLaunchAgentInstalled() bool {
	path, err := launchAgentPath()
	if err != nil {
		return false
	}
	_, err = os.Stat(path)
	return err == nil
}
