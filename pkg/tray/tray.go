package tray

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"fyne.io/systray"
	"github.com/beam-cloud/airstore/pkg/mount"
	"github.com/rs/zerolog/log"
)

type App struct {
	cfg Config
	mgr *mount.MountManager
	ui  *ui
}

func Run(cfg Config) error {
	app := &App{cfg: cfg}
	app.mgr = mount.NewMountManager(mount.Config{
		MountPoint:  cfg.MountPoint,
		ConfigPath:  cfg.ConfigPath,
		GatewayAddr: cfg.GatewayAddr,
		Token:       LoadToken(),
	}, app.onStateChange)

	systray.Run(app.onReady, app.onExit)
	return nil
}

func (a *App) onReady() {
	a.ui = newUI(a)
	writePID()

	if a.cfg.AutoMount {
		go a.mgr.Start()
	}
}

func (a *App) onExit() {
	a.mgr.Stop()
	removePID()
}

func (a *App) onStateChange(state mount.State, err error) {
	if a.ui != nil {
		a.ui.updateState(state, err)
	}
	if err != nil {
		log.Error().Err(err).Str("state", state.String()).Msg("state change")
	}
}

func (a *App) toggleMount() {
	switch a.mgr.State() {
	case mount.Mounted:
		go a.mgr.Stop()
	case mount.Idle, mount.Error:
		go a.mgr.Start()
	}
}

func (a *App) openFolder() {
	cmd := "xdg-open"
	if runtime.GOOS == "darwin" {
		cmd = "open"
	}
	exec.Command(cmd, a.cfg.MountPoint).Start()
}

func (a *App) toggleAutostart() {
	var err error
	if IsAutostartEnabled() {
		err = DisableAutostart()
	} else {
		err = EnableAutostart()
	}
	if err != nil {
		log.Error().Err(err).Msg("autostart toggle")
	}
	if a.ui != nil {
		a.ui.updateAutostartItem()
	}
}

func (a *App) statusText() string {
	state := a.mgr.State()
	switch state {
	case mount.Mounted:
		return "Mounted at " + a.cfg.MountPoint
	case mount.Mounting:
		return "Mounting..."
	case mount.Unmounting:
		return "Unmounting..."
	case mount.Error:
		if err := a.mgr.Err(); err != nil {
			return "Error: " + formatError(err)
		}
		return "Error"
	}
	return "Not mounted"
}

// formatError converts gRPC errors to human-readable messages.
func formatError(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()

	// Parse "rpc error: code = X desc = Y" format
	re := regexp.MustCompile(`rpc error: code = (\w+) desc = (.+)`)
	if matches := re.FindStringSubmatch(msg); len(matches) == 3 {
		code, desc := matches[1], matches[2]
		switch code {
		case "Unauthenticated":
			return "Not logged in - run 'airstore login'"
		case "PermissionDenied":
			return "Access denied"
		case "Unavailable":
			return "Gateway unavailable"
		case "DeadlineExceeded":
			return "Request timed out"
		default:
			return desc
		}
	}

	// Clean up common patterns
	if strings.Contains(msg, "connection refused") {
		return "Gateway not running"
	}

	return msg
}

func (a *App) gatewayText() string {
	if a.mgr.State() == mount.Mounted {
		return a.mgr.GatewayAddr()
	}
	return "-"
}

// PID file for stop command
var pidPath = func() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".airstore", "airstore.pid")
}()

func writePID() {
	os.MkdirAll(filepath.Dir(pidPath), 0755)
	os.WriteFile(pidPath, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
}

func removePID() { os.Remove(pidPath) }

func ReadPID() int {
	data, _ := os.ReadFile(pidPath)
	var pid int
	fmt.Sscanf(string(data), "%d", &pid)
	return pid
}
