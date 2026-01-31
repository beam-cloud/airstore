package menubar

import (
	"fmt"
	"os/exec"

	"github.com/beam-cloud/airstore/pkg/mount"
	"github.com/rs/zerolog/log"
)

// App wires a MountManager to the systray menu bar UI.
type App struct {
	prefs   Preferences
	mgr     *mount.MountManager
	ui      *ui
	quitting bool
}

// NewApp creates a new menu bar application.
func NewApp(prefs Preferences) *App {
	a := &App{prefs: prefs}

	a.mgr = mount.NewMountManager(mount.Config{
		MountPoint:  prefs.MountPoint,
		ConfigPath:  prefs.ConfigPath,
		Verbose:     false,
	}, a.onStateChange)

	return a
}

// OnReady is called by systray once the menu bar is initialized.
func (a *App) OnReady() {
	a.ui = newUI(a)

	if a.prefs.AutoMount {
		go func() {
			if err := a.mgr.Start(); err != nil {
				log.Error().Err(err).Msg("auto-mount failed")
			}
		}()
	}
}

// OnExit is called by systray when the app is quitting.
func (a *App) OnExit() {
	a.quitting = true
	a.mgr.Stop()
}

func (a *App) onStateChange(state mount.State, err error) {
	if a.ui == nil {
		return
	}
	if err != nil {
		log.Error().Err(err).Str("state", state.String()).Msg("mount state error")
	}
	a.ui.updateState(state, err)
}

func (a *App) toggleMount() {
	switch a.mgr.State() {
	case mount.Mounted:
		go a.mgr.Stop()
	case mount.Idle, mount.Error:
		go func() {
			if err := a.mgr.Start(); err != nil {
				log.Error().Err(err).Msg("mount failed")
			}
		}()
	}
}

func (a *App) reloadConfig() {
	go func() {
		if err := a.mgr.Reload(); err != nil {
			log.Error().Err(err).Msg("reload failed")
		}
	}()
}

func (a *App) openInFinder() {
	exec.Command("open", a.prefs.MountPoint).Start()
}

func (a *App) toggleStartAtLogin() {
	if IsLaunchAgentInstalled() {
		if err := UninstallLaunchAgent(); err != nil {
			log.Error().Err(err).Msg("failed to uninstall launch agent")
		}
	} else {
		if err := InstallLaunchAgent(); err != nil {
			log.Error().Err(err).Msg("failed to install launch agent")
		}
	}
	if a.ui != nil {
		a.ui.updateLoginItem()
	}
}

func (a *App) statusText() string {
	switch a.mgr.State() {
	case mount.Mounted:
		return fmt.Sprintf("Status: Mounted at %s", a.prefs.MountPoint)
	case mount.Mounting:
		return "Status: Mounting..."
	case mount.Unmounting:
		return "Status: Unmounting..."
	case mount.Error:
		if err := a.mgr.Err(); err != nil {
			return fmt.Sprintf("Status: Error - %s", err)
		}
		return "Status: Error"
	default:
		return "Status: Not mounted"
	}
}

func (a *App) gatewayText() string {
	if a.mgr.State() == mount.Mounted {
		return fmt.Sprintf("Gateway: %s", a.mgr.GatewayAddr())
	}
	return "Gateway: -"
}
