package desktop

import (
	"regexp"
	"strings"
	"sync"
	"time"

	"fyne.io/systray"
	"github.com/beam-cloud/airstore/pkg/mount"
)

type traySnapshot struct {
	ShowTitle       string
	StatusTitle     string
	GatewayTitle    string
	MountTitle      string
	Mounted         bool
	MountBusy       bool
	AutostartEnable bool
}

var (
	trayMu     sync.Mutex
	activeTray *desktopTray
)

type desktopTray struct {
	end      func()
	stopOnce sync.Once
	stopCh   chan struct{}

	mShow      *systray.MenuItem
	mStatus    *systray.MenuItem
	mGateway   *systray.MenuItem
	mToggle    *systray.MenuItem
	mOpen      *systray.MenuItem
	mAutostart *systray.MenuItem
}

func (t *desktopTray) stop() {
	t.stopOnce.Do(func() {
		close(t.stopCh)
	})
}

func setActiveTray(tray *desktopTray) {
	trayMu.Lock()
	defer trayMu.Unlock()
	activeTray = tray
}

func getActiveTray() *desktopTray {
	trayMu.Lock()
	defer trayMu.Unlock()
	return activeTray
}

func clearActiveTray() {
	trayMu.Lock()
	defer trayMu.Unlock()
	activeTray = nil
}

func initDesktopTray(app *App) error {
	tray := &desktopTray{stopCh: make(chan struct{})}
	setActiveTray(tray)
	start, end := systray.RunWithExternalLoop(func() {
		systray.SetIcon(iconDisconnected)
		systray.SetTooltip("Airstore")

		title := systray.AddMenuItem("Airstore", "")
		title.Disable()
		systray.AddSeparator()

		tray.mStatus = systray.AddMenuItem("Not mounted", "")
		tray.mStatus.Disable()
		tray.mGateway = systray.AddMenuItem("Gateway: -", "")
		tray.mGateway.Disable()
		systray.AddSeparator()

		tray.mShow = systray.AddMenuItem("Hide Airstore", "")
		tray.mToggle = systray.AddMenuItem("Mount", "")
		tray.mOpen = systray.AddMenuItem("Open Folder", "")
		tray.mOpen.Disable()
		systray.AddSeparator()

		tray.mAutostart = systray.AddMenuItemCheckbox("Start at Login", "", IsAutostartEnabled())
		systray.AddSeparator()

		mQuit := systray.AddMenuItem("Quit", "")

		go func() {
			for {
				select {
				case <-tray.mShow.ClickedCh:
					app.toggleWindow()
				case <-tray.mToggle.ClickedCh:
					app.toggleMount()
				case <-tray.mOpen.ClickedCh:
					_ = app.openFolder()
				case <-tray.mAutostart.ClickedCh:
					app.toggleAutostart()
				case <-mQuit.ClickedCh:
					app.requestQuit()
				case <-tray.stopCh:
					return
				}
			}
		}()

		app.setTrayReady(true)
		app.updateNativeTray()
	}, func() {
		tray.stop()
	})
	tray.end = end

	start()
	return nil
}

func shutdownDesktopTray() {
	tray := getActiveTray()
	clearActiveTray()

	if tray == nil {
		return
	}

	if tray.end != nil {
		done := make(chan struct{})
		go func() {
			tray.end()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(750 * time.Millisecond):
		}
	}
	tray.stop()
}

func (a *App) updateNativeTray() {
	if !a.isTrayReady() {
		return
	}

	tray := getActiveTray()
	if tray == nil {
		return
	}

	snapshot := a.currentTraySnapshot()
	if snapshot.Mounted {
		systray.SetIcon(iconConnected)
	} else {
		systray.SetIcon(iconDisconnected)
	}

	if tray.mShow != nil {
		tray.mShow.SetTitle(snapshot.ShowTitle)
	}
	if tray.mStatus != nil {
		tray.mStatus.SetTitle(snapshot.StatusTitle)
	}
	if tray.mGateway != nil {
		tray.mGateway.SetTitle(snapshot.GatewayTitle)
	}
	if tray.mToggle != nil {
		tray.mToggle.SetTitle(snapshot.MountTitle)
		if snapshot.MountBusy {
			tray.mToggle.Disable()
		} else {
			tray.mToggle.Enable()
		}
	}
	if tray.mOpen != nil {
		if snapshot.Mounted {
			tray.mOpen.Enable()
		} else {
			tray.mOpen.Disable()
		}
	}
	if tray.mAutostart != nil {
		if snapshot.AutostartEnable {
			tray.mAutostart.Check()
		} else {
			tray.mAutostart.Uncheck()
		}
	}
}

func (a *App) currentTraySnapshot() traySnapshot {
	showTitle := "Show Airstore"
	if a.isWindowVisible() {
		showTitle = "Hide Airstore"
	}

	snapshot := traySnapshot{
		ShowTitle:       showTitle,
		StatusTitle:     "Not mounted",
		GatewayTitle:    "Gateway: -",
		MountTitle:      "Mount",
		AutostartEnable: IsAutostartEnabled(),
	}

	if a.mgr == nil {
		return snapshot
	}

	state := a.mgr.State()
	switch state {
	case mount.Mounted:
		snapshot.Mounted = true
		snapshot.StatusTitle = "Mounted at " + a.cfg.MountPoint
		snapshot.GatewayTitle = "Gateway: " + a.mgr.GatewayAddr()
		snapshot.MountTitle = "Unmount"
	case mount.Mounting:
		snapshot.StatusTitle = "Mounting..."
		snapshot.MountTitle = "Mounting..."
		snapshot.MountBusy = true
	case mount.Unmounting:
		snapshot.StatusTitle = "Unmounting..."
		snapshot.MountTitle = "Unmounting..."
		snapshot.MountBusy = true
	case mount.Error:
		snapshot.StatusTitle = "Error"
		if err := a.mgr.Err(); err != nil {
			snapshot.StatusTitle = "Error: " + formatTrayError(err)
		}
	}

	return snapshot
}

func formatTrayError(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
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

	if strings.Contains(msg, "connection refused") {
		return "Gateway not running"
	}
	return msg
}
