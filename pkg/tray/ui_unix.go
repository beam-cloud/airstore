//go:build darwin || linux

package tray

import (
	"fyne.io/systray"
	"github.com/beam-cloud/airstore/pkg/mount"
)

type ui struct {
	app        *App
	mStatus    *systray.MenuItem
	mGateway   *systray.MenuItem
	mToggle    *systray.MenuItem
	mOpen      *systray.MenuItem
	mAutostart *systray.MenuItem
	mQuit      *systray.MenuItem
}

func newUI(app *App) *ui {
	u := &ui{app: app}

	systray.SetIcon(iconDisconnected)
	systray.SetTooltip("Airstore")

	title := systray.AddMenuItem("Airstore", "")
	title.Disable()
	systray.AddSeparator()

	u.mStatus = systray.AddMenuItem("Not mounted", "")
	u.mStatus.Disable()
	u.mGateway = systray.AddMenuItem("Gateway: -", "")
	u.mGateway.Disable()
	systray.AddSeparator()

	u.mToggle = systray.AddMenuItem("Mount", "")
	u.mOpen = systray.AddMenuItem("Open Folder", "")
	u.mOpen.Disable()
	systray.AddSeparator()

	addManagedMenuItems(u)
	u.mAutostart = systray.AddMenuItemCheckbox("Start at Login", "", IsAutostartEnabled())
	systray.AddSeparator()

	u.mQuit = systray.AddMenuItem("Quit", "")

	go u.run()
	return u
}

func (u *ui) run() {
	loginCh := getManagedClickCh()

	for {
		select {
		case <-u.mToggle.ClickedCh:
			u.app.toggleMount()
		case <-u.mOpen.ClickedCh:
			u.app.openFolder()
		case <-u.mAutostart.ClickedCh:
			u.app.toggleAutostart()
		case <-u.mQuit.ClickedCh:
			systray.Quit()
		case <-loginCh:
			handleManagedClick()
		}
	}
}

func (u *ui) updateState(state mount.State, _ error) {
	u.mStatus.SetTitle(u.app.statusText())
	u.mGateway.SetTitle("Gateway: " + u.app.gatewayText())

	mounted := state == mount.Mounted
	busy := state == mount.Mounting || state == mount.Unmounting

	if mounted {
		systray.SetIcon(iconConnected)
		u.mToggle.SetTitle("Unmount")
	} else {
		systray.SetIcon(iconDisconnected)
		u.mToggle.SetTitle("Mount")
	}

	if busy {
		u.mToggle.SetTitle(state.String() + "...")
		u.mToggle.Disable()
	} else {
		u.mToggle.Enable()
	}

	if mounted {
		u.mOpen.Enable()
	} else {
		u.mOpen.Disable()
	}
}

func (u *ui) updateAutostartItem() {
	if IsAutostartEnabled() {
		u.mAutostart.Check()
	} else {
		u.mAutostart.Uncheck()
	}
}
