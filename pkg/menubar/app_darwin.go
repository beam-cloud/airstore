package menubar

import (
	"fyne.io/systray"
	"github.com/beam-cloud/airstore/pkg/mount"
)

// ui holds the systray menu items and updates them based on state changes.
type ui struct {
	app *App

	mStatus  *systray.MenuItem
	mGateway *systray.MenuItem
	mToggle  *systray.MenuItem
	mOpen    *systray.MenuItem
	mReload  *systray.MenuItem
	mLogin   *systray.MenuItem
	mQuit    *systray.MenuItem
}

func newUI(app *App) *ui {
	u := &ui{app: app}

	systray.SetIcon(iconDisconnected)
	systray.SetTooltip("Airstore")

	u.mStatus = systray.AddMenuItem("Status: Not mounted", "")
	u.mStatus.Disable()

	u.mGateway = systray.AddMenuItem("Gateway: -", "")
	u.mGateway.Disable()

	systray.AddSeparator()

	u.mToggle = systray.AddMenuItem("Mount", "Mount the Airstore filesystem")
	u.mOpen = systray.AddMenuItem("Open in Finder", "Open mount point in Finder")
	u.mOpen.Disable()

	systray.AddSeparator()

	u.mReload = systray.AddMenuItem("Reload Config", "Reload configuration and remount")
	u.mLogin = systray.AddMenuItemCheckbox("Start at Login", "Toggle launch agent", IsLaunchAgentInstalled())

	systray.AddSeparator()

	u.mQuit = systray.AddMenuItem("Quit Airstore", "Unmount and quit")

	patchMenuPosition()

	go u.eventLoop()

	return u
}

func (u *ui) eventLoop() {
	for {
		select {
		case <-u.mToggle.ClickedCh:
			u.app.toggleMount()
		case <-u.mOpen.ClickedCh:
			u.app.openInFinder()
		case <-u.mReload.ClickedCh:
			u.app.reloadConfig()
		case <-u.mLogin.ClickedCh:
			u.app.toggleStartAtLogin()
		case <-u.mQuit.ClickedCh:
			systray.Quit()
		}
	}
}

func (u *ui) updateState(state mount.State, err error) {
	u.mStatus.SetTitle(u.app.statusText())
	u.mGateway.SetTitle(u.app.gatewayText())

	switch state {
	case mount.Mounted:
		systray.SetIcon(iconConnected)
		u.mToggle.SetTitle("Unmount")
		u.mToggle.Enable()
		u.mOpen.Enable()
		u.mReload.Enable()
	case mount.Mounting, mount.Unmounting:
		u.mToggle.SetTitle(state.String() + "...")
		u.mToggle.Disable()
		u.mOpen.Disable()
		u.mReload.Disable()
	case mount.Idle:
		systray.SetIcon(iconDisconnected)
		u.mToggle.SetTitle("Mount")
		u.mToggle.Enable()
		u.mOpen.Disable()
		u.mReload.Enable()
	case mount.Error:
		systray.SetIcon(iconDisconnected)
		u.mToggle.SetTitle("Mount")
		u.mToggle.Enable()
		u.mOpen.Disable()
		u.mReload.Enable()
	}
}

func (u *ui) updateLoginItem() {
	if IsLaunchAgentInstalled() {
		u.mLogin.Check()
	} else {
		u.mLogin.Uncheck()
	}
}
