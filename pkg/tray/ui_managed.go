//go:build managed

package tray

import (
	"os/exec"
	"runtime"

	"fyne.io/systray"
	"github.com/beam-cloud/airstore/pkg/cli"
)

var mLogin *systray.MenuItem

func addManagedMenuItems(u *ui) {
	mLogin = systray.AddMenuItem("Login to Airstore...", "")
}

func getManagedClickCh() <-chan struct{} {
	if mLogin != nil {
		return mLogin.ClickedCh
	}
	return make(chan struct{})
}

func handleManagedClick() {
	cmd := "xdg-open"
	if runtime.GOOS == "darwin" {
		cmd = "open"
	}
	exec.Command(cmd, cli.DashboardURL()+"/login").Start()
}
