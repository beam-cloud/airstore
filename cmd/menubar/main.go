package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"

	"github.com/beam-cloud/airstore/pkg/menubar"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"fyne.io/systray"
)

func main() {
	runtime.LockOSThread()

	installAgent := flag.Bool("install-launchagent", false, "Install LaunchAgent for start-at-login")
	uninstallAgent := flag.Bool("uninstall-launchagent", false, "Uninstall LaunchAgent")
	flag.Parse()

	// Handle LaunchAgent CLI flags and exit
	if *installAgent {
		if err := menubar.InstallLaunchAgent(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("LaunchAgent installed")
		return
	}
	if *uninstallAgent {
		if err := menubar.UninstallLaunchAgent(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("LaunchAgent uninstalled")
		return
	}

	// Configure logging
	log.Logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).
		With().Timestamp().Logger()

	prefs, err := menubar.LoadPreferences()
	if err != nil {
		log.Warn().Err(err).Msg("failed to load preferences, using defaults")
	}

	app := menubar.NewApp(prefs)
	systray.Run(app.OnReady, app.OnExit)
}
