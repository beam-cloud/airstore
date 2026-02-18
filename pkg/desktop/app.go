package desktop

import (
	"context"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/mount"
	"github.com/rs/zerolog/log"
	webview "github.com/webview/webview_go"
)

// App is the main desktop application with a webview window and systray.
type App struct {
	cfg Config
	mgr *mount.MountManager

	mu            sync.Mutex
	webview       webview.WebView
	windowVisible bool
	trayReady     bool
	lastUIError   string
	hotkey        hotkeyController
	server        *desktopServer
}

type hotkeyController interface {
	Stop()
}

// Run starts the desktop application (webview + systray + hotkey).
func Run(cfg Config) error {
	app := newApp(cfg)
	return app.run()
}

func newApp(cfg Config) *App {
	defaults := DefaultConfig()
	if cfg.MountPoint == "" {
		cfg.MountPoint = defaults.MountPoint
	}
	if cfg.GatewayAddr == "" {
		cfg.GatewayAddr = defaults.GatewayAddr
	}
	if cfg.GatewayHTTPAddr == "" {
		cfg.GatewayHTTPAddr = "http://localhost:1994"
	}
	return &App{cfg: cfg}
}

func (a *App) run() error {
	a.mgr = mount.NewMountManager(mount.Config{
		MountPoint:  a.cfg.MountPoint,
		ConfigPath:  a.cfg.ConfigPath,
		GatewayAddr: a.cfg.GatewayAddr,
		Token:       a.cfg.Token,
	}, a.onStateChange)

	server, err := startDesktopServer(a)
	if err != nil {
		return err
	}
	a.server = server

	WritePID()
	defer RemovePID()

	if err := a.createWindow(server.baseURL); err != nil {
		_ = a.shutdown()
		return err
	}

	if err := initDesktopTray(a); err != nil {
		log.Warn().Err(err).Msg("failed to initialize tray")
	}

	if err := a.startHotkey(); err != nil {
		log.Warn().Err(err).Msg("failed to register global hotkey")
	}

	if a.cfg.AutoMount {
		go func() {
			if err := a.mgr.Start(); err != nil {
				log.Error().Err(err).Msg("auto-mount failed")
			}
		}()
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)

	go func() {
		<-signals
		a.requestQuit()
	}()

	a.runWindow()
	return a.shutdown()
}

func (a *App) shutdown() error {
	a.stopHotkey()
	shutdownDesktopTray()
	a.setTrayReady(false)

	if a.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := a.server.shutdown(ctx); err != nil && !isClosedNetworkError(err) {
			log.Warn().Err(err).Msg("desktop server shutdown failed")
		}
	}

	if a.mgr != nil {
		a.mgr.Stop()
	}

	a.destroyWindow()
	return nil
}

func (a *App) onStateChange(state mount.State, err error) {
	if err != nil {
		log.Error().Err(err).Str("state", state.String()).Msg("mount state change")
	}
	a.updateNativeTray()
}

func (a *App) toggleMount() {
	switch a.mgr.State() {
	case mount.Mounted:
		go a.mgr.Stop()
	case mount.Idle, mount.Error:
		go func() {
			if err := a.mgr.Start(); err != nil {
				log.Error().Err(err).Msg("mount start failed")
			}
		}()
	}
}

func (a *App) openFolder() error {
	command := "xdg-open"
	if runtime.GOOS == "darwin" {
		command = "open"
	}
	return exec.Command(command, a.cfg.MountPoint).Start()
}

func (a *App) toggleAutostart() {
	var err error
	if IsAutostartEnabled() {
		err = DisableAutostart()
	} else {
		err = EnableAutostart()
	}
	if err != nil {
		log.Error().Err(err).Msg("failed to toggle autostart")
	}
	a.updateNativeTray()
}

func (a *App) setTrayReady(v bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.trayReady = v
}

func (a *App) isTrayReady() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.trayReady
}

func (a *App) setWindowVisible(v bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.windowVisible = v
}

func (a *App) isWindowVisible() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.windowVisible
}

func (a *App) setUIError(err string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.lastUIError = err
}

func (a *App) getUIError() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.lastUIError
}
