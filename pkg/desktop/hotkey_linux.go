//go:build linux

package desktop

import (
	"golang.design/x/hotkey"
)

type linuxHotkey struct {
	hk   *hotkey.Hotkey
	stop chan struct{}
}

func (h *linuxHotkey) Stop() {
	close(h.stop)
	_ = h.hk.Unregister()
}

func (a *App) startHotkey() error {
	hk := hotkey.New([]hotkey.Modifier{hotkey.ModCtrl, hotkey.ModShift}, hotkey.KeyA)
	if err := hk.Register(); err != nil {
		return err
	}

	registered := &linuxHotkey{
		hk:   hk,
		stop: make(chan struct{}),
	}

	a.mu.Lock()
	a.hotkey = registered
	a.mu.Unlock()

	go func() {
		for {
			select {
			case <-registered.hk.Keydown():
				a.toggleWindow()
			case <-registered.stop:
				return
			}
		}
	}()

	return nil
}

func (a *App) stopHotkey() {
	a.mu.Lock()
	hotkey := a.hotkey
	a.hotkey = nil
	a.mu.Unlock()

	if hotkey != nil {
		hotkey.Stop()
	}
}
