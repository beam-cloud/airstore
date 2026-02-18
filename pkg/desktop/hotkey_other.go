//go:build !darwin && !linux

package desktop

func (a *App) startHotkey() error {
	return nil
}

func (a *App) stopHotkey() {}
