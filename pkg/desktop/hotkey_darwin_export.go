//go:build darwin

package desktop

/*
#include <stdint.h>
*/
import "C"

//export desktopHotkeyOnPressed
func desktopHotkeyOnPressed() {
	darwinHotkeyAppMu.Lock()
	app := darwinHotkeyApp
	darwinHotkeyAppMu.Unlock()

	if app != nil {
		go app.toggleWindow()
	}
}
