//go:build darwin

package desktop

/*
#cgo CFLAGS: -x objective-c -fobjc-arc
#cgo LDFLAGS: -framework Cocoa
#import <Cocoa/Cocoa.h>

extern void desktopHotkeyOnPressed(void);

static id g_hotkeyGlobalMonitor = nil;
static id g_hotkeyLocalMonitor = nil;

static void maybe_trigger_hotkey(NSEvent *event) {
    if (event == nil) {
        return;
    }
    NSEventModifierFlags flags = [event modifierFlags] & NSEventModifierFlagDeviceIndependentFlagsMask;
    BOOL cmd = (flags & NSEventModifierFlagCommand) != 0;
    BOOL shift = (flags & NSEventModifierFlagShift) != 0;
    if (!cmd || !shift) {
        return;
    }
    NSString *chars = [event charactersIgnoringModifiers];
    if (chars == nil) {
        return;
    }
    if ([[chars lowercaseString] isEqualToString:@"a"]) {
        desktopHotkeyOnPressed();
    }
}

void desktop_hotkey_start(void) {
    if (g_hotkeyGlobalMonitor != nil || g_hotkeyLocalMonitor != nil) {
        return;
    }

    g_hotkeyGlobalMonitor = [NSEvent addGlobalMonitorForEventsMatchingMask:NSEventMaskKeyDown
        handler:^(NSEvent *event) {
            maybe_trigger_hotkey(event);
        }];

    g_hotkeyLocalMonitor = [NSEvent addLocalMonitorForEventsMatchingMask:NSEventMaskKeyDown
        handler:^NSEvent *(NSEvent *event) {
            maybe_trigger_hotkey(event);
            return event;
        }];
}

void desktop_hotkey_stop(void) {
    if (g_hotkeyGlobalMonitor != nil) {
        [NSEvent removeMonitor:g_hotkeyGlobalMonitor];
        g_hotkeyGlobalMonitor = nil;
    }
    if (g_hotkeyLocalMonitor != nil) {
        [NSEvent removeMonitor:g_hotkeyLocalMonitor];
        g_hotkeyLocalMonitor = nil;
    }
}
*/
import "C"
import "sync"

type darwinHotkey struct{}

var (
	darwinHotkeyAppMu sync.Mutex
	darwinHotkeyApp   *App
)

func (d *darwinHotkey) Stop() {
	C.desktop_hotkey_stop()
	darwinHotkeyAppMu.Lock()
	darwinHotkeyApp = nil
	darwinHotkeyAppMu.Unlock()
}

func (a *App) startHotkey() error {
	darwinHotkeyAppMu.Lock()
	darwinHotkeyApp = a
	darwinHotkeyAppMu.Unlock()

	C.desktop_hotkey_start()

	a.mu.Lock()
	a.hotkey = &darwinHotkey{}
	a.mu.Unlock()
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
