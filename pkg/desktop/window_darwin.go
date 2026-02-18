//go:build darwin

package desktop

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Cocoa
#import <Cocoa/Cocoa.h>

void desktop_window_show(void *window_ptr) {
    if (window_ptr == NULL) {
        return;
    }
    NSWindow *window = (__bridge NSWindow *)window_ptr;
    [window makeKeyAndOrderFront:nil];
    [NSApp activateIgnoringOtherApps:YES];
}

void desktop_window_hide(void *window_ptr) {
    if (window_ptr == NULL) {
        return;
    }
    NSWindow *window = (__bridge NSWindow *)window_ptr;
    [window orderOut:nil];
}
*/
import "C"
import "unsafe"

func nativeWindowShow(window unsafe.Pointer) {
	C.desktop_window_show(window)
}

func nativeWindowHide(window unsafe.Pointer) {
	C.desktop_window_hide(window)
}
