package menubar

/*
#cgo CFLAGS: -x objective-c
#cgo LDFLAGS: -framework Cocoa -lobjc

#include <objc/runtime.h>
#include <objc/message.h>
#import <AppKit/AppKit.h>

// Replacement implementation for SystrayAppDelegate's show_menu method.
// The original positions the popup menu at y = button.height + 6, which places
// the menu origin above the visible screen edge on macOS (where y=0 is the
// bottom of the coordinate system). macOS then clamps the menu downward and
// adds a scroll arrow at the top to compensate.
//
// Positioning at (0, 0) — the bottom-left corner of the status item button —
// lets the menu drop down naturally with the full screen height available.
static void patchedShowMenu(id self, SEL _cmd) {
    Ivar statusItemIvar = class_getInstanceVariable([self class], "statusItem");
    Ivar menuIvar       = class_getInstanceVariable([self class], "menu");

    NSStatusItem *statusItem = object_getIvar(self, statusItemIvar);
    NSMenu       *menu       = object_getIvar(self, menuIvar);

    [menu popUpMenuPositioningItem:nil
                        atLocation:NSMakePoint(0, 0)
                            inView:statusItem.button];
}

// patchSystrayMenuPosition swizzles SystrayAppDelegate's show_menu method
// with the corrected positioning implementation above.
static void patchSystrayMenuPosition(void) {
    Class cls = NSClassFromString(@"SystrayAppDelegate");
    if (!cls) {
        return;
    }

    SEL sel = @selector(show_menu);
    Method m = class_getInstanceMethod(cls, sel);
    if (!m) {
        return;
    }

    method_setImplementation(m, (IMP)patchedShowMenu);
}
*/
import "C"

// patchMenuPosition replaces the systray library's show_menu implementation
// with one that positions the popup menu at the bottom of the status bar button
// instead of above it. This prevents macOS from adding a scroll arrow.
func patchMenuPosition() {
	C.patchSystrayMenuPosition()
}
