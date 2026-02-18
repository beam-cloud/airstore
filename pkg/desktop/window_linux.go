//go:build linux

package desktop

/*
#cgo pkg-config: gtk+-3.0
#include <gtk/gtk.h>

void desktop_window_show(void *window_ptr) {
    if (window_ptr == NULL) {
        return;
    }
    GtkWidget *window = GTK_WIDGET(window_ptr);
    gtk_widget_show(window);
    gtk_window_present(GTK_WINDOW(window));
}

void desktop_window_hide(void *window_ptr) {
    if (window_ptr == NULL) {
        return;
    }
    GtkWidget *window = GTK_WIDGET(window_ptr);
    gtk_widget_hide(window);
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
