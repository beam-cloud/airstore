package desktop

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	webview "github.com/webview/webview_go"
)

func (a *App) createWindow(url string) error {
	w := webview.New(false)
	if w == nil {
		return errors.New("failed to create webview window")
	}

	w.SetTitle("Airstore")
	w.SetSize(1200, 800, webview.HintNone)
	if err := w.Bind("desktopReportError", func(kind string, message string) {
		trimmed := strings.TrimSpace(message)
		if trimmed == "" {
			return
		}
		payload := strings.TrimSpace(kind) + ": " + trimmed
		a.setUIError(payload)
		log.Warn().Str("ui_error", payload).Msg("desktop webview runtime error")
	}); err != nil {
		return fmt.Errorf("failed to bind desktopReportError: %w", err)
	}
	w.Init(`
		(function() {
			function report(kind, message) {
				if (typeof window.desktopReportError === "function") {
					try { window.desktopReportError(kind, String(message || "")); } catch (_) {}
				}
			}

			window.addEventListener("error", function(event) {
				if (!event) return;
				var msg = event.message || "unknown error";
				var file = event.filename || "";
				var line = event.lineno || 0;
				report("window.error", msg + " @ " + file + ":" + line);
			});

			window.addEventListener("unhandledrejection", function(event) {
				if (!event) return;
				var reason = event.reason;
				if (reason && typeof reason === "object" && "message" in reason) {
					report("unhandledrejection", reason.message);
					return;
				}
				report("unhandledrejection", String(reason || "unknown rejection"));
			});
		})();
	`)
	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	w.Navigate(fmt.Sprintf("%s%sv=%d", url, sep, time.Now().UnixNano()))

	a.mu.Lock()
	a.webview = w
	a.windowVisible = true
	a.mu.Unlock()

	return nil
}

func (a *App) runWindow() {
	a.mu.Lock()
	w := a.webview
	a.mu.Unlock()
	if w == nil {
		return
	}
	w.Run()
	a.setWindowVisible(false)
}

func (a *App) destroyWindow() {
	a.mu.Lock()
	w := a.webview
	a.webview = nil
	a.windowVisible = false
	a.mu.Unlock()

	if w != nil {
		w.Destroy()
	}
}

func (a *App) requestQuit() {
	a.dispatchToWindow(func(w webview.WebView) {
		w.Terminate()
	})
}

func (a *App) showWindow() {
	a.dispatchToWindow(func(w webview.WebView) {
		nativeWindowShow(w.Window())
		a.setWindowVisible(true)
		a.updateNativeTray()
	})
}

func (a *App) hideWindow() {
	a.dispatchToWindow(func(w webview.WebView) {
		nativeWindowHide(w.Window())
		a.setWindowVisible(false)
		a.updateNativeTray()
	})
}

func (a *App) toggleWindow() {
	if a.isWindowVisible() {
		a.hideWindow()
		return
	}
	a.showWindow()
}

func (a *App) dispatchToWindow(fn func(webview.WebView)) {
	a.mu.Lock()
	w := a.webview
	a.mu.Unlock()

	if w == nil {
		return
	}
	w.Dispatch(func() {
		fn(w)
	})
}

func (a *App) dispatchToUI(fn func()) {
	a.dispatchToWindow(func(_ webview.WebView) {
		fn()
	})
}
