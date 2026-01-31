//go:build !managed

package tray

// OSS build: no login menu item

func addManagedMenuItems(u *ui) {}

func getManagedClickCh() <-chan struct{} {
	// Return a channel that never receives (login not available in OSS)
	return make(chan struct{})
}

func handleManagedClick() {}
