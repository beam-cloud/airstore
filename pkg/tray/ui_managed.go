//go:build managed

package tray

// Managed build - no additional menu items needed.
// Users should run `airstore login` from terminal to authenticate.

func addManagedMenuItems(u *ui) {}

func getManagedClickCh() <-chan struct{} {
	return make(chan struct{})
}

func handleManagedClick() {}
