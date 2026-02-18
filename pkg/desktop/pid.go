package desktop

import (
	"fmt"
	"os"
	"path/filepath"
)

var pidPath = func() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".airstore", "airstore.pid")
}()

// WritePID stores the current process PID in ~/.airstore/airstore.pid.
func WritePID() {
	os.MkdirAll(filepath.Dir(pidPath), 0755)
	os.WriteFile(pidPath, []byte(fmt.Sprintf("%d", os.Getpid())), 0644)
}

// RemovePID removes ~/.airstore/airstore.pid.
func RemovePID() { os.Remove(pidPath) }

// ReadPID reads the PID from ~/.airstore/airstore.pid.
func ReadPID() int {
	data, _ := os.ReadFile(pidPath)
	var pid int
	fmt.Sscanf(string(data), "%d", &pid)
	return pid
}
