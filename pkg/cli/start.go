package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/tray"
	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start Airstore in background",
	Long:  `Start the Airstore service with system tray icon.`,
	RunE:  runStart,
}

func init() {
	rootCmd.AddCommand(startCmd)
}

func runStart(cmd *cobra.Command, args []string) error {
	// Already running?
	if pid := tray.ReadPID(); pid != 0 && processExists(pid) {
		PrintWarning("Already running")
		PrintHint(fmt.Sprintf("PID %d. Use 'airstore stop' to stop.", pid))
		return nil
	}

	// Logged in?
	if tray.LoadToken() == "" {
		PrintErrorMsg("Not logged in")
		PrintHint("Run 'airstore login' first")
		return nil
	}

	// Internal flag: run tray directly (called by spawned process)
	if os.Getenv("AIRSTORE_TRAY") == "1" {
		return runTray()
	}

	// Spawn background process
	return spawn()
}

func runTray() error {
	runtime.LockOSThread()
	cfg := tray.LoadConfig()
	cfg.GatewayAddr = gatewayAddr
	return tray.Run(cfg)
}

func spawn() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	exe, _ = filepath.EvalSymlinks(exe)

	// Build args, preserving gateway flag
	args := []string{"start"}
	if gatewayAddr != defaultGRPCAddr() {
		args = append(args, "--gateway", gatewayAddr)
	}

	proc := exec.Command(exe, args...)
	proc.Env = append(os.Environ(), "AIRSTORE_TRAY=1")
	proc.Dir = "/"
	proc.Stdin = nil
	proc.Stdout = nil
	proc.Stderr = nil
	proc.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := proc.Start(); err != nil {
		PrintFormattedError("Failed to start", err)
		return nil
	}

	// Wait briefly for process to initialize and write PID
	time.Sleep(200 * time.Millisecond)

	// Verify it started
	if pid := tray.ReadPID(); pid != 0 && processExists(pid) {
		cfg := tray.LoadConfig()
		PrintSuccess("Started")
		PrintKeyValue("PID", fmt.Sprintf("%d", pid))
		PrintKeyValue("Mount", cfg.MountPoint)
		PrintKeyValue("Gateway", gatewayAddr)
	} else {
		PrintErrorMsg("Failed to start")
		PrintHint("Try 'airstore start' again or check logs")
	}

	return nil
}

func processExists(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	return proc.Signal(syscall.Signal(0)) == nil
}
