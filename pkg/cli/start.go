package cli

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/desktop"
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
	if pid := desktop.ReadPID(); pid != 0 && processExists(pid) {
		PrintWarning("Already running")
		PrintHint(fmt.Sprintf("PID %d. Use 'airstore stop' to stop.", pid))
		return nil
	}

	// Resolve token: prefer --token / AIRSTORE_TOKEN, then credentials file
	effectiveToken := authToken
	if effectiveToken == "" {
		effectiveToken = desktop.LoadToken()
	}

	if effectiveToken == "" {
		PrintErrorMsg("Not logged in")
		PrintHint("Run 'airstore login' first, or pass --token / AIRSTORE_TOKEN")
		return nil
	}

	authToken = effectiveToken

	// Internal flag: run desktop directly (called by spawned process).
	if os.Getenv("AIRSTORE_DESKTOP") == "1" {
		return runDesktop()
	}

	// Spawn background process
	return spawn()
}

func runDesktop() error {
	runtime.LockOSThread()
	cfg := desktop.LoadConfig()
	cfg.GatewayAddr = gatewayAddr
	cfg.GatewayHTTPAddr = gatewayHTTPAddr
	cfg.Token = authToken
	return desktop.Run(cfg)
}

func spawn() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	exe, _ = filepath.EvalSymlinks(exe)

	args := []string{"start"}
	if gatewayAddr != defaultGRPCAddr() {
		args = append(args, "--gateway", gatewayAddr)
	}
	if gatewayHTTPAddr != defaultHTTPAddr() {
		args = append(args, "--gateway-http", gatewayHTTPAddr)
	}

	proc := exec.Command(exe, args...)
	proc.Env = append(os.Environ(), "AIRSTORE_DESKTOP=1", "AIRSTORE_TOKEN="+authToken)
	proc.Dir = "/"
	proc.Stdin = nil
	proc.Stdout = nil
	proc.Stderr = nil
	proc.SysProcAttr = detachedSysProcAttr()

	if err := proc.Start(); err != nil {
		PrintFormattedError("Failed to start", err)
		return nil
	}

	// Poll for up to 5 seconds for process to initialize and write PID
	var pid int
	for i := 0; i < 25; i++ {
		time.Sleep(200 * time.Millisecond)
		pid = desktop.ReadPID()
		if pid != 0 && processExists(pid) {
			break
		}
	}

	if pid != 0 && processExists(pid) {
		cfg := desktop.LoadConfig()
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
