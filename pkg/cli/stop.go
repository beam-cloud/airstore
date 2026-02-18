package cli

import (
	"os"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/desktop"
	"github.com/spf13/cobra"
)

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop Airstore",
	Long:  `Stop the running Airstore service.`,
	RunE:  runStop,
}

func init() {
	rootCmd.AddCommand(stopCmd)
}

func runStop(cmd *cobra.Command, args []string) error {
	pid := desktop.ReadPID()
	if pid == 0 || !processExists(pid) {
		PrintWarning("Not running")
		return nil
	}

	proc, _ := os.FindProcess(pid)

	PrintInfo("Stopping...")
	proc.Signal(syscall.SIGTERM)

	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		if !processExists(pid) {
			PrintSuccess("Stopped")
			return nil
		}
	}

	proc.Signal(syscall.SIGKILL)
	PrintSuccess("Stopped")
	return nil
}
