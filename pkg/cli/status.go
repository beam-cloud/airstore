package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/beam-cloud/airstore/pkg/tray"
	"github.com/spf13/cobra"
)

type StatusInfo struct {
	Running bool   `json:"running"`
	PID     int    `json:"pid,omitempty"`
	Mount   string `json:"mount,omitempty"`
	Mounted bool   `json:"mounted"`
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show status",
	Long:  `Show the current status of Airstore.`,
	RunE:  runStatus,
}

func init() {
	rootCmd.AddCommand(statusCmd)
}

func runStatus(cmd *cobra.Command, args []string) error {
	cfg := tray.LoadConfig()
	pid := tray.ReadPID()

	status := StatusInfo{
		Running: pid != 0 && processExists(pid),
		PID:     pid,
		Mount:   cfg.MountPoint,
		Mounted: isMounted(cfg.MountPoint),
	}

	if jsonOutput {
		data, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(data))
		return nil
	}

	fmt.Println()
	if status.Running {
		PrintKeyValue("Status", SuccessStyle.Render("running"))
		PrintKeyValue("PID", fmt.Sprintf("%d", status.PID))
	} else {
		PrintKeyValue("Status", DimStyle.Render("stopped"))
	}

	PrintKeyValue("Mount", status.Mount)
	if status.Mounted {
		PrintKeyValue("Mounted", SuccessStyle.Render("yes"))
	} else {
		PrintKeyValue("Mounted", DimStyle.Render("no"))
	}
	fmt.Println()

	if !status.Running {
		PrintHint("Run 'airstore start' to start")
	}

	return nil
}

func isMounted(path string) bool {
	marker := filepath.Join(path, ".airstore")
	_, err := os.Stat(marker)
	return err == nil
}
