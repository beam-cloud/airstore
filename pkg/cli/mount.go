package cli

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/mount"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var (
	mountVerbose bool
	configPath   string
)

var mountCmd = &cobra.Command{
	Use:   "mount <path>",
	Short: "Mount the Airstore filesystem",
	Long: `Mount the Airstore virtual filesystem at the specified path.

The filesystem provides:
  /tools/*           - Virtual tool binaries (github, weather, exa, etc.)
  /sources/*         - Read-only integration sources (github, etc.)
  /.airstore/config  - Configuration for tools

This command blocks until the filesystem is unmounted (Ctrl+C).

In LOCAL MODE (mode: local in config):
  The gateway runs embedded within this CLI process.
  Use --config to specify a config file.

In REMOTE MODE (mode: remote or default):
  Connects to a remote gateway via --gateway flag.

Examples:
  # Local mode - gateway embedded in CLI
  cli mount /tmp/airstore --config config.local.yaml

  # Remote mode - connect to existing gateway
  cli mount /tmp/airstore --gateway localhost:1993 --token stk_xxx
  cli mount /tmp/airstore --verbose`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		mgr := mount.NewMountManager(mount.Config{
			MountPoint:  args[0],
			ConfigPath:  configPath,
			GatewayAddr: gatewayAddr,
			Token:       authToken,
			Verbose:     mountVerbose,
		}, nil)

		if err := mgr.Start(); err != nil {
			return err
		}

		log.Info().Msg("press ctrl+c to unmount")

		// Handle signals for unmount (Ctrl+C).
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigChan)

		// Wait for either the mount to exit on its own or a signal.
		mountDone := make(chan error, 1)
		go func() { mountDone <- mgr.Wait() }()

		select {
		case err := <-mountDone:
			return err
		case <-sigChan:
			go mgr.Stop()

			// Wait briefly; second Ctrl+C force-exits.
			select {
			case <-mountDone:
			case <-sigChan:
				os.Exit(1)
			case <-time.After(3 * time.Second):
				os.Exit(0)
			}
		}

		return nil
	},
}

func init() {
	mountCmd.Flags().BoolVarP(&mountVerbose, "verbose", "v", false, "Verbose logging")
	mountCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file (enables local mode if config has mode: local)")
}
