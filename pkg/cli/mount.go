package cli

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/filesystem"
	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/beam-cloud/airstore/pkg/filesystem/vnode/embed"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/types"
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
  /.airstore/config - Configuration for tools

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
		mountPoint := args[0]

		// Set CONFIG_PATH if --config flag is provided
		if configPath != "" {
			os.Setenv("CONFIG_PATH", configPath)
			if mountVerbose {
				log.Debug().Str("config", configPath).Msg("using config")
			}
		}

		// Create mount point if it doesn't exist
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			return fmt.Errorf("failed to create mount point: %w", err)
		}

		// Determine if we should run in local mode by loading config
		var gw *gateway.Gateway
		effectiveGatewayAddr := gatewayAddr

		// Try to load config to check for local mode
		configManager, err := common.NewConfigManager[types.AppConfig]()
		if err == nil {
			config := configManager.GetConfig()
			if config.IsLocalMode() {
				// Local mode: start embedded gateway
				if mountVerbose {
					log.Debug().Msg("local mode detected, starting embedded gateway")
				}

				gw, err = gateway.NewGateway()
				if err != nil {
					return fmt.Errorf("failed to create embedded gateway: %w", err)
				}

				if err := gw.StartAsync(); err != nil {
					return fmt.Errorf("failed to start embedded gateway: %w", err)
				}

				// Use the embedded gateway's address
				effectiveGatewayAddr = gw.GRPCAddr()

				// Give the gateway a moment to be ready
				time.Sleep(100 * time.Millisecond)

				if mountVerbose {
					log.Debug().Str("addr", effectiveGatewayAddr).Msg("embedded gateway started")
				}
			}
		}

		if mountVerbose {
			log.Debug().Str("gateway", effectiveGatewayAddr).Bool("auth", authToken != "").Msg("connecting to gateway")
		}

		fs, err := filesystem.NewFilesystem(filesystem.Config{
			MountPoint:  mountPoint,
			GatewayAddr: effectiveGatewayAddr,
			Token:       authToken,
			Verbose:     mountVerbose,
		})
		if err != nil {
			if gw != nil {
				gw.Shutdown()
			}
			// Provide cleaner error for connection failures
			errStr := err.Error()
			if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "Unavailable") {
				return fmt.Errorf("cannot connect to gateway at %s - is it running?", effectiveGatewayAddr)
			}
			return fmt.Errorf("failed to create filesystem: %w", err)
		}

		// Get the shim binary for the current platform
		shim, err := embed.GetShim()
		if err != nil {
			if gw != nil {
				gw.Shutdown()
			}
			return fmt.Errorf("failed to load shim for %s: %w", embed.Current(), err)
		}

		// Register virtual nodes
		configNode := vnode.NewConfigVNode(effectiveGatewayAddr, authToken)
		fs.RegisterVNode(configNode)

		toolsNode := vnode.NewToolsVNode(effectiveGatewayAddr, shim)
		fs.RegisterVNode(toolsNode)

		if mountVerbose {
			log.Debug().Str("platform", embed.Current().String()).Int("shim_bytes", len(shim)).Msg("vnodes registered")
		}

		// Track intentional shutdown via signal
		shuttingDown := false

		// Handle signals for clean unmount
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			<-sigChan
			shuttingDown = true
			fs.Unmount()
			if gw != nil {
				gw.Shutdown()
			}
		}()

		log.Info().Str("path", mountPoint).Str("gateway", effectiveGatewayAddr).Msg("filesystem mounted")
		log.Info().Msg("press ctrl+c to unmount")

		err = fs.Mount()

		// Clean shutdown via signal - not an error
		if shuttingDown {
			log.Info().Msg("unmounted")
			return nil
		}

		// Actual mount failure
		if err != nil {
			if gw != nil {
				gw.Shutdown()
			}
			return fmt.Errorf("mount failed: %w", err)
		}

		return nil
	},
}

func init() {
	mountCmd.Flags().BoolVarP(&mountVerbose, "verbose", "v", false, "Verbose logging")
	mountCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file (enables local mode if config has mode: local)")
}
