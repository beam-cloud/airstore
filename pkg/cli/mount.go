package cli

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/filesystem"
	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/beam-cloud/airstore/pkg/filesystem/vnode/embed"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/charmbracelet/huh/spinner"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
  /sources/*         - Integration data (github, gmail, etc.)
  /skills/*          - Agent Skills
  /.airstore/config  - Configuration for tools

This command blocks until the filesystem is unmounted (Ctrl+C).

In LOCAL MODE (mode: local in config):
  The gateway runs embedded within this CLI process.
  Use --config to specify a config file.
  
In REMOTE MODE (mode: remote or default):
  Connects to a remote gateway via --gateway flag.

Examples:
  # Local mode - gateway embedded in CLI
  airstore mount /tmp/airstore --config config.local.yaml

  # Remote mode - connect to existing gateway  
  airstore mount /tmp/airstore --gateway localhost:1993 --token stk_xxx
  airstore mount /tmp/airstore --verbose`,
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
			PrintFormattedError("Failed to create mount point", err)
			return nil
		}

		// Determine if we should run in local mode by loading config
		var gw *gateway.Gateway
		var config types.AppConfig
		effectiveGatewayAddr := gatewayAddr
		mode := "remote"

		// Try to load config to check for local mode
		configManager, err := common.NewConfigManager[types.AppConfig]()
		if err == nil {
			config = configManager.GetConfig()
			if config.IsLocalMode() {
				mode = "local"
				// Local mode: start embedded gateway
				if mountVerbose {
					log.Debug().Msg("local mode detected, starting embedded gateway")
				}

				err = runMountSpinner("Starting embedded gateway...", func() error {
					var gwErr error
					gw, gwErr = gateway.NewGateway()
					if gwErr != nil {
						return fmt.Errorf("failed to create embedded gateway: %w", gwErr)
					}

					if gwErr = gw.StartAsync(); gwErr != nil {
						return fmt.Errorf("failed to start embedded gateway: %w", gwErr)
					}

					// Use the embedded gateway's address
					effectiveGatewayAddr = gw.GRPCAddr()

					// Give the gateway a moment to be ready
					time.Sleep(100 * time.Millisecond)
					return nil
				})

				if err != nil {
					PrintFormattedError("Failed to start gateway", err)
					return nil
				}

				PrintSuccessWithValue("Gateway ready", effectiveGatewayAddr)

				if mountVerbose {
					log.Debug().Str("addr", effectiveGatewayAddr).Msg("embedded gateway started")
				}
			}
		}

		if mountVerbose {
			log.Debug().Str("gateway", effectiveGatewayAddr).Bool("auth", authToken != "").Msg("connecting to gateway")
		}

		// Connect to gateway and create filesystem
		var fs *filesystem.Filesystem

		err = runMountSpinner("Connecting to gateway...", func() error {
			var fsErr error
			fs, fsErr = filesystem.NewFilesystem(filesystem.Config{
				MountPoint:  mountPoint,
				GatewayAddr: effectiveGatewayAddr,
				Token:       authToken,
				Verbose:     mountVerbose,
			})
			return fsErr
		})

		if err != nil {
			if gw != nil {
				gw.Shutdown()
			}
			// Provide cleaner error for connection failures
			errStr := err.Error()
			if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "Unavailable") {
				PrintConnectionError(effectiveGatewayAddr, err)
				return nil
			}
			PrintFormattedError("Failed to mount filesystem", err)
			return nil
		}

		PrintSuccess("Connected")

		// Get the shim binary for the current platform
		var shim []byte
		err = runMountSpinner("Loading platform shim...", func() error {
			var shimErr error
			shim, shimErr = embed.GetShim()
			return shimErr
		})

		if err != nil {
			if gw != nil {
				gw.Shutdown()
			}
			PrintFormattedError(fmt.Sprintf("Failed to load shim for %s", embed.Current()), err)
			return nil
		}

		PrintSuccessWithValue("Shim loaded", embed.Current().String())

		// Register virtual nodes
		configNode := vnode.NewConfigVNode(effectiveGatewayAddr, authToken)
		fs.RegisterVNode(configNode)

		toolsNode := vnode.NewToolsVNode(effectiveGatewayAddr, authToken, shim)
		fs.RegisterVNode(toolsNode)

		// Create gRPC connection for sources vnode
		sourcesConn, err := grpc.NewClient(
			effectiveGatewayAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			if gw != nil {
				gw.Shutdown()
			}
			PrintFormattedError("Failed to create sources connection", err)
			return nil
		}

		// Register sources VNode - handles /sources/ with smart queries via gRPC
		fs.RegisterVNode(vnode.NewSourcesVNode(sourcesConn, authToken))

		// Register skills VNode - handles /skills/ with S3-backed storage via gRPC
		fs.RegisterVNode(vnode.NewContextVNodeGRPC(sourcesConn, authToken))

		// Register tasks VNode - fetches tasks from gateway via gRPC
		fs.RegisterVNode(vnode.NewTasksVNodeGRPC(sourcesConn, authToken))

		// Register storage fallback - handles user-created folders and any unmatched S3 paths
		fs.SetStorageFallback(vnode.NewStorageVNode(sourcesConn, authToken))

		if mountVerbose {
			log.Debug().Str("platform", embed.Current().String()).Int("shim_bytes", len(shim)).Msg("vnodes registered")
		}

		// Mount filesystem
		err = runMountSpinner("Mounting filesystem...", func() error {
			// The actual mount happens in the background, but we want to show progress
			// The Mount() call blocks, so we'll start it in a goroutine later
			return nil
		})

		PrintSuccessWithValue("Mounted", mountPoint)

		// Show the status display
		printMountStatus(mountPoint, effectiveGatewayAddr, mode)

		// Run the mount loop in the background so we can coordinate shutdown.
		mountErrCh := make(chan error, 1)
		go func() { mountErrCh <- fs.Mount() }()

		// Handle signals for unmount (Ctrl+C).
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigChan)

		select {
		case err = <-mountErrCh:
			// Mount returned without an external signal.
		case <-sigChan:
			// Show unmounting message
			fmt.Println()

			// Best-effort unmount with spinner
			var unmountErr error
			spinner.New().
				Title("  Unmounting...").
				Action(func() {
					if gw != nil {
						// Shutdown in background; don't block Ctrl+C on gateway shutdown.
						go gw.Shutdown()
					}
					// Run unmount asynchronously; some system tools can block.
					bestEffortUnmountMountPoint(mountPoint)
					time.Sleep(200 * time.Millisecond) // Brief delay for clean unmount
				}).
				Run()

			if unmountErr == nil {
				PrintSuccess("Unmounted")
			}

			// Wait briefly for Mount() to return; if it doesn't, force exit.
			select {
			case err = <-mountErrCh:
				// Mount returned after unmount attempt.
			case <-sigChan:
				// Second Ctrl+C: hard exit.
				fmt.Println()
				PrintWarning("Force exit")
				os.Exit(1)
			case <-time.After(3 * time.Second):
				os.Exit(0)
			}
		}

		if gw != nil {
			gw.Shutdown()
		}

		if err != nil {
			PrintFormattedError("Mount failed", err)
			return nil
		}

		return nil
	},
}

// runMountSpinner runs an action with a spinner, doesn't print success (caller handles that)
func runMountSpinner(title string, fn func() error) error {
	var actionErr error

	spinner.New().
		Title("  " + title).
		Action(func() {
			actionErr = fn()
		}).
		Run()

	return actionErr
}

// printMountStatus shows the active mount status
func printMountStatus(mountPoint, gateway, mode string) {
	fmt.Println()
	fmt.Printf("  %s\n", BrandStyle.Render("airstore mounted"))
	fmt.Println()

	PrintKeyValue("Mount", mountPoint)
	PrintKeyValue("Gateway", gateway)
	PrintKeyValue("Mode", mode)
	fmt.Println()

	fmt.Printf("  %s\n", DimStyle.Render("Available paths:"))
	paths := []struct {
		path string
		desc string
	}{
		{"/tools/*", "Tool binaries"},
		{"/sources/*", "Integration data"},
		{"/skills/*", "Skills and context"},
		{"/tasks/*", "Active tasks"},
	}
	for _, p := range paths {
		fmt.Printf("    %s  %s\n", CodeStyle.Render(fmt.Sprintf("%-14s", p.path)), DimStyle.Render(p.desc))
	}
	fmt.Println()
	fmt.Printf("  %s\n", DimStyle.Render("Press Ctrl+C to unmount"))
	fmt.Println()
}
func bestEffortUnmountMountPoint(mountPoint string) {
	// On macOS with FUSE-T (especially SMB backend), cgofuse's internal signal handler
	// can hang inside host.Unmount(). Use the OS unmount tools instead.
	if runtime.GOOS != "darwin" {
		return
	}

	// Try a small sequence of common unmount commands. Accept the first success.
	cmds := [][]string{
		{"diskutil", "unmount", "force", mountPoint},
		{"diskutil", "unmount", mountPoint},
		{"umount", mountPoint},
		{"umount", "-f", mountPoint},
	}

	for _, args := range cmds {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := exec.CommandContext(ctx, args[0], args[1:]...).Run()
		cancel()
		if err == nil {
			return
		}
		if mountVerbose {
			log.Debug().Strs("cmd", args).Err(err).Msg("unmount attempt failed")
		}
	}
}

func init() {
	mountCmd.Flags().BoolVarP(&mountVerbose, "verbose", "v", false, "Verbose logging")
	mountCmd.Flags().StringVarP(&configPath, "config", "c", "", "Path to config file (enables local mode if config has mode: local)")
}
