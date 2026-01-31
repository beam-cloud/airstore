package cli

import (
	"context"
	"fmt"
	"io"
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
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
)

var (
	mountVerbose bool
	configPath   string
)

var mountCmd = &cobra.Command{
	Use:   "mount <path>",
	Short: "Mount filesystem",
	Long: `Mount the Airstore virtual filesystem at the specified path.

Examples:
  airstore mount ~/airstore
  airstore mount /tmp/airstore --gateway localhost:1993
  airstore mount /tmp/airstore --config config.local.yaml`,
	Args: cobra.ExactArgs(1),
	RunE: runMount,
}

func init() {
	mountCmd.Flags().BoolVarP(&mountVerbose, "verbose", "v", false, "Verbose logging")
	mountCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config file (for local mode)")
	rootCmd.AddCommand(mountCmd)
}

func runMount(cmd *cobra.Command, args []string) error {
	mountPoint := args[0]

	// Suppress logs unless verbose
	if !mountVerbose {
		log.Logger = zerolog.New(io.Discard)
	}

	if configPath != "" {
		os.Setenv("CONFIG_PATH", configPath)
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		PrintFormattedError("Failed to create mount point", err)
		return nil
	}

	// Check for local mode
	var gw *gateway.Gateway
	effectiveGateway := gatewayAddr
	mode := "remote"

	if cm, err := common.NewConfigManager[types.AppConfig](); err == nil {
		if cm.GetConfig().Mode == types.ModeLocal {
			mode = "local"

			err := withSpinner("Starting gateway...", func() error {
				var err error
				gw, err = gateway.NewGateway()
				if err != nil {
					return err
				}
				if err = gw.StartAsync(); err != nil {
					return err
				}
				effectiveGateway = gw.GRPCAddr()
				time.Sleep(100 * time.Millisecond)
				return nil
			})

			if err != nil {
				PrintFormattedError("Failed to start gateway", err)
				return nil
			}
			PrintSuccessWithValue("Gateway ready", effectiveGateway)
		}
	}

	// Connect and create filesystem
	var fs *filesystem.Filesystem
	var conn *grpc.ClientConn

	err := withSpinner("Connecting...", func() error {
		var err error
		fs, err = filesystem.NewFilesystem(filesystem.Config{
			MountPoint:  mountPoint,
			GatewayAddr: effectiveGateway,
			Token:       authToken,
			Verbose:     mountVerbose,
		})
		if err != nil {
			return err
		}

		// Create gRPC connection for vnodes
		conn, err = grpc.NewClient(effectiveGateway, grpc.WithTransportCredentials(TransportCredentials(effectiveGateway)))
		if err != nil {
			return err
		}

		// Load shim binary for tools
		shim, err := embed.GetShim()
		if err != nil {
			return err
		}

		// Register all vnodes
		fs.RegisterVNode(vnode.NewConfigVNode(effectiveGateway, authToken))
		fs.RegisterVNode(vnode.NewToolsVNode(effectiveGateway, authToken, shim))
		fs.RegisterVNode(vnode.NewSourcesVNode(conn, authToken))
		fs.RegisterVNode(vnode.NewContextVNodeGRPC(conn, authToken))  // /skills
		fs.RegisterVNode(vnode.NewTasksVNodeGRPC(conn, authToken))    // /tasks
		fs.SetStorageFallback(vnode.NewStorageVNode(conn, authToken)) // user folders

		return nil
	})

	if err != nil {
		if gw != nil {
			gw.Shutdown()
		}
		if conn != nil {
			conn.Close()
		}
		if strings.Contains(err.Error(), "connection refused") {
			PrintConnectionError(effectiveGateway, err)
		} else {
			PrintFormattedError("Failed to connect", err)
		}
		return nil
	}

	PrintSuccessWithValue("Mounted", mountPoint)
	printMountStatus(mountPoint, effectiveGateway, mode)

	// Run mount in background
	mountErr := make(chan error, 1)
	go func() { mountErr <- fs.Mount() }()

	// Wait for signal
	sig := make(chan os.Signal, 2)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sig)

	select {
	case err = <-mountErr:
		// Mount exited
	case <-sig:
		fmt.Println()
		withSpinner("Unmounting...", func() error {
			if gw != nil {
				go gw.Shutdown()
			}
			unmount(mountPoint)
			time.Sleep(200 * time.Millisecond)
			return nil
		})
		PrintSuccess("Unmounted")

		select {
		case <-mountErr:
		case <-sig:
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
	}

	return nil
}

func withSpinner(title string, fn func() error) error {
	var err error
	spinner.New().
		Title("  " + title).
		Action(func() { err = fn() }).
		Run()
	return err
}

func printMountStatus(mount, gateway, mode string) {
	fmt.Println()
	fmt.Printf("  %s\n\n", BrandStyle.Render("airstore mounted"))

	PrintKeyValue("Mount", mount)
	PrintKeyValue("Gateway", gateway)
	PrintKeyValue("Mode", mode)

	fmt.Println()
	fmt.Printf("  %s\n", DimStyle.Render("Available paths:"))
	paths := []struct{ path, desc string }{
		{"/tools/*", "Tool binaries"},
		{"/sources/*", "Integration data"},
		{"/skills/*", "Skills and context"},
		{"/tasks/*", "Active tasks"},
	}
	for _, p := range paths {
		fmt.Printf("    %s  %s\n", CodeStyle.Render(fmt.Sprintf("%-14s", p.path)), DimStyle.Render(p.desc))
	}

	fmt.Println()
	fmt.Printf("  %s\n\n", DimStyle.Render("Press Ctrl+C to unmount"))
}

func unmount(path string) {
	if runtime.GOOS != "darwin" {
		return
	}

	cmds := [][]string{
		{"diskutil", "unmount", "force", path},
		{"diskutil", "unmount", path},
		{"umount", path},
		{"umount", "-f", path},
	}

	for _, args := range cmds {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		if exec.CommandContext(ctx, args[0], args[1:]...).Run() == nil {
			cancel()
			return
		}
		cancel()
	}
}
