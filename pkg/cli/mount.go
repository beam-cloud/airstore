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
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/charmbracelet/huh/spinner"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	grpcKeepaliveTime    = 60 * time.Second
	grpcKeepaliveTimeout = 10 * time.Second
)

var (
	mountVerbose     bool
	mountDaemon      bool
	configPath       string
	mountUID         uint32
	mountGID         uint32
	mountBackend     string
	mountCompression string
	mountSession     string
	mountAccessLog   bool
)

var mountCommandLong = fmt.Sprintf(`Mount the Airstore virtual filesystem at the specified path.

Examples:
  airstore mount ~/airstore
  airstore mount /tmp/airstore --gateway %s
  airstore mount /tmp/airstore --uid 1000 --gid 1000`, types.DefaultGatewayGRPCAddr())

var mountCmd = &cobra.Command{
	Use:   "mount <path>",
	Short: "Mount filesystem",
	Long:  mountCommandLong,
	Args:  cobra.ExactArgs(1),
	RunE:  runMount,
}

func init() {
	mountCmd.Flags().BoolVarP(&mountVerbose, "verbose", "v", false, "Verbose logging")
	mountCmd.Flags().BoolVarP(&mountDaemon, "daemon", "d", false, "Daemon mode: suppress interactive UI, suitable for headless/systemd use")
	mountCmd.Flags().StringVarP(&configPath, "config", "c", "", "Config file (for local mode)")
	mountCmd.Flags().Uint32Var(&mountUID, "uid", 0, "File owner UID (default: current user)")
	mountCmd.Flags().Uint32Var(&mountGID, "gid", 0, "File owner GID (default: current user)")
	mountCmd.Flags().StringVar(&mountBackend, "backend", "", "Mount backend: fuse, nfs, or auto-detect (default)")
	mountCmd.Flags().StringVar(&mountCompression, "compression", "", "Compression strategy: strip (omit to disable)")
	mountCmd.Flags().StringVar(&mountSession, "session", "", "Custom access session ID for telemetry (default: workspace ID)")
	mountCmd.Flags().BoolVar(&mountAccessLog, "access-log", true, "Enable access logging (default: true)")
	rootCmd.AddCommand(mountCmd)
}

func runMount(cmd *cobra.Command, args []string) error {
	mountPoint := args[0]

	// Suppress logs unless verbose or FUSE trace is enabled
	if !mountVerbose && os.Getenv("AIRSTORE_FUSE_TRACE") == "" {
		// In non-verbose mode, suppress info/debug logs but keep warn/error on stderr.
		log.Logger = zerolog.New(os.Stderr).Level(zerolog.WarnLevel).With().Timestamp().Logger()
	}

	if configPath != "" {
		os.Setenv("CONFIG_PATH", configPath)
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		PrintFormattedError("Failed to create mount point", err)
		return nil
	}

	// Validate compression strategy
	if mountCompression != "" {
		switch mountCompression {
		case "strip":
			// valid
		default:
			PrintFormattedError("Invalid compression strategy", fmt.Errorf("%q is not supported (valid: strip)", mountCompression))
			return nil
		}
	}

	// Check for local mode
	var gw *gateway.Gateway
	effectiveGateway := gatewayAddr
	mode := "remote"

	if cm, err := common.NewConfigManager[types.AppConfig](); err == nil {
		if cm.GetConfig().Mode == types.ModeLocal {
			mode = "local"

			err := withMaybeSpinner("Starting gateway...", func() error {
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
			if !mountDaemon {
				PrintSuccessWithValue("Gateway ready", effectiveGateway)
			}
		}
	}

	// Connect and create filesystem
	var fs *filesystem.Filesystem
	var conn *grpc.ClientConn

	// Convert UID/GID: nil means use current user, pointer means explicit value
	var uidPtr, gidPtr *uint32
	if cmd.Flags().Changed("uid") {
		uidPtr = &mountUID
	}
	if cmd.Flags().Changed("gid") {
		gidPtr = &mountGID
	}

	err := withMaybeSpinner("Connecting...", func() error {
		var err error
		fs, err = filesystem.NewFilesystem(filesystem.Config{
			MountPoint:  mountPoint,
			GatewayAddr: effectiveGateway,
			Token:       authToken,
			Verbose:     mountVerbose,
			Uid:         uidPtr,
			Gid:         gidPtr,
			Backend:     mountBackend,
			Compression: mountCompression,
			Session:     mountSession,
			AccessLog:   mountAccessLog,
		})
		if err != nil {
			return err
		}

		// Create gRPC connection for vnodes.
		// When access logging is on, a client-side interceptor injects the
		// session header on every outgoing RPC so the gateway can record events.
		dialOpts := []grpc.DialOption{
			grpc.WithTransportCredentials(TransportCredentials(effectiveGateway)),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                grpcKeepaliveTime,
				Timeout:             grpcKeepaliveTimeout,
				PermitWithoutStream: true,
			}),
		}
		if mountAccessLog {
			session := mountSession // may be empty — gateway defaults to workspace ID
			dialOpts = append(dialOpts, grpc.WithUnaryInterceptor(filesystem.MountAccessUnaryInterceptor(session)))
		}
		conn, err = grpc.NewClient(effectiveGateway, dialOpts...)
		if err != nil {
			return err
		}
		if mountAccessLog {
			fs.SetAccessCollector(filesystem.NewAccessCollectorWithToken(
				pb.NewAccessLogServiceClient(conn),
				authToken,
			))
		}

		// Load shim binary for tools
		shim, err := embed.GetShim()
		if err != nil {
			return err
		}

		// Register all vnodes
		fs.RegisterVNode(vnode.NewConfigVNode(effectiveGateway, authToken, shim))
		fs.RegisterVNode(vnode.NewToolsVNode(effectiveGateway, authToken))

		var sourcesOpts []vnode.SourcesVNodeOption
		if mountCompression != "" {
			sourcesOpts = append(sourcesOpts, vnode.WithCompression(mountCompression))
		}
		fs.RegisterVNode(vnode.NewSourcesVNode(conn, authToken, sourcesOpts...))
		fs.RegisterVNode(vnode.NewContextVNodeGRPC(conn, authToken, types.PathSkills)) // /Skills
		fs.RegisterVNode(vnode.NewTasksVNodeGRPC(conn, authToken))                     // /Tasks
		fs.SetStorageFallback(vnode.NewStorageVNode(conn, authToken))                  // user folders

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

	if mountDaemon {
		// Daemon mode: minimal output suitable for scripts/systemd
		fmt.Printf("airstore: mounted at %s (gateway=%s, mode=%s)\n", mountPoint, effectiveGateway, mode)
	} else {
		PrintSuccessWithValue("Mounted", mountPoint)
		printMountStatus(mountPoint, effectiveGateway, mode, mountCompression)
	}

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
		withMaybeSpinner("Unmounting...", func() error {
			if gw != nil {
				go gw.Shutdown()
			}
			unmount(mountPoint)
			time.Sleep(200 * time.Millisecond)
			return nil
		})
		if mountDaemon {
			fmt.Println("airstore: unmounted")
		} else {
			PrintSuccess("Unmounted")
		}

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

// withMaybeSpinner runs fn with a spinner in interactive mode, or directly in daemon mode.
func withMaybeSpinner(title string, fn func() error) error {
	if mountDaemon {
		return fn()
	}
	return withSpinner(title, fn)
}

func printMountStatus(mount, gateway, mode, compression string) {
	fmt.Println()
	fmt.Printf("  %s\n\n", BrandStyle.Render("airstore mounted"))

	PrintKeyValue("Mount", mount)
	PrintKeyValue("Gateway", gateway)
	PrintKeyValue("Mode", mode)
	if compression != "" {
		PrintKeyValue("Compression", compression)
	} else {
		PrintKeyValue("Compression", "off")
	}

	fmt.Println()
	fmt.Printf("  %s\n", DimStyle.Render("Available paths:"))
	paths := []struct{ path, desc string }{
		{types.PathTools + "/*", "Tool binaries"},
		{types.PathSources + "/*", "Integration data"},
		{types.PathSkills + "/*", "Skills and context"},
		{types.PathTasks + "/*", "Active tasks"},
	}
	for _, p := range paths {
		fmt.Printf("    %s  %s\n", CodeStyle.Render(fmt.Sprintf("%-14s", p.path)), DimStyle.Render(p.desc))
	}

	fmt.Println()
	fmt.Printf("  %s\n\n", DimStyle.Render("Press Ctrl+C to unmount"))
}

func unmount(path string) {
	var cmds [][]string
	if runtime.GOOS == "darwin" {
		cmds = [][]string{
			{"diskutil", "unmount", "force", path},
			{"diskutil", "unmount", path},
			{"umount", path},
			{"umount", "-f", path},
		}
	} else {
		cmds = [][]string{
			{"umount", path},
			{"umount", "-f", path},
		}
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
