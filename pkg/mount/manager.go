package mount

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/filesystem"
	"github.com/beam-cloud/airstore/pkg/filesystem/vnode"
	"github.com/beam-cloud/airstore/pkg/filesystem/vnode/embed"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Config holds the configuration for a MountManager.
type Config struct {
	MountPoint  string
	ConfigPath  string // Path to config file (sets CONFIG_PATH env)
	GatewayAddr string // Override gateway address (remote mode)
	Token       string
	Verbose     bool
}

// StateCallback is called whenever the mount state changes.
type StateCallback func(state State, err error)

// MountManager manages the full mount lifecycle: config loading, embedded
// gateway startup, filesystem creation, mounting, and clean shutdown.
type MountManager struct {
	cfg      Config
	callback StateCallback

	mu          sync.Mutex
	state       State
	lastErr     error
	gw          *gateway.Gateway
	fs          *filesystem.Filesystem
	sourcesConn *grpc.ClientConn
	mountErrCh  chan error
	stopOnce    sync.Once
}

// NewMountManager creates a new MountManager with the given config.
// The optional callback is invoked on every state transition.
func NewMountManager(cfg Config, cb StateCallback) *MountManager {
	return &MountManager{
		cfg:      cfg,
		callback: cb,
		state:    Idle,
	}
}

// State returns the current mount state.
func (m *MountManager) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// Err returns the last error, if any.
func (m *MountManager) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastErr
}

// GatewayAddr returns the effective gateway address once started.
func (m *MountManager) GatewayAddr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gw != nil {
		return m.gw.GRPCAddr()
	}
	return m.cfg.GatewayAddr
}

func (m *MountManager) setState(s State, err error) {
	m.mu.Lock()
	m.state = s
	m.lastErr = err
	cb := m.callback
	m.mu.Unlock()
	if cb != nil {
		cb(s, err)
	}
}

// Start begins the mount lifecycle asynchronously. It loads config, starts
// the embedded gateway (if local mode), creates the filesystem, registers
// vnodes, and mounts. The method returns once the mount is running or an
// error occurs during setup. The FUSE event loop runs in a background
// goroutine; call Stop() to unmount and shut down.
func (m *MountManager) Start() error {
	m.mu.Lock()
	if m.state == Mounting || m.state == Mounted {
		m.mu.Unlock()
		return fmt.Errorf("already %s", m.state)
	}
	m.stopOnce = sync.Once{}
	m.mu.Unlock()

	m.setState(Mounting, nil)

	if err := m.startInternal(); err != nil {
		m.setState(Error, err)
		return err
	}

	m.setState(Mounted, nil)
	return nil
}

func (m *MountManager) startInternal() error {
	// Set CONFIG_PATH if provided
	if m.cfg.ConfigPath != "" {
		os.Setenv("CONFIG_PATH", m.cfg.ConfigPath)
	}

	// Create mount point
	if err := os.MkdirAll(m.cfg.MountPoint, 0755); err != nil {
		return fmt.Errorf("failed to create mount point: %w", err)
	}

	// Determine mode
	effectiveAddr := m.cfg.GatewayAddr
	if effectiveAddr == "" {
		effectiveAddr = "localhost:1993"
	}

	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err == nil {
		config := configManager.GetConfig()
		if config.IsLocalMode() {
			if m.cfg.Verbose {
				log.Debug().Msg("local mode detected, starting embedded gateway")
			}

			gw, err := gateway.NewGateway()
			if err != nil {
				return fmt.Errorf("failed to create embedded gateway: %w", err)
			}

			if err := gw.StartAsync(); err != nil {
				return fmt.Errorf("failed to start embedded gateway: %w", err)
			}

			m.mu.Lock()
			m.gw = gw
			m.mu.Unlock()

			effectiveAddr = gw.GRPCAddr()
			time.Sleep(100 * time.Millisecond)

			if m.cfg.Verbose {
				log.Debug().Str("addr", effectiveAddr).Msg("embedded gateway started")
			}
		}
	}

	if m.cfg.Verbose {
		log.Debug().Str("gateway", effectiveAddr).Bool("auth", m.cfg.Token != "").Msg("connecting to gateway")
	}

	// Create filesystem
	fs, err := filesystem.NewFilesystem(filesystem.Config{
		MountPoint:  m.cfg.MountPoint,
		GatewayAddr: effectiveAddr,
		Token:       m.cfg.Token,
		Verbose:     m.cfg.Verbose,
	})
	if err != nil {
		m.shutdownGateway()
		errStr := err.Error()
		if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "Unavailable") {
			return fmt.Errorf("cannot connect to gateway at %s - is it running?", effectiveAddr)
		}
		return fmt.Errorf("failed to create filesystem: %w", err)
	}

	// Load shim
	shim, err := embed.GetShim()
	if err != nil {
		m.shutdownGateway()
		return fmt.Errorf("failed to load shim for %s: %w", embed.Current(), err)
	}

	// Register vnodes
	fs.RegisterVNode(vnode.NewConfigVNode(effectiveAddr, m.cfg.Token))
	fs.RegisterVNode(vnode.NewToolsVNode(effectiveAddr, m.cfg.Token, shim))

	sourcesConn, err := grpc.NewClient(
		effectiveAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		m.shutdownGateway()
		return fmt.Errorf("failed to create sources connection: %w", err)
	}

	fs.RegisterVNode(vnode.NewSourcesVNode(sourcesConn, m.cfg.Token))

	m.mu.Lock()
	m.fs = fs
	m.sourcesConn = sourcesConn
	m.mountErrCh = make(chan error, 1)
	m.mu.Unlock()

	if m.cfg.Verbose {
		log.Debug().Str("platform", embed.Current().String()).Int("shim_bytes", len(shim)).Msg("vnodes registered")
	}

	log.Info().Str("path", m.cfg.MountPoint).Str("gateway", effectiveAddr).Msg("filesystem mounted")

	// Run FUSE event loop in background
	go func() {
		m.mountErrCh <- fs.Mount()
	}()

	return nil
}

// Stop unmounts the filesystem and shuts down the gateway. It is safe to call
// multiple times. Stop blocks until cleanup is complete or a timeout elapses.
func (m *MountManager) Stop() {
	m.stopOnce.Do(func() {
		m.mu.Lock()
		if m.state != Mounted && m.state != Mounting && m.state != Error {
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()

		m.setState(Unmounting, nil)

		// Best-effort unmount
		m.mu.Lock()
		fs := m.fs
		mountPoint := m.cfg.MountPoint
		mountErrCh := m.mountErrCh
		m.mu.Unlock()

		if fs != nil {
			go bestEffortUnmount(mountPoint)
		}

		// Wait for mount goroutine to exit
		if mountErrCh != nil {
			select {
			case <-mountErrCh:
			case <-time.After(3 * time.Second):
			}
		}

		m.shutdownGateway()

		m.mu.Lock()
		if m.sourcesConn != nil {
			m.sourcesConn.Close()
			m.sourcesConn = nil
		}
		m.fs = nil
		m.mu.Unlock()

		m.setState(Idle, nil)
		log.Info().Msg("unmounted")
	})
}

// Reload stops the current mount, then starts again. Useful for picking up
// config changes.
func (m *MountManager) Reload() error {
	m.Stop()
	return m.Start()
}

// Wait blocks until the FUSE mount exits (e.g. external unmount or error).
// Returns the mount error, if any.
func (m *MountManager) Wait() error {
	m.mu.Lock()
	ch := m.mountErrCh
	m.mu.Unlock()

	if ch == nil {
		return nil
	}

	err := <-ch
	m.shutdownGateway()

	m.mu.Lock()
	if m.sourcesConn != nil {
		m.sourcesConn.Close()
		m.sourcesConn = nil
	}
	m.fs = nil
	m.mu.Unlock()

	if err != nil {
		m.setState(Error, err)
	} else {
		m.setState(Idle, nil)
	}
	return err
}

func (m *MountManager) shutdownGateway() {
	m.mu.Lock()
	gw := m.gw
	m.gw = nil
	m.mu.Unlock()

	if gw != nil {
		gw.Shutdown()
	}
}

func bestEffortUnmount(mountPoint string) {
	if runtime.GOOS != "darwin" {
		return
	}

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
	}
}
