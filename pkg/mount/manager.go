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
	"google.golang.org/grpc/keepalive"
)

type Config struct {
	MountPoint  string
	ConfigPath  string
	GatewayAddr string
	Token       string
	Verbose     bool
}

type StateCallback func(State, error)

type MountManager struct {
	cfg      Config
	callback StateCallback

	mu      sync.Mutex
	state   State
	lastErr error

	gw      *gateway.Gateway
	fs      *filesystem.Filesystem
	conn    *grpc.ClientConn
	done    chan error
	stopped bool
}

func NewMountManager(cfg Config, cb StateCallback) *MountManager {
	return &MountManager{cfg: cfg, callback: cb, state: Idle}
}

func (m *MountManager) State() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

func (m *MountManager) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastErr
}

func (m *MountManager) GatewayAddr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.gw != nil {
		return m.gw.GRPCAddr()
	}
	return m.cfg.GatewayAddr
}

func (m *MountManager) transition(s State, err error) {
	m.mu.Lock()
	m.state = s
	m.lastErr = err
	cb := m.callback
	m.mu.Unlock()

	if cb != nil {
		cb(s, err)
	}
}

func (m *MountManager) Start() error {
	m.mu.Lock()
	if m.state == Mounting || m.state == Mounted {
		m.mu.Unlock()
		return fmt.Errorf("already %s", m.state)
	}
	m.stopped = false
	m.mu.Unlock()

	m.transition(Mounting, nil)

	addr, err := m.setup()
	if err != nil {
		m.transition(Error, err)
		return err
	}

	fs, conn, err := m.createFilesystem(addr)
	if err != nil {
		m.cleanup()
		m.transition(Error, err)
		return err
	}

	m.mu.Lock()
	m.fs = fs
	m.conn = conn
	m.done = make(chan error, 1)
	m.mu.Unlock()

	go func() { m.done <- fs.Mount() }()

	log.Info().Str("path", m.cfg.MountPoint).Str("gateway", addr).Msg("mounted")
	m.transition(Mounted, nil)
	return nil
}

// setup prepares config and starts embedded gateway if needed.
// Returns the effective gateway address.
func (m *MountManager) setup() (string, error) {
	if m.cfg.ConfigPath != "" {
		os.Setenv("CONFIG_PATH", m.cfg.ConfigPath)
	}

	if err := os.MkdirAll(m.cfg.MountPoint, 0755); err != nil {
		return "", err
	}

	addr := m.cfg.GatewayAddr
	if addr == "" {
		addr = "localhost:1993"
	}

	// Check for local mode
	cm, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return addr, nil // no config, use remote mode
	}

	cfg := cm.GetConfig()
	if cfg.Mode != types.ModeLocal {
		return addr, nil
	}

	// Start embedded gateway
	gw, err := gateway.NewGateway()
	if err != nil {
		return "", fmt.Errorf("create gateway: %w", err)
	}

	if err := gw.StartAsync(); err != nil {
		return "", fmt.Errorf("start gateway: %w", err)
	}

	m.mu.Lock()
	m.gw = gw
	m.mu.Unlock()

	time.Sleep(100 * time.Millisecond) // let gateway settle
	return gw.GRPCAddr(), nil
}

func (m *MountManager) createFilesystem(addr string) (*filesystem.Filesystem, *grpc.ClientConn, error) {
	fs, err := filesystem.NewFilesystem(filesystem.Config{
		MountPoint:  m.cfg.MountPoint,
		GatewayAddr: addr,
		Token:       m.cfg.Token,
		Verbose:     m.cfg.Verbose,
	})
	if err != nil {
		if strings.Contains(err.Error(), "connection refused") {
			return nil, nil, fmt.Errorf("gateway not running at %s", addr)
		}
		return nil, nil, err
	}

	shim, err := embed.GetShim()
	if err != nil {
		return nil, nil, fmt.Errorf("load shim: %w", err)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(common.TransportCredentials(addr)),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // Ping every 30s if idle — detects dead connections after sleep/wake
			Timeout:             10 * time.Second, // Wait 10s for ping ack before marking connection dead
			PermitWithoutStream: true,             // Send pings even when no RPCs are in flight
		}),
	)
	if err != nil {
		return nil, nil, err
	}

	// Register all vnodes
	fs.RegisterVNode(vnode.NewConfigVNode(addr, m.cfg.Token))
	fs.RegisterVNode(vnode.NewToolsVNode(addr, m.cfg.Token, shim))
	fs.RegisterVNode(vnode.NewSourcesVNode(conn, m.cfg.Token))
	fs.RegisterVNode(vnode.NewContextVNodeGRPC(conn, m.cfg.Token))  // /skills
	fs.RegisterVNode(vnode.NewTasksVNodeGRPC(conn, m.cfg.Token))    // /tasks
	fs.SetStorageFallback(vnode.NewStorageVNode(conn, m.cfg.Token)) // user folders

	return fs, conn, nil
}

func (m *MountManager) Stop() {
	m.mu.Lock()
	if m.stopped || (m.state != Mounted && m.state != Mounting && m.state != Error) {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	done := m.done
	mountPoint := m.cfg.MountPoint
	m.mu.Unlock()

	m.transition(Unmounting, nil)

	unmount(mountPoint)

	if done != nil {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}

	m.cleanup()
	m.transition(Idle, nil)
	log.Info().Msg("unmounted")
}

func (m *MountManager) cleanup() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}
	if m.gw != nil {
		m.gw.Shutdown()
		m.gw = nil
	}
	m.fs = nil
}

func (m *MountManager) Wait() error {
	m.mu.Lock()
	done := m.done
	m.mu.Unlock()

	if done == nil {
		return nil
	}

	err := <-done
	m.cleanup()

	if err != nil {
		m.transition(Error, err)
	} else {
		m.transition(Idle, nil)
	}
	return err
}

func (m *MountManager) Reload() error {
	m.Stop()
	return m.Start()
}

func unmount(path string) {
	if runtime.GOOS != "darwin" {
		return
	}

	cmds := [][]string{
		{"diskutil", "unmount", "force", path},
		{"diskutil", "unmount", path},
		{"umount", "-f", path},
	}

	for _, args := range cmds {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if exec.CommandContext(ctx, args[0], args[1:]...).Run() == nil {
			cancel()
			return
		}
		cancel()
	}
}
