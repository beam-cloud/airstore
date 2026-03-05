package vnode

import (
	"encoding/json"
	"io/fs"
	"syscall"
)

// Config holds the filesystem configuration exposed to tools
type Config struct {
	GatewayAddr string `json:"gateway_addr"`
	Token       string `json:"token,omitempty"`
}

// ConfigVNode exposes filesystem configuration at /.airstore
// Tools read /.airstore/config to discover gateway settings
type ConfigVNode struct {
	ReadOnlyBase // Embeds read-only defaults for write operations

	configJSON []byte
	toolShim   []byte
}

// NewConfigVNode creates a ConfigVNode with the given settings
func NewConfigVNode(gatewayAddr, token string, toolShim []byte) *ConfigVNode {
	cfg := Config{
		GatewayAddr: gatewayAddr,
		Token:       token,
	}

	// Pre-serialize config
	data, _ := json.MarshalIndent(cfg, "", "  ")

	return &ConfigVNode{
		configJSON: data,
		toolShim:   toolShim,
	}
}

// Prefix returns the path prefix this node handles
func (c *ConfigVNode) Prefix() string {
	return ConfigDir
}

// Getattr returns file attributes
func (c *ConfigVNode) Getattr(path string) (*FileInfo, error) {
	if path == ConfigDir {
		return NewDirInfo(configDirIno()), nil
	}

	payload, ino, ok := c.payloadForPath(path)
	if !ok {
		return nil, fs.ErrNotExist
	}
	return NewFileInfo(ino, int64(len(payload)), 0444), nil
}

// Readdir returns directory entries
func (c *ConfigVNode) Readdir(path string) ([]DirEntry, error) {
	if path != ConfigDir {
		return nil, syscall.ENOTDIR
	}

	return []DirEntry{
		{Name: "config", Mode: syscall.S_IFREG | 0444, Ino: configFileIno()},
		{Name: "tool-shim", Mode: syscall.S_IFREG | 0444, Ino: configToolShimIno()},
	}, nil
}

// Open opens a file
func (c *ConfigVNode) Open(path string, flags int) (FileHandle, error) {
	if path == ConfigDir {
		return 0, syscall.EISDIR
	}
	if _, _, ok := c.payloadForPath(path); ok {
		return 0, nil
	}
	return 0, fs.ErrNotExist
}

// Read reads from the config file
func (c *ConfigVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	payload, _, ok := c.payloadForPath(path)
	if !ok {
		return 0, fs.ErrNotExist
	}
	if off >= int64(len(payload)) {
		return 0, nil
	}
	return copy(buf, payload[off:]), nil
}

func (c *ConfigVNode) payloadForPath(path string) ([]byte, uint64, bool) {
	switch path {
	case ConfigFile:
		return c.configJSON, configFileIno(), true
	case ConfigToolShim:
		return c.toolShim, configToolShimIno(), true
	default:
		return nil, 0, false
	}
}

func configDirIno() uint64 {
	return PathIno(ConfigDir)
}

func configFileIno() uint64 {
	return PathIno(ConfigFile)
}

func configToolShimIno() uint64 {
	return PathIno(ConfigToolShim)
}
