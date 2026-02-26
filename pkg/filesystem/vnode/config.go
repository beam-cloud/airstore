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

	config     Config
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
		config:     cfg,
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
	switch path {
	case ConfigDir:
		return NewDirInfo(configDirIno()), nil
	case ConfigFile:
		return NewFileInfo(configFileIno(), int64(len(c.configJSON)), 0444), nil
	case ConfigToolShim:
		return NewFileInfo(configToolShimIno(), int64(len(c.toolShim)), 0444), nil
	default:
		return nil, fs.ErrNotExist
	}
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
	if path == ConfigFile {
		return 0, nil
	}
	if path == ConfigToolShim {
		return 0, nil
	}
	return 0, fs.ErrNotExist
}

// Read reads from the config file
func (c *ConfigVNode) Read(path string, buf []byte, off int64, fh FileHandle) (int, error) {
	switch path {
	case ConfigFile:
		if off >= int64(len(c.configJSON)) {
			return 0, nil
		}
		return copy(buf, c.configJSON[off:]), nil
	case ConfigToolShim:
		if off >= int64(len(c.toolShim)) {
			return 0, nil
		}
		return copy(buf, c.toolShim[off:]), nil
	default:
		return 0, fs.ErrNotExist
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
