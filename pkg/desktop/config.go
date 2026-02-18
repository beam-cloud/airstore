package desktop

import (
	"encoding/json"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const configDir = ".airstore"
const configFile = "tray.yaml"
const credsFile = "credentials"

// Config holds user-configurable settings for the desktop/tray app.
type Config struct {
	MountPoint      string `yaml:"mountPoint"`
	ConfigPath      string `yaml:"configPath"`
	GatewayAddr     string `yaml:"gatewayAddr"`
	GatewayHTTPAddr string `yaml:"gatewayHTTPAddr,omitempty"`
	AutoMount       bool   `yaml:"autoMount"`
	Token           string `yaml:"-"` // Runtime only, not persisted.
}

// DefaultConfig returns the default configuration.
func DefaultConfig() Config {
	home, _ := os.UserHomeDir()
	return Config{
		MountPoint:  filepath.Join(home, "Desktop", "Airstore"),
		GatewayAddr: defaultGateway(),
		AutoMount:   true,
	}
}

func cfgPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, configDir, configFile)
}

// LoadConfig loads configuration from ~/.airstore/tray.yaml.
// Returns defaults if the file does not exist.
func LoadConfig() Config {
	cfg := DefaultConfig()

	data, err := os.ReadFile(cfgPath())
	if err != nil {
		return cfg
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return DefaultConfig()
	}

	defaults := DefaultConfig()
	if cfg.MountPoint == "" {
		cfg.MountPoint = defaults.MountPoint
	}
	if cfg.GatewayAddr == "" {
		cfg.GatewayAddr = defaults.GatewayAddr
	}

	return cfg
}

// SaveConfig writes configuration to ~/.airstore/tray.yaml.
func SaveConfig(cfg Config) error {
	path := cfgPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := yaml.Marshal(&cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// LoadToken returns the stored auth token from ~/.airstore/credentials.
func LoadToken() string {
	home, _ := os.UserHomeDir()
	data, err := os.ReadFile(filepath.Join(home, configDir, credsFile))
	if err != nil {
		return ""
	}
	var creds map[string]string
	if json.Unmarshal(data, &creds) != nil {
		return ""
	}
	return creds["token"]
}
