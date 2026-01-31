package menubar

import (
	"os"
	"path/filepath"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
)

const prefsDir = ".config/airstore"
const prefsFile = "menubar.yaml"

// Preferences holds user-configurable settings for the menu bar app.
type Preferences struct {
	MountPoint string `koanf:"mountPoint"`
	ConfigPath string `koanf:"configPath"`
	AutoMount  bool   `koanf:"autoMount"`
}

func defaultPreferences() Preferences {
	home, _ := os.UserHomeDir()
	return Preferences{
		MountPoint: filepath.Join(home, "Airstore"),
		ConfigPath: "",
		AutoMount:  true,
	}
}

func prefsPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, prefsDir, prefsFile)
}

// LoadPreferences loads preferences from ~/.config/airstore/menubar.yaml.
// Returns defaults if the file does not exist.
func LoadPreferences() (Preferences, error) {
	prefs := defaultPreferences()

	path := prefsPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return prefs, nil
	}

	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return prefs, err
	}

	if err := k.Unmarshal("", &prefs); err != nil {
		return defaultPreferences(), err
	}

	// Expand ~ in mount point
	if prefs.MountPoint == "" {
		prefs.MountPoint = defaultPreferences().MountPoint
	}

	return prefs, nil
}

// SavePreferences writes preferences to ~/.config/airstore/menubar.yaml.
func SavePreferences(prefs Preferences) error {
	path := prefsPath()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	data := "mountPoint: " + prefs.MountPoint + "\n" +
		"configPath: " + prefs.ConfigPath + "\n" +
		"autoMount: "
	if prefs.AutoMount {
		data += "true\n"
	} else {
		data += "false\n"
	}

	// Validate round-trip
	k := koanf.New(".")
	if err := k.Load(rawbytes.Provider([]byte(data)), yaml.Parser()); err != nil {
		return err
	}

	return os.WriteFile(path, []byte(data), 0644)
}
