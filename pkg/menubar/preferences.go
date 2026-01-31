package menubar

import (
	"os"
	"path/filepath"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	goyaml "gopkg.in/yaml.v3"
)

const prefsDir = ".config/airstore"
const prefsFile = "menubar.yaml"

// Preferences holds user-configurable settings for the menu bar app.
type Preferences struct {
	MountPoint string `koanf:"mountPoint" yaml:"mountPoint"`
	ConfigPath string `koanf:"configPath" yaml:"configPath"`
	AutoMount  bool   `koanf:"autoMount" yaml:"autoMount"`
}

func defaultPreferences() (Preferences, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return Preferences{}, err
	}
	return Preferences{
		MountPoint: filepath.Join(home, "Airstore"),
		ConfigPath: "",
		AutoMount:  true,
	}, nil
}

func prefsPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, prefsDir, prefsFile), nil
}

// LoadPreferences loads preferences from ~/.config/airstore/menubar.yaml.
// Returns defaults if the file does not exist.
func LoadPreferences() (Preferences, error) {
	prefs, err := defaultPreferences()
	if err != nil {
		return Preferences{}, err
	}

	path, err := prefsPath()
	if err != nil {
		return Preferences{}, err
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		return prefs, nil
	}

	k := koanf.New(".")
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return prefs, err
	}

	if err := k.Unmarshal("", &prefs); err != nil {
		defPrefs, defErr := defaultPreferences()
		if defErr != nil {
			return Preferences{}, defErr
		}
		return defPrefs, err
	}

	// Expand ~ in mount point
	if prefs.MountPoint == "" {
		defPrefs, defErr := defaultPreferences()
		if defErr != nil {
			return Preferences{}, defErr
		}
		prefs.MountPoint = defPrefs.MountPoint
	}

	return prefs, nil
}

// SavePreferences writes preferences to ~/.config/airstore/menubar.yaml.
func SavePreferences(prefs Preferences) error {
	path, err := prefsPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	data, err := goyaml.Marshal(&prefs)
	if err != nil {
		return err
	}

	return os.WriteFile(path, data, 0644)
}
