package common

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/rs/zerolog/log"
)

// ContainerOverlay manages overlay filesystem layers for containers
// This creates a writable layer on top of a read-only base (e.g., CLIP FUSE mount)
type ContainerOverlay struct {
	sandboxID   string
	layers      []OverlayLayer
	root        string // Base rootfs path (e.g., CLIP FUSE mount)
	overlayPath string // Where to store overlay data
}

// OverlayLayer represents a single overlay layer
type OverlayLayer struct {
	index  int
	lower  string
	upper  string
	work   string
	merged string
}

// NewContainerOverlay creates a new ContainerOverlay
// rootPath: the base rootfs (e.g., CLIP FUSE mount)
// overlayPath: where to store overlay layer data
func NewContainerOverlay(sandboxID, rootPath, overlayPath string) *ContainerOverlay {
	return &ContainerOverlay{
		sandboxID:   sandboxID,
		layers:      []OverlayLayer{},
		root:        rootPath,
		overlayPath: overlayPath,
	}
}

// Setup creates the initial overlay layer on top of the base rootfs
func (co *ContainerOverlay) Setup() error {
	// Add an empty writable layer on top of the base rootfs
	return co.AddEmptyLayer()
}

// AddEmptyLayer adds an empty writable layer on top of the current stack
func (co *ContainerOverlay) AddEmptyLayer() error {
	index := 0
	lowerDir := co.root
	if len(co.layers) > 0 {
		index = len(co.layers)
		lowerDir = co.layers[index-1].merged
	}

	layerDir := filepath.Join(co.overlayPath, co.sandboxID, fmt.Sprintf("layer-%d", index))

	workDir := filepath.Join(layerDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("failed to create overlay work dir: %w", err)
	}

	upperDir := filepath.Join(layerDir, "upper")
	if err := os.MkdirAll(upperDir, 0755); err != nil {
		return fmt.Errorf("failed to create overlay upper dir: %w", err)
	}

	// Create required directories in the upper layer
	// These ensure they exist regardless of the base image
	for _, dir := range []string{"workspace", "volumes", "tmp"} {
		requiredDir := filepath.Join(upperDir, dir)
		if err := os.MkdirAll(requiredDir, 0755); err != nil {
			log.Warn().Err(err).Str("path", requiredDir).Msg("failed to create required directory in upper layer")
		}
	}

	mergedDir := filepath.Join(layerDir, "merged")
	if err := os.MkdirAll(mergedDir, 0755); err != nil {
		return fmt.Errorf("failed to create overlay merged dir: %w", err)
	}

	layer := OverlayLayer{
		lower:  lowerDir,
		upper:  upperDir,
		work:   workDir,
		merged: mergedDir,
		index:  index,
	}

	if err := co.mount(&layer); err != nil {
		return err
	}

	co.layers = append(co.layers, layer)
	return nil
}

// AddLayer adds a pre-populated upper layer on top of the current stack
func (co *ContainerOverlay) AddLayer(upperDir string) error {
	index := 0
	lowerDir := co.root
	if len(co.layers) > 0 {
		index = len(co.layers)
		lowerDir = co.layers[index-1].merged
	}

	layerDir := filepath.Join(co.overlayPath, co.sandboxID, fmt.Sprintf("layer-%d", index))

	workDir := filepath.Join(layerDir, "work")
	if err := os.MkdirAll(workDir, 0755); err != nil {
		return fmt.Errorf("failed to create overlay work dir: %w", err)
	}

	mergedDir := filepath.Join(layerDir, "merged")
	if err := os.MkdirAll(mergedDir, 0755); err != nil {
		return fmt.Errorf("failed to create overlay merged dir: %w", err)
	}

	layer := OverlayLayer{
		lower:  lowerDir,
		upper:  upperDir,
		work:   workDir,
		merged: mergedDir,
		index:  index,
	}

	if err := co.mount(&layer); err != nil {
		return err
	}

	co.layers = append(co.layers, layer)
	return nil
}

// Cleanup unmounts all layers and removes overlay directories
func (co *ContainerOverlay) Cleanup() error {
	var lastErr error

	// Unmount layers in reverse order (top to bottom)
	for len(co.layers) > 0 {
		i := len(co.layers) - 1
		layer := co.layers[i]

		log.Debug().Str("sandbox_id", co.sandboxID).Str("layer_path", layer.merged).Msg("unmounting overlay layer")

		cmd := exec.Command("umount", "-f", layer.merged)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			log.Error().
				Str("sandbox_id", co.sandboxID).
				Str("layer_path", layer.merged).
				Str("stderr", stderr.String()).
				Err(err).
				Msg("failed to unmount overlay layer")
			lastErr = err
		}

		layerDir := filepath.Join(co.overlayPath, co.sandboxID, fmt.Sprintf("layer-%d", i))
		if err := os.RemoveAll(layerDir); err != nil {
			log.Warn().Str("path", layerDir).Err(err).Msg("failed to remove overlay layer dir")
		}

		co.layers = co.layers[:i]
	}

	// Clean up the sandbox overlay directory
	if err := os.RemoveAll(filepath.Join(co.overlayPath, co.sandboxID)); err != nil {
		log.Warn().Str("sandbox_id", co.sandboxID).Err(err).Msg("failed to remove sandbox overlay dir")
	}

	return lastErr
}

// TopLayerPath returns the merged path of the top overlay layer
// This is the path that should be used as the container rootfs
func (co *ContainerOverlay) TopLayerPath() string {
	if len(co.layers) == 0 {
		return co.root
	}
	return co.layers[len(co.layers)-1].merged
}

// UpperLayerPath returns the upper (writable) path of the top layer
func (co *ContainerOverlay) UpperLayerPath() string {
	if len(co.layers) == 0 {
		return ""
	}
	return co.layers[len(co.layers)-1].upper
}

// mount mounts an overlay layer using kernel overlay (matches beta9)
func (co *ContainerOverlay) mount(layer *OverlayLayer) error {
	startTime := time.Now()

	// Use kernel overlay like beta9
	mntOptions := fmt.Sprintf("lowerdir=%s,upperdir=%s,workdir=%s", layer.lower, layer.upper, layer.work)
	cmd := exec.Command("mount", "-t", "overlay", "overlay", "-o", mntOptions, layer.merged)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to mount overlay (lower=%s, upper=%s, merged=%s): %w (stderr: %s)",
			layer.lower, layer.upper, layer.merged, err, stderr.String())
	}

	log.Info().
		Str("sandbox_id", co.sandboxID).
		Int("layer_index", layer.index).
		Dur("duration", time.Since(startTime)).
		Msg("mounted overlay layer")

	return nil
}
