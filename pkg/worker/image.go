package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/beam-cloud/clip/pkg/clip"
	"github.com/beam-cloud/airstore/pkg/registry"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog/log"
)

// Default paths for CLIP storage directories
const (
	DefaultCachePath = "/var/lib/clip/cache"
	DefaultWorkPath  = "/var/lib/clip/work"
	DefaultMountPath = "/var/lib/clip/mnt"
)

// Timeouts for CLIP operations
const (
	convertTimeout     = 10 * time.Minute
	lockAcquireTimeout = 5 * time.Minute
	mountVerifyTimeout = 10 * time.Second
	lockPollInterval   = 100 * time.Millisecond
)

// ImageManager provides container image management functionality.
type ImageManager interface {
	// PrepareRootfs prepares a rootfs from a container image.
	// Returns the path to the rootfs and a cleanup function.
	PrepareRootfs(ctx context.Context, imageRef string) (rootfsPath string, cleanup func(), err error)

	// Close cleans up all resources.
	Close() error
}

// CLIPImageManager implements ImageManager using CLIP for lazy-loading images from OCI registries.
// It manages FUSE mounts with reference counting for efficient image reuse.
type CLIPImageManager struct {
	config   types.ImageConfig
	registry *registry.ImageRegistry // S3 registry for archive storage

	// Mount tracking with reference counting
	mounts   map[string]*clipMount
	mountsMu sync.RWMutex

	// Per-image locks prevent concurrent operations on the same image
	imageLocks   map[string]*sync.Mutex
	imageLocksMu sync.Mutex
}

// clipMount tracks an active CLIP FUSE mount with reference counting.
type clipMount struct {
	imageID     string
	archivePath string
	mountPoint  string
	server      *fuse.Server
	refCount    int
}

// NewImageManager creates a new CLIP-based image manager.
func NewImageManager(config types.ImageConfig) (ImageManager, error) {
	config = applyConfigDefaults(config)

	if err := createDirectories(config); err != nil {
		return nil, err
	}

	clip.SetLogLevel("info")

	// Initialize S3 registry for archive storage
	var reg *registry.ImageRegistry
	if config.S3.Bucket != "" {
		var err error
		reg, err = registry.NewImageRegistry(registry.Config{
			Bucket:         config.S3.Bucket,
			Region:         config.S3.Region,
			Endpoint:       config.S3.Endpoint,
			AccessKey:      config.S3.AccessKey,
			SecretKey:      config.S3.SecretKey,
			ForcePathStyle: config.S3.ForcePathStyle,
		})
		if err != nil {
			log.Warn().Err(err).Msg("failed to initialize S3 registry, continuing without remote storage")
		}
	}

	log.Info().
		Str("cache", config.CachePath).
		Str("work", config.WorkPath).
		Str("mount", config.MountPath).
		Str("s3_bucket", config.S3.Bucket).
		Msg("CLIP image manager initialized")

	return &CLIPImageManager{
		config:     config,
		registry:   reg,
		mounts:     make(map[string]*clipMount),
		imageLocks: make(map[string]*sync.Mutex),
	}, nil
}

// PrepareRootfs converts an OCI image to CLIP format and mounts it as a FUSE filesystem.
// If the image is already mounted, it increments the reference count and returns the existing mount.
func (m *CLIPImageManager) PrepareRootfs(ctx context.Context, imageRef string) (string, func(), error) {
	normalizedRef := normalizeImageRef(imageRef)
	imageID := imageIDFromRef(normalizedRef)

	log.Info().
		Str("source", imageRef).
		Str("image_id", imageID).
		Msg("preparing CLIP rootfs")

	// Serialize operations on the same image
	lock := m.getOrCreateImageLock(imageID)
	lock.Lock()
	defer lock.Unlock()

	// Fast path: reuse existing mount
	if mount := m.getExistingMount(imageID); mount != nil {
		return mount.mountPoint, m.createReleaseFunc(imageID), nil
	}

	// Create new mount
	archivePath := filepath.Join(m.config.WorkPath, imageID+".clip")
	mountPoint := filepath.Join(m.config.MountPath, imageID)

	if err := m.ensureArchive(ctx, normalizedRef, archivePath); err != nil {
		return "", nil, fmt.Errorf("prepare archive: %w", err)
	}

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		return "", nil, fmt.Errorf("create mount point: %w", err)
	}

	server, err := m.mount(archivePath, mountPoint)
	if err != nil {
		os.RemoveAll(mountPoint)
		return "", nil, fmt.Errorf("mount archive: %w", err)
	}

	m.registerMount(imageID, archivePath, mountPoint, server)

	log.Info().
		Str("image", imageRef).
		Str("mount", mountPoint).
		Msg("CLIP rootfs mounted")

	return mountPoint, m.createReleaseFunc(imageID), nil
}

// Close unmounts all FUSE filesystems and releases resources.
func (m *CLIPImageManager) Close() error {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	var lastErr error
	for imageID, mount := range m.mounts {
		if err := m.unmountLocked(imageID, mount); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ============================================================================
// Internal: Configuration
// ============================================================================

func applyConfigDefaults(config types.ImageConfig) types.ImageConfig {
	if config.CachePath == "" {
		config.CachePath = DefaultCachePath
	}
	if config.WorkPath == "" {
		config.WorkPath = DefaultWorkPath
	}
	if config.MountPath == "" {
		config.MountPath = DefaultMountPath
	}
	return config
}

func createDirectories(config types.ImageConfig) error {
	dirs := []string{config.CachePath, config.WorkPath, config.MountPath}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("create directory %s: %w", dir, err)
		}
	}
	return nil
}

// ============================================================================
// Internal: Locking
// ============================================================================

func (m *CLIPImageManager) getOrCreateImageLock(imageID string) *sync.Mutex {
	m.imageLocksMu.Lock()
	defer m.imageLocksMu.Unlock()

	if lock, ok := m.imageLocks[imageID]; ok {
		return lock
	}

	lock := &sync.Mutex{}
	m.imageLocks[imageID] = lock
	return lock
}

// ============================================================================
// Internal: Mount Management
// ============================================================================

func (m *CLIPImageManager) getExistingMount(imageID string) *clipMount {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	mount, exists := m.mounts[imageID]
	if !exists {
		return nil
	}

	mount.refCount++
	log.Debug().
		Str("image_id", imageID).
		Int("ref_count", mount.refCount).
		Msg("reusing existing CLIP mount")

	return mount
}

func (m *CLIPImageManager) registerMount(imageID, archivePath, mountPoint string, server *fuse.Server) {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	m.mounts[imageID] = &clipMount{
		imageID:     imageID,
		archivePath: archivePath,
		mountPoint:  mountPoint,
		server:      server,
		refCount:    1,
	}
}

func (m *CLIPImageManager) createReleaseFunc(imageID string) func() {
	return func() { m.releaseMount(imageID) }
}

func (m *CLIPImageManager) releaseMount(imageID string) {
	m.mountsMu.Lock()
	defer m.mountsMu.Unlock()

	mount, exists := m.mounts[imageID]
	if !exists {
		return
	}

	mount.refCount--
	if mount.refCount > 0 {
		log.Debug().
			Str("image_id", imageID).
			Int("ref_count", mount.refCount).
			Msg("CLIP mount still in use")
		return
	}

	m.unmountLocked(imageID, mount)
}

func (m *CLIPImageManager) unmountLocked(imageID string, mount *clipMount) error {
	log.Info().
		Str("image_id", imageID).
		Str("mount", mount.mountPoint).
		Msg("unmounting CLIP filesystem")

	var err error
	if mount.server != nil {
		if unmountErr := mount.server.Unmount(); unmountErr != nil {
			log.Warn().Err(unmountErr).Str("mount", mount.mountPoint).Msg("unmount failed")
			err = unmountErr
		}
	}

	if removeErr := os.RemoveAll(mount.mountPoint); removeErr != nil {
		log.Warn().Err(removeErr).Str("mount", mount.mountPoint).Msg("remove mount point failed")
	}

	delete(m.mounts, imageID)
	return err
}

// ============================================================================
// Internal: Archive Management
// ============================================================================

func (m *CLIPImageManager) ensureArchive(ctx context.Context, imageRef, archivePath string) error {
	imageID := imageIDFromRef(imageRef)

	// Fast path: archive exists locally
	if fileExists(archivePath) {
		log.Debug().Str("archive", archivePath).Msg("using locally cached CLIP archive")
		return nil
	}

	// Try pulling from S3 registry
	if m.registry != nil {
		if exists, _ := m.registry.Exists(ctx, imageID); exists {
			log.Info().Str("image_id", imageID).Msg("pulling CLIP archive from S3")
			if pullErr := m.registry.Pull(ctx, archivePath, imageID); pullErr == nil {
				return nil
			} else {
				log.Warn().Err(pullErr).Str("image_id", imageID).Msg("failed to pull from S3, will create locally")
			}
		}
	}

	// Create archive locally
	if err := m.createArchiveWithLock(ctx, imageRef, archivePath); err != nil {
		return err
	}

	// Push to S3 registry in background for other workers
	if m.registry != nil {
		go func() {
			pushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			if err := m.registry.Push(pushCtx, archivePath, imageID); err != nil {
				log.Warn().Err(err).Str("image_id", imageID).Msg("failed to push archive to S3")
			}
		}()
	}

	return nil
}

func (m *CLIPImageManager) createArchiveWithLock(ctx context.Context, imageRef, archivePath string) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(archivePath), 0755); err != nil {
		return fmt.Errorf("create archive directory: %w", err)
	}

	// Acquire file lock for exclusive access
	lockPath := archivePath + ".lock"
	unlock, err := m.acquireFileLock(ctx, lockPath)
	if err != nil {
		return fmt.Errorf("acquire lock: %w", err)
	}
	defer unlock()

	// Double-check after acquiring lock
	if fileExists(archivePath) {
		log.Debug().Str("archive", archivePath).Msg("archive created while waiting for lock")
		return nil
	}

	// Convert to temp file, then atomically rename
	tempPath := archivePath + ".tmp"
	defer os.Remove(tempPath)

	log.Info().
		Str("image", imageRef).
		Str("archive", archivePath).
		Msg("converting OCI image to CLIP format")

	if err := m.convert(ctx, imageRef, tempPath); err != nil {
		return fmt.Errorf("convert image: %w", err)
	}

	if err := os.Rename(tempPath, archivePath); err != nil {
		return fmt.Errorf("rename archive: %w", err)
	}

	log.Info().Str("archive", archivePath).Msg("CLIP archive created successfully")
	return nil
}

func (m *CLIPImageManager) acquireFileLock(ctx context.Context, lockPath string) (unlock func(), err error) {
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("create lock file: %w", err)
	}

	lockCtx, cancel := context.WithTimeout(ctx, lockAcquireTimeout)
	defer cancel()

	if err := waitForFileLock(lockCtx, lockFile); err != nil {
		lockFile.Close()
		return nil, err
	}

	return func() {
		syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN)
		lockFile.Close()
		os.Remove(lockPath)
	}, nil
}

func waitForFileLock(ctx context.Context, file *os.File) error {
	fd := int(file.Fd())

	// Try non-blocking first
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		return nil
	}

	// Poll until lock acquired or context cancelled
	ticker := time.NewTicker(lockPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for lock: %w", ctx.Err())
		case <-ticker.C:
			err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB)
			if err == nil {
				return nil
			}
			if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
				return fmt.Errorf("flock: %w", err)
			}
		}
	}
}

// ============================================================================
// Internal: CLIP Operations
// ============================================================================

func (m *CLIPImageManager) convert(ctx context.Context, imageRef, outputPath string) error {
	start := time.Now()

	progressChan := make(chan clip.OCIIndexProgress, 100)
	done := make(chan struct{})

	go logConversionProgress(progressChan, done)

	ctx, cancel := context.WithTimeout(ctx, convertTimeout)
	defer cancel()

	err := clip.CreateFromOCIImage(ctx, clip.CreateFromOCIImageOptions{
		ImageRef:     imageRef,
		OutputPath:   outputPath,
		ProgressChan: progressChan,
	})

	close(progressChan)
	<-done

	if err != nil {
		return err
	}

	log.Info().
		Str("image", imageRef).
		Dur("duration", time.Since(start)).
		Msg("CLIP conversion completed")

	return nil
}

func logConversionProgress(progressChan <-chan clip.OCIIndexProgress, done chan<- struct{}) {
	defer close(done)
	for progress := range progressChan {
		if progress.TotalLayers > 0 {
			log.Debug().
				Str("stage", progress.Stage).
				Int("layer", progress.LayerIndex).
				Int("total", progress.TotalLayers).
				Msg("CLIP conversion progress")
		}
	}
}

func (m *CLIPImageManager) mount(archivePath, mountPoint string) (*fuse.Server, error) {
	opts := clip.MountOptions{
		ArchivePath:    archivePath,
		MountPoint:     mountPoint,
		CachePath:      m.config.CachePath,
		UseCheckpoints: false,
	}

	startServer, _, server, err := clip.MountArchive(opts)
	if err != nil {
		return nil, fmt.Errorf("create mount: %w", err)
	}

	if err := startServer(); err != nil {
		return nil, fmt.Errorf("start FUSE server: %w", err)
	}

	if err := verifyMount(mountPoint, mountVerifyTimeout); err != nil {
		log.Warn().Err(err).Str("mount", mountPoint).Msg("mount verification failed")
	}

	return server, nil
}

func verifyMount(mountPoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(mountPoint)
		if err == nil && len(entries) > 0 {
			log.Debug().
				Str("mount", mountPoint).
				Int("entries", len(entries)).
				Msg("CLIP mount verified")
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("mount appears empty after %v", timeout)
}

// ============================================================================
// Utilities
// ============================================================================

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// normalizeImageRef converts short image references to fully qualified references.
// Examples:
//   - "ubuntu:22.04" -> "docker.io/library/ubuntu:22.04"
//   - "user/repo" -> "docker.io/user/repo:latest"
func normalizeImageRef(ref string) string {
	// Add docker.io prefix if no registry specified
	if !strings.Contains(ref, "/") {
		ref = "docker.io/library/" + ref
	} else if strings.Count(ref, "/") == 1 {
		first := strings.Split(ref, "/")[0]
		if !strings.Contains(first, ".") && !strings.Contains(first, ":") {
			ref = "docker.io/" + ref
		}
	}

	// Add :latest if no tag or digest specified
	if !strings.Contains(ref, ":") && !strings.Contains(ref, "@") {
		ref = ref + ":latest"
	}

	return ref
}

// imageIDFromRef creates a filesystem-safe identifier from an image reference.
// Includes architecture to prevent cross-platform cache collisions.
func imageIDFromRef(ref string) string {
	replacer := strings.NewReplacer(
		"/", "_",
		":", "_",
		"@", "_",
		".", "_",
	)
	safe := replacer.Replace(ref) + "_" + runtime.GOARCH

	if len(safe) > 100 {
		safe = safe[:100]
	}

	return safe
}
