package common

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	// FileExtension is the extension used for CLIP archive files
	FileExtension = "clip"
)

// Config holds the configuration for the image registry.
type Config struct {
	Bucket         string
	Region         string
	Endpoint       string
	AccessKey      string
	SecretKey      string
	ForcePathStyle bool
}

// ImageRegistry manages CLIP archive storage in S3.
type ImageRegistry struct {
	store  ObjectStore
	bucket string
}

// NewImageRegistry creates a new image registry with S3 storage.
func NewImageRegistry(cfg Config) (*ImageRegistry, error) {
	store, err := NewS3Store(cfg)
	if err != nil {
		return nil, fmt.Errorf("create S3 store: %w", err)
	}

	log.Info().
		Str("bucket", cfg.Bucket).
		Str("endpoint", cfg.Endpoint).
		Msg("image registry initialized")

	return &ImageRegistry{
		store:  store,
		bucket: cfg.Bucket,
	}, nil
}

// Push uploads a CLIP archive to the registry.
func (r *ImageRegistry) Push(ctx context.Context, localPath, imageID string) error {
	key := r.keyForImage(imageID)

	start := time.Now()
	if err := r.store.Put(ctx, localPath, key); err != nil {
		return fmt.Errorf("push %s: %w", imageID, err)
	}

	info, _ := os.Stat(localPath)
	sizeMB := float64(info.Size()) / 1024 / 1024

	log.Info().
		Str("image_id", imageID).
		Float64("size_mb", sizeMB).
		Dur("duration", time.Since(start)).
		Msg("pushed CLIP archive to registry")

	return nil
}

// Pull downloads a CLIP archive from the registry.
func (r *ImageRegistry) Pull(ctx context.Context, localPath, imageID string) error {
	key := r.keyForImage(imageID)

	start := time.Now()
	if err := r.store.Get(ctx, key, localPath); err != nil {
		return fmt.Errorf("pull %s: %w", imageID, err)
	}

	info, _ := os.Stat(localPath)
	sizeMB := float64(info.Size()) / 1024 / 1024

	log.Info().
		Str("image_id", imageID).
		Float64("size_mb", sizeMB).
		Dur("duration", time.Since(start)).
		Msg("pulled CLIP archive from registry")

	return nil
}

// Exists checks if a CLIP archive exists in the registry.
func (r *ImageRegistry) Exists(ctx context.Context, imageID string) (bool, error) {
	return r.store.Exists(ctx, r.keyForImage(imageID))
}

func (r *ImageRegistry) keyForImage(imageID string) string {
	return fmt.Sprintf("%s.%s", imageID, FileExtension)
}

// ObjectStore defines the interface for object storage backends.
type ObjectStore interface {
	Put(ctx context.Context, localPath, key string) error
	Get(ctx context.Context, key, localPath string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// S3Store implements ObjectStore using AWS S3.
type S3Store struct {
	client *s3.Client
	bucket string
}

// NewS3Store creates a new S3-backed object store.
func NewS3Store(cfg Config) (*S3Store, error) {
	awsCfg, err := buildAWSConfig(cfg)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.ForcePathStyle {
			o.UsePathStyle = true
		}
	})

	return &S3Store{
		client: client,
		bucket: cfg.Bucket,
	}, nil
}

func buildAWSConfig(cfg Config) (aws.Config, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
	}

	// Use static credentials if provided
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	// Use custom endpoint for LocalStack
	if cfg.Endpoint != "" {
		opts = append(opts, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               cfg.Endpoint,
					HostnameImmutable: true,
				}, nil
			}),
		))
	}

	return config.LoadDefaultConfig(context.Background(), opts...)
}

// Put uploads a file to S3.
func (s *S3Store) Put(ctx context.Context, localPath, key string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer f.Close()

	uploader := manager.NewUploader(s.client)
	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}

	return nil
}

// Get downloads a file from S3.
func (s *S3Store) Get(ctx context.Context, key, localPath string) error {
	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		return fmt.Errorf("create directory: %w", err)
	}

	// Download to temp file then rename atomically
	tmpPath := fmt.Sprintf("%s.%s", localPath, uuid.New().String()[:6])
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	downloader := manager.NewDownloader(s.client)
	_, err = downloader.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	}, func(d *manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB parts
		d.Concurrency = 5
	})

	f.Close()

	if err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("download: %w", err)
	}

	if err := os.Rename(tmpPath, localPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}

	return nil
}

// Exists checks if an object exists in S3.
func (s *S3Store) Exists(ctx context.Context, key string) (bool, error) {
	// Use a range request to minimize data transfer
	_, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String("bytes=0-0"),
	})

	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}
