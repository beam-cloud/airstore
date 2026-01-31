package clients

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	bucketCreateRetries = 3
	bucketCreateDelay   = 500 * time.Millisecond

	// Presigned URL expiry times
	PresignUploadExpiry   = 15 * time.Minute // Upload URLs valid for 15 minutes
	PresignDownloadExpiry = 1 * time.Hour    // Download URLs valid for 1 hour
)

// StorageClient manages S3 operations for workspace storage
type StorageClient struct {
	s3            *s3.Client
	presign       *s3.PresignClient
	cfg           types.WorkspaceStorageConfig
}

func NewStorageClient(ctx context.Context, cfg types.WorkspaceStorageConfig) (*StorageClient, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.DefaultRegion),
		config.WithRetryMaxAttempts(3),
		config.WithRetryMode(aws.RetryModeStandard),
	}

	if cfg.DefaultAccessKey != "" && cfg.DefaultSecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.DefaultAccessKey, cfg.DefaultSecretKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.DefaultEndpointUrl != "" {
			o.BaseEndpoint = &cfg.DefaultEndpointUrl
			o.UsePathStyle = true
		}
	})

	log.Info().
		Str("region", cfg.DefaultRegion).
		Str("endpoint", cfg.DefaultEndpointUrl).
		Str("bucket_prefix", cfg.DefaultBucketPrefix).
		Msg("storage client initialized")

	return &StorageClient{
		s3:      s3Client,
		presign: s3.NewPresignClient(s3Client),
		cfg:     cfg,
	}, nil
}

func (c *StorageClient) S3Client() *s3.Client                      { return c.s3 }
func (c *StorageClient) PresignClient() *s3.PresignClient          { return c.presign }
func (c *StorageClient) Config() types.WorkspaceStorageConfig      { return c.cfg }

func (c *StorageClient) WorkspaceBucketName(workspaceExternalId string) string {
	return types.WorkspaceBucketName(c.cfg.DefaultBucketPrefix, workspaceExternalId)
}

// Bucket operations

func (c *StorageClient) CreateBucket(ctx context.Context, bucket string) error {
	var lastErr error
	for i := 0; i < bucketCreateRetries; i++ {
		_, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(bucket)})
		if err == nil {
			log.Info().Str("bucket", bucket).Msg("created S3 bucket")
			return nil
		}

		var exists *s3types.BucketAlreadyExists
		var owned *s3types.BucketAlreadyOwnedByYou
		if errors.As(err, &exists) || errors.As(err, &owned) {
			return nil
		}

		lastErr = err
		time.Sleep(bucketCreateDelay)
	}
	return fmt.Errorf("create bucket %s: %w", bucket, lastErr)
}

func (c *StorageClient) BucketExists(ctx context.Context, bucket string) (bool, error) {
	_, err := c.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		var notFound *s3types.NotFound
		if errors.As(err, &notFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *StorageClient) CreateWorkspaceBucket(ctx context.Context, workspaceExternalId string) (string, error) {
	bucket := c.WorkspaceBucketName(workspaceExternalId)

	if err := c.CreateBucket(ctx, bucket); err != nil {
		return "", err
	}

	log.Info().Str("bucket", bucket).Str("workspace", workspaceExternalId).Msg("workspace bucket ready")
	return bucket, nil
}

// Object operations

func (c *StorageClient) Upload(ctx context.Context, bucket, key string, data []byte) error {
	_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (c *StorageClient) Download(ctx context.Context, bucket, key string) ([]byte, error) {
	resp, err := c.s3.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (c *StorageClient) Delete(ctx context.Context, bucket, key string) error {
	_, err := c.s3.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	return err
}

func (c *StorageClient) Head(ctx context.Context, bucket, key string) (*s3.HeadObjectOutput, error) {
	return c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
}

func (c *StorageClient) Exists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if isNotFoundError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (c *StorageClient) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int32) (*s3.ListObjectsV2Output, error) {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		MaxKeys: aws.Int32(maxKeys),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}
	return c.s3.ListObjectsV2(ctx, input)
}

func (c *StorageClient) CopyObject(ctx context.Context, srcBucket, srcKey, dstBucket, dstKey string) error {
	_, err := c.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(dstBucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(srcBucket + "/" + srcKey),
	})
	return err
}

func isNotFoundError(err error) bool {
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "NotFound") || strings.Contains(errStr, "NoSuchKey")
}

// Presigned URL operations

// PresignUpload generates a presigned PUT URL for uploading a file
func (c *StorageClient) PresignUpload(ctx context.Context, bucket, key, contentType string, expires time.Duration) (string, error) {
	input := &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	resp, err := c.presign.PresignPutObject(ctx, input, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign upload: %w", err)
	}
	return resp.URL, nil
}

// PresignDownload generates a presigned GET URL for downloading a file
func (c *StorageClient) PresignDownload(ctx context.Context, bucket, key string, expires time.Duration) (string, error) {
	resp, err := c.presign.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = expires
	})
	if err != nil {
		return "", fmt.Errorf("presign download: %w", err)
	}
	return resp.URL, nil
}
