package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
)

const (
	s3Timeout    = 30 * time.Second // Per-operation timeout
	maxReadSize  = 64 * 1024 * 1024 // 64MB max read to prevent OOM
	maxWriteSize = 64 * 1024 * 1024 // 64MB max write
)

// ContextService provides S3-backed file storage for workspace context.
type ContextService struct {
	pb.UnimplementedContextServiceServer
	s3     *s3.Client
	bucket string
}

// NewContextService creates a new context service with retry and timeout configuration
func NewContextService(cfg types.S3Config) (*ContextService, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.Region),
		config.WithRetryMaxAttempts(3),
		config.WithRetryMode(aws.RetryModeStandard),
	}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = &cfg.Endpoint
			o.UsePathStyle = cfg.ForcePathStyle
		}
	})

	log.Info().Str("bucket", cfg.Bucket).Str("endpoint", cfg.Endpoint).Msg("context service initialized")
	return &ContextService{s3: s3Client, bucket: cfg.Bucket}, nil
}

// withTimeout wraps a context with the standard S3 operation timeout
func (s *ContextService) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, s3Timeout)
}

func (s *ContextService) key(ctx context.Context, path string) (string, error) {
	rc := auth.FromContext(ctx)
	if rc == nil {
		return "", fmt.Errorf("no auth context")
	}
	prefix := rc.WorkspaceExt
	if prefix == "" {
		if rc.IsGatewayAuth {
			prefix = "_gateway"
		} else {
			return "", fmt.Errorf("no workspace in context")
		}
	}
	return prefix + "/" + strings.TrimPrefix(path, "/"), nil
}

// Stat returns file/directory attributes
func (s *ContextService) Stat(ctx context.Context, req *pb.ContextStatRequest) (*pb.ContextStatResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextStatResponse{Ok: false, Error: err.Error()}, nil
	}

	if req.Path == "" || req.Path == "/" {
		return &pb.ContextStatResponse{Ok: true, Info: &pb.FileInfo{Mode: uint32(syscall.S_IFDIR | 0755), IsDir: true}}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// Try as file
	if resp, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key}); err == nil {
		_, isLink := resp.Metadata["symlink-target"]
		mode := uint32(syscall.S_IFREG | 0644)
		if isLink {
			mode = uint32(syscall.S_IFLNK | 0777)
		}

		return &pb.ContextStatResponse{Ok: true, Info: &pb.FileInfo{
			Size: aws.ToInt64(resp.ContentLength), Mode: mode,
			Mtime: resp.LastModified.Unix(), IsLink: isLink,
		}}, nil
	}

	// Check if directory
	listResp, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &s.bucket, Prefix: aws.String(key + "/"), Delimiter: aws.String("/"), MaxKeys: aws.Int32(1),
	})
	if err == nil && (len(listResp.Contents) > 0 || len(listResp.CommonPrefixes) > 0) {
		return &pb.ContextStatResponse{Ok: true, Info: &pb.FileInfo{Mode: uint32(syscall.S_IFDIR | 0755), IsDir: true}}, nil
	}

	return &pb.ContextStatResponse{Ok: false, Error: "not found"}, nil
}

// ReadDir lists directory contents with full metadata
func (s *ContextService) ReadDir(ctx context.Context, req *pb.ContextReadDirRequest) (*pb.ContextReadDirResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextReadDirResponse{Ok: false, Error: err.Error()}, nil
	}

	prefix := key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	resp, err := s.s3.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: &s.bucket, Prefix: &prefix, Delimiter: aws.String("/"),
	})
	if err != nil {
		return &pb.ContextReadDirResponse{Ok: false, Error: err.Error()}, nil
	}

	entries := make([]*pb.ContextDirEntry, 0, len(resp.Contents)+len(resp.CommonPrefixes))

	// Directories
	for _, p := range resp.CommonPrefixes {
		name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(p.Prefix), prefix), "/")
		if name != "" {
			entries = append(entries, &pb.ContextDirEntry{Name: name, Mode: uint32(syscall.S_IFDIR | 0755), IsDir: true})
		}
	}

	// Files - assume regular files; symlink detection happens on Stat/Readlink
	// This avoids N+1 HeadObject calls which are slow for large directories
	for _, obj := range resp.Contents {
		name := strings.TrimPrefix(aws.ToString(obj.Key), prefix)
		if name != "" && !strings.Contains(name, "/") {
			var mtime int64
			if obj.LastModified != nil {
				mtime = obj.LastModified.Unix()
			}
			entries = append(entries, &pb.ContextDirEntry{
				Name: name, Mode: uint32(syscall.S_IFREG | 0644),
				Size: aws.ToInt64(obj.Size), Mtime: mtime, Etag: aws.ToString(obj.ETag),
			})
		}
	}

	return &pb.ContextReadDirResponse{Ok: true, Entries: entries}, nil
}

// Read reads file content with size limits to prevent OOM
func (s *ContextService) Read(ctx context.Context, req *pb.ContextReadRequest) (*pb.ContextReadResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextReadResponse{Ok: false, Error: err.Error()}, nil
	}

	// Enforce read size limit
	length := req.Length
	if length <= 0 || length > maxReadSize {
		length = maxReadSize
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	input := &s3.GetObjectInput{Bucket: &s.bucket, Key: &key}
	if length > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", req.Offset, req.Offset+length-1))
	} else if req.Offset > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", req.Offset))
	}

	resp, err := s.s3.GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return &pb.ContextReadResponse{Ok: false, Error: "not found"}, nil
		}
		return &pb.ContextReadResponse{Ok: false, Error: err.Error()}, nil
	}
	defer resp.Body.Close()

	// Read with limit to prevent unbounded memory allocation
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxReadSize))
	if err != nil {
		return &pb.ContextReadResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextReadResponse{Ok: true, Data: data}, nil
}

// Write writes file content with size limits
func (s *ContextService) Write(ctx context.Context, req *pb.ContextWriteRequest) (*pb.ContextWriteResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextWriteResponse{Ok: false, Error: err.Error()}, nil
	}

	// Enforce write size limit
	if int64(len(req.Data)) > maxWriteSize {
		return &pb.ContextWriteResponse{Ok: false, Error: "write exceeds maximum size"}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	data := req.Data
	if req.Offset > 0 {
		// Offset writes require read-modify-write; enforce total size limit
		existing, _ := s.readFile(ctx, key)
		end := req.Offset + int64(len(req.Data))
		if end > maxWriteSize {
			return &pb.ContextWriteResponse{Ok: false, Error: "offset write exceeds maximum size"}, nil
		}
		if end > int64(len(existing)) {
			newData := make([]byte, end)
			copy(newData, existing)
			existing = newData
		}
		copy(existing[req.Offset:], req.Data)
		data = existing
	}

	if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		return &pb.ContextWriteResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextWriteResponse{Ok: true, Written: int32(len(req.Data))}, nil
}

// Create creates a new empty file
func (s *ContextService) Create(ctx context.Context, req *pb.ContextCreateRequest) (*pb.ContextCreateResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextCreateResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(nil)}); err != nil {
		return &pb.ContextCreateResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextCreateResponse{Ok: true}, nil
}

// Delete removes a file or directory
func (s *ContextService) Delete(ctx context.Context, req *pb.ContextDeleteRequest) (*pb.ContextDeleteResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextDeleteResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	// Try deleting as file first
	if _, err := s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key}); err != nil {
		if !isNotFound(err) {
			return &pb.ContextDeleteResponse{Ok: false, Error: err.Error()}, nil
		}
	}
	// Also try deleting directory marker (trailing slash)
	dirKey := key + "/"
	if _, err := s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &dirKey}); err != nil {
		if !isNotFound(err) {
			return &pb.ContextDeleteResponse{Ok: false, Error: err.Error()}, nil
		}
	}
	return &pb.ContextDeleteResponse{Ok: true}, nil
}

// Mkdir creates a directory
func (s *ContextService) Mkdir(ctx context.Context, req *pb.ContextMkdirRequest) (*pb.ContextMkdirResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextMkdirResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	dirKey := strings.TrimSuffix(key, "/") + "/"
	if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &dirKey, Body: bytes.NewReader(nil)}); err != nil {
		return &pb.ContextMkdirResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextMkdirResponse{Ok: true}, nil
}

// Rename moves/renames a file or directory (copy then delete)
func (s *ContextService) Rename(ctx context.Context, req *pb.ContextRenameRequest) (*pb.ContextRenameResponse, error) {
	oldKey, err := s.key(ctx, req.OldPath)
	if err != nil {
		return &pb.ContextRenameResponse{Ok: false, Error: err.Error()}, nil
	}
	newKey, err := s.key(ctx, req.NewPath)
	if err != nil {
		return &pb.ContextRenameResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	if _, err := s.s3.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: &s.bucket, Key: &newKey, CopySource: aws.String(s.bucket + "/" + oldKey),
	}); err != nil {
		return &pb.ContextRenameResponse{Ok: false, Error: err.Error()}, nil
	}

	// Delete old key; log but don't fail if delete fails (copy already succeeded)
	if _, err := s.s3.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &oldKey}); err != nil {
		log.Warn().Err(err).Str("key", oldKey).Msg("rename: failed to delete old key after copy")
	}
	return &pb.ContextRenameResponse{Ok: true}, nil
}

// Truncate changes file size
func (s *ContextService) Truncate(ctx context.Context, req *pb.ContextTruncateRequest) (*pb.ContextTruncateResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextTruncateResponse{Ok: false, Error: err.Error()}, nil
	}

	size := req.Size
	if size < 0 {
		size = 0
	}
	if size > maxWriteSize {
		return &pb.ContextTruncateResponse{Ok: false, Error: "truncate size exceeds maximum"}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	existing, _ := s.readFile(ctx, key)
	var data []byte
	if size <= int64(len(existing)) {
		data = existing[:size]
	} else {
		data = make([]byte, size)
		copy(data, existing)
	}

	if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: &s.bucket, Key: &key, Body: bytes.NewReader(data)}); err != nil {
		return &pb.ContextTruncateResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextTruncateResponse{Ok: true}, nil
}

// Symlink creates a symbolic link
func (s *ContextService) Symlink(ctx context.Context, req *pb.ContextSymlinkRequest) (*pb.ContextSymlinkResponse, error) {
	key, err := s.key(ctx, req.LinkPath)
	if err != nil {
		return &pb.ContextSymlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	if _, err := s.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket, Key: &key, Body: bytes.NewReader([]byte(req.Target)),
		Metadata: map[string]string{"symlink-target": req.Target},
	}); err != nil {
		return &pb.ContextSymlinkResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextSymlinkResponse{Ok: true}, nil
}

// Readlink reads a symbolic link target
func (s *ContextService) Readlink(ctx context.Context, req *pb.ContextReadlinkRequest) (*pb.ContextReadlinkResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ContextReadlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	resp, err := s.s3.HeadObject(ctx, &s3.HeadObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		if isNotFound(err) {
			return &pb.ContextReadlinkResponse{Ok: false, Error: "not found"}, nil
		}
		return &pb.ContextReadlinkResponse{Ok: false, Error: err.Error()}, nil
	}
	target, ok := resp.Metadata["symlink-target"]
	if !ok {
		return &pb.ContextReadlinkResponse{Ok: false, Error: "not a symlink"}, nil
	}
	return &pb.ContextReadlinkResponse{Ok: true, Target: target}, nil
}

// ListTree returns flat listing of subtree (for future prefetching)
func (s *ContextService) ListTree(ctx context.Context, req *pb.ListTreeRequest) (*pb.ListTreeResponse, error) {
	key, err := s.key(ctx, req.Path)
	if err != nil {
		return &pb.ListTreeResponse{Ok: false, Error: err.Error()}, nil
	}

	prefix := key
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	maxKeys := req.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	ctx, cancel := s.withTimeout(ctx)
	defer cancel()

	input := &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix, MaxKeys: &maxKeys}
	if req.ContinuationToken != "" {
		input.ContinuationToken = &req.ContinuationToken
	}

	resp, err := s.s3.ListObjectsV2(ctx, input)
	if err != nil {
		return &pb.ListTreeResponse{Ok: false, Error: err.Error()}, nil
	}

	entries := make([]*pb.TreeEntry, 0, len(resp.Contents))
	seenDirs := make(map[string]bool)

	for _, obj := range resp.Contents {
		relPath := strings.TrimPrefix(aws.ToString(obj.Key), prefix)
		if relPath == "" {
			continue
		}

		isDir := strings.HasSuffix(relPath, "/")
		relPath = strings.TrimSuffix(relPath, "/")

		// Register parent directories
		parts := strings.Split(relPath, "/")
		dirPath := ""
		for i := 0; i < len(parts)-1; i++ {
			if dirPath != "" {
				dirPath += "/"
			}
			dirPath += parts[i]
			if !seenDirs[dirPath] {
				seenDirs[dirPath] = true
				entries = append(entries, &pb.TreeEntry{Path: dirPath, Mode: uint32(syscall.S_IFDIR | 0755)})
			}
		}

		var mtime int64
		if obj.LastModified != nil {
			mtime = obj.LastModified.Unix()
		}
		mode := uint32(syscall.S_IFREG | 0644)
		if isDir {
			mode = uint32(syscall.S_IFDIR | 0755)
		}
		entries = append(entries, &pb.TreeEntry{
			Path: relPath, Size: aws.ToInt64(obj.Size), Mtime: mtime, Mode: mode, Etag: aws.ToString(obj.ETag),
		})
	}

	var nextToken string
	if resp.NextContinuationToken != nil {
		nextToken = *resp.NextContinuationToken
	}

	return &pb.ListTreeResponse{Ok: true, Entries: entries, NextToken: nextToken, Truncated: aws.ToBool(resp.IsTruncated)}, nil
}

// helpers

func (s *ContextService) readFile(ctx context.Context, key string) ([]byte, error) {
	resp, err := s.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func isNotFound(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "NoSuchKey") || strings.Contains(err.Error(), "NotFound"))
}
