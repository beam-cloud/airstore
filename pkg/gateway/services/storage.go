package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/singleflight"
)

const (
	storageTimeout  = 30 * time.Second
	maxStorageSize  = 64 << 20 // 64MB
	cacheTTL        = 30 * time.Second
	cacheMaxEntries = 10000
)

// StorageService provides S3-backed file storage with per-workspace buckets
type StorageService struct {
	pb.UnimplementedContextServiceServer
	client *clients.StorageClient
	cache  *metadataCache
}

func NewStorageService(client *clients.StorageClient) (*StorageService, error) {
	if client == nil {
		return nil, fmt.Errorf("storage client required")
	}
	log.Info().Str("prefix", client.Config().DefaultBucketPrefix).Msg("storage service ready")
	return &StorageService{
		client: client,
		cache:  newMetadataCache(cacheTTL, cacheMaxEntries),
	}, nil
}

func (s *StorageService) bucket(ctx context.Context) (string, error) {
	rc := auth.AuthInfoFromContext(ctx)
	if rc == nil {
		return "", fmt.Errorf("no auth context")
	}
	wsExt := auth.WorkspaceExtId(ctx)
	if wsExt == "" {
		if rc.IsClusterAdmin() {
			return s.client.WorkspaceBucketName("_gateway"), nil
		}
		return "", fmt.Errorf("no workspace")
	}
	return s.client.WorkspaceBucketName(wsExt), nil
}

func (s *StorageService) key(path string) string {
	return strings.TrimPrefix(path, "/")
}

func (s *StorageService) timeout(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, storageTimeout)
}

// Stat returns file/directory attributes
func (s *StorageService) Stat(ctx context.Context, req *pb.ContextStatRequest) (*pb.ContextStatResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return statErr(err), nil
	}

	if req.Path == "" || req.Path == "/" {
		return statOk(dirInfo()), nil
	}

	key := s.key(req.Path)
	cacheKey := s.cache.key(bucket, key, "stat")

	if info, ok := s.cache.getInfo(cacheKey); ok {
		return statOk(info), nil
	}

	result, err := s.cache.doOnce(cacheKey, func() (any, error) {
		ctx, cancel := s.timeout(ctx)
		defer cancel()

		// Try as file first
		if resp, err := s.client.S3Client().HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: &bucket, Key: &key,
		}); err == nil {
			info := fileInfo(resp)
			s.cache.setInfo(cacheKey, info)
			return info, nil
		}

		// Check if directory
		prefix := key + "/"
		list, err := s.client.S3Client().ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &bucket, Prefix: &prefix, Delimiter: aws.String("/"), MaxKeys: aws.Int32(1),
		})
		if err == nil && (len(list.Contents) > 0 || len(list.CommonPrefixes) > 0) {
			info := dirInfo()
			s.cache.setInfo(cacheKey, info)
			return info, nil
		}

		return nil, fmt.Errorf("not found")
	})

	if err != nil {
		return statErr(err), nil
	}
	return statOk(result.(*pb.FileInfo)), nil
}

// ReadDir lists directory contents
func (s *StorageService) ReadDir(ctx context.Context, req *pb.ContextReadDirRequest) (*pb.ContextReadDirResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextReadDirResponse{Ok: false, Error: err.Error()}, nil
	}

	key := s.key(req.Path)
	prefix := key
	if prefix != "" {
		prefix += "/"
	}

	cacheKey := s.cache.key(bucket, key, "readdir")
	if entries, ok := s.cache.getEntries(cacheKey); ok {
		return &pb.ContextReadDirResponse{Ok: true, Entries: entries}, nil
	}

	result, err := s.cache.doOnce(cacheKey, func() (any, error) {
		ctx, cancel := s.timeout(ctx)
		defer cancel()

		resp, err := s.client.S3Client().ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: &bucket, Prefix: &prefix, Delimiter: aws.String("/"),
		})
		if err != nil {
			return nil, err
		}

		entries := make([]*pb.ContextDirEntry, 0, len(resp.Contents)+len(resp.CommonPrefixes))

		for _, p := range resp.CommonPrefixes {
			name := strings.TrimSuffix(strings.TrimPrefix(aws.ToString(p.Prefix), prefix), "/")
			if name == "" || isHiddenFile(name) {
				continue
			}
			entries = append(entries, &pb.ContextDirEntry{
				Name: name, Mode: uint32(syscall.S_IFDIR | 0755), IsDir: true,
			})
		}

		for _, obj := range resp.Contents {
			name := strings.TrimPrefix(aws.ToString(obj.Key), prefix)
			if name == "" || strings.Contains(name, "/") || isHiddenFile(name) {
				continue
			}
			var mtime int64
			if obj.LastModified != nil {
				mtime = obj.LastModified.Unix()
			}
			entries = append(entries, &pb.ContextDirEntry{
				Name: name, Mode: uint32(syscall.S_IFREG | 0644),
				Size: aws.ToInt64(obj.Size), Mtime: mtime, Etag: aws.ToString(obj.ETag),
			})
		}

		s.cache.setEntries(cacheKey, entries)
		return entries, nil
	})

	if err != nil {
		return &pb.ContextReadDirResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextReadDirResponse{Ok: true, Entries: result.([]*pb.ContextDirEntry)}, nil
}

// Read reads file content
func (s *StorageService) Read(ctx context.Context, req *pb.ContextReadRequest) (*pb.ContextReadResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextReadResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	length := req.Length
	if length <= 0 || length > maxStorageSize {
		length = maxStorageSize
	}

	input := &s3.GetObjectInput{Bucket: &bucket, Key: aws.String(s.key(req.Path))}
	if length > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-%d", req.Offset, req.Offset+length-1))
	} else if req.Offset > 0 {
		input.Range = aws.String(fmt.Sprintf("bytes=%d-", req.Offset))
	}

	resp, err := s.client.S3Client().GetObject(ctx, input)
	if err != nil {
		if isNotFound(err) {
			return &pb.ContextReadResponse{Ok: false, Error: "not found"}, nil
		}
		return &pb.ContextReadResponse{Ok: false, Error: err.Error()}, nil
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxStorageSize))
	if err != nil {
		return &pb.ContextReadResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextReadResponse{Ok: true, Data: data}, nil
}

// Write writes file content
func (s *StorageService) Write(ctx context.Context, req *pb.ContextWriteRequest) (*pb.ContextWriteResponse, error) {
	if int64(len(req.Data)) > maxStorageSize {
		return &pb.ContextWriteResponse{Ok: false, Error: "data too large"}, nil
	}

	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextWriteResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	key := s.key(req.Path)
	data := req.Data

	if req.Offset > 0 {
		existing, _ := s.readFile(ctx, bucket, key)
		end := req.Offset + int64(len(req.Data))
		if end > maxStorageSize {
			return &pb.ContextWriteResponse{Ok: false, Error: "data too large"}, nil
		}
		if end > int64(len(existing)) {
			newData := make([]byte, end)
			copy(newData, existing)
			existing = newData
		}
		copy(existing[req.Offset:], req.Data)
		data = existing
	}

	_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
	})
	if err != nil {
		return &pb.ContextWriteResponse{Ok: false, Error: err.Error()}, nil
	}

	s.invalidate(bucket, key)
	return &pb.ContextWriteResponse{Ok: true, Written: int32(len(req.Data))}, nil
}

// Create creates an empty file
func (s *StorageService) Create(ctx context.Context, req *pb.ContextCreateRequest) (*pb.ContextCreateResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextCreateResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	key := s.key(req.Path)
	_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(nil),
	})
	if err != nil {
		return &pb.ContextCreateResponse{Ok: false, Error: err.Error()}, nil
	}

	s.invalidate(bucket, key)
	return &pb.ContextCreateResponse{Ok: true}, nil
}

func deleteErr(msg string) *pb.ContextDeleteResponse {
	return &pb.ContextDeleteResponse{Ok: false, Error: msg}
}

// deletePrefix removes all objects under a prefix in batches of 1000
func (s *StorageService) deletePrefix(ctx context.Context, bucket, prefix string) error {
	s3c := s.client.S3Client()
	var token *string

	for {
		listCtx, cancel := s.timeout(ctx)
		input := &s3.ListObjectsV2Input{
			Bucket:            &bucket,
			Prefix:            &prefix,
			MaxKeys:           aws.Int32(1000),
			ContinuationToken: token,
		}

		resp, err := s3c.ListObjectsV2(listCtx, input)
		cancel()
		if err != nil {
			return err
		}

		if len(resp.Contents) == 0 {
			if !aws.ToBool(resp.IsTruncated) {
				return nil
			}
			token = resp.NextContinuationToken
			continue
		}

		objects := make([]s3types.ObjectIdentifier, len(resp.Contents))
		for i, obj := range resp.Contents {
			objects[i] = s3types.ObjectIdentifier{Key: obj.Key}
		}

		delCtx, cancel := s.timeout(ctx)
		_, err = s3c.DeleteObjects(delCtx, &s3.DeleteObjectsInput{
			Bucket: &bucket,
			Delete: &s3types.Delete{Objects: objects, Quiet: aws.Bool(true)},
		})
		cancel()
		if err != nil {
			return err
		}

		for _, obj := range resp.Contents {
			s.invalidate(bucket, aws.ToString(obj.Key))
		}

		if !aws.ToBool(resp.IsTruncated) {
			return nil
		}
		token = resp.NextContinuationToken
	}
}

// Delete removes a file or directory
func (s *StorageService) Delete(ctx context.Context, req *pb.ContextDeleteRequest) (*pb.ContextDeleteResponse, error) {
	path := strings.TrimPrefix(req.Path, "/")
	if path != "" && !strings.Contains(path, "/") && types.IsReservedFolder(path) {
		return deleteErr("cannot delete reserved folder"), nil
	}

	bucket, err := s.bucket(ctx)
	if err != nil {
		return deleteErr(err.Error()), nil
	}

	key := s.key(req.Path)

	if req.Recursive {
		prefix := key + "/"
		if key == "" {
			prefix = ""
		}
		if err := s.deletePrefix(ctx, bucket, prefix); err != nil {
			return deleteErr(err.Error()), nil
		}
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	s3c := s.client.S3Client()
	s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: &key})
	s3c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: aws.String(key + "/")})

	s.invalidate(bucket, key)
	return &pb.ContextDeleteResponse{Ok: true}, nil
}

// Mkdir creates a directory
func (s *StorageService) Mkdir(ctx context.Context, req *pb.ContextMkdirRequest) (*pb.ContextMkdirResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextMkdirResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	key := s.key(req.Path)
	dirKey := strings.TrimSuffix(key, "/") + "/"

	_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &dirKey, Body: bytes.NewReader(nil),
	})
	if err != nil {
		return &pb.ContextMkdirResponse{Ok: false, Error: err.Error()}, nil
	}

	s.invalidate(bucket, key)
	return &pb.ContextMkdirResponse{Ok: true}, nil
}

// Rename moves/renames a file or directory
func (s *StorageService) Rename(ctx context.Context, req *pb.ContextRenameRequest) (*pb.ContextRenameResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextRenameResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	oldKey, newKey := s.key(req.OldPath), s.key(req.NewPath)

	_, err = s.client.S3Client().CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: &bucket, Key: &newKey, CopySource: aws.String(bucket + "/" + oldKey),
	})
	if err != nil {
		return &pb.ContextRenameResponse{Ok: false, Error: err.Error()}, nil
	}

	if _, err := s.client.S3Client().DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket, Key: &oldKey,
	}); err != nil {
		log.Warn().Err(err).Str("key", oldKey).Msg("rename: delete old failed")
	}

	s.invalidate(bucket, oldKey)
	s.invalidate(bucket, newKey)
	return &pb.ContextRenameResponse{Ok: true}, nil
}

// Truncate changes file size
func (s *StorageService) Truncate(ctx context.Context, req *pb.ContextTruncateRequest) (*pb.ContextTruncateResponse, error) {
	if req.Size > maxStorageSize {
		return &pb.ContextTruncateResponse{Ok: false, Error: "size too large"}, nil
	}

	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextTruncateResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	key := s.key(req.Path)
	existing, _ := s.readFile(ctx, bucket, key)

	size := req.Size
	if size < 0 {
		size = 0
	}

	var data []byte
	if size <= int64(len(existing)) {
		data = existing[:size]
	} else {
		data = make([]byte, size)
		copy(data, existing)
	}

	_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data),
	})
	if err != nil {
		return &pb.ContextTruncateResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextTruncateResponse{Ok: true}, nil
}

// Symlink creates a symbolic link
func (s *StorageService) Symlink(ctx context.Context, req *pb.ContextSymlinkRequest) (*pb.ContextSymlinkResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextSymlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket:   &bucket,
		Key:      aws.String(s.key(req.LinkPath)),
		Body:     bytes.NewReader([]byte(req.Target)),
		Metadata: map[string]string{"symlink-target": req.Target},
	})
	if err != nil {
		return &pb.ContextSymlinkResponse{Ok: false, Error: err.Error()}, nil
	}
	return &pb.ContextSymlinkResponse{Ok: true}, nil
}

// Readlink reads symbolic link target
func (s *StorageService) Readlink(ctx context.Context, req *pb.ContextReadlinkRequest) (*pb.ContextReadlinkResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextReadlinkResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	resp, err := s.client.S3Client().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket, Key: aws.String(s.key(req.Path)),
	})
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

// ListTree returns flat listing of subtree for prefetching
func (s *StorageService) ListTree(ctx context.Context, req *pb.ListTreeRequest) (*pb.ListTreeResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ListTreeResponse{Ok: false, Error: err.Error()}, nil
	}

	ctx, cancel := s.timeout(ctx)
	defer cancel()

	key := s.key(req.Path)
	prefix := key
	if prefix != "" {
		prefix += "/"
	}

	maxKeys := req.MaxKeys
	if maxKeys <= 0 {
		maxKeys = 1000
	}

	input := &s3.ListObjectsV2Input{Bucket: &bucket, Prefix: &prefix, MaxKeys: &maxKeys}
	if req.ContinuationToken != "" {
		input.ContinuationToken = &req.ContinuationToken
	}

	resp, err := s.client.S3Client().ListObjectsV2(ctx, input)
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

		// Synthesize parent directories
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

// Presigned URL operations

// GetUploadURL generates a presigned PUT URL for uploading a file
func (s *StorageService) GetUploadURL(ctx context.Context, path, contentType string) (string, string, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return "", "", err
	}

	key := s.key(path)
	if key == "" {
		return "", "", fmt.Errorf("path required")
	}

	url, err := s.client.PresignUpload(ctx, bucket, key, contentType, clients.PresignUploadExpiry)
	if err != nil {
		return "", "", err
	}

	return url, key, nil
}

// GetDownloadURL generates a presigned GET URL for downloading a file
func (s *StorageService) GetDownloadURL(ctx context.Context, path string) (string, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return "", err
	}

	key := s.key(path)
	if key == "" {
		return "", fmt.Errorf("path required")
	}

	return s.client.PresignDownload(ctx, bucket, key, clients.PresignDownloadExpiry)
}

// NotifyUploadComplete invalidates caches after a file upload
func (s *StorageService) NotifyUploadComplete(ctx context.Context, path string) error {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return err
	}

	key := s.key(path)
	s.invalidate(bucket, key)
	return nil
}

// Helpers

func (s *StorageService) readFile(ctx context.Context, bucket, key string) ([]byte, error) {
	resp, err := s.client.S3Client().GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func (s *StorageService) invalidate(bucket, key string) {
	s.cache.invalidate(bucket, key)
	if idx := strings.LastIndex(key, "/"); idx > 0 {
		s.cache.invalidate(bucket, key[:idx])
	} else {
		s.cache.invalidate(bucket, "")
	}
}

func dirInfo() *pb.FileInfo {
	return &pb.FileInfo{Mode: uint32(syscall.S_IFDIR | 0755), IsDir: true}
}

func fileInfo(resp *s3.HeadObjectOutput) *pb.FileInfo {
	_, isLink := resp.Metadata["symlink-target"]
	mode := uint32(syscall.S_IFREG | 0644)
	if isLink {
		mode = uint32(syscall.S_IFLNK | 0777)
	}
	return &pb.FileInfo{
		Size: aws.ToInt64(resp.ContentLength), Mode: mode,
		Mtime: resp.LastModified.Unix(), IsLink: isLink,
	}
}

func statOk(info *pb.FileInfo) *pb.ContextStatResponse  { return &pb.ContextStatResponse{Ok: true, Info: info} }
func statErr(err error) *pb.ContextStatResponse         { return &pb.ContextStatResponse{Ok: false, Error: err.Error()} }

// isHiddenFile returns true for files that should be hidden from listings
func isHiddenFile(name string) bool {
	return strings.HasPrefix(name, "._") || name == ".DS_Store"
}

// Metadata cache with singleflight for request coalescing

type metadataCache struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
	ttl     time.Duration
	maxSize int
	group   singleflight.Group
}

type cacheEntry struct {
	info    *pb.FileInfo
	entries []*pb.ContextDirEntry
	expires time.Time
}

func newMetadataCache(ttl time.Duration, maxSize int) *metadataCache {
	c := &metadataCache{
		entries: make(map[string]*cacheEntry),
		ttl:     ttl,
		maxSize: maxSize,
	}
	go c.cleanup()
	return c
}

func (c *metadataCache) key(bucket, path, op string) string {
	return bucket + ":" + path + ":" + op
}

func (c *metadataCache) getInfo(key string) (*pb.FileInfo, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.info, true
}

func (c *metadataCache) getEntries(key string) ([]*pb.ContextDirEntry, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e.entries, true
}

func (c *metadataCache) setInfo(key string, info *pb.FileInfo) {
	c.set(key, &cacheEntry{info: info, expires: time.Now().Add(c.ttl)})
}

func (c *metadataCache) setEntries(key string, entries []*pb.ContextDirEntry) {
	c.set(key, &cacheEntry{entries: entries, expires: time.Now().Add(c.ttl)})
}

func (c *metadataCache) set(key string, entry *cacheEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= c.maxSize {
		n := c.maxSize / 10
		for k := range c.entries {
			delete(c.entries, k)
			if n--; n <= 0 {
				break
			}
		}
	}
	c.entries[key] = entry
}

func (c *metadataCache) invalidate(bucket, path string) {
	prefix := bucket + ":" + path + ":"
	c.mu.Lock()
	defer c.mu.Unlock()
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			delete(c.entries, k)
		}
	}
}

func (c *metadataCache) doOnce(key string, fn func() (any, error)) (any, error) {
	v, err, _ := c.group.Do(key, fn)
	return v, err
}

func (c *metadataCache) cleanup() {
	for range time.Tick(time.Minute) {
		c.mu.Lock()
		now := time.Now()
		for k, e := range c.entries {
			if now.After(e.expires) {
				delete(c.entries, k)
			}
		}
		c.mu.Unlock()
	}
}
