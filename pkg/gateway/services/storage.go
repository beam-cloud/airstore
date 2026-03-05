package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
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

	uploadReadyProbeWindow         = 2 * time.Second
	uploadReadyProbeTimeoutPerHead = 750 * time.Millisecond
	uploadReadyProbeInitialBackoff = 50 * time.Millisecond
	uploadReadyProbeMaxBackoff     = 400 * time.Millisecond

	storageModeMetadataKey = "airstore-mode"
	defaultStorageFileMode = uint32(syscall.S_IFREG | 0644)
	defaultStorageDirMode  = uint32(syscall.S_IFDIR | 0755)
)

// StorageService provides S3-backed file storage with per-workspace buckets
type StorageService struct {
	pb.UnimplementedContextServiceServer
	client     *clients.StorageClient
	cache      *metadataCache
	eventBus   *common.EventBus
	hookStream common.EventEmitter
	recorder   instrumentation.EventRecorder
}

// SetEventRecorder sets the product analytics event recorder.
func (s *StorageService) SetEventRecorder(r instrumentation.EventRecorder) {
	s.recorder = r
}

func NewStorageService(client *clients.StorageClient, eventBus *common.EventBus) (*StorageService, error) {
	if client == nil {
		return nil, fmt.Errorf("storage client required")
	}

	s := &StorageService{
		client:   client,
		cache:    newMetadataCache(cacheTTL, cacheMaxEntries),
		eventBus: eventBus,
	}

	// Subscribe to cache invalidations from other replicas
	if eventBus != nil {
		eventBus.On(common.EventCacheInvalidate, func(e common.Event) {
			if key, ok := e.Data["key"].(string); ok {
				s.cache.invalidateByKey(key)
			}
		})
	}

	log.Info().Str("prefix", client.Config().DefaultBucketPrefix).Msg("storage service ready")
	return s, nil
}

// SetHookStream sets the event emitter for hook event emission.
func (s *StorageService) SetHookStream(emitter common.EventEmitter) {
	s.hookStream = emitter
}

// emitHookEvent sends a filesystem event to the hook event stream.
func (s *StorageService) emitHookEvent(ctx context.Context, eventType string, path string) {
	s.emitHookEventWithData(ctx, eventType, path, nil)
}

// emitHookEventWithData sends a filesystem event plus optional metadata to the hook stream.
func (s *StorageService) emitHookEventWithData(ctx context.Context, eventType string, path string, meta map[string]any) {
	if s.hookStream == nil {
		return
	}

	wsId := auth.WorkspaceId(ctx)
	if wsId == 0 {
		return
	}

	path = hooks.NormalizePath(path)
	if types.IsHiddenDotPath(path) {
		return
	}

	payload := map[string]any{
		"event":            eventType,
		"workspace_id":     fmt.Sprintf("%d", wsId),
		"workspace_ext_id": auth.WorkspaceExtId(ctx),
		"path":             path,
	}
	for key, value := range meta {
		if value != nil {
			payload[key] = value
		}
	}

	logEvent := log.Debug().
		Str("event", eventType).
		Str("path", path).
		Uint("workspace", wsId)
	if rawOldPath, ok := payload["old_path"]; ok {
		if oldPath, ok := rawOldPath.(string); ok && strings.TrimSpace(oldPath) != "" {
			logEvent = logEvent.Str("old_path", strings.TrimSpace(oldPath))
		}
	}
	if rawNewPath, ok := payload["new_path"]; ok {
		if newPath, ok := rawNewPath.(string); ok && strings.TrimSpace(newPath) != "" {
			logEvent = logEvent.Str("new_path", strings.TrimSpace(newPath))
		}
	}
	logEvent.Msg("hook event emitted")

	s.hookStream.Emit(ctx, payload)
}

// emitHookMoveEvents emits source+destination hook events for a move/rename.
func (s *StorageService) emitHookMoveEvents(ctx context.Context, oldPath, newPath string) {
	oldPath = hooks.NormalizePath(oldPath)
	newPath = hooks.NormalizePath(newPath)
	if oldPath == "" || newPath == "" || oldPath == newPath {
		return
	}

	moveOpID := fmt.Sprintf("mv-%d", time.Now().UnixNano())
	meta := map[string]any{
		"old_path":   oldPath,
		"new_path":   newPath,
		"move_op_id": moveOpID,
	}

	s.emitHookEventWithData(ctx, hooks.EventFsDelete, oldPath, meta)
	s.emitHookEventWithData(ctx, hooks.EventFsWrite, newPath, meta)
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
			info := s.statDirInfo(ctx, bucket, key)
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
			prefixKey := aws.ToString(p.Prefix)
			name := strings.TrimSuffix(strings.TrimPrefix(prefixKey, prefix), "/")
			if name == "" || isHiddenFile(name) {
				continue
			}
			mode, mtime := s.statDirModeMtime(ctx, bucket, prefixKey)
			entries = append(entries, &pb.ContextDirEntry{
				Name: name, Mode: mode, IsDir: true, Mtime: mtime,
			})
		}

		for _, obj := range resp.Contents {
			objKey := aws.ToString(obj.Key)
			name := strings.TrimPrefix(objKey, prefix)
			if name == "" || strings.Contains(name, "/") || isHiddenFile(name) {
				continue
			}
			var mtime int64
			if obj.LastModified != nil {
				mtime = obj.LastModified.Unix()
			}
			mode, statMtime := s.statFileModeMtime(ctx, bucket, objKey)
			if statMtime > 0 {
				mtime = statMtime
			}
			entries = append(entries, &pb.ContextDirEntry{
				Name: name, Mode: mode,
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
		if isInvalidRange(err) {
			return &pb.ContextReadResponse{Ok: true, Data: nil}, nil
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
	metadata := s.objectMetadataForWrite(ctx, bucket, key)

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
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), Metadata: metadata,
	})
	if err != nil {
		return &pb.ContextWriteResponse{Ok: false, Error: err.Error()}, nil
	}

	s.invalidate(bucket, key)
	s.emitHookEvent(ctx, hooks.EventFsWrite, req.Path)
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
	mode := sanitizeMode(req.Mode, syscall.S_IFREG, 0644)

	headResp, headErr := s.client.S3Client().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket, Key: &key,
	})
	if headErr == nil {
		metadata := withModeMetadata(headResp.Metadata, mode)

		_, err = s.client.S3Client().CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            &bucket,
			Key:               &key,
			CopySource:        aws.String(bucket + "/" + key),
			Metadata:          metadata,
			MetadataDirective: s3types.MetadataDirectiveReplace,
		})
		if err != nil {
			return &pb.ContextCreateResponse{Ok: false, Error: err.Error()}, nil
		}
	} else {
		if !isNotFound(headErr) {
			return &pb.ContextCreateResponse{Ok: false, Error: headErr.Error()}, nil
		}

		_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
			Bucket:   &bucket,
			Key:      &key,
			Body:     bytes.NewReader(nil),
			Metadata: withModeMetadata(nil, mode),
		})
		if err != nil {
			return &pb.ContextCreateResponse{Ok: false, Error: err.Error()}, nil
		}
	}

	s.invalidate(bucket, key)

	// Don't emit fs.create here -- this creates an empty file.
	// Content arrives via Write() which emits fs.write (debounced).
	// on_create hooks fire on debounced writes, not empty creates.
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
	s.emitHookEvent(ctx, hooks.EventFsDelete, req.Path)

	if s.recorder != nil {
		s.recorder.Record(ctx, instrumentation.NewEvent("filesystem.operation", map[string]any{
			"operation":    "delete",
			"path":         req.Path,
			"recursive":    req.Recursive,
			"workspace_id": auth.WorkspaceExtId(ctx),
		}))
	}

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
	mode := sanitizeMode(req.Mode, syscall.S_IFDIR, 0755)

	_, err = s.client.S3Client().PutObject(ctx, &s3.PutObjectInput{
		Bucket:   &bucket,
		Key:      &dirKey,
		Body:     bytes.NewReader(nil),
		Metadata: withModeMetadata(nil, mode),
	})
	if err != nil {
		return &pb.ContextMkdirResponse{Ok: false, Error: err.Error()}, nil
	}

	s.invalidate(bucket, key)

	if s.recorder != nil {
		s.recorder.Record(ctx, instrumentation.NewEvent("filesystem.operation", map[string]any{
			"operation":    "mkdir",
			"path":         req.Path,
			"workspace_id": auth.WorkspaceExtId(ctx),
		}))
	}

	return &pb.ContextMkdirResponse{Ok: true}, nil
}

// Rename moves/renames a file or directory
func (s *StorageService) Rename(ctx context.Context, req *pb.ContextRenameRequest) (*pb.ContextRenameResponse, error) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return &pb.ContextRenameResponse{Ok: false, Error: err.Error()}, nil
	}

	oldKey, newKey := s.key(req.OldPath), s.key(req.NewPath)

	// First try as a single file
	copied, err := s.renameSingleObject(ctx, bucket, oldKey, newKey)
	if err != nil {
		// If single file copy failed, try as a directory (prefix rename)
		dirErr := s.renamePrefix(ctx, bucket, oldKey, newKey)
		if dirErr != nil {
			return &pb.ContextRenameResponse{Ok: false, Error: fmt.Sprintf("rename failed: %v", err)}, nil
		}
		s.emitHookMoveEvents(ctx, req.OldPath, req.NewPath)
		return &pb.ContextRenameResponse{Ok: true}, nil
	}

	if copied {
		s.invalidate(bucket, oldKey)
		s.invalidate(bucket, newKey)
	}
	s.emitHookMoveEvents(ctx, req.OldPath, req.NewPath)
	return &pb.ContextRenameResponse{Ok: true}, nil
}

// renameSingleObject copies a single S3 object and deletes the original.
// Returns (true, nil) on success, (false, err) if the object doesn't exist or copy fails.
func (s *StorageService) renameSingleObject(ctx context.Context, bucket, oldKey, newKey string) (bool, error) {
	ctx, cancel := s.timeout(ctx)
	defer cancel()

	_, err := s.client.S3Client().CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:            &bucket,
		Key:               &newKey,
		CopySource:        aws.String(bucket + "/" + oldKey),
		MetadataDirective: s3types.MetadataDirectiveCopy,
	})
	if err != nil {
		return false, err
	}

	if _, err := s.client.S3Client().DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket, Key: &oldKey,
	}); err != nil {
		log.Warn().Err(err).Str("key", oldKey).Msg("rename: delete old failed")
	}

	return true, nil
}

// renamePrefix renames all objects under an S3 prefix (directory rename).
func (s *StorageService) renamePrefix(ctx context.Context, bucket, oldPrefix, newPrefix string) error {
	// Ensure prefixes end with /
	if oldPrefix != "" && oldPrefix[len(oldPrefix)-1] != '/' {
		oldPrefix += "/"
	}
	if newPrefix != "" && newPrefix[len(newPrefix)-1] != '/' {
		newPrefix += "/"
	}

	listCtx, listCancel := s.timeout(ctx)
	defer listCancel()

	resp, err := s.client.S3Client().ListObjectsV2(listCtx, &s3.ListObjectsV2Input{
		Bucket: &bucket, Prefix: &oldPrefix,
	})
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}
	if len(resp.Contents) == 0 {
		return fmt.Errorf("no objects found under prefix %q", oldPrefix)
	}

	for _, obj := range resp.Contents {
		if obj.Key == nil {
			continue
		}
		oldObjKey := *obj.Key
		suffix := strings.TrimPrefix(oldObjKey, oldPrefix)
		newObjKey := newPrefix + suffix

		copyCtx, copyCancel := s.timeout(ctx)
		_, copyErr := s.client.S3Client().CopyObject(copyCtx, &s3.CopyObjectInput{
			Bucket:            &bucket,
			Key:               &newObjKey,
			CopySource:        aws.String(bucket + "/" + oldObjKey),
			MetadataDirective: s3types.MetadataDirectiveCopy,
		})
		copyCancel()
		if copyErr != nil {
			return fmt.Errorf("copy %s -> %s: %w", oldObjKey, newObjKey, copyErr)
		}

		delCtx, delCancel := s.timeout(ctx)
		_, delErr := s.client.S3Client().DeleteObject(delCtx, &s3.DeleteObjectInput{
			Bucket: &bucket, Key: &oldObjKey,
		})
		delCancel()
		if delErr != nil {
			log.Warn().Err(delErr).Str("key", oldObjKey).Msg("rename prefix: delete old failed")
		}

		s.invalidate(bucket, oldObjKey)
		s.invalidate(bucket, newObjKey)
	}

	return nil
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
	metadata := s.objectMetadataForWrite(ctx, bucket, key)
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
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(data), Metadata: metadata,
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
		Metadata: withModeMetadata(map[string]string{"symlink-target": req.Target}, syscall.S_IFLNK|0777),
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
				mode, _ := s.statDirModeMtime(ctx, bucket, storageKeyForChild(prefix, dirPath)+"/")
				entries = append(entries, &pb.TreeEntry{Path: dirPath, Mode: mode})
			}
		}

		var mtime int64
		if obj.LastModified != nil {
			mtime = obj.LastModified.Unix()
		}
		mode := defaultStorageFileMode
		if isDir {
			mode, _ = s.statDirModeMtime(ctx, bucket, aws.ToString(obj.Key))
		} else {
			mode, _ = s.statFileModeMtime(ctx, bucket, aws.ToString(obj.Key))
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
	s.waitForUploadReadiness(ctx, bucket, key)
	s.invalidate(bucket, key)
	s.emitHookEvent(ctx, hooks.EventFsCreate, path)
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
	// Invalidate locally (file + parent listing scope).
	keys := []string{key}
	if idx := strings.LastIndex(key, "/"); idx > 0 {
		keys = append(keys, key[:idx])
	} else {
		keys = append(keys, "")
	}
	for _, k := range keys {
		s.cache.invalidate(bucket, k)
	}

	// Broadcast to other replicas
	if s.eventBus != nil {
		sent := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			cacheKey := bucket + ":" + k
			if _, ok := sent[cacheKey]; ok {
				continue
			}
			sent[cacheKey] = struct{}{}
			s.eventBus.Emit(common.Event{
				Type: common.EventCacheInvalidate,
				Data: map[string]any{"key": cacheKey},
			})
		}
	}
}

func (s *StorageService) waitForUploadReadiness(ctx context.Context, bucket, key string) {
	if bucket == "" || key == "" || s.client == nil || s.client.S3Client() == nil {
		return
	}

	deadline := time.Now().Add(uploadReadyProbeWindow)
	backoff := uploadReadyProbeInitialBackoff
	for {
		probeCtx, cancel := context.WithTimeout(ctx, uploadReadyProbeTimeoutPerHead)
		_, err := s.client.S3Client().HeadObject(probeCtx, &s3.HeadObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
		cancel()

		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		if time.Now().After(deadline) {
			log.Debug().
				Err(err).
				Str("bucket", bucket).
				Str("key", key).
				Msg("upload-complete readiness probe timed out; continuing")
			return
		}

		sleep := backoff
		remaining := time.Until(deadline)
		if sleep > remaining {
			sleep = remaining
		}
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		if backoff < uploadReadyProbeMaxBackoff {
			backoff *= 2
			if backoff > uploadReadyProbeMaxBackoff {
				backoff = uploadReadyProbeMaxBackoff
			}
		}
	}
}

// InvalidateCache clears the cache for a path (used when client requests fresh data)
func (s *StorageService) InvalidateCache(ctx context.Context, path string) {
	bucket, err := s.bucket(ctx)
	if err != nil {
		return
	}
	key := s.key(path)
	s.invalidate(bucket, key)
}

func dirInfo() *pb.FileInfo {
	return &pb.FileInfo{Mode: defaultStorageDirMode, IsDir: true}
}

func fileInfo(resp *s3.HeadObjectOutput) *pb.FileInfo {
	_, isLink := resp.Metadata["symlink-target"]
	mode := modeFromMetadata(resp.Metadata, defaultStorageFileMode)
	if isLink {
		mode = withFileType(mode, syscall.S_IFLNK)
	}
	var mtime int64
	if resp.LastModified != nil {
		mtime = resp.LastModified.Unix()
	}
	return &pb.FileInfo{
		Size: aws.ToInt64(resp.ContentLength), Mode: mode,
		Mtime: mtime, IsLink: isLink,
	}
}

func statOk(info *pb.FileInfo) *pb.ContextStatResponse {
	return &pb.ContextStatResponse{Ok: true, Info: info}
}
func statErr(err error) *pb.ContextStatResponse {
	return &pb.ContextStatResponse{Ok: false, Error: err.Error()}
}

func (s *StorageService) statDirInfo(ctx context.Context, bucket, key string) *pb.FileInfo {
	markerKey := strings.TrimSuffix(key, "/") + "/"
	info := dirInfo()
	if markerKey == "/" {
		return info
	}

	resp, err := s.client.S3Client().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &markerKey,
	})
	if err != nil {
		return info
	}

	info.Mode = withFileType(modeFromMetadata(resp.Metadata, defaultStorageDirMode), syscall.S_IFDIR)
	if resp.LastModified != nil {
		info.Mtime = resp.LastModified.Unix()
	}
	return info
}

func (s *StorageService) statDirModeMtime(ctx context.Context, bucket, markerKey string) (uint32, int64) {
	statPath := strings.TrimSuffix(markerKey, "/")
	cacheKey := s.cache.key(bucket, statPath, "stat")
	if info, ok := s.cache.getInfo(cacheKey); ok && info.IsDir {
		return info.Mode, info.Mtime
	}

	info := s.statDirInfo(ctx, bucket, statPath)
	s.cache.setInfo(cacheKey, info)
	return info.Mode, info.Mtime
}

func (s *StorageService) statFileModeMtime(ctx context.Context, bucket, key string) (uint32, int64) {
	cacheKey := s.cache.key(bucket, key, "stat")
	if info, ok := s.cache.getInfo(cacheKey); ok && !info.IsDir {
		return info.Mode, info.Mtime
	}

	resp, err := s.client.S3Client().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return defaultStorageFileMode, 0
	}

	info := fileInfo(resp)
	s.cache.setInfo(cacheKey, info)
	return info.Mode, info.Mtime
}

func (s *StorageService) objectMetadataForWrite(ctx context.Context, bucket, key string) map[string]string {
	metadata := map[string]string{}
	if resp, err := s.client.S3Client().HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &key,
	}); err == nil {
		metadata = cloneObjectMetadata(resp.Metadata)
	}

	mode := modeFromMetadata(metadata, defaultStorageFileMode)
	if mode&syscall.S_IFMT == 0 {
		mode = withFileType(mode, syscall.S_IFREG)
	}
	return withModeMetadata(metadata, mode)
}

func storageKeyForChild(prefix, relPath string) string {
	base := strings.TrimSuffix(prefix, "/")
	rel := strings.TrimPrefix(relPath, "/")
	if base == "" {
		return rel
	}
	if rel == "" {
		return base
	}
	return base + "/" + rel
}

func sanitizeMode(mode uint32, fileType uint32, defaultPerm uint32) uint32 {
	if mode == 0 {
		return fileType | defaultPerm
	}
	if requestedType := mode & syscall.S_IFMT; requestedType != 0 {
		fileType = requestedType
	}
	return fileType | (mode & 07777)
}

func withFileType(mode uint32, fileType uint32) uint32 {
	return fileType | (mode & 07777)
}

func cloneObjectMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return map[string]string{}
	}
	copyMap := make(map[string]string, len(metadata))
	for key, value := range metadata {
		copyMap[key] = value
	}
	return copyMap
}

func withModeMetadata(metadata map[string]string, mode uint32) map[string]string {
	out := cloneObjectMetadata(metadata)
	out[storageModeMetadataKey] = encodeModeMetadata(mode)
	return out
}

func encodeModeMetadata(mode uint32) string {
	return strconv.FormatUint(uint64(mode), 8)
}

func decodeModeMetadata(value string) (uint32, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if mode, err := strconv.ParseUint(value, 8, 32); err == nil {
		return uint32(mode), true
	}
	if mode, err := strconv.ParseUint(value, 10, 32); err == nil {
		return uint32(mode), true
	}
	return 0, false
}

func modeFromMetadata(metadata map[string]string, fallback uint32) uint32 {
	if len(metadata) == 0 {
		return fallback
	}
	value, ok := metadata[storageModeMetadataKey]
	if !ok {
		return fallback
	}
	mode, ok := decodeModeMetadata(value)
	if !ok {
		return fallback
	}
	fileType := mode & syscall.S_IFMT
	if fileType == 0 {
		fileType = fallback & syscall.S_IFMT
	}
	return fileType | (mode & 07777)
}

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

// invalidateByKey invalidates all cache entries matching a bucket:path prefix
// Used for cross-replica invalidation via event bus
func (c *metadataCache) invalidateByKey(key string) {
	prefix := key + ":"
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
