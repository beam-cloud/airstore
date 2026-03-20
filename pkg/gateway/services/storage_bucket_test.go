package services

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestStorageServiceEnsureBucketReadyCreatesMissingBucketOnce(t *testing.T) {
	var existsCalls atomic.Int32
	var createCalls atomic.Int32
	var corsCalls atomic.Int32

	svc := &StorageService{
		bucketExistsFn: func(_ context.Context, bucket string) (bool, error) {
			existsCalls.Add(1)
			if bucket != "prefix-ws-124" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return false, nil
		},
		createBucketFn: func(_ context.Context, bucket string) error {
			createCalls.Add(1)
			if bucket != "prefix-ws-124" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return nil
		},
		setBucketCORSFn: func(_ context.Context, bucket string) error {
			corsCalls.Add(1)
			if bucket != "prefix-ws-124" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return nil
		},
	}

	if err := svc.ensureBucketReady(workspaceCtx(124, "ws-124"), "prefix-ws-124"); err != nil {
		t.Fatalf("ensureBucketReady returned error: %v", err)
	}
	if err := svc.ensureBucketReady(workspaceCtx(124, "ws-124"), "prefix-ws-124"); err != nil {
		t.Fatalf("ensureBucketReady second call returned error: %v", err)
	}

	if got := existsCalls.Load(); got != 1 {
		t.Fatalf("expected 1 bucket existence check, got %d", got)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("expected 1 bucket creation, got %d", got)
	}
	if got := corsCalls.Load(); got != 1 {
		t.Fatalf("expected 1 CORS update, got %d", got)
	}
}

func TestStorageServiceEnsureBucketReadySkipsCreateForExistingBucket(t *testing.T) {
	var existsCalls atomic.Int32

	svc := &StorageService{
		bucketExistsFn: func(_ context.Context, bucket string) (bool, error) {
			existsCalls.Add(1)
			if bucket != "prefix-ws-125" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return true, nil
		},
		createBucketFn: func(_ context.Context, bucket string) error {
			t.Fatalf("did not expect createBucket for %q", bucket)
			return nil
		},
		setBucketCORSFn: func(_ context.Context, bucket string) error {
			t.Fatalf("did not expect setBucketCORS for %q", bucket)
			return nil
		},
	}

	if err := svc.ensureBucketReady(workspaceCtx(125, "ws-125"), "prefix-ws-125"); err != nil {
		t.Fatalf("ensureBucketReady returned error: %v", err)
	}
	if err := svc.ensureBucketReady(workspaceCtx(125, "ws-125"), "prefix-ws-125"); err != nil {
		t.Fatalf("ensureBucketReady second call returned error: %v", err)
	}

	if got := existsCalls.Load(); got != 1 {
		t.Fatalf("expected 1 bucket existence check, got %d", got)
	}
}

func TestStorageServiceEnsureBucketReadyIgnoresCORSError(t *testing.T) {
	var createCalls atomic.Int32

	svc := &StorageService{
		bucketExistsFn: func(_ context.Context, bucket string) (bool, error) {
			if bucket != "prefix-ws-126" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return false, nil
		},
		createBucketFn: func(_ context.Context, bucket string) error {
			createCalls.Add(1)
			if bucket != "prefix-ws-126" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return nil
		},
		setBucketCORSFn: func(_ context.Context, bucket string) error {
			if bucket != "prefix-ws-126" {
				t.Fatalf("unexpected bucket %q", bucket)
			}
			return errors.New("cors unavailable")
		},
	}

	if err := svc.ensureBucketReady(workspaceCtx(126, "ws-126"), "prefix-ws-126"); err != nil {
		t.Fatalf("expected CORS error to be ignored, got %v", err)
	}
	if got := createCalls.Load(); got != 1 {
		t.Fatalf("expected 1 bucket creation, got %d", got)
	}
}
