package repository

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRedisTerminalIORepository_PublishSubscribe(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	if err != nil {
		t.Fatalf("new redis test client: %v", err)
	}

	repo := NewRedisTerminalIORepository(rdb)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const taskID = "task-123"

	inputCh, inputCleanup, err := repo.SubscribeInput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe input: %v", err)
	}
	defer inputCleanup()

	outputCh, outputCleanup, err := repo.SubscribeOutput(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe output: %v", err)
	}
	defer outputCleanup()

	resizeCh, resizeCleanup, err := repo.SubscribeResize(ctx, taskID)
	if err != nil {
		t.Fatalf("subscribe resize: %v", err)
	}
	defer resizeCleanup()

	wantInput := []byte("echo hi\n")
	if err := repo.PublishInput(ctx, taskID, wantInput); err != nil {
		t.Fatalf("publish input: %v", err)
	}

	gotInput, err := waitFor[[]byte](inputCh)
	if err != nil {
		t.Fatalf("wait input: %v", err)
	}
	if string(gotInput) != string(wantInput) {
		t.Fatalf("input mismatch: got %q want %q", gotInput, wantInput)
	}

	wantOutput := []byte("hello from shell")
	if err := repo.PublishOutput(ctx, taskID, wantOutput); err != nil {
		t.Fatalf("publish output: %v", err)
	}

	gotOutput, err := waitFor[[]byte](outputCh)
	if err != nil {
		t.Fatalf("wait output: %v", err)
	}
	if string(gotOutput) != string(wantOutput) {
		t.Fatalf("output mismatch: got %q want %q", gotOutput, wantOutput)
	}

	if err := repo.PublishResize(ctx, taskID, 120, 40); err != nil {
		t.Fatalf("publish resize: %v", err)
	}

	gotResize, err := waitFor[TerminalResizeEvent](resizeCh)
	if err != nil {
		t.Fatalf("wait resize: %v", err)
	}
	if gotResize.Cols != 120 || gotResize.Rows != 40 {
		t.Fatalf("resize mismatch: got %+v", gotResize)
	}
}

func waitFor[T any](ch <-chan T) (T, error) {
	var zero T
	select {
	case v, ok := <-ch:
		if !ok {
			return zero, errors.New("channel closed")
		}
		return v, nil
	case <-time.After(2 * time.Second):
		return zero, errors.New("timeout")
	}
}
