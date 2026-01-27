package types

import (
	"errors"
	"fmt"
)

// MetadataNotFoundError is returned when metadata is not found
type MetadataNotFoundError struct {
	Key string
}

func (e *MetadataNotFoundError) Error() string {
	return fmt.Sprintf("metadata not found: %s", e.Key)
}

// ErrWorkerNotFound is returned when a worker is not found
type ErrWorkerNotFound struct {
	WorkerId string
}

func (e *ErrWorkerNotFound) Error() string {
	return fmt.Sprintf("worker not found: %s", e.WorkerId)
}

// From checks if the given error is an ErrWorkerNotFound
func (e *ErrWorkerNotFound) From(err error) bool {
	var workerNotFound *ErrWorkerNotFound
	return errors.As(err, &workerNotFound)
}

// ErrWorkerPoolNotFound is returned when a worker pool is not found
type ErrWorkerPoolNotFound struct {
	PoolName string
}

func (e *ErrWorkerPoolNotFound) Error() string {
	return fmt.Sprintf("worker pool not found: %s", e.PoolName)
}
