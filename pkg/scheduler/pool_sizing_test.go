package scheduler

import (
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCalculateDesiredReplicasScalesUpWhenQueuedWorkExists(t *testing.T) {
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			MinReplicas:    1,
			MaxReplicas:    10,
			ScaleDownDelay: 5 * time.Minute,
		},
	}

	desired, shouldScale := scaler.calculateDesiredReplicas(2, 3, 0, time.Now())
	require.True(t, shouldScale)
	require.Equal(t, int32(3), desired)
	require.False(t, scaler.isQueueEmptySince)
}

func TestCalculateDesiredReplicasTracksIdleWindowBeforeScalingDown(t *testing.T) {
	now := time.Now()
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			MinReplicas:    1,
			MaxReplicas:    10,
			ScaleDownDelay: 5 * time.Minute,
		},
	}

	desired, shouldScale := scaler.calculateDesiredReplicas(5, 0, 0, now)
	require.False(t, shouldScale)
	require.Equal(t, int32(0), desired)
	require.True(t, scaler.isQueueEmptySince)
	require.Equal(t, now, scaler.lastQueueEmpty)
}

func TestCalculateDesiredReplicasScalesToMinAfterIdleDelay(t *testing.T) {
	now := time.Now()
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			MinReplicas:    1,
			MaxReplicas:    10,
			ScaleDownDelay: 5 * time.Minute,
		},
		isQueueEmptySince: true,
		lastQueueEmpty:    now.Add(-6 * time.Minute),
	}

	desired, shouldScale := scaler.calculateDesiredReplicas(5, 0, 0, now)
	require.True(t, shouldScale)
	require.Equal(t, int32(1), desired)
}

func TestCalculateDesiredReplicasDoesNotScaleDownWhileInFlightWorkExists(t *testing.T) {
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			MinReplicas:    1,
			MaxReplicas:    10,
			ScaleDownDelay: 5 * time.Minute,
		},
		isQueueEmptySince: true,
		lastQueueEmpty:    time.Now().Add(-10 * time.Minute),
	}

	desired, shouldScale := scaler.calculateDesiredReplicas(5, 0, 2, time.Now())
	require.False(t, shouldScale)
	require.Equal(t, int32(0), desired)
	require.False(t, scaler.isQueueEmptySince)
}

func TestCalculateDesiredReplicasDoesNotScaleBelowMin(t *testing.T) {
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			MinReplicas:    3,
			MaxReplicas:    10,
			ScaleDownDelay: 5 * time.Minute,
		},
		isQueueEmptySince: true,
		lastQueueEmpty:    time.Now().Add(-10 * time.Minute),
	}

	desired, shouldScale := scaler.calculateDesiredReplicas(3, 0, 0, time.Now())
	require.False(t, shouldScale)
	require.Equal(t, int32(0), desired)
}

func TestCalculateDesiredReplicasMatchesConfigDefaults(t *testing.T) {
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			MinReplicas:    types.NewWorkerPoolConfig().MinReplicas,
			MaxReplicas:    types.NewWorkerPoolConfig().MaxReplicas,
			ScaleDownDelay: types.NewWorkerPoolConfig().ScaleDownDelay,
		},
	}

	desired, shouldScale := scaler.calculateDesiredReplicas(2, 0, 0, time.Now())
	require.False(t, shouldScale)
	require.Equal(t, int32(0), desired)
}
