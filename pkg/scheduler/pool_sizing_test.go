package scheduler

import (
	"testing"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
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

func TestBuildDeploymentIncludesWorkerNetworkParitySettings(t *testing.T) {
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			PoolName:                  "default",
			DeploymentName:            "airstore-worker-default",
			Namespace:                 "airstore",
			MinReplicas:               1,
			WorkerImage:               "worker:latest",
			WorkerCpu:                 "1000m",
			WorkerMemory:              "1024Mi",
			GatewayServiceName:        "airstore-gateway",
			GatewayPort:               2993,
			UseGatewayServiceHostname: true,
			WorkerToken:               "token",
			WorkerServiceAccountName:  "airstore-worker",
			WorkerHostNetwork:         true,
			WorkerImagePullSecrets:    []string{"pull-secret"},
			RuntimeClassName:          "gvisor",
			NodeSelector:              map[string]string{"node-role.kubernetes.io/worker": "true"},
			AppConfig: types.AppConfig{
				Scheduler: types.SchedulerConfig{
					WorkerShutdownTimeout: 2 * time.Minute,
				},
			},
		},
	}

	deployment := scaler.buildDeployment()
	spec := deployment.Spec.Template.Spec
	require.Equal(t, "airstore-worker", spec.ServiceAccountName)
	require.NotNil(t, spec.AutomountServiceAccountToken)
	require.True(t, *spec.AutomountServiceAccountToken)
	require.True(t, spec.HostNetwork)
	require.Equal(t, corev1.DNSClusterFirstWithHostNet, spec.DNSPolicy)
	require.NotNil(t, spec.EnableServiceLinks)
	require.False(t, *spec.EnableServiceLinks)
	require.NotNil(t, spec.RuntimeClassName)
	require.Equal(t, "gvisor", *spec.RuntimeClassName)
	require.Equal(t, map[string]string{"node-role.kubernetes.io/worker": "true"}, spec.NodeSelector)
	require.Len(t, spec.ImagePullSecrets, 1)
	require.Equal(t, "pull-secret", spec.ImagePullSecrets[0].Name)

	env := map[string]string{}
	envNames := map[string]struct{}{}
	for _, entry := range spec.Containers[0].Env {
		envNames[entry.Name] = struct{}{}
		if entry.Value != "" {
			env[entry.Name] = entry.Value
		}
	}

	require.Equal(t, "default", env["WORKER_POOL"])
	require.Equal(t, "airstore-gateway.airstore.svc.cluster.local:2993", env["GATEWAY_GRPC_ADDR"])
	require.Equal(t, "airstore", env["POD_NAMESPACE"])
	_, hasPodIP := envNames["POD_IP"]
	_, hasNetworkPrefix := envNames["NETWORK_PREFIX"]
	require.True(t, hasPodIP)
	require.True(t, hasNetworkPrefix)
}

func TestGatewayGRPCAddrUsesExternalAddressWhenServiceHostnameDisabled(t *testing.T) {
	scaler := &PoolScaler{
		config: PoolScalerConfig{
			GatewayServiceName:        "airstore-gateway",
			Namespace:                 "airstore",
			GatewayPort:               1993,
			UseGatewayServiceHostname: false,
			GatewayExternalGRPCAddr:   "gateway.example.com:443",
		},
	}

	require.Equal(t, "gateway.example.com:443", scaler.gatewayGRPCAddr())
}
