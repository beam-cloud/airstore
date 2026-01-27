package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	// Default scaling parameters
	defaultMinReplicas     = 1
	defaultMaxReplicas     = 10
	defaultScaleDownDelay  = 5 * time.Minute
	defaultScalingInterval = 10 * time.Second

	// Default worker configuration
	defaultWorkerImage  = "registry.localhost:5000/airstore-worker:latest"
	defaultWorkerCpu    = "500m"
	defaultWorkerMemory = "512Mi"

	// Labels
	labelRole = "airstore.beam.cloud/role"
	labelPool = "airstore.beam.cloud/pool"
)

// PoolScalerConfig defines the configuration for a pool scaler
type PoolScalerConfig struct {
	// PoolName is the name of the pool
	PoolName string

	// DeploymentName is the K8s Deployment to scale
	DeploymentName string

	// Namespace is the K8s namespace
	Namespace string

	// MinReplicas is the minimum number of workers
	MinReplicas int32

	// MaxReplicas is the maximum number of workers
	MaxReplicas int32

	// ScaleDownDelay is how long queue must be empty before scaling down
	ScaleDownDelay time.Duration

	// ScalingInterval is how often to check queue depth
	ScalingInterval time.Duration

	// WorkerImage is the container image for workers
	WorkerImage string

	// WorkerCpu is the CPU request/limit for workers
	WorkerCpu string

	// WorkerMemory is the memory request/limit for workers
	WorkerMemory string

	// GatewayServiceName is the K8s service name for the gateway
	GatewayServiceName string

	// GatewayPort is the HTTP port of the gateway
	GatewayPort int

	// RedisAddr is the Redis address for workers
	RedisAddr string
}

// PoolScaler monitors queue depth and scales a K8s Deployment
type PoolScaler struct {
	config            PoolScalerConfig
	taskQueue         repository.TaskQueue
	kubeClient        kubernetes.Interface
	ctx               context.Context
	cancel            context.CancelFunc
	lastQueueEmpty    time.Time
	isQueueEmptySince bool
}

// NewPoolScaler creates a new pool scaler
func NewPoolScaler(ctx context.Context, config PoolScalerConfig, taskQueue repository.TaskQueue) (*PoolScaler, error) {
	// Apply defaults
	if config.MinReplicas <= 0 {
		config.MinReplicas = defaultMinReplicas
	}
	if config.MaxReplicas <= 0 {
		config.MaxReplicas = defaultMaxReplicas
	}
	if config.ScaleDownDelay <= 0 {
		config.ScaleDownDelay = defaultScaleDownDelay
	}
	if config.ScalingInterval <= 0 {
		config.ScalingInterval = defaultScalingInterval
	}
	if config.Namespace == "" {
		config.Namespace = "airstore"
	}
	if config.DeploymentName == "" {
		config.DeploymentName = fmt.Sprintf("airstore-worker-%s", config.PoolName)
	}
	if config.WorkerImage == "" {
		config.WorkerImage = defaultWorkerImage
	}
	if config.WorkerCpu == "" {
		config.WorkerCpu = defaultWorkerCpu
	}
	if config.WorkerMemory == "" {
		config.WorkerMemory = defaultWorkerMemory
	}
	if config.GatewayServiceName == "" {
		config.GatewayServiceName = "airstore-gateway"
	}
	if config.GatewayPort == 0 {
		config.GatewayPort = 1994
	}
	if config.RedisAddr == "" {
		config.RedisAddr = "redis-master:6379"
	}

	// Create K8s client
	kubeConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubernetes config: %w", err)
	}

	kubeClient, err := kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	scalerCtx, cancel := context.WithCancel(ctx)

	return &PoolScaler{
		config:     config,
		taskQueue:  taskQueue,
		kubeClient: kubeClient,
		ctx:        scalerCtx,
		cancel:     cancel,
	}, nil
}

// EnsureDeployment checks if the worker deployment exists and creates it if missing
func (s *PoolScaler) EnsureDeployment() error {
	ctx := context.Background()

	// Check if deployment already exists
	_, err := s.kubeClient.AppsV1().Deployments(s.config.Namespace).Get(
		ctx, s.config.DeploymentName, metav1.GetOptions{})
	if err == nil {
		log.Info().
			Str("pool", s.config.PoolName).
			Str("deployment", s.config.DeploymentName).
			Msg("worker deployment already exists")
		return nil
	}

	if !errors.IsNotFound(err) {
		return fmt.Errorf("failed to check deployment existence: %w", err)
	}

	// Create the deployment
	deployment := s.buildDeployment()

	_, err = s.kubeClient.AppsV1().Deployments(s.config.Namespace).Create(
		ctx, deployment, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create worker deployment: %w", err)
	}

	log.Info().
		Str("pool", s.config.PoolName).
		Str("deployment", s.config.DeploymentName).
		Str("image", s.config.WorkerImage).
		Int32("replicas", s.config.MinReplicas).
		Msg("created worker deployment")

	return nil
}

// buildDeployment constructs the K8s Deployment spec for workers
func (s *PoolScaler) buildDeployment() *appsv1.Deployment {
	labels := map[string]string{
		"app":     s.config.DeploymentName,
		labelRole: "worker",
		labelPool: s.config.PoolName,
	}

	replicas := s.config.MinReplicas

	// Build gateway gRPC address (workers use gRPC to communicate with gateway)
	gatewayGRPCAddr := fmt.Sprintf("%s.%s.svc.cluster.local:1993",
		s.config.GatewayServiceName, s.config.Namespace)

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.config.DeploymentName,
			Namespace: s.config.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "worker",
							Image:           s.config.WorkerImage,
							ImagePullPolicy: corev1.PullAlways,
							Env: []corev1.EnvVar{
								{
									Name: "WORKER_ID",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "metadata.name",
										},
									},
								},
								{
									Name:  "WORKER_POOL",
									Value: s.config.PoolName,
								},
								{
									Name:  "GATEWAY_GRPC_ADDR",
									Value: gatewayGRPCAddr,
								},
								{
									Name:  "REDIS_ADDR",
									Value: s.config.RedisAddr,
								},
								{
									Name:  "CPU_LIMIT",
									Value: s.config.WorkerCpu,
								},
								{
									Name:  "MEMORY_LIMIT",
									Value: s.config.WorkerMemory,
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(s.config.WorkerCpu),
									corev1.ResourceMemory: resource.MustParse(s.config.WorkerMemory),
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse(s.config.WorkerCpu),
									corev1.ResourceMemory: resource.MustParse(s.config.WorkerMemory),
								},
							},
							SecurityContext: &corev1.SecurityContext{
								Privileged: boolPtr(true), // Required for FUSE mounts
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "overlay-tmpfs",
									MountPath: "/mnt/overlay",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "overlay-tmpfs",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{
									Medium: corev1.StorageMediumMemory, // tmpfs
								},
							},
						},
					},
				},
			},
		},
	}
}

// boolPtr returns a pointer to a bool
func boolPtr(b bool) *bool {
	return &b
}

// Start begins the scaling loop
func (s *PoolScaler) Start() {
	ticker := time.NewTicker(s.config.ScalingInterval)
	defer ticker.Stop()

	log.Info().
		Str("pool", s.config.PoolName).
		Str("deployment", s.config.DeploymentName).
		Int32("min_replicas", s.config.MinReplicas).
		Int32("max_replicas", s.config.MaxReplicas).
		Dur("scale_down_delay", s.config.ScaleDownDelay).
		Msg("pool scaler started")

	for {
		select {
		case <-s.ctx.Done():
			log.Info().Str("pool", s.config.PoolName).Msg("pool scaler stopped")
			return
		case <-ticker.C:
			s.tick()
		}
	}
}

// Stop stops the scaler
func (s *PoolScaler) Stop() {
	s.cancel()
}

// tick performs one scaling check
func (s *PoolScaler) tick() {
	// Get queue depth
	queueDepth, err := s.taskQueue.Len(s.ctx)
	if err != nil {
		log.Warn().Err(err).Str("pool", s.config.PoolName).Msg("failed to get queue depth")
		return
	}

	// Get current replica count
	currentReplicas, err := s.getDeploymentReplicas()
	if err != nil {
		log.Warn().Err(err).Str("pool", s.config.PoolName).Msg("failed to get deployment replicas")
		return
	}

	log.Debug().
		Str("pool", s.config.PoolName).
		Int64("queue_depth", queueDepth).
		Int32("current_replicas", currentReplicas).
		Msg("scaling check")

	// Decide on scaling action
	var desiredReplicas int32

	if queueDepth > 0 {
		// Tasks waiting - scale up if below max
		s.isQueueEmptySince = false
		if currentReplicas < s.config.MaxReplicas {
			// Scale up by 1 (could be more aggressive)
			desiredReplicas = min(currentReplicas+1, s.config.MaxReplicas)
			s.scaleDeployment(desiredReplicas)
		}
	} else {
		// Queue empty - track how long it's been empty
		if !s.isQueueEmptySince {
			s.isQueueEmptySince = true
			s.lastQueueEmpty = time.Now()
		}

		// Scale down if queue has been empty for long enough
		idleDuration := time.Since(s.lastQueueEmpty)
		if idleDuration >= s.config.ScaleDownDelay && currentReplicas > s.config.MinReplicas {
			desiredReplicas = max(currentReplicas-1, s.config.MinReplicas)
			s.scaleDeployment(desiredReplicas)
		}
	}
}

// getDeploymentReplicas gets the current replica count
func (s *PoolScaler) getDeploymentReplicas() (int32, error) {
	deployment, err := s.kubeClient.AppsV1().Deployments(s.config.Namespace).Get(
		s.ctx, s.config.DeploymentName, metav1.GetOptions{})
	if err != nil {
		return 0, err
	}

	if deployment.Spec.Replicas == nil {
		return 1, nil // Default
	}
	return *deployment.Spec.Replicas, nil
}

// scaleDeployment scales the deployment to the desired replica count
func (s *PoolScaler) scaleDeployment(replicas int32) {
	scale, err := s.kubeClient.AppsV1().Deployments(s.config.Namespace).GetScale(
		s.ctx, s.config.DeploymentName, metav1.GetOptions{})
	if err != nil {
		log.Error().Err(err).Str("pool", s.config.PoolName).Msg("failed to get deployment scale")
		return
	}

	if scale.Spec.Replicas == replicas {
		return // Already at desired scale
	}

	scale.Spec.Replicas = replicas
	_, err = s.kubeClient.AppsV1().Deployments(s.config.Namespace).UpdateScale(
		s.ctx, s.config.DeploymentName, scale, metav1.UpdateOptions{})
	if err != nil {
		log.Error().Err(err).Str("pool", s.config.PoolName).Int32("replicas", replicas).Msg("failed to scale deployment")
		return
	}

	log.Info().
		Str("pool", s.config.PoolName).
		Str("deployment", s.config.DeploymentName).
		Int32("replicas", replicas).
		Msg("scaled deployment")
}

// GetStatus returns the current scaling status
func (s *PoolScaler) GetStatus(ctx context.Context) (*types.PoolScalerStatus, error) {
	queueDepth, err := s.taskQueue.Len(ctx)
	if err != nil {
		return nil, err
	}

	inFlight, err := s.taskQueue.InFlightCount(ctx)
	if err != nil {
		return nil, err
	}

	replicas, err := s.getDeploymentReplicas()
	if err != nil {
		return nil, err
	}

	return &types.PoolScalerStatus{
		PoolName:        s.config.PoolName,
		QueueDepth:      queueDepth,
		InFlightTasks:   inFlight,
		CurrentReplicas: replicas,
		MinReplicas:     s.config.MinReplicas,
		MaxReplicas:     s.config.MaxReplicas,
	}, nil
}
