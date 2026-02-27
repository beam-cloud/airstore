package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync/atomic"
	"syscall"

	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	apiv1 "github.com/beam-cloud/airstore/pkg/api/v1"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/channels"
	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/compression"
	"github.com/beam-cloud/airstore/pkg/gateway/services"
	"github.com/beam-cloud/airstore/pkg/hooks"
	"github.com/beam-cloud/airstore/pkg/instrumentation"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/scheduler"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/sources/providers"
	"github.com/beam-cloud/airstore/pkg/tools"
	_ "github.com/beam-cloud/airstore/pkg/tools/builtin" // self-registering tools
	toolclients "github.com/beam-cloud/airstore/pkg/tools/clients"
	"github.com/beam-cloud/airstore/pkg/tools/definitions"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type Gateway struct {
	Config      types.AppConfig
	RedisClient *common.RedisClient
	BackendRepo repository.BackendRepository
	httpServer  *http.Server
	grpcServer  *grpc.Server
	echo        *echo.Echo
	ctx         context.Context
	cancelFunc  context.CancelFunc

	baseRouteGroup *echo.Group
	rootRouteGroup *echo.Group

	scheduler      *scheduler.Scheduler
	toolRegistry   *tools.Registry
	sourceRegistry *sources.Registry
	mcpManager     *tools.MCPManager

	storageService  *services.StorageService
	storageClient   *clients.StorageClient
	oauthStore      *oauth.Store
	oauthRegistry   *oauth.Registry
	s2Client        *common.S2Client
	eventBus        *common.EventBus
	migrationsReady atomic.Bool

	// Compression middleware (optional)
	compressionRecorder instrumentation.AccessRecorder
}

func NewGateway() (*Gateway, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()
	config.Gateway.GRPC.Port = types.ResolveGatewayGRPCPort(config.Gateway.GRPC.Port)
	config.Gateway.HTTP.Port = types.ResolveGatewayHTTPPort(config.Gateway.HTTP.Port)

	// Setup logging
	if config.PrettyLogs {
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	var redisClient *common.RedisClient
	var backendRepo repository.BackendRepository
	migrationsReady := true

	// Local mode: skip Redis and Postgres
	if config.IsLocalMode() {
		log.Info().Msg("running in local mode - Redis and Postgres disabled")
	} else {
		// Remote mode: initialize Redis
		redisClient, err = common.NewRedisClient(config.Database.Redis, common.WithClientName("AirstoreGateway"))
		if err != nil {
			return nil, err
		}

		// Initialize Postgres backend (optional - may not be configured)
		if config.Database.Postgres.Host != "" {
			migrationsReady = false
			backendRepo, err = repository.NewPostgresBackend(config.Database.Postgres)
			if err != nil {
				return nil, fmt.Errorf("failed to connect to postgres: %w", err)
			} else {
				// Run migrations
				if err := backendRepo.RunMigrations(); err != nil {
					_ = backendRepo.Close()
					return nil, fmt.Errorf("failed to run postgres migrations: %w", err)
				}
				migrationsReady = true
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Initialize OAuth registry - providers self-register their integrations
	oauthRegistry := oauth.NewRegistry()
	callbackURL := config.OAuth.CallbackURL
	oauthRegistry.Register(oauth.NewGoogleProvider(config.OAuth.Google, callbackURL))
	oauthRegistry.Register(oauth.NewGitHubProvider(config.OAuth.GitHub, callbackURL))
	oauthRegistry.Register(oauth.NewNotionProvider(config.OAuth.Notion, callbackURL))
	oauthRegistry.Register(oauth.NewSlackProvider(config.OAuth.Slack, callbackURL))
	oauthRegistry.Register(oauth.NewLinearProvider(config.OAuth.Linear, callbackURL))

	// Initialize S2 client for task log streaming if configured
	var s2Client *common.S2Client
	if config.Streams.Token != "" && config.Streams.Basin != "" {
		s2Client = common.NewS2Client(common.S2Config{
			Token: config.Streams.Token,
			Basin: config.Streams.Basin,
		})
		log.Info().Str("basin", config.Streams.Basin).Msg("S2 log streaming enabled")
	}

	gateway := &Gateway{
		Config:         config,
		RedisClient:    redisClient,
		BackendRepo:    backendRepo,
		ctx:            ctx,
		cancelFunc:     cancel,
		toolRegistry:   tools.NewRegistry(),
		sourceRegistry: sources.NewRegistry(),
		mcpManager:     tools.NewMCPManager(),
		oauthStore:     oauth.NewStore(redisClient, 0),
		oauthRegistry:  oauthRegistry,
		s2Client:       s2Client,
	}
	gateway.migrationsReady.Store(migrationsReady)

	return gateway, nil
}

func (g *Gateway) initLock(name string) (func(), error) {
	// Skip locking in local mode (no Redis)
	if g.RedisClient == nil {
		return func() {}, nil
	}

	lockKey := common.Keys.GatewayInitLock(name)
	lock := common.NewRedisLock(g.RedisClient)

	if err := lock.Acquire(g.ctx, lockKey, common.RedisLockOptions{TtlS: 10, Retries: 1}); err != nil {
		return nil, err
	}

	return func() {
		if err := lock.Release(lockKey); err != nil {
			log.Error().Str("lock_key", lockKey).Err(err).Msg("failed to release init lock")
		}
	}, nil
}

func (g *Gateway) initHTTP() error {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true

	e.Pre(middleware.RemoveTrailingSlash())

	// Configure logging middleware
	if g.Config.Gateway.HTTP.EnablePrettyLogs {
		e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
			Format:  "${time_rfc3339} ${method} ${uri} ${status} ${latency_human}\n",
			Skipper: shouldSkipHTTPRequestLog,
		}))
	}

	// CORS
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: g.Config.Gateway.HTTP.CORS.AllowedOrigins,
		AllowHeaders: g.Config.Gateway.HTTP.CORS.AllowedHeaders,
		AllowMethods: g.Config.Gateway.HTTP.CORS.AllowedMethods,
	}))

	e.Use(middleware.Recover())

	// Create validator for HTTP auth middleware
	var validator auth.TokenValidator
	if g.BackendRepo != nil {
		validator = auth.NewCompositeValidator(g.Config.Gateway.AuthToken, g.BackendRepo)
	} else {
		validator = auth.NewStaticValidator(g.Config.Gateway.AuthToken)
	}

	// Add HTTP auth middleware (validates tokens, allows pass-through)
	e.Use(auth.HTTPMiddleware(validator))

	g.echo = e
	g.httpServer = &http.Server{
		Addr:              fmt.Sprintf("%s:%d", g.Config.Gateway.HTTP.Host, g.Config.Gateway.HTTP.Port),
		Handler:           e,
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	g.baseRouteGroup = e.Group(apiv1.HttpServerBaseRoute)
	g.rootRouteGroup = e.Group(apiv1.HttpServerRootRoute)

	// Register API groups (health check works without Redis in local mode)
	var redisProbe func(context.Context) error
	if g.RedisClient != nil {
		redisProbe = func(ctx context.Context) error {
			return g.RedisClient.Ping(ctx).Err()
		}
	}

	var postgresProbe func(context.Context) error
	if g.BackendRepo != nil {
		postgresProbe = g.BackendRepo.Ping
	}

	apiv1.NewHealthGroup(
		g.baseRouteGroup.Group("/health"),
		redisProbe,
		postgresProbe,
		func() bool { return g.migrationsReady.Load() },
	)

	return nil
}

func (g *Gateway) initGRPC() error {
	// Create token validator
	// Use CompositeValidator if we have a database backend (for workspace/worker tokens)
	var validator auth.TokenValidator
	if g.BackendRepo != nil {
		validator = auth.NewCompositeValidator(g.Config.Gateway.AuthToken, g.BackendRepo)
	} else {
		validator = auth.NewStaticValidator(g.Config.Gateway.AuthToken)
	}
	authInterceptor := auth.NewGRPCInterceptor(validator)

	// Create access recorder (S2-backed or noop) — used by both the gRPC
	// interceptor and the compression middleware.
	if g.s2Client != nil {
		g.compressionRecorder = instrumentation.NewEventFlusher(g.s2Client)
	} else {
		g.compressionRecorder = instrumentation.NewNoopRecorder()
	}
	accessInterceptor := instrumentation.NewAccessLogInterceptor(g.compressionRecorder)

	serverOptions := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpcUnaryPanicRecoveryInterceptor(), authInterceptor.Unary(), accessInterceptor.Unary()),
		grpc.ChainStreamInterceptor(grpcStreamPanicRecoveryInterceptor(), authInterceptor.Stream()),
		grpc.MaxRecvMsgSize(g.Config.Gateway.GRPC.MaxRecvMsgSize * 1024 * 1024),
		grpc.MaxSendMsgSize(g.Config.Gateway.GRPC.MaxSendMsgSize * 1024 * 1024),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle:     15 * time.Minute, // close idle connections to reclaim resources
			MaxConnectionAge:      30 * time.Minute, // force reconnect to rebalance across replicas
			MaxConnectionAgeGrace: 10 * time.Second, // grace period for in-flight RPCs
			Time:                  60 * time.Second, // ping idle clients every 60s
			Timeout:               10 * time.Second, // wait 10s for ping ack
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             30 * time.Second, // minimum time between client pings
			PermitWithoutStream: true,             // allow pings without active streams (FUSE mounts)
		}),
	}

	g.grpcServer = grpc.NewServer(serverOptions...)

	// Register health service
	hs := health.NewServer()
	hs.Resume()
	go func() {
		<-g.ctx.Done()
		hs.Shutdown()
	}()
	grpc_health_v1.RegisterHealthServer(g.grpcServer, hs)

	return nil
}

func (g *Gateway) registerServices() error {
	// Initialize scheduler (requires Redis, skip in local mode)
	if g.Config.Scheduler.Enabled && g.RedisClient != nil {
		sched, err := scheduler.NewScheduler(g.ctx, g.Config, g.RedisClient, g.BackendRepo)
		if err != nil {
			return fmt.Errorf("failed to create scheduler: %w", err)
		}
		g.scheduler = sched

		// Start scheduler background processes
		if err := g.scheduler.Start(); err != nil {
			return fmt.Errorf("failed to start scheduler: %w", err)
		}

		log.Info().Msg("scheduler started")
	}

	// Register filesystem gRPC service
	// Uses unified FilesystemStore: Postgres+Redis in remote mode, memory in local mode
	var filesystemStore repository.FilesystemStore
	if g.BackendRepo != nil && g.RedisClient != nil {
		filesystemStore = repository.NewFilesystemStore(g.BackendRepo.DB(), g.RedisClient, nil)
		log.Info().Msg("filesystem service registered (postgres+redis backend)")
	} else {
		filesystemStore = repository.NewMemoryFilesystemStore()
		log.Info().Msg("filesystem service registered (memory backend)")
	}
	filesystemService := services.NewFilesystemService(filesystemStore)
	pb.RegisterFilesystemServiceServer(g.grpcServer, filesystemService)

	// Initialize event bus for cross-replica cache invalidation
	if g.RedisClient != nil {
		g.eventBus = common.NewEventBus(g.ctx, g.RedisClient)
		go g.eventBus.Start()
	}

	// Initialize hook event emitter.
	// Redis mode: EventStream (reliable, exactly-once via consumer groups).
	// Local mode: LocalEventEmitter (direct in-process dispatch).
	var hookEmitter common.EventEmitter
	var hookStreamConsumer *common.EventStream
	if g.RedisClient != nil {
		hostname, _ := os.Hostname()
		consumer := fmt.Sprintf("gw-%s-%d", hostname, os.Getpid())
		stream := common.NewEventStream(
			g.RedisClient,
			common.Keys.HookStream(),
			common.Keys.HookConsumerGroup(),
			consumer,
		)
		hookEmitter = stream
		hookStreamConsumer = stream
	} else {
		hookEmitter = common.NewLocalEventEmitter()
	}

	// Initialize seen tracker for source change detection (Redis only)
	var seenTracker *hooks.SeenTracker
	if g.RedisClient != nil {
		seenTracker = hooks.NewSeenTracker(g.RedisClient)
	}

	// Initialize workspace storage (per-workspace S3 buckets)
	if g.Config.Filesystem.WorkspaceStorage.IsConfigured() {
		if client, err := clients.NewStorageClient(g.ctx, g.Config.Filesystem.WorkspaceStorage); err != nil {
			log.Warn().Err(err).Msg("storage client init failed")
		} else if svc, err := services.NewStorageService(client, g.eventBus); err != nil {
			log.Warn().Err(err).Msg("storage service init failed")
		} else {
			g.storageClient = client
			g.storageService = svc
			svc.SetHookStream(hookEmitter)
			pb.RegisterContextServiceServer(g.grpcServer, svc)
		}
	}

	// Register worker gRPC service (for worker-to-gateway communication)
	if g.scheduler != nil {
		taskQueue := repository.NewRedisTaskQueue(g.RedisClient, "default")
		workerService := services.NewWorkerService(g.scheduler, g.BackendRepo, g.scheduler.WorkerRepo(), taskQueue, g.RedisClient, g.Config.Scheduler)
		workerService.StartRecoveryLoop(g.ctx)
		pb.RegisterWorkerServiceServer(g.grpcServer, workerService)
		log.Info().Msg("worker service registered")
	}

	// Initialize and register tools
	if err := g.initTools(); err != nil {
		return fmt.Errorf("failed to initialize tools: %w", err)
	}

	// Register tools gRPC service (with backend for credential lookups and OAuth refresh)
	var toolService *services.ToolService
	if g.BackendRepo != nil && len(g.oauthRegistry.ListConfiguredProviders()) > 0 {
		toolService = services.NewToolServiceWithOAuth(g.toolRegistry, g.BackendRepo, g.oauthRegistry)
	} else if g.BackendRepo != nil {
		toolService = services.NewToolServiceWithBackend(g.toolRegistry, g.BackendRepo)
	} else {
		toolService = services.NewToolService(g.toolRegistry)
	}
	pb.RegisterToolServiceServer(g.grpcServer, toolService)
	log.Info().Msg("tools service registered")

	// Register gateway gRPC service (workspace/member/token/connection/task management)
	var gatewayService *services.GatewayService
	if g.BackendRepo != nil {
		gatewayService = services.NewGatewayService(g.BackendRepo, filesystemStore, g.eventBus, g.sourceRegistry)
		pb.RegisterGatewayServiceServer(g.grpcServer, gatewayService)
		log.Info().Msg("gateway service registered")
	}

	// Register batched access-log ingestion service for mount-side logical reads.
	accessService := services.NewAccessService(g.compressionRecorder, g.RedisClient)
	pb.RegisterAccessLogServiceServer(g.grpcServer, accessService)
	log.Info().Msg("access log service registered")

	// Register source providers
	g.initSources()

	// Register sources gRPC service (read-only integration access with OAuth refresh)
	// Pass hook stream and seen tracker as optional deps for change detection
	var hookOpts []services.SourceServiceOption
	if hookEmitter != nil {
		hookOpts = append(hookOpts, services.WithHookStream(hookEmitter))
	}
	if seenTracker != nil {
		hookOpts = append(hookOpts, services.WithSeenTracker(seenTracker))
	}

	// Set model API keys as env vars so BAML clients (clients.baml) can pick them up.
	if key := g.Config.AnthropicAPIKey(); key != "" {
		os.Setenv("ANTHROPIC_API_KEY", key)
	}
	if key := g.Config.CerebrasAPIKey(); key != "" {
		os.Setenv("CEREBRAS_API_KEY", key)
	}

	// Wire compression middleware if a strategy is configured.
	// The recorder is already on g.compressionRecorder (created in initGRPC).
	recorder := g.compressionRecorder

	var compStore *compression.CompressedStore
	if g.Config.Compression.Strategy != "" {
		compCfg := compression.Config{
			Strategy:             compression.CompressionStrategy(g.Config.Compression.Strategy),
			CacheEnabled:         g.Config.Compression.CacheEnabled,
			TokenThreshold:       g.Config.Compression.TokenThreshold,
			MaxContentBytes:      g.Config.Compression.MaxContentBytes,
			TokenEncoding:        g.Config.Compression.TokenEncoding,
			Timeout:              g.Config.Compression.Timeout,
			ContentCacheMaxBytes: g.Config.Compression.ContentCacheMaxBytes,
			ContentCacheTTL:      g.Config.Compression.ContentCacheTTL,
		}

		compressor, err := compression.NewCompressor(compCfg.Strategy, compCfg)
		if err != nil {
			return fmt.Errorf("compression: %w", err)
		}

		// Compressed content cache (optional Redis cache, gated by config)
		if compCfg.CacheEnabled && g.RedisClient != nil {
			compStore = compression.NewCompressedStore(g.RedisClient, compCfg)
			log.Info().Msg("compression cache enabled (Redis)")
		}

		hookOpts = append(hookOpts, services.WithRecorder(recorder))
		hookOpts = append(hookOpts, services.WithCompressionMiddleware(compressor, compStore, compCfg))
		log.Info().Str("strategy", g.Config.Compression.Strategy).Msg("compression middleware enabled")
	}

	var sourceService *services.SourceService
	if g.BackendRepo != nil && len(g.oauthRegistry.ListConfiguredProviders()) > 0 {
		sourceService = services.NewSourceServiceWithOAuth(g.sourceRegistry, g.BackendRepo, filesystemStore, g.oauthRegistry, hookOpts...)
	} else {
		sourceService = services.NewSourceService(g.sourceRegistry, g.BackendRepo, filesystemStore, hookOpts...)
	}

	pb.RegisterSourceServiceServer(g.grpcServer, sourceService)
	log.Info().Int("providers", len(g.sourceRegistry.List())).Strs("available", g.sourceRegistry.List()).Msg("sources service registered")

	// Auth introspection (any valid token)
	if g.BackendRepo != nil {
		apiv1.NewAuthGroup(g.baseRouteGroup.Group("/auth"), g.BackendRepo)
	}

	// Register task and workspace APIs (requires Postgres)
	if g.BackendRepo != nil {
		taskQueue := repository.NewRedisTaskQueue(g.RedisClient, "default")

		// Wire source cache invalidation hooks into gateway service.
		if gatewayService != nil {
			gatewayService.SetSourceService(sourceService)
		}

		// Workspace CRUD endpoints (cluster admin or org tokens)
		workspacesAdminGroup := g.baseRouteGroup.Group("/workspaces")
		workspacesAdminGroup.Use(requireClusterAdminOrOrgMiddleware())
		apiv1.NewWorkspacesGroup(workspacesAdminGroup, g.BackendRepo, g.storageClient)

		// Worker tokens API (cluster admin only)
		workerTokensGroup := g.baseRouteGroup.Group("/worker-tokens")
		workerTokensGroup.Use(auth.RequireClusterAdminMiddleware())
		apiv1.NewWorkerTokensGroup(workerTokensGroup, g.BackendRepo)
		log.Info().Msg("worker tokens API registered at /api/v1/worker-tokens")

		// Organization tokens API (cluster admin only)
		orgTokensGroup := g.baseRouteGroup.Group("/org-tokens")
		orgTokensGroup.Use(auth.RequireClusterAdminMiddleware())
		apiv1.NewOrgTokensGroup(orgTokensGroup, g.BackendRepo)
		log.Info().Msg("org tokens API registered at /api/v1/org-tokens")

		// Workspace-scoped APIs (support both admin and member tokens)
		workspaceAuthConfig := apiv1.WorkspaceAuthConfig{
			AdminToken: g.Config.Gateway.AuthToken,
			Backend:    g.BackendRepo,
		}

		// Members API (nested under workspaces, workspace-scoped auth)
		membersGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/members")
		membersGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewMembersGroup(membersGroup, g.BackendRepo)

		// Tokens API (nested under workspaces, workspace-scoped auth)
		tokensGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/tokens")
		tokensGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewTokensGroup(tokensGroup, g.BackendRepo)

		// Connections API (nested under workspaces, workspace-scoped auth)
		connectionsGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/connections")
		connectionsGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewConnectionsGroup(connectionsGroup, g.BackendRepo, g.sourceRegistry, g.storageClient)

		// Hooks API (nested under workspaces, workspace-scoped auth)
		hooksGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/hooks")
		hooksGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		hooksSvc := &hooks.Service{
			Store:    filesystemStore,
			Backend:  g.BackendRepo,
			EventBus: g.eventBus,
			Seen:     seenTracker,
		}
		apiv1.NewHooksGroup(hooksGroup, g.BackendRepo, hooksSvc)
		log.Info().Msg("hooks API registered at /api/v1/workspaces/:workspace_id/hooks")

		// Skills API (nested under workspaces, workspace-scoped auth)
		if g.storageClient != nil {
			skillsGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/skills")
			skillsGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
			apiv1.NewSkillsGroup(skillsGroup, g.BackendRepo, g.storageClient)
			log.Info().Msg("skills API registered at /api/v1/workspaces/:workspace_id/skills")
		}

		// Filesystem API (nested under workspaces, workspace-scoped auth)
		filesystemGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/fs")
		filesystemGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewFilesystemGroup(filesystemGroup, g.BackendRepo, g.storageService, sourceService, g.sourceRegistry, g.toolRegistry, g.s2Client)
		log.Info().Msg("filesystem API registered at /api/v1/workspaces/:workspace_id/fs")

		// Cache management API (nested under workspaces, workspace-scoped auth)
		cacheGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/cache")
		cacheGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewCacheGroup(cacheGroup, compStore)

		// Access log API (nested under workspaces, workspace-scoped auth)
		accessLogGroup := g.baseRouteGroup.Group("/workspaces/:workspace_id/access-log")
		accessLogGroup.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewAccessLogGroup(accessLogGroup, g.BackendRepo, g.s2Client, sourceService)

		// Agent/task/run engine and gRPC service.
		orchestratorSvc := orchestration.NewAgentService(
			g.ctx,
			g.BackendRepo,
			taskQueue,
			g.RedisClient,
			g.s2Client,
			g.Config.Sandbox.GetDefaultImage(),
		)
		orchestratorSvc.Start(g.ctx)
		agentAPI := orchestration.NewAgentAPI(g.BackendRepo, orchestratorSvc)
		channelRegistry := channels.NewRegistry(channels.NewDirect(agentAPI))
		agentService := services.NewAgentService(g.BackendRepo, agentAPI, g.s2Client)
		pb.RegisterAgentServiceServer(g.grpcServer, agentService)
		log.Info().Msg("agent service registered")

		// Hook task factory routes filesystem events through AcceptAgentCommand.
		taskFactory := hooks.NewTaskFactory(
			g.BackendRepo,
			taskQueue,
			g.Config.Sandbox.GetDefaultImage(),
			agentAPI,
		)

		// Agent/task/run HTTP APIs (workspace-scoped)
		agentAPIRoot := g.baseRouteGroup.Group("/workspaces/:workspace_id")
		agentAPIRoot.Use(apiv1.NewWorkspaceAuthMiddleware(workspaceAuthConfig))
		apiv1.NewAgentsGroup(agentAPIRoot.Group("/agents"), agentAPI, hooksSvc)
		apiv1.NewWorkspaceTasksGroup(agentAPIRoot.Group("/tasks"), agentAPI)
		apiv1.NewRunsGroup(agentAPIRoot.Group("/runs"), agentAPI)
		apiv1.NewWorkspaceChannelsGroup(agentAPIRoot.Group("/channels"), channelRegistry)

		// Hook engine: matches events → hooks → agent tasks.
		var skillReader hooks.SkillReader
		if g.storageClient != nil {
			skillReader = hooks.NewStorageSkillReader(g.storageClient, g.BackendRepo)
		}
		engine := hooks.NewEngine(filesystemStore, taskFactory, g.BackendRepo, skillReader)
		go engine.Start(g.ctx)

		if hookStreamConsumer != nil {
			go hookStreamConsumer.Consume(g.ctx, engine.Handle)
			log.Info().Msg("hook engine started (redis stream)")
			if g.eventBus != nil {
				g.eventBus.On(common.EventCacheInvalidate, func(e common.Event) {
					if scope, _ := e.Data["scope"].(string); scope == "hooks" {
						if wsId := hooks.ParseUint(e.Data["workspace_id"]); wsId > 0 {
							engine.InvalidateCache(wsId)
						}
					}
				})
			}
		} else if local, ok := hookEmitter.(*common.LocalEventEmitter); ok {
			local.SetHandler(engine.Handle)
			log.Info().Msg("hook engine started (local)")
		} else {
			log.Warn().Msg("hook engine not started: no event emitter")
		}

		// Source poller: refreshes watched source queries so hooks fire on new results
		if g.RedisClient != nil {
			sourcePoller := hooks.NewSourcePoller(filesystemStore, sourceService, g.RedisClient)
			go sourcePoller.Start(g.ctx)
		}

		// OAuth API for workspace integrations (gmail, gdrive, github, notion, slack)
		apiv1.NewOAuthGroup(g.baseRouteGroup.Group("/oauth"), g.oauthStore, g.oauthRegistry, g.BackendRepo, g.storageClient, g.Config.Gateway.AuthToken)
		if providers := g.oauthRegistry.ListConfiguredProviders(); len(providers) > 0 {
			log.Info().Strs("providers", providers).Msg("oauth API registered at /api/v1/oauth")
		} else {
			log.Warn().Msg("oauth API registered but no providers configured (set oauth.callbackUrl and provider credentials)")
		}

		log.Info().Msg("workspace, members, tokens, connections, hooks, and agent/task/run APIs registered")
	}

	return nil
}

// Start is the gateway entry point
// StartAsync starts the gateway servers without blocking.
// Use this when embedding the gateway in another process (e.g., CLI).
func (g *Gateway) StartAsync() error {
	err := g.initHTTP()
	if err != nil {
		return fmt.Errorf("failed to initialize http server: %w", err)
	}

	err = g.initGRPC()
	if err != nil {
		return fmt.Errorf("failed to initialize grpc server: %w", err)
	}

	err = g.registerServices()
	if err != nil {
		return fmt.Errorf("failed to register services: %w", err)
	}

	httpAddr := fmt.Sprintf("%s:%d", g.Config.Gateway.HTTP.Host, g.Config.Gateway.HTTP.Port)
	httpListener, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return fmt.Errorf("failed to bind http listener: %w", err)
	}

	grpcAddr := fmt.Sprintf(":%d", g.Config.Gateway.GRPC.Port)
	grpcListener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		_ = httpListener.Close()
		return fmt.Errorf("failed to bind grpc listener: %w", err)
	}

	// Start HTTP server
	go func() {
		if err := g.httpServer.Serve(httpListener); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http server error")
		}
	}()

	// Start gRPC server
	go func() {
		if err := g.grpcServer.Serve(grpcListener); err != nil {
			log.Error().Err(err).Msg("grpc server error")
		}
	}()

	log.Info().
		Str("host", g.Config.Gateway.HTTP.Host).
		Int("port", g.Config.Gateway.HTTP.Port).
		Msg("gateway http server running")

	log.Info().
		Int("port", g.Config.Gateway.GRPC.Port).
		Msg("gateway grpc server running")

	return nil
}

// GRPCAddr returns the gateway's gRPC address
func (g *Gateway) GRPCAddr() string {
	return fmt.Sprintf("localhost:%d", g.Config.Gateway.GRPC.Port)
}

// Shutdown gracefully shuts down the gateway (exported for external use)
func (g *Gateway) Shutdown() {
	g.shutdown()
}

func (g *Gateway) Start() error {
	if err := g.StartAsync(); err != nil {
		return err
	}

	terminationSignal := make(chan os.Signal, 1)
	signal.Notify(terminationSignal, os.Interrupt, syscall.SIGTERM)
	<-terminationSignal

	log.Info().Msg("termination signal received. shutting down...")
	g.shutdown()

	return nil
}

// shutdown gracefully shuts down the gateway
func (g *Gateway) shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), g.Config.Gateway.ShutdownTimeout)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)

	// Stop HTTP server
	eg.Go(func() error {
		return g.httpServer.Shutdown(ctx)
	})

	// Stop gRPC server
	if g.grpcServer != nil {
		eg.Go(func() error {
			g.grpcServer.GracefulStop()
			return nil
		})
	}

	// Stop scheduler
	if g.scheduler != nil {
		eg.Go(func() error {
			return g.scheduler.Stop()
		})
	}

	// Close Postgres backend
	if g.BackendRepo != nil {
		eg.Go(func() error {
			return g.BackendRepo.Close()
		})
	}

	// Close MCP manager
	if g.mcpManager != nil {
		eg.Go(func() error {
			return g.mcpManager.Close()
		})
	}

	// Flush compression event recorder
	if g.compressionRecorder != nil {
		g.compressionRecorder.Flush()
	}

	g.cancelFunc()

	if err := eg.Wait(); err != nil {
		log.Error().Err(err).Msg("failed to shutdown gateway gracefully")
	}

	log.Info().Msg("gateway stopped")
}

// GRPCServer returns the gRPC server for registering services
func (g *Gateway) GRPCServer() *grpc.Server {
	return g.grpcServer
}

// Scheduler returns the scheduler instance
func (g *Gateway) Scheduler() *scheduler.Scheduler {
	return g.scheduler
}

// ToolRegistry returns the tool registry for registering providers
func (g *Gateway) ToolRegistry() *tools.Registry {
	return g.toolRegistry
}

// SourceRegistry returns the source registry for registering providers
func (g *Gateway) SourceRegistry() *sources.Registry {
	return g.sourceRegistry
}

// initSources initializes source providers
func (g *Gateway) initSources() {
	// Register source providers (all use connection-based auth)
	g.sourceRegistry.Register(providers.NewGitHubProvider())
	g.sourceRegistry.Register(providers.NewGmailProvider())
	g.sourceRegistry.Register(providers.NewNotionProvider())
	g.sourceRegistry.Register(providers.NewGDriveProvider())
	g.sourceRegistry.Register(providers.NewSlackProvider())
	g.sourceRegistry.Register(providers.NewLinearProvider())
	g.sourceRegistry.Register(providers.NewPostHogProvider())
	g.sourceRegistry.Register(providers.NewWebProvider(g.Config.Sources.Firecrawl.APIKey))

	log.Info().Strs("providers", g.sourceRegistry.List()).Msg("source providers registered")
}

// initTools initializes the tool system by loading schemas and registering clients
func (g *Gateway) initTools() error {
	// Register self-registering tools first (from pkg/tools/builtin)
	// These register via init() when the package is imported
	for _, t := range tools.GetRegisteredTools() {
		g.toolRegistry.Register(tools.NewToolAdapter(t))
		log.Debug().Str("tool", t.Name()).Msg("registered self-registering tool")
	}

	// Create client registry for YAML-based tools
	clientRegistry := tools.NewClientRegistry()

	// API key integrations (only register if configured)
	if g.Config.Tools.Integrations.Weather.APIKey != "" {
		clientRegistry.Register(toolclients.NewWeatherClient(g.Config.Tools.Integrations.Weather.APIKey))
		log.Debug().Msg("weather integration enabled")
	}

	if g.Config.Tools.Integrations.Exa.APIKey != "" {
		clientRegistry.Register(toolclients.NewExaClient(g.Config.Tools.Integrations.Exa.APIKey))
		log.Debug().Msg("exa integration enabled")
	}

	// Connection-based integrations (always registered, credentials checked at runtime)
	clientRegistry.Register(toolclients.NewGitHubClient())
	clientRegistry.Register(toolclients.NewGmailClient())
	clientRegistry.Register(toolclients.NewGDriveClient())
	clientRegistry.Register(toolclients.NewSlackClient())
	clientRegistry.Register(toolclients.NewNotionClient())
	clientRegistry.Register(toolclients.NewLinearClient())
	log.Debug().Msg("oauth source integrations registered (connection-based)")

	// Load tool definitions from embedded YAML files
	// Schemas are matched to clients by name - unmatched schemas are skipped
	loader := tools.NewLoader(clientRegistry)
	if err := loader.RegisterProviders(definitions.FS, ".", g.toolRegistry); err != nil {
		return fmt.Errorf("load tool definitions: %w", err)
	}

	// Load MCP servers from config (works in both local and remote mode)
	if len(g.Config.Tools.MCP) > 0 {
		if err := g.mcpManager.LoadServers(g.Config.Tools.MCP, g.toolRegistry); err != nil {
			log.Warn().Err(err).Msg("failed to load some MCP servers")
		}
	}

	log.Info().
		Int("tools", len(g.toolRegistry.List())).
		Strs("available", g.toolRegistry.List()).
		Msg("tools initialized")

	return nil
}

// requireClusterAdminOrOrgMiddleware allows cluster_admin or organization tokens.
func requireClusterAdminOrOrgMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			info := auth.AuthInfoFromContext(c.Request().Context())
			if info == nil {
				return c.JSON(http.StatusUnauthorized, apiv1.Response{
					Success: false,
					Error:   "authentication required",
				})
			}
			if info.IsClusterAdmin() || info.IsOrganization() {
				return next(c)
			}
			return c.JSON(http.StatusForbidden, apiv1.Response{
				Success: false,
				Error:   "admin or organization token required",
			})
		}
	}
}

func grpcUnaryPanicRecoveryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (_ interface{}, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error().
					Interface("panic", recovered).
					Str("method", info.FullMethod).
					Bytes("stack", debug.Stack()).
					Msg("panic recovered in gRPC unary handler")
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}

func grpcStreamPanicRecoveryInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.Error().
					Interface("panic", recovered).
					Str("method", info.FullMethod).
					Bytes("stack", debug.Stack()).
					Msg("panic recovered in gRPC stream handler")
				err = status.Error(codes.Internal, "internal server error")
			}
		}()
		return handler(srv, stream)
	}
}
