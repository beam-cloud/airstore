package gateway

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	"github.com/beam-cloud/airstore/pkg/admin"
	apiv1 "github.com/beam-cloud/airstore/pkg/api/v1"
	"github.com/beam-cloud/airstore/pkg/auth"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/gateway/services"
	"github.com/beam-cloud/airstore/pkg/oauth"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/scheduler"
	"github.com/beam-cloud/airstore/pkg/sources"
	"github.com/beam-cloud/airstore/pkg/sources/providers"
	"github.com/beam-cloud/airstore/pkg/tools"
	_ "github.com/beam-cloud/airstore/pkg/tools/builtin" // self-registering tools
	"github.com/beam-cloud/airstore/pkg/tools/clients"
	"github.com/beam-cloud/airstore/pkg/tools/definitions"
	"github.com/beam-cloud/airstore/pkg/types"
	pb "github.com/beam-cloud/airstore/proto"
)

type Gateway struct {
	Config      types.AppConfig
	RedisClient *common.RedisClient
	BackendRepo *repository.PostgresBackend
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

	// Context service for S3-backed file storage
	contextService *services.ContextService

	// OAuth for workspace integrations
	oauthStore  *oauth.Store
	googleOAuth *oauth.GoogleClient
}

func NewGateway() (*Gateway, error) {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		return nil, err
	}
	config := configManager.GetConfig()

	// Setup logging
	if config.PrettyLogs {
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	var redisClient *common.RedisClient
	var backendRepo *repository.PostgresBackend

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
			backendRepo, err = repository.NewPostgresBackend(config.Database.Postgres)
			if err != nil {
				log.Warn().Err(err).Msg("failed to connect to postgres, task API will be disabled")
			} else {
				// Run migrations
				if err := backendRepo.RunMigrations(); err != nil {
					log.Warn().Err(err).Msg("failed to run postgres migrations")
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	gateway := &Gateway{
		Config:         config,
		RedisClient:    redisClient,
		BackendRepo:    backendRepo,
		ctx:            ctx,
		cancelFunc:     cancel,
		toolRegistry:   tools.NewRegistry(),
		sourceRegistry: sources.NewRegistry(),
		mcpManager:     tools.NewMCPManager(),
		oauthStore:     oauth.NewStore(0), // Default TTL
		googleOAuth:    oauth.NewGoogleClient(config.OAuth.Google),
	}

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
			Format: "${time_rfc3339} ${method} ${uri} ${status} ${latency_human}\n",
		}))
	}

	// CORS
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: g.Config.Gateway.HTTP.CORS.AllowedOrigins,
		AllowHeaders: g.Config.Gateway.HTTP.CORS.AllowedHeaders,
		AllowMethods: g.Config.Gateway.HTTP.CORS.AllowedMethods,
	}))

	e.Use(middleware.Recover())

	g.echo = e
	g.httpServer = &http.Server{
		Addr:    fmt.Sprintf("%s:%d", g.Config.Gateway.HTTP.Host, g.Config.Gateway.HTTP.Port),
		Handler: e,
	}

	g.baseRouteGroup = e.Group(apiv1.HttpServerBaseRoute)
	g.rootRouteGroup = e.Group(apiv1.HttpServerRootRoute)

	// Register API groups (health check works without Redis in local mode)
	apiv1.NewHealthGroup(g.baseRouteGroup.Group("/health"), g.RedisClient)

	return nil
}

func (g *Gateway) initGRPC() error {
	// Create token validator
	// Use CompositeValidator if we have a database backend (for workspace tokens)
	var validator auth.TokenValidator
	if g.BackendRepo != nil {
		validator = auth.NewCompositeValidator(g.Config.Gateway.AuthToken, g.BackendRepo)
	} else {
		validator = auth.NewStaticValidator(g.Config.Gateway.AuthToken)
	}
	authInterceptor := auth.NewInterceptor(validator)

	serverOptions := []grpc.ServerOption{
		grpc.UnaryInterceptor(authInterceptor.Unary()),
		grpc.StreamInterceptor(authInterceptor.Stream()),
		grpc.MaxRecvMsgSize(g.Config.Gateway.GRPC.MaxRecvMsgSize * 1024 * 1024),
		grpc.MaxSendMsgSize(g.Config.Gateway.GRPC.MaxSendMsgSize * 1024 * 1024),
	}

	g.grpcServer = grpc.NewServer(serverOptions...)
	return nil
}

func (g *Gateway) registerServices() error {
	// Initialize scheduler (requires Redis, skip in local mode)
	if g.Config.Scheduler.Enabled && g.RedisClient != nil {
		sched, err := scheduler.NewScheduler(g.ctx, g.Config, g.RedisClient)
		if err != nil {
			return fmt.Errorf("failed to create scheduler: %w", err)
		}
		g.scheduler = sched

		// Start scheduler background processes
		if err := g.scheduler.Start(); err != nil {
			return fmt.Errorf("failed to start scheduler: %w", err)
		}

		// Register scheduler HTTP service
		schedulerService := scheduler.NewSchedulerService(g.scheduler)
		schedulerService.RegisterRoutes(g.baseRouteGroup.Group("/scheduler"))

		log.Info().Msg("scheduler service registered")
	}

	// Register filesystem gRPC service (requires Redis, skip in local mode)
	// Register filesystem service - use Redis in remote mode, memory in local mode
	var filesystemRepo repository.FilesystemRepository
	if g.RedisClient != nil {
		filesystemRepo = repository.NewFilesystemRedisRepository(g.RedisClient)
		log.Info().Msg("filesystem service registered (redis backend)")
	} else {
		filesystemRepo = repository.NewFilesystemMemoryRepository()
		log.Info().Msg("filesystem service registered (memory backend)")
	}
	filesystemService := services.NewFilesystemService(filesystemRepo)
	pb.RegisterFilesystemServiceServer(g.grpcServer, filesystemService)

	// Register context gRPC service (S3-backed file storage)
	if g.Config.Filesystem.Context.Bucket != "" {
		contextService, err := services.NewContextService(g.Config.Filesystem.Context)
		if err != nil {
			log.Warn().Err(err).Msg("failed to create context service - /context will not be available")
		} else {
			g.contextService = contextService
			pb.RegisterContextServiceServer(g.grpcServer, contextService)
			log.Info().Str("bucket", g.Config.Filesystem.Context.Bucket).Msg("context service registered")
		}
	}

	// Register worker gRPC service (for worker-to-gateway communication)
	if g.scheduler != nil {
		workerService := services.NewWorkerService(g.scheduler, g.BackendRepo)
		pb.RegisterWorkerServiceServer(g.grpcServer, workerService)
		log.Info().Msg("worker service registered")
	}

	// Initialize and register tools
	if err := g.initTools(); err != nil {
		return fmt.Errorf("failed to initialize tools: %w", err)
	}

	// Register tools gRPC service (with backend for credential lookups and OAuth refresh)
	var toolService *services.ToolService
	if g.BackendRepo != nil && g.googleOAuth.IsConfigured() {
		toolService = services.NewToolServiceWithOAuth(g.toolRegistry, g.BackendRepo, g.googleOAuth)
	} else if g.BackendRepo != nil {
		toolService = services.NewToolServiceWithBackend(g.toolRegistry, g.BackendRepo)
	} else {
		toolService = services.NewToolService(g.toolRegistry)
	}
	pb.RegisterToolServiceServer(g.grpcServer, toolService)
	log.Info().Msg("tools service registered")

	// Register gateway gRPC service (workspace/member/token/connection management)
	if g.BackendRepo != nil {
		gatewayService := services.NewGatewayService(g.BackendRepo)
		pb.RegisterGatewayServiceServer(g.grpcServer, gatewayService)
		log.Info().Msg("gateway service registered")
	}

	// Register source providers
	g.initSources()

	// Register sources gRPC service (read-only integration access with OAuth refresh)
	var sourceService *services.SourceService
	if g.BackendRepo != nil && g.googleOAuth.IsConfigured() {
		sourceService = services.NewSourceServiceWithOAuth(g.sourceRegistry, g.BackendRepo, g.googleOAuth)
	} else {
		sourceService = services.NewSourceService(g.sourceRegistry, g.BackendRepo)
	}
	pb.RegisterSourceServiceServer(g.grpcServer, sourceService)
	log.Info().Int("providers", len(g.sourceRegistry.List())).Strs("available", g.sourceRegistry.List()).Msg("sources service registered")

	// Register task and workspace APIs (requires Postgres)
	if g.BackendRepo != nil {
		taskQueue := repository.NewRedisTaskQueue(g.RedisClient, "default")

		// Workspaces API (protected by admin token)
		workspacesGroup := g.baseRouteGroup.Group("/workspaces")
		workspacesGroup.Use(g.requireAdminToken())
		apiv1.NewWorkspacesGroup(workspacesGroup, g.BackendRepo)

		// Members API (nested under workspaces)
		apiv1.NewMembersGroup(workspacesGroup.Group("/:workspace_id/members"), g.BackendRepo)

		// Tokens API (nested under workspaces)
		apiv1.NewTokensGroup(workspacesGroup.Group("/:workspace_id/tokens"), g.BackendRepo)

		// Connections API (nested under workspaces)
		apiv1.NewConnectionsGroup(workspacesGroup.Group("/:workspace_id/connections"), g.BackendRepo)

		// Filesystem API (nested under workspaces, supports both admin and workspace tokens)
		filesystemGroup := workspacesGroup.Group("/:workspace_id/fs")
		filesystemGroup.Use(apiv1.NewFilesystemAuthMiddleware(apiv1.FilesystemAuthConfig{
			AdminToken: g.Config.Gateway.AuthToken,
			Backend:    g.BackendRepo,
		}))
		apiv1.NewFilesystemGroup(filesystemGroup, g.BackendRepo, g.contextService, g.sourceRegistry, g.toolRegistry)
		log.Info().Msg("filesystem API registered at /api/v1/workspaces/:workspace_id/fs")

		// Tasks API
		apiv1.NewTasksGroup(g.baseRouteGroup.Group("/tasks"), g.BackendRepo, taskQueue)

		// OAuth API for workspace integrations (gmail, gdrive)
		if g.googleOAuth.IsConfigured() {
			apiv1.NewOAuthGroup(g.baseRouteGroup.Group("/oauth"), g.oauthStore, g.googleOAuth, g.BackendRepo)
			log.Info().Msg("oauth API registered at /api/v1/oauth")
		}

		log.Info().Msg("workspace, members, tokens, connections, and tasks APIs registered")

		// Register admin UI if enabled
		if g.Config.Admin.Enabled {
			admin.NewService(g.Config.Admin, g.BackendRepo).RegisterRoutes(g.echo)
		}
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

	// Start HTTP server
	go func() {
		addr := fmt.Sprintf("%s:%d", g.Config.Gateway.HTTP.Host, g.Config.Gateway.HTTP.Port)
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			log.Error().Err(err).Msg("failed to listen on http")
			return
		}

		if err := g.httpServer.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("http server error")
		}
	}()

	// Start gRPC server
	go func() {
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", g.Config.Gateway.GRPC.Port))
		if err != nil {
			log.Error().Err(err).Msg("failed to listen on grpc")
			return
		}

		if err := g.grpcServer.Serve(lis); err != nil {
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

// requireAdminToken returns middleware that validates the admin token
func (g *Gateway) requireAdminToken() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			// Skip auth if no admin token is configured
			if g.Config.Gateway.AuthToken == "" {
				return next(c)
			}

			token := c.Request().Header.Get("Authorization")
			expected := "Bearer " + g.Config.Gateway.AuthToken
			if token == "" || token != expected {
				log.Debug().
					Str("path", c.Path()).
					Str("token_present", fmt.Sprintf("%v", token != "")).
					Msg("admin token validation failed")
				return c.JSON(http.StatusUnauthorized, map[string]string{
					"error":   "unauthorized",
					"message": "admin token required",
				})
			}
			return next(c)
		}
	}
}

// initSources initializes source providers
func (g *Gateway) initSources() {
	// Register source providers (all use connection-based auth)
	g.sourceRegistry.Register(providers.NewGitHubProvider())
	g.sourceRegistry.Register(providers.NewGmailProvider())
	g.sourceRegistry.Register(providers.NewNotionProvider())
	g.sourceRegistry.Register(providers.NewGDriveProvider())
	log.Debug().Strs("providers", g.sourceRegistry.List()).Msg("source providers registered")
}

// initTools initializes the tool system by loading schemas and registering clients
func (g *Gateway) initTools() error {
	// Register self-registering tools first (from pkg/tools/builtin)
	// These register via init() when the package is imported
	for _, t := range tools.GetRegisteredTools() {
		g.toolRegistry.Register(tools.NewToolAdapter(t))
		log.Debug().Str("tool", t.Name()).Msg("registered self-registering tool")
	}

	// Create client registry for legacy YAML-based tools
	clientRegistry := tools.NewClientRegistry()

	// API key integrations (only register if configured)
	if g.Config.Tools.Integrations.Weather.APIKey != "" {
		clientRegistry.Register(clients.NewWeatherClient(g.Config.Tools.Integrations.Weather.APIKey))
		log.Debug().Msg("weather integration enabled")
	}

	if g.Config.Tools.Integrations.Exa.APIKey != "" {
		clientRegistry.Register(clients.NewExaClient(g.Config.Tools.Integrations.Exa.APIKey))
		log.Debug().Msg("exa integration enabled")
	}

	// Connection-based integrations (always registered, credentials checked at runtime)
	clientRegistry.Register(clients.NewGitHubClient())
	log.Debug().Msg("github integration registered (connection-based)")

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
