package types

import (
	"time"
)

// Mode constants for gateway operation
const (
	ModeLocal  = "local"  // No Redis/Postgres, S3 direct
	ModeRemote = "remote" // Full infrastructure
)

// AppConfig is the root configuration for the airstore gateway
type AppConfig struct {
	Mode       string `key:"mode" json:"mode"` // "local" or "remote"
	DebugMode  bool   `key:"debugMode" json:"debug_mode"`
	PrettyLogs bool   `key:"prettyLogs" json:"pretty_logs"`

	ClusterName string           `key:"clusterName" json:"cluster_name"`
	Database    DatabaseConfig   `key:"database" json:"database"`
	Image       ImageConfig      `key:"image" json:"image"`
	Filesystem  FilesystemConfig `key:"filesystem" json:"filesystem"`
	Gateway     GatewayConfig    `key:"gateway" json:"gateway"`
	Scheduler   SchedulerConfig  `key:"scheduler" json:"scheduler"`
	Tools       ToolsConfig      `key:"tools" json:"tools"`
	Admin       AdminConfig      `key:"admin" json:"admin"`
	OAuth       IntegrationOAuth `key:"oauth" json:"oauth"` // OAuth for workspace integrations (gmail, gdrive)
}

// IsLocalMode returns true if running in local mode (no Redis/Postgres)
func (c *AppConfig) IsLocalMode() bool {
	return c.Mode == ModeLocal
}

// ----------------------------------------------------------------------------
// Database Configuration
// ----------------------------------------------------------------------------

type DatabaseConfig struct {
	Redis    RedisConfig    `key:"redis" json:"redis"`
	Postgres PostgresConfig `key:"postgres" json:"postgres"`
}

type RedisMode string

const (
	RedisModeSingle  RedisMode = "single"
	RedisModeCluster RedisMode = "cluster"
)

type RedisConfig struct {
	Mode               RedisMode     `key:"mode" json:"mode"`
	Addrs              []string      `key:"addrs" json:"addrs"`
	Username           string        `key:"username" json:"username"`
	Password           string        `key:"password" json:"password"`
	ClientName         string        `key:"clientName" json:"client_name"`
	EnableTLS          bool          `key:"enableTLS" json:"enable_tls"`
	InsecureSkipVerify bool          `key:"insecureSkipVerify" json:"insecure_skip_verify"`
	PoolSize           int           `key:"poolSize" json:"pool_size"`
	MinIdleConns       int           `key:"minIdleConns" json:"min_idle_conns"`
	MaxIdleConns       int           `key:"maxIdleConns" json:"max_idle_conns"`
	ConnMaxIdleTime    time.Duration `key:"connMaxIdleTime" json:"conn_max_idle_time"`
	ConnMaxLifetime    time.Duration `key:"connMaxLifetime" json:"conn_max_lifetime"`
	DialTimeout        time.Duration `key:"dialTimeout" json:"dial_timeout"`
	ReadTimeout        time.Duration `key:"readTimeout" json:"read_timeout"`
	WriteTimeout       time.Duration `key:"writeTimeout" json:"write_timeout"`
	MaxRedirects       int           `key:"maxRedirects" json:"max_redirects"`
	MaxRetries         int           `key:"maxRetries" json:"max_retries"`
	RouteByLatency     bool          `key:"routeByLatency" json:"route_by_latency"`
}

type PostgresConfig struct {
	Host            string        `key:"host" json:"host"`
	Port            int           `key:"port" json:"port"`
	User            string        `key:"user" json:"user"`
	Password        string        `key:"password" json:"password"`
	Database        string        `key:"database" json:"database"`
	SSLMode         string        `key:"sslMode" json:"ssl_mode"`
	MaxOpenConns    int           `key:"maxOpenConns" json:"max_open_conns"`
	MaxIdleConns    int           `key:"maxIdleConns" json:"max_idle_conns"`
	ConnMaxLifetime time.Duration `key:"connMaxLifetime" json:"conn_max_lifetime"`
}

// ----------------------------------------------------------------------------
// Storage Configuration
// ----------------------------------------------------------------------------

type S3Config struct {
	Bucket         string `key:"bucket" json:"bucket"`
	Region         string `key:"region" json:"region"`
	Endpoint       string `key:"endpoint" json:"endpoint"`
	AccessKey      string `key:"accessKey" json:"access_key"`
	SecretKey      string `key:"secretKey" json:"secret_key"`
	ForcePathStyle bool   `key:"forcePathStyle" json:"force_path_style"`
}

type ImageConfig struct {
	S3        S3Config `key:"s3" json:"s3"`
	CachePath string   `key:"cachePath" json:"cache_path"`
	WorkPath  string   `key:"workPath" json:"work_path"`
	MountPath string   `key:"mountPath" json:"mount_path"`
}

type FilesystemConfig struct {
	MountPoint string   `key:"mountPoint" json:"mount_point"`
	Verbose    bool     `key:"verbose" json:"verbose"`
	Context    S3Config `key:"context" json:"context"`
}

// ----------------------------------------------------------------------------
// Gateway Configuration
// ----------------------------------------------------------------------------

type GatewayConfig struct {
	GRPC            GRPCConfig    `key:"grpc" json:"grpc"`
	HTTP            HTTPConfig    `key:"http" json:"http"`
	ShutdownTimeout time.Duration `key:"shutdownTimeout" json:"shutdown_timeout"`
	AuthToken       string        `key:"authToken" json:"auth_token"`
}

type GRPCConfig struct {
	Port           int `key:"port" json:"port"`
	MaxRecvMsgSize int `key:"maxRecvMsgSize" json:"max_recv_msg_size"`
	MaxSendMsgSize int `key:"maxSendMsgSize" json:"max_send_msg_size"`
}

type HTTPConfig struct {
	Host             string     `key:"host" json:"host"`
	Port             int        `key:"port" json:"port"`
	EnablePrettyLogs bool       `key:"enablePrettyLogs" json:"enable_pretty_logs"`
	CORS             CORSConfig `key:"cors" json:"cors"`
}

type CORSConfig struct {
	AllowedOrigins []string `key:"allowOrigins" json:"allow_origins"`
	AllowedMethods []string `key:"allowMethods" json:"allow_methods"`
	AllowedHeaders []string `key:"allowHeaders" json:"allow_headers"`
}

// ----------------------------------------------------------------------------
// Tools Configuration
// ----------------------------------------------------------------------------

// ToolsConfig configures all tool sources: builtin integrations and MCP servers
type ToolsConfig struct {
	// Builtin API-key integrations
	Integrations IntegrationsConfig `key:"integrations" json:"integrations"`

	// External MCP servers (tools auto-discovered and exposed as POSIX commands)
	MCP map[string]MCPServerConfig `key:"mcp" json:"mcp"`
}

// IntegrationsConfig configures builtin tools that require API keys
type IntegrationsConfig struct {
	Weather IntegrationAPIKey `key:"weather" json:"weather"`
	Exa     IntegrationAPIKey `key:"exa" json:"exa"`
	GitHub  GitHubConfig      `key:"github" json:"github"`
}

// IntegrationAPIKey is a simple API key configuration
type IntegrationAPIKey struct {
	APIKey string `key:"apiKey" json:"api_key"`
}

// GitHubConfig configures GitHub integration
type GitHubConfig struct {
	Enabled bool `key:"enabled" json:"enabled"`
}

// MCPServerConfig defines an MCP server to spawn or connect to.
// Set Command for local stdio servers, or URL for remote HTTP/SSE servers.
type MCPServerConfig struct {
	// Local (stdio) server - spawn process
	Command    string            `key:"command" json:"command,omitempty"` // Executable (npx, uvx, python)
	Args       []string          `key:"args" json:"args,omitempty"`       // Command arguments
	Env        map[string]string `key:"env" json:"env,omitempty"`         // Environment variables
	WorkingDir string            `key:"cwd" json:"cwd,omitempty"`         // Working directory

	// Remote (HTTP/SSE) server - connect to URL
	URL string `key:"url" json:"url,omitempty"`

	// Auth for both local (env vars) and remote (HTTP headers)
	Auth *MCPAuthConfig `key:"auth" json:"auth,omitempty"`
}

// IsRemote returns true if this is a remote HTTP/SSE server (URL is set)
func (c MCPServerConfig) IsRemote() bool {
	return c.URL != ""
}

// MCPAuthConfig configures authentication for an MCP server.
// For stdio servers, auth is passed via environment variables.
// For remote servers, auth is passed via HTTP headers.
type MCPAuthConfig struct {
	// Token is the bearer token for authentication.
	// For stdio: passed as MCP_AUTH_TOKEN env var (or custom var via TokenEnv)
	// For remote: passed as Authorization: Bearer <token> header
	Token string `key:"token" json:"token,omitempty"`

	// TokenEnv specifies a custom env var name for the token (stdio only, default: MCP_AUTH_TOKEN)
	TokenEnv string `key:"tokenEnv" json:"token_env,omitempty"`

	// Headers are additional auth headers.
	// For stdio: passed as MCP_AUTH_HEADER_<NAME> env vars
	// For remote: passed directly as HTTP headers
	Headers map[string]string `key:"headers" json:"headers,omitempty"`
}

// ----------------------------------------------------------------------------
// Admin UI Configuration
// ----------------------------------------------------------------------------

// AdminConfig configures the admin UI
type AdminConfig struct {
	Enabled     bool        `key:"enabled" json:"enabled"`
	SessionKey  string      `key:"sessionKey" json:"session_key"` // Secret for JWT signing
	OAuth       OAuthConfig `key:"oauth" json:"oauth"`
}

// OAuthConfig configures OAuth providers
type OAuthConfig struct {
	Google GoogleOAuthConfig `key:"google" json:"google"`
}

// GoogleOAuthConfig configures Google OAuth
type GoogleOAuthConfig struct {
	ClientID      string   `key:"clientId" json:"client_id"`
	ClientSecret  string   `key:"clientSecret" json:"client_secret"`
	RedirectURL   string   `key:"redirectUrl" json:"redirect_url"` // e.g., http://localhost:1994/auth/google/callback
	AllowedEmails []string `key:"allowedEmails" json:"allowed_emails"` // Optional whitelist, empty = allow all
}

// ----------------------------------------------------------------------------
// Integration OAuth Configuration (for workspace connections)
// ----------------------------------------------------------------------------

// IntegrationOAuth configures OAuth for workspace integrations (gmail, gdrive, etc.)
// This is separate from admin.oauth which is for admin UI login only.
type IntegrationOAuth struct {
	Google IntegrationGoogleOAuth `key:"google" json:"google"`
}

// IntegrationGoogleOAuth configures Google OAuth for workspace integrations
type IntegrationGoogleOAuth struct {
	ClientID     string `key:"clientId" json:"client_id"`
	ClientSecret string `key:"clientSecret" json:"client_secret"`
	RedirectURL  string `key:"redirectUrl" json:"redirect_url"` // e.g., http://localhost:1994/api/v1/oauth/google/callback
}

