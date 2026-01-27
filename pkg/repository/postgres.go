package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/types"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"
	"github.com/rs/zerolog/log"

	// Import migrations to register them with goose
	_ "github.com/beam-cloud/airstore/pkg/repository/backend_postgres_migrations"
)

// PostgresBackend implements BackendRepository using Postgres
type PostgresBackend struct {
	db     *sql.DB
	config types.PostgresConfig
}

// NewPostgresBackend creates a new Postgres backend
func NewPostgresBackend(cfg types.PostgresConfig) (*PostgresBackend, error) {
	// Apply defaults
	if cfg.Host == "" {
		cfg.Host = "localhost"
	}
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.Database == "" {
		cfg.Database = "airstore"
	}
	if cfg.SSLMode == "" {
		cfg.SSLMode = "disable"
	}
	if cfg.MaxOpenConns == 0 {
		cfg.MaxOpenConns = 25
	}
	if cfg.MaxIdleConns == 0 {
		cfg.MaxIdleConns = 5
	}

	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	// Test connection
	if err := db.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	log.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Str("database", cfg.Database).
		Msg("connected to postgres")

	return &PostgresBackend{
		db:     db,
		config: cfg,
	}, nil
}

// DB returns the underlying database connection
func (b *PostgresBackend) DB() *sql.DB {
	return b.db
}

// Close closes the database connection
func (b *PostgresBackend) Close() error {
	return b.db.Close()
}

// Ping checks the database connection
func (b *PostgresBackend) Ping(ctx context.Context) error {
	return b.db.PingContext(ctx)
}

// RunMigrations runs database migrations using goose
func (b *PostgresBackend) RunMigrations() error {
	// Set goose to use postgres dialect
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("failed to set goose dialect: %w", err)
	}

	// Run migrations (goose uses registered migrations from init())
	if err := goose.Up(b.db, "."); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	version, err := goose.GetDBVersion(b.db)
	if err != nil {
		return fmt.Errorf("failed to get migration version: %w", err)
	}

	log.Info().Int64("version", version).Msg("migrations complete")
	return nil
}
