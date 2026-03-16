package common

import (
	"context"
	"fmt"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type MongoClient struct {
	client *mongo.Client
	db     *mongo.Database
}

func NewMongoClient(cfg types.MongoConfig) (*MongoClient, error) {
	if cfg.URI == "" {
		return nil, fmt.Errorf("mongo URI is empty")
	}

	opts := options.Client().ApplyURI(cfg.URI)
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("mongo connect: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx, nil); err != nil {
		disconnectCtx, disconnectCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer disconnectCancel()
		if disconnectErr := client.Disconnect(disconnectCtx); disconnectErr != nil {
			log.Warn().Err(disconnectErr).Msg("mongo disconnect after ping failure")
		}
		return nil, fmt.Errorf("mongo ping: %w", err)
	}

	dbName := cfg.Database
	if dbName == "" {
		return nil, fmt.Errorf("mongo database is empty")
	}

	log.Info().Str("database", dbName).Msg("MongoDB connected")
	return &MongoClient{client: client, db: client.Database(dbName)}, nil
}

func (m *MongoClient) Collection(name string) *mongo.Collection {
	return m.db.Collection(name)
}

func (m *MongoClient) Close(ctx context.Context) error {
	return m.client.Disconnect(ctx)
}
