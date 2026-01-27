package main

import (
	"os"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/beam-cloud/airstore/pkg/worker"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}
	config := configManager.GetConfig()

	// Setup logging
	if config.PrettyLogs {
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	notFoundErr := &types.ErrWorkerNotFound{}
	w, err := worker.NewWorker()
	if err != nil {
		if notFoundErr.From(err) {
			log.Info().Msg("worker not found, shutting down")
			return
		}
		log.Fatal().Err(err).Msg("worker initialization failed")
	}

	err = w.Run()
	if err != nil {
		if notFoundErr.From(err) {
			log.Info().Msg("worker not found, shutting down")
			return
		}
		log.Fatal().Err(err).Msg("worker failed to run")
	}
}
