package main

import (
	"os"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	// Initialize logging
	configManager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating config manager")
	}
	config := configManager.GetConfig()
	if config.PrettyLogs {
		log.Logger = log.Logger.Level(zerolog.DebugLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stdout})
	}

	// Set ANTHROPIC_API_KEY from config for BAML inference
	// This must happen before gateway.NewGateway() which imports the BAML package
	if config.Anthropic.APIKey != "" {
		os.Setenv("ANTHROPIC_API_KEY", config.Anthropic.APIKey)
	}

	gw, err := gateway.NewGateway()
	if err != nil {
		log.Fatal().Err(err).Msg("error creating gateway service")
	}

	// Tools are automatically loaded from YAML definitions in gateway.initTools()
	gw.Start()
	log.Info().Msg("Gateway stopped")
}
