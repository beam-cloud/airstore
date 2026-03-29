package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
)

func main() {
	ctx := context.Background()
	cfg := types.PostgresConfig{
		Host:     "localhost",
		Port:     5432,
		User:     "airstore",
		Password: "airstore",
		Database: "airstore",
	}
	if h := os.Getenv("POSTGRES_HOST"); h != "" {
		cfg.Host = h
	}

	backend, err := repository.NewPostgresBackend(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "db connect failed: %v\n", err)
		os.Exit(1)
	}

	contexts, err := types.LoadViewOutputSchemaContexts(ctx, backend, 347, "16101cc1-b294-4cd1-af52-5a1dd4b68f35")
	if err != nil {
		fmt.Fprintf(os.Stderr, "load error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("context count: %d\n", len(contexts))
	for i, c := range contexts {
		b, _ := json.MarshalIndent(c, "", "  ")
		s := string(b)
		if len(s) > 600 {
			s = s[:600] + "..."
		}
		fmt.Printf("--- context %d ---\n%s\n\n", i, s)
	}

	if len(contexts) > 0 {
		pv := types.ViewOutputSchemaPolicyValue(contexts)
		fmt.Printf("ViewOutputSchemaPolicyValue nil? %v\n", pv == nil)
	} else {
		fmt.Println("NO CONTEXTS — this is why view schema is empty on worker")
	}
}
