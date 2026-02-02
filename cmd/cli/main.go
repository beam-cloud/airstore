package main

import (
	"fmt"
	"os"

	// Import init package first to set up environment defaults before BAML loads
	_ "github.com/beam-cloud/airstore/internal/init"

	"github.com/beam-cloud/airstore/pkg/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
