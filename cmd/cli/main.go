package main

import (
	"fmt"
	"os"

	"github.com/beam-cloud/airstore/pkg/cli"
)

func main() {
	if err := cli.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
