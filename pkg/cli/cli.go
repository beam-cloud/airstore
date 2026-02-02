package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// Build information (injected at compile time via ldflags)
var (
	Version = "dev"
	Release = "false" // "true" in release builds, controls logging defaults
)

var (
	gatewayAddr     string
	gatewayHTTPAddr string
	authToken       string
)

var rootCmd = &cobra.Command{
	Use:     "airstore",
	Short:   "Airstore CLI",
	Long:    `Command-line interface for managing workspaces, members, tokens, integrations, and mounting the filesystem.`,
	Version: Version,
}

func init() {
	configureLogging()

	rootCmd.PersistentFlags().StringVar(&gatewayAddr, "gateway", getEnv("AIRSTORE_GATEWAY", "localhost:1993"), "Gateway gRPC address")
	rootCmd.PersistentFlags().StringVar(&gatewayHTTPAddr, "gateway-http", getEnv("AIRSTORE_GATEWAY_HTTP", "http://localhost:1994"), "Gateway HTTP address")
	rootCmd.PersistentFlags().StringVar(&authToken, "token", getEnv("AIRSTORE_TOKEN", ""), "Authentication token")

	rootCmd.AddCommand(workspaceCmd)
	rootCmd.AddCommand(memberCmd)
	rootCmd.AddCommand(tokenCmd)
	rootCmd.AddCommand(connectionCmd)
	rootCmd.AddCommand(taskCmd)
	rootCmd.AddCommand(mountCmd)
}

// configureLogging sets up logging defaults based on build type
func configureLogging() {
	if os.Getenv("BAML_LOG") == "" {
		if Release == "true" {
			os.Setenv("BAML_LOG", "off")
		} else {
			os.Setenv("BAML_LOG", "info")
		}
	}
}

// Execute runs the CLI
func Execute() error {
	return rootCmd.Execute()
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getClient() (*Client, error) {
	return NewClient(gatewayAddr, authToken)
}

func exitError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
