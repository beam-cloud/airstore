package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	gatewayAddr     string
	gatewayHTTPAddr string
	authToken       string
)

var rootCmd = &cobra.Command{
	Use:   "cli",
	Short: "Airstore CLI",
	Long:  `Command-line interface for managing workspaces, members, tokens, integrations, and mounting the filesystem.`,
}

func init() {
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
