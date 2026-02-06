package cli

import (
	"fmt"
	"os"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"
)

// Build information (injected at compile time via ldflags)
var (
	Version = "dev"
	Release = "false" // "true" in release builds
)

// Endpoints - edit these to change where the CLI points
const (
	prodDashboard   = "https://app.airstore.ai"
	prodAPI         = "https://internal-api.airstore.ai"
	prodGatewayGRPC = "gateway.airstore.ai:443"
	prodGatewayHTTP = "https://api.airstore.ai"

	localDashboard   = "http://localhost:3001"
	localAPI         = "http://localhost:8113"
	localGatewayGRPC = "localhost:1993"
	localGatewayHTTP = "http://localhost:1994"
)

var (
	gatewayAddr     string
	gatewayHTTPAddr string
	authToken       string
	jsonOutput      bool
)

// DashboardURL returns the dashboard URL based on build type
func DashboardURL() string {
	if Release == "true" {
		return prodDashboard
	}
	return localDashboard
}

// APIURL returns the backend API URL based on build type
func APIURL() string {
	if Release == "true" {
		return prodAPI
	}
	return localAPI
}

func defaultGRPCAddr() string {
	if Release == "true" {
		return prodGatewayGRPC
	}
	return localGatewayGRPC
}

func defaultHTTPAddr() string {
	if Release == "true" {
		return prodGatewayHTTP
	}
	return localGatewayHTTP
}

// Custom help template with styled output
var helpTemplate = `{{with .Long}}{{. | trim}}

{{end}}{{if .HasAvailableSubCommands}}` + `{{.CommandPath}}` + ` ` + `<command>` + `

{{end}}{{if .HasAvailableSubCommands}}Commands:
{{range .Commands}}{{if .IsAvailableCommand}}  {{rpad .Name .NamePadding }}  {{.Short}}
{{end}}{{end}}{{end}}{{if .HasAvailableLocalFlags}}
Flags:
{{.LocalFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasAvailableInheritedFlags}}

Global Flags:
{{.InheritedFlags.FlagUsages | trimTrailingWhitespaces}}{{end}}{{if .HasExample}}

Examples:
{{.Example}}{{end}}
`

var rootCmd = &cobra.Command{
	Use:   "airstore",
	Short: "Virtual filesystem for AI agents",
	Long: lipgloss.NewStyle().Foreground(ColorPrimary).Bold(true).Render("airstore") + ` - Virtual filesystem for AI agents

Mount a virtual filesystem that provides tools, integrations, and context
for AI agents to interact with the world.`,
	Version: Version,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		SetJSONOutput(jsonOutput)

		// Auto-load credentials if no token provided
		if authToken == "" {
			authToken = LoadCredentials()
		}
	},
}

func init() {
	// Set custom templates
	rootCmd.SetHelpTemplate(helpTemplate)

	// Version template
	rootCmd.SetVersionTemplate(fmt.Sprintf("  %s version %s\n", BrandStyle.Render("airstore"), Version))

	rootCmd.PersistentFlags().StringVar(&gatewayAddr, "gateway", getEnv("AIRSTORE_GATEWAY", defaultGRPCAddr()), "Gateway gRPC address")
	rootCmd.PersistentFlags().StringVar(&gatewayHTTPAddr, "gateway-http", getEnv("AIRSTORE_GATEWAY_HTTP", defaultHTTPAddr()), "Gateway HTTP address")
	rootCmd.PersistentFlags().StringVar(&authToken, "token", getEnv("AIRSTORE_TOKEN", ""), "Authentication token")
	rootCmd.PersistentFlags().BoolVar(&jsonOutput, "json", false, "Output in JSON format")

	rootCmd.AddCommand(workspaceCmd)
	rootCmd.AddCommand(memberCmd)
	rootCmd.AddCommand(tokenCmd)
	rootCmd.AddCommand(connectionCmd)
	rootCmd.AddCommand(taskCmd)
	rootCmd.AddCommand(hookCmd)
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
	PrintError(err)
	os.Exit(1)
}
