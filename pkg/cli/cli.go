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

// Production endpoints (used in release builds)
const (
	prodGatewayGRPC = "gateway.airstore.ai:443"
	prodGatewayHTTP = "https://api.airstore.ai"
)

// Local endpoints (used in dev builds or embedded mode)
const (
	localGatewayGRPC = "localhost:1993"
	localGatewayHTTP = "http://localhost:1994"
)

var (
	gatewayAddr     string
	gatewayHTTPAddr string
	authToken       string
	jsonOutput      bool
)

// defaultGRPCAddr returns the default gRPC endpoint based on build type
func defaultGRPCAddr() string {
	if Release == "true" {
		return prodGatewayGRPC
	}
	return localGatewayGRPC
}

// defaultHTTPAddr returns the default HTTP endpoint based on build type
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
		// Set JSON output mode based on flag
		SetJSONOutput(jsonOutput)
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
	PrintError(err)
	os.Exit(1)
}
