package cli

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var readCmd = &cobra.Command{
	Use:   "read <airstore://slug/path>",
	Short: "Read content from an airstore:// URI",
	Long: `Read raw file content from a public workspace using an airstore:// URI.

The URI format is: airstore://{slug}/{path}

Output goes to stdout so you can pipe it or redirect it.

Examples:
  airstore read airstore://luke/skills/email-triage/SKILL.md
  airstore read airstore://luke/memory/email-triage/2026-02-08.md
  airstore read airstore://luke/memory/email-triage/2026-02-08.md > brief.md`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		uri := args[0]

		slug, path, err := parseAirstoreURI(uri)
		if err != nil {
			PrintErrorMsg(err.Error())
			return nil
		}

		// Fetch from the public raw read endpoint
		url := gatewayHTTPAddr + "/r/" + slug + "/" + path
		resp, err := http.Get(url)
		if err != nil {
			PrintError(err)
			return nil
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			PrintErrorMsg("not found: " + uri)
			return nil
		}
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			PrintErrorMsg(fmt.Sprintf("server returned %d: %s", resp.StatusCode, string(body)))
			return nil
		}

		_, err = io.Copy(os.Stdout, resp.Body)
		if err != nil {
			PrintError(err)
		}
		return nil
	},
}

// parseAirstoreURI parses an airstore://{slug}/{path} URI.
func parseAirstoreURI(uri string) (slug, path string, err error) {
	const prefix = "airstore://"
	if !strings.HasPrefix(uri, prefix) {
		return "", "", fmt.Errorf("invalid URI: must start with airstore://")
	}

	rest := strings.TrimPrefix(uri, prefix)
	parts := strings.SplitN(rest, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		return "", "", fmt.Errorf("invalid URI: missing slug")
	}

	slug = parts[0]
	if len(parts) > 1 {
		path = parts[1]
	}

	return slug, path, nil
}

func init() {
	rootCmd.AddCommand(readCmd)
}
