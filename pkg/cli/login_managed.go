//go:build managed

package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

type deviceAuthResp struct {
	DeviceCode      string `json:"device_code"`
	UserCode        string `json:"user_code"`
	VerificationURI string `json:"verification_uri"`
	ExpiresIn       int    `json:"expires_in"`
	Interval        int    `json:"interval"`
}

type pollResp struct {
	AccessToken string `json:"access_token,omitempty"`
	Email       string `json:"email,omitempty"`
	Error       string `json:"error,omitempty"`
}

func apiBase() string {
	return APIURL()
}

func credPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".airstore", "credentials")
}

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Log in to Airstore",
	Long: `Log in to your Airstore account using your browser.

Your credentials are stored in ~/.airstore/credentials.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		base := apiBase()

		// Request device code
		PrintInfo("Requesting authorization...")
		resp, err := http.Post(base+"/api/auth/device/initiate", "application/json", nil)
		if err != nil {
			PrintFormattedError("Failed to connect", err)
			return nil
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			PrintErrorMsg("Server error - try again later")
			return nil
		}

		var auth deviceAuthResp
		if err := json.NewDecoder(resp.Body).Decode(&auth); err != nil {
			PrintFormattedError("Invalid response", err)
			return nil
		}

		// Display code and open browser
		fmt.Println()
		fmt.Printf("  %s\n\n", BrandStyle.Render("Log in to Airstore"))
		fmt.Printf("  Visit: %s\n", CodeStyle.Render(auth.VerificationURI))
		fmt.Printf("  Code:  %s\n\n", SuccessStyle.Bold(true).Render(auth.UserCode))

		openBrowser(auth.VerificationURI + "?code=" + auth.UserCode)
		fmt.Print("  Waiting for authorization")

		// Poll for authorization
		interval := max(auth.Interval, 2)
		deadline := time.Now().Add(time.Duration(auth.ExpiresIn) * time.Second)

		for time.Now().Before(deadline) {
			time.Sleep(time.Duration(interval) * time.Second)
			fmt.Print(".")

			body := fmt.Sprintf(`{"device_code":"%s"}`, auth.DeviceCode)
			r, err := http.Post(base+"/api/auth/device/poll", "application/json", bytes.NewReader([]byte(body)))
			if err != nil {
				continue
			}

			var p pollResp
			json.NewDecoder(r.Body).Decode(&p)
			r.Body.Close()

			if p.AccessToken != "" {
				fmt.Println(" " + SuccessStyle.Render("✓"))
				return saveAndConfirm(p.AccessToken, p.Email)
			}

			if p.Error != "" && p.Error != "authorization_pending" {
				fmt.Println()
				PrintErrorMsg("Authorization failed: " + p.Error)
				return nil
			}
		}

		fmt.Println()
		PrintErrorMsg("Timed out - run 'airstore login' to retry")
		return nil
	},
}

func saveAndConfirm(token, email string) error {
	path := credPath()
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		PrintFormattedError("Failed to save credentials", err)
		return nil
	}

	data, _ := json.MarshalIndent(map[string]string{"token": token, "email": email}, "", "  ")
	if err := os.WriteFile(path, data, 0600); err != nil {
		PrintFormattedError("Failed to save credentials", err)
		return nil
	}

	fmt.Println()
	if email != "" {
		PrintSuccessWithValue("Logged in as", email)
	} else {
		PrintSuccess("Logged in")
	}

	fmt.Println()
	fmt.Println("  " + DimStyle.Render("Next steps:"))
	fmt.Println("    " + CodeStyle.Render("airstore start") + "   Start with system tray")
	fmt.Println("    " + CodeStyle.Render("airstore mount") + "   Mount filesystem manually")
	return nil
}

// LoadCredentials returns the stored token, or empty string if not found.
func LoadCredentials() string {
	data, err := os.ReadFile(credPath())
	if err != nil {
		return ""
	}
	var creds map[string]string
	if json.Unmarshal(data, &creds) != nil {
		return ""
	}
	return creds["token"]
}

func init() {
	rootCmd.AddCommand(loginCmd)
}
