//go:build !managed

package cli

// LoadCredentials returns empty string in OSS builds (no login support).
func LoadCredentials() string {
	return ""
}
