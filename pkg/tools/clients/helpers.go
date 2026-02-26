package clients

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/beam-cloud/airstore/pkg/types"
)

type OAuthCommandHandler func(ctx context.Context, token string, args map[string]any) (any, error)

// GetStringArg extracts a string argument from args map
func GetStringArg(args map[string]any, key, defaultVal string) string {
	if v, ok := args[key].(string); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return defaultVal
}

// GetIntArg extracts an int argument from args map
func GetIntArg(args map[string]any, key string, defaultVal int) int {
	if v, ok := args[key].(int); ok {
		return v
	}
	if v, ok := args[key].(float64); ok {
		return int(v)
	}
	return defaultVal
}

// GetBoolArg extracts a bool argument from args map
func GetBoolArg(args map[string]any, key string, defaultVal bool) bool {
	if v, ok := args[key].(bool); ok {
		return v
	}
	return defaultVal
}

// WriteJSON writes a JSON payload to a tool output stream.
func WriteJSON(w io.Writer, payload any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(payload)
}

// WriteToolError writes a standard structured tool error.
func WriteToolError(w io.Writer, msg string) error {
	return WriteJSON(w, map[string]any{
		"error":   true,
		"message": msg,
	})
}

// RequireAccessToken validates OAuth credentials and returns the access token.
// On failure it writes a structured error to stdout for consistent CLI behavior.
func RequireAccessToken(toolName string, creds *types.IntegrationCredentials, stdout io.Writer) (string, error) {
	if creds == nil || strings.TrimSpace(creds.AccessToken) == "" {
		return "", WriteToolError(stdout, fmt.Sprintf("%s: not connected", toolName))
	}
	return creds.AccessToken, nil
}

// ExecuteOAuthCommand runs a command for OAuth-backed tool clients with
// consistent auth checking, error shaping, and JSON output.
func ExecuteOAuthCommand(
	ctx context.Context,
	toolName string,
	command string,
	args map[string]any,
	creds *types.IntegrationCredentials,
	handlers map[string]OAuthCommandHandler,
	stdout io.Writer,
) error {
	token, err := RequireAccessToken(toolName, creds, stdout)
	if err != nil {
		return err
	}

	handler, ok := handlers[command]
	if !ok {
		return fmt.Errorf("unknown command: %s", command)
	}

	result, err := handler(ctx, token, args)
	if err != nil {
		return WriteToolError(stdout, err.Error())
	}
	return WriteJSON(stdout, result)
}

func RequireStringArgs(args map[string]any, keys ...string) (map[string]string, error) {
	values := make(map[string]string, len(keys))
	missing := make([]string, 0, len(keys))
	for _, key := range keys {
		value := GetStringArg(args, key, "")
		if value == "" {
			missing = append(missing, key)
			continue
		}
		values[key] = value
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("%s", requiredArgsMessage(missing))
	}
	return values, nil
}

func RequirePositiveIntArg(args map[string]any, key string) (int, error) {
	value := GetIntArg(args, key, 0)
	if value <= 0 {
		return 0, fmt.Errorf("%s is required", key)
	}
	return value, nil
}

func requiredArgsMessage(keys []string) string {
	if len(keys) == 0 {
		return "required argument is missing"
	}
	if len(keys) == 1 {
		return keys[0] + " is required"
	}
	if len(keys) == 2 {
		return keys[0] + " and " + keys[1] + " are required"
	}
	return strings.Join(keys[:len(keys)-1], ", ") + ", and " + keys[len(keys)-1] + " are required"
}
