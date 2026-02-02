package cli

import (
	"fmt"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCErrorMessages maps gRPC error codes to human-readable messages
var RPCErrorMessages = map[codes.Code]string{
	codes.Unauthenticated:  "Authentication failed - invalid or missing token",
	codes.PermissionDenied: "Access denied - you don't have permission for this action",
	codes.NotFound:         "Resource not found",
	codes.AlreadyExists:    "Resource already exists",
	codes.InvalidArgument:  "Invalid request parameters",
	codes.Unavailable:      "Service unavailable - the gateway may be down or unreachable",
	codes.DeadlineExceeded: "Request timed out",
	codes.ResourceExhausted: "Rate limit exceeded - please try again later",
	codes.Internal:         "Internal server error",
	codes.Unimplemented:    "This feature is not yet available",
	codes.Canceled:         "Request was canceled",
	codes.FailedPrecondition: "Operation cannot be performed in current state",
	codes.Aborted:          "Operation was aborted",
	codes.OutOfRange:       "Value out of valid range",
	codes.DataLoss:         "Unrecoverable data loss or corruption",
}

// RPCErrorSuggestions provides helpful suggestions for specific error codes
var RPCErrorSuggestions = map[codes.Code][]string{
	codes.Unauthenticated: {
		"Check that your token is correct: " + CodeStyle.Render("--token <token>"),
		"Generate a new token in the dashboard if needed",
	},
	codes.PermissionDenied: {
		"Verify you have access to this workspace",
		"Check with your team admin for permissions",
	},
	codes.Unavailable: {
		"Check that the gateway is running",
		"Verify the gateway address: " + CodeStyle.Render("--gateway <addr>"),
		"Try again in a few moments",
	},
	codes.DeadlineExceeded: {
		"The server may be under heavy load",
		"Try again with a simpler request",
	},
}

// rpcErrorRegex matches "rpc error: code = X desc = Y" format
var rpcErrorRegex = regexp.MustCompile(`rpc error: code = (\w+) desc = (.+)`)

// FormatError converts an error to a human-readable message.
// It handles gRPC errors specially, converting them to friendly messages.
func FormatError(err error) string {
	if err == nil {
		return ""
	}

	// Try to extract gRPC status
	if st, ok := status.FromError(err); ok {
		return formatGRPCError(st)
	}

	// Try to parse the error string for RPC errors
	errStr := err.Error()
	if matches := rpcErrorRegex.FindStringSubmatch(errStr); len(matches) == 3 {
		codeName := matches[1]
		desc := matches[2]
		if code, ok := parseCodeName(codeName); ok {
			if msg, exists := RPCErrorMessages[code]; exists {
				// Include original description if it adds context
				if desc != "" && !strings.Contains(strings.ToLower(msg), strings.ToLower(desc)) {
					return fmt.Sprintf("%s (%s)", msg, desc)
				}
				return msg
			}
		}
		// Fallback: just return the description part
		return desc
	}

	// For non-RPC errors, clean up common patterns
	return cleanErrorMessage(errStr)
}

// formatGRPCError formats a gRPC status to a friendly message
func formatGRPCError(st *status.Status) string {
	if msg, ok := RPCErrorMessages[st.Code()]; ok {
		desc := st.Message()
		// Include original description if it provides extra context
		if desc != "" && !strings.Contains(strings.ToLower(msg), strings.ToLower(desc)) {
			return fmt.Sprintf("%s (%s)", msg, desc)
		}
		return msg
	}
	return st.Message()
}

// GetErrorSuggestions returns helpful suggestions for an error
func GetErrorSuggestions(err error) []string {
	if err == nil {
		return nil
	}

	// Try to extract gRPC status
	if st, ok := status.FromError(err); ok {
		if suggestions, exists := RPCErrorSuggestions[st.Code()]; exists {
			return suggestions
		}
	}

	// Try to parse the error string
	errStr := err.Error()
	if matches := rpcErrorRegex.FindStringSubmatch(errStr); len(matches) == 3 {
		codeName := matches[1]
		if code, ok := parseCodeName(codeName); ok {
			if suggestions, exists := RPCErrorSuggestions[code]; exists {
				return suggestions
			}
		}
	}

	return nil
}

// parseCodeName converts a gRPC code name string to a codes.Code
func parseCodeName(name string) (codes.Code, bool) {
	codeMap := map[string]codes.Code{
		"OK":                  codes.OK,
		"Canceled":            codes.Canceled,
		"Unknown":             codes.Unknown,
		"InvalidArgument":     codes.InvalidArgument,
		"DeadlineExceeded":    codes.DeadlineExceeded,
		"NotFound":            codes.NotFound,
		"AlreadyExists":       codes.AlreadyExists,
		"PermissionDenied":    codes.PermissionDenied,
		"ResourceExhausted":   codes.ResourceExhausted,
		"FailedPrecondition":  codes.FailedPrecondition,
		"Aborted":             codes.Aborted,
		"OutOfRange":          codes.OutOfRange,
		"Unimplemented":       codes.Unimplemented,
		"Internal":            codes.Internal,
		"Unavailable":         codes.Unavailable,
		"DataLoss":            codes.DataLoss,
		"Unauthenticated":     codes.Unauthenticated,
	}
	code, ok := codeMap[name]
	return code, ok
}

// cleanErrorMessage cleans up common error message patterns
func cleanErrorMessage(msg string) string {
	// Remove redundant prefixes
	msg = strings.TrimPrefix(msg, "error: ")
	msg = strings.TrimPrefix(msg, "Error: ")
	
	// Clean up wrapped errors
	if strings.Contains(msg, ": ") {
		// For deeply nested errors, just show the most relevant part
		parts := strings.Split(msg, ": ")
		if len(parts) > 3 {
			// Keep first and last parts
			msg = parts[0] + ": " + parts[len(parts)-1]
		}
	}
	
	return msg
}

// PrintFormattedError prints an error with styling and optional suggestions
func PrintFormattedError(title string, err error) {
	fmt.Println()
	PrintErrorMsg(title)
	
	if err != nil {
		friendlyMsg := FormatError(err)
		fmt.Printf("  %s\n", DimStyle.Render(friendlyMsg))
		
		// Show suggestions if available
		if suggestions := GetErrorSuggestions(err); len(suggestions) > 0 {
			fmt.Println()
			PrintSuggestions("Suggestions:", suggestions)
		}
	}
	fmt.Println()
}
