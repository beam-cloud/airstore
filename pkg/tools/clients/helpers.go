package clients

import "strings"

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
