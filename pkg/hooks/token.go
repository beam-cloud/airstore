package hooks

import (
	"encoding/json"
	"fmt"
)

// EncodeToken serializes a raw token for storage.
// TODO: replace with real encryption.
func EncodeToken(raw string) ([]byte, error) {
	return json.Marshal(raw)
}

// DecodeToken deserializes a stored token.
func DecodeToken(stored []byte) (string, error) {
	if len(stored) == 0 {
		return "", fmt.Errorf("empty token")
	}
	var token string
	if err := json.Unmarshal(stored, &token); err != nil {
		return "", fmt.Errorf("decode token: %w", err)
	}
	return token, nil
}
