package hooks

import (
	"encoding/json"
	"fmt"
)

// DecryptToken decodes an encrypted token. Currently JSON-encoded (matching
// IntegrationConnection.Credentials pattern). TODO: real encryption.
func DecryptToken(encrypted []byte) (string, error) {
	if len(encrypted) == 0 {
		return "", fmt.Errorf("empty token")
	}
	var token string
	if err := json.Unmarshal(encrypted, &token); err != nil {
		return string(encrypted), nil
	}
	return token, nil
}

// EncryptToken encodes a raw token for storage. Currently JSON-encoded.
// TODO: real encryption.
func EncryptToken(raw string) ([]byte, error) {
	return json.Marshal(raw)
}
