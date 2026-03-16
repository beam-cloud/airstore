package types

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
)

// SchemaSignature produces a short content-addressable hash for any JSON-serializable value.
func SchemaSignature(value any) string {
	raw, err := json.Marshal(value)
	if err != nil || len(raw) == 0 {
		return ""
	}
	sum := sha1.Sum(raw)
	return hex.EncodeToString(sum[:])[:12]
}
