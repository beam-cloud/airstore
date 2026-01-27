package common

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// GenerateID generates a unique ID with the given prefix.
// Format: prefix-timestamp-random
func GenerateID(prefix string) string {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	randomBytes := make([]byte, 4)
	rand.Read(randomBytes)
	random := hex.EncodeToString(randomBytes)
	return fmt.Sprintf("%s-%d-%s", prefix, timestamp, random)
}

// GenerateSessionID generates a unique session ID.
func GenerateSessionID() string {
	return GenerateID("sess")
}

// GenerateWorkerID generates a unique worker ID.
func GenerateWorkerID() string {
	return GenerateID("wkr")
}

// GenerateRandomID generates a random ID of the specified length.
func GenerateRandomID(length int) string {
	bytes := make([]byte, length/2+1)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)[:length]
}
