package common

import (
	"crypto/tls"
	"net"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// NeedsTLS determines if TLS should be used for the given address.
//
// The logic follows grpcurl's convention:
//   - Default to TLS for external/production addresses
//   - Use plaintext only for explicitly local addresses
//
// TLS is disabled (plaintext) for:
//   - localhost, 127.0.0.1, ::1, *.local, *.localhost
//   - Kubernetes internal addresses (*.svc, *.svc.cluster.local)
//
// TLS is enabled for everything else (production default).
func NeedsTLS(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port specified - addr is just the host
		host = addr
	}

	// Local/internal addresses → plaintext
	if isLocalAddress(host) {
		return false
	}

	// Everything else → TLS (secure by default)
	return true
}

// isLocalAddress returns true for addresses that should use plaintext.
func isLocalAddress(host string) bool {
	host = strings.ToLower(host)

	// Standard localhost addresses
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || host == "[::1]" {
		return true
	}

	// Local domain suffixes
	if strings.HasSuffix(host, ".local") || strings.HasSuffix(host, ".localhost") {
		return true
	}

	// Kubernetes internal service addresses
	if strings.HasSuffix(host, ".svc") || strings.HasSuffix(host, ".svc.cluster.local") {
		return true
	}

	// Private IP check (optional, for internal networks)
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate()
	}

	return false
}

// TransportCredentials returns appropriate gRPC credentials for the address.
// Uses TLS by default, plaintext only for local/internal addresses.
func TransportCredentials(addr string) credentials.TransportCredentials {
	if NeedsTLS(addr) {
		return credentials.NewTLS(&tls.Config{})
	}
	return insecure.NewCredentials()
}

// InsecureCredentials returns plaintext credentials (no TLS).
// Use this when the --insecure flag is explicitly set.
func InsecureCredentials() credentials.TransportCredentials {
	return insecure.NewCredentials()
}

// TLSCredentials returns TLS credentials with system CAs.
func TLSCredentials() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{})
}
