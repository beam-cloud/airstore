package types

import "fmt"

const (
	DefaultGatewayGRPCPort = 1993
	DefaultGatewayHTTPPort = 1994
	DefaultGatewayHost     = "localhost"
)

func ResolveGatewayGRPCPort(port int) int {
	if port != 0 {
		return port
	}
	return DefaultGatewayGRPCPort
}

func ResolveGatewayHTTPPort(port int) int {
	if port != 0 {
		return port
	}
	return DefaultGatewayHTTPPort
}

func DefaultGatewayGRPCAddr() string {
	return fmt.Sprintf("%s:%d", DefaultGatewayHost, DefaultGatewayGRPCPort)
}

func DefaultGatewayHTTPURL() string {
	return fmt.Sprintf("http://%s:%d", DefaultGatewayHost, DefaultGatewayHTTPPort)
}
