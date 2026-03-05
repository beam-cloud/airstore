//go:build !managed

package tray

import "github.com/beam-cloud/airstore/pkg/types"

// defaultGateway returns the local gateway for OSS builds.
func defaultGateway() string {
	return types.DefaultGatewayGRPCAddr()
}
