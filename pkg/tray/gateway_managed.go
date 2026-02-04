//go:build managed

package tray

// defaultGateway returns the production gateway for managed builds.
func defaultGateway() string {
	return "gateway.airstore.ai:443"
}
