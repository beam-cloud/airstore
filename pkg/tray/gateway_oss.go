//go:build !managed

package tray

// defaultGateway returns the local gateway for OSS builds.
func defaultGateway() string {
	return "localhost:1993"
}
