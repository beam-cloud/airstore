//go:build !managed

package desktop

func defaultGateway() string {
	return "localhost:1993"
}
