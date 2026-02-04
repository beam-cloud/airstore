package worker

import (
	"bufio"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/beam-cloud/airstore/pkg/gateway"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/coreos/go-iptables/iptables"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	bridgeName     = "airstore_br0"
	vethHostPrefix = "as_vh_"
	vethContPrefix = "as_vc_"
)

// NetworkManager handles container networking with dual-stack NAT.
// IP allocation is coordinated via gRPC with the gateway.
type NetworkManager struct {
	ctx      context.Context
	workerID string
	client   *gateway.GatewayClient
	extIface netlink.Link
	ipt      *iptables.IPTables
	ipt6     *iptables.IPTables // nil if IPv6 not available
	mu       sync.Mutex
}

func NewNetworkManager(ctx context.Context, workerID string, client *gateway.GatewayClient) (*NetworkManager, error) {
	extIface, err := defaultInterface()
	if err != nil {
		return nil, fmt.Errorf("default interface: %w", err)
	}

	// Detect iptables mode (nftables vs legacy)
	ipv4Path, ipv6Path := detectIptablesMode()

	ipt, err := iptables.New(iptables.Path(ipv4Path), iptables.IPFamily(iptables.ProtocolIPv4))
	if err != nil {
		return nil, fmt.Errorf("iptables: %w", err)
	}

	// Initialize ip6tables for IPv6 support (graceful fallback)
	var ipt6 *iptables.IPTables
	ipt6, err = iptables.New(iptables.Path(ipv6Path), iptables.IPFamily(iptables.ProtocolIPv6))
	if err != nil {
		log.Warn().Err(err).Msg("IPv6 iptables init failed, falling back to IPv4 only")
	} else {
		// Verify NAT table is accessible
		if _, err := ipt6.List("nat", "POSTROUTING"); err != nil {
			log.Warn().Err(err).Msg("IPv6 NAT table not available, falling back to IPv4 only")
			ipt6 = nil
		}
	}

	m := &NetworkManager{
		ctx:      ctx,
		workerID: workerID,
		client:   client,
		extIface: extIface,
		ipt:      ipt,
		ipt6:     ipt6,
	}

	if err := m.ensureBridge(); err != nil {
		return nil, fmt.Errorf("bridge setup: %w", err)
	}

	ipv6Status := "disabled"
	if ipt6 != nil {
		ipv6Status = "enabled"
	}
	log.Info().
		Str("bridge", bridgeName).
		Str("subnet_v4", types.DefaultSubnet).
		Str("subnet_v6", types.DefaultSubnetIPv6).
		Str("ipv6", ipv6Status).
		Msg("network ready")
	return m, nil
}

// detectIptablesMode returns paths for iptables/ip6tables based on host config.
func detectIptablesMode() (ipv4Path, ipv6Path string) {
	// Check if nftables is in use by looking for KUBE-FORWARD chain
	iptNft, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4), iptables.Path("/usr/sbin/iptables-nft"))
	if err == nil {
		if exists, _ := iptNft.ChainExists("filter", "KUBE-FORWARD"); exists {
			return "/usr/sbin/iptables-nft", "/usr/sbin/ip6tables-nft"
		}
	}

	iptLegacy, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4), iptables.Path("/usr/sbin/iptables-legacy"))
	if err == nil {
		if exists, _ := iptLegacy.ChainExists("filter", "KUBE-FORWARD"); exists {
			return "/usr/sbin/iptables-legacy", "/usr/sbin/ip6tables-legacy"
		}
	}

	// Default
	return "/usr/sbin/iptables", "/usr/sbin/ip6tables"
}

// Setup creates network namespace and veth pair for a sandbox.
// Returns the allocated IP address.
func (m *NetworkManager) Setup(sandboxID string, spec *specs.Spec) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	// Allocate IP from gateway
	alloc, err := m.client.AllocateIP(m.ctx, sandboxID, m.workerID)
	if err != nil {
		return "", fmt.Errorf("allocate IP: %w", err)
	}

	ip, err := m.setup(sandboxID, alloc, spec)
	if err != nil {
		m.client.ReleaseIP(m.ctx, sandboxID)
		return "", err
	}

	log.Info().Str("sandbox", sandboxID).Str("ip", ip).Msg("network configured")
	return ip, nil
}

func (m *NetworkManager) setup(sandboxID string, alloc *types.IPAllocation, spec *specs.Spec) (string, error) {
	vethHost, vethCont := m.vethNames(sandboxID)

	hostNS, err := netns.Get()
	if err != nil {
		return "", fmt.Errorf("get host ns: %w", err)
	}
	defer hostNS.Close()

	// Create and configure veth pair
	if err := m.createVeth(vethHost, vethCont); err != nil {
		return "", fmt.Errorf("create veth: %w", err)
	}

	if err := m.attachToBridge(vethHost); err != nil {
		return "", fmt.Errorf("attach to bridge: %w", err)
	}

	// Create container network namespace
	contNS, err := netns.NewNamed(sandboxID)
	if err != nil {
		return "", fmt.Errorf("create netns: %w", err)
	}
	defer contNS.Close()

	// Return to host NS for veth operations
	if err := netns.Set(hostNS); err != nil {
		return "", fmt.Errorf("return to host ns: %w", err)
	}

	// Move container end of veth to new namespace
	contVeth, err := netlink.LinkByName(vethCont)
	if err != nil {
		return "", fmt.Errorf("get container veth: %w", err)
	}
	if err := netlink.LinkSetNsFd(contVeth, int(contNS)); err != nil {
		return "", fmt.Errorf("move veth: %w", err)
	}

	// Configure container namespace networking
	if err := netns.Set(contNS); err != nil {
		return "", fmt.Errorf("enter container ns: %w", err)
	}
	if err := m.configureContainer(alloc, contVeth); err != nil {
		netns.Set(hostNS)
		return "", fmt.Errorf("configure container: %w", err)
	}
	if err := netns.Set(hostNS); err != nil {
		return "", fmt.Errorf("return to host: %w", err)
	}

	// Update OCI spec to use our namespace
	spec.Linux.Namespaces = setNetworkNamespace(spec.Linux.Namespaces, sandboxID)

	return alloc.IP, nil
}

// TearDown removes all networking resources for a sandbox.
func (m *NetworkManager) TearDown(sandboxID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	vethHost, _ := m.vethNames(sandboxID)

	// Delete veth (peer goes automatically)
	if link, err := netlink.LinkByName(vethHost); err == nil {
		netlink.LinkDel(link)
	}

	netns.DeleteNamed(sandboxID)

	if err := m.client.ReleaseIP(m.ctx, sandboxID); err != nil {
		log.Warn().Err(err).Str("sandbox", sandboxID).Msg("failed to release IP")
	}

	return nil
}

// vethNames generates consistent veth interface names from sandbox ID.
func (m *NetworkManager) vethNames(sandboxID string) (host, cont string) {
	id := sandboxID
	if len(id) > 8 {
		id = id[len(id)-8:]
	}
	return vethHostPrefix + id, vethContPrefix + id
}

func (m *NetworkManager) ensureBridge() error {
	if _, err := netlink.LinkByName(bridgeName); err == nil {
		return nil
	}

	bridge := &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{
			Name:         bridgeName,
			MTU:          m.extIface.Attrs().MTU,
			HardwareAddr: randomMAC(),
		},
	}
	if err := netlink.LinkAdd(bridge); err != nil && err != unix.EEXIST {
		return err
	}

	br, err := netlink.LinkByName(bridgeName)
	if err != nil {
		return err
	}
	if err := netlink.LinkSetUp(br); err != nil {
		return err
	}

	// Assign IPv4 gateway to bridge
	addrV4 := &netlink.Addr{IPNet: &net.IPNet{
		IP:   net.ParseIP(types.DefaultGateway),
		Mask: net.CIDRMask(types.DefaultPrefixLen, 32),
	}}
	if err := netlink.AddrAdd(br, addrV4); err != nil && err != unix.EEXIST {
		return err
	}

	// Assign IPv6 gateway to bridge (if supported)
	if m.ipt6 != nil {
		_, ipv6Net, _ := net.ParseCIDR(types.DefaultSubnetIPv6)
		addrV6 := &netlink.Addr{IPNet: &net.IPNet{
			IP:   net.ParseIP(types.DefaultGatewayIPv6),
			Mask: ipv6Net.Mask,
		}}
		if err := netlink.AddrAdd(br, addrV6); err != nil && err != unix.EEXIST {
			log.Warn().Err(err).Msg("failed to add IPv6 address to bridge")
		}
	}

	extName := m.extIface.Attrs().Name

	// IPv4 NAT
	if err := m.ipt.AppendUnique("nat", "POSTROUTING", "-s", types.DefaultSubnet, "-o", extName, "-j", "MASQUERADE"); err != nil {
		return err
	}

	// IPv6 NAT (if supported)
	if m.ipt6 != nil {
		if err := m.ipt6.AppendUnique("nat", "POSTROUTING", "-s", types.DefaultSubnetIPv6, "-o", extName, "-j", "MASQUERADE"); err != nil {
			log.Warn().Err(err).Msg("failed to add IPv6 NAT rule")
		}
	}

	// Allow forwarding
	if err := m.ipt.InsertUnique("filter", "FORWARD", 1, "-i", bridgeName, "-o", extName, "-j", "ACCEPT"); err != nil {
		return err
	}
	return m.ipt.InsertUnique("filter", "FORWARD", 1, "-i", extName, "-o", bridgeName, "-j", "ACCEPT")
}

func (m *NetworkManager) createVeth(host, cont string) error {
	veth := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{
			Name:         host,
			MTU:          m.extIface.Attrs().MTU,
			HardwareAddr: randomMAC(),
		},
		PeerName:         cont,
		PeerHardwareAddr: randomMAC(),
	}
	return netlink.LinkAdd(veth)
}

func (m *NetworkManager) attachToBridge(vethHost string) error {
	link, err := netlink.LinkByName(vethHost)
	if err != nil {
		return err
	}
	bridge, err := netlink.LinkByName(bridgeName)
	if err != nil {
		return err
	}
	if err := netlink.LinkSetMaster(link, bridge.(*netlink.Bridge)); err != nil {
		return err
	}
	return netlink.LinkSetUp(link)
}

func (m *NetworkManager) configureContainer(alloc *types.IPAllocation, veth netlink.Link) error {
	// Loopback
	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return err
	}
	if err := netlink.LinkSetUp(lo); err != nil {
		return err
	}

	// Container veth
	if err := netlink.LinkSetUp(veth); err != nil {
		return err
	}

	addr := &netlink.Addr{IPNet: &net.IPNet{
		IP:   net.ParseIP(alloc.IP),
		Mask: net.CIDRMask(alloc.PrefixLen, 32),
	}}
	if err := netlink.AddrAdd(veth, addr); err != nil {
		return err
	}

	return netlink.RouteAdd(&netlink.Route{
		LinkIndex: veth.Attrs().Index,
		Gw:        net.ParseIP(alloc.Gateway),
	})
}

func setNetworkNamespace(namespaces []specs.LinuxNamespace, sandboxID string) []specs.LinuxNamespace {
	out := make([]specs.LinuxNamespace, 0, len(namespaces))
	for _, ns := range namespaces {
		if ns.Type == specs.NetworkNamespace {
			out = append(out, specs.LinuxNamespace{
				Type: specs.NetworkNamespace,
				Path: "/var/run/netns/" + sandboxID,
			})
		} else {
			out = append(out, ns)
		}
	}
	return out
}

func defaultInterface() (netlink.Link, error) {
	f, err := os.Open("/proc/net/route")
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) >= 2 && fields[1] == "00000000" {
			return netlink.LinkByName(fields[0])
		}
	}
	return nil, errors.New("no default route")
}

func randomMAC() net.HardwareAddr {
	mac := make([]byte, 6)
	rand.Read(mac)
	mac[0] = (mac[0] | 0x02) & 0xfe // locally administered, unicast
	return mac
}
