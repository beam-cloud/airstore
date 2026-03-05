package repository

import (
	"context"
	"testing"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAllocateIPReturnsDualStackAndIsIdempotent(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	repo := NewWorkerRedisRepository(rdb).(*WorkerRedisRepository)
	ctx := context.Background()

	first, err := repo.AllocateIP(ctx, "sandbox-1", "worker-1")
	require.NoError(t, err)
	require.NotEmpty(t, first.IP)
	require.NotEmpty(t, first.IPv6)
	require.Equal(t, types.DefaultGateway, first.Gateway)
	require.Equal(t, types.DefaultGatewayIPv6, first.GatewayIPv6)
	require.Equal(t, types.DefaultPrefixLen, first.PrefixLen)
	require.Equal(t, types.DefaultPrefixLenIPv6, first.PrefixLenIPv6)

	second, err := repo.AllocateIP(ctx, "sandbox-1", "worker-1")
	require.NoError(t, err)
	require.Equal(t, first, second)
}

func TestReleaseIPAndGetSandboxIPHandleDualStackStorage(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	repo := NewWorkerRedisRepository(rdb).(*WorkerRedisRepository)
	ctx := context.Background()

	alloc, err := repo.AllocateIP(ctx, "sandbox-2", "worker-1")
	require.NoError(t, err)

	ip, ok := repo.GetSandboxIP(ctx, "sandbox-2")
	require.True(t, ok)
	require.Equal(t, alloc.IP, ip)

	require.NoError(t, repo.ReleaseIP(ctx, "sandbox-2"))
	_, ok = repo.GetSandboxIP(ctx, "sandbox-2")
	require.False(t, ok)
}

func TestAllocateIPReadsLegacyPlainIPv4Mapping(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	repo := NewWorkerRedisRepository(rdb).(*WorkerRedisRepository)
	ctx := context.Background()

	const legacyIP = "10.200.0.12"
	require.NoError(t, rdb.HSet(ctx, common.Keys.NetworkIPMap(), "legacy-sandbox", legacyIP).Err())
	require.NoError(t, rdb.SAdd(ctx, common.Keys.NetworkIPPool(), legacyIP).Err())

	alloc, err := repo.AllocateIP(ctx, "legacy-sandbox", "worker-1")
	require.NoError(t, err)
	require.Equal(t, legacyIP, alloc.IP)
	require.NotEmpty(t, alloc.IPv6)
	require.Equal(t, types.DefaultGatewayIPv6, alloc.GatewayIPv6)
}

func TestAllocateIPReturnsErrorForCorruptedStoredMapping(t *testing.T) {
	rdb, err := NewRedisClientForTest()
	require.NoError(t, err)

	repo := NewWorkerRedisRepository(rdb).(*WorkerRedisRepository)
	ctx := context.Background()

	require.NoError(t, rdb.HSet(ctx, common.Keys.NetworkIPMap(), "broken-sandbox", "not-json-not-ip").Err())

	_, err = repo.AllocateIP(ctx, "broken-sandbox", "worker-1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid stored ip allocation")
}

func TestDeriveIPv6AddressNoCollisionForAdjacentIPv4s(t *testing.T) {
	ipv6a, err := deriveIPv6Address("10.200.0.1")
	require.NoError(t, err)

	ipv6b, err := deriveIPv6Address("10.200.0.2")
	require.NoError(t, err)

	require.NotEqual(t, ipv6a, ipv6b)
}
