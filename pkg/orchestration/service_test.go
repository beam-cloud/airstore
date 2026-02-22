package orchestration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/repository"
	"github.com/beam-cloud/airstore/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type fakeBackend struct {
	repository.BackendRepository
	mu           sync.Mutex
	envelopes    map[string]*types.AgentTaskEnvelope
	idempotency  map[string]string
	droppedCount int
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{
		envelopes:   map[string]*types.AgentTaskEnvelope{},
		idempotency: map[string]string{},
	}
}

func idempotencyKey(workspaceID uint, agentID *string, key string) string {
	agent := "_"
	if agentID != nil {
		agent = *agentID
	}
	return fmt.Sprintf("%d:%s:%s", workspaceID, agent, key)
}

func (f *fakeBackend) CreateAgentTaskEnvelope(_ context.Context, envelope *types.AgentTaskEnvelope) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	envelope.ID = uuid.NewString()
	f.envelopes[envelope.ID] = envelope
	f.idempotency[idempotencyKey(envelope.WorkspaceID, envelope.AgentID, envelope.IdempotencyKey)] = envelope.ID
	return nil
}

func (f *fakeBackend) GetAgentTaskEnvelopeByIdempotency(_ context.Context, workspaceID uint, agentID *string, key string) (*types.AgentTaskEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.idempotency[idempotencyKey(workspaceID, agentID, key)]
	if !ok {
		return nil, &types.ErrAgentTaskEnvelopeNotFound{ID: key}
	}
	return f.envelopes[id], nil
}

func (f *fakeBackend) GetAgentTaskEnvelopeByID(_ context.Context, envelopeID string) (*types.AgentTaskEnvelope, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.envelopes[envelopeID]
	if !ok {
		return nil, &types.ErrAgentTaskEnvelopeNotFound{ID: envelopeID}
	}
	return env, nil
}

func (f *fakeBackend) UpdateAgentTaskEnvelopeState(_ context.Context, envelopeID string, state types.AgentEnvelopeState, droppedReason *string, targetRunID *string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	env, ok := f.envelopes[envelopeID]
	if !ok {
		return &types.ErrAgentTaskEnvelopeNotFound{ID: envelopeID}
	}
	env.State = state
	env.TargetRunID = targetRunID
	if state == types.AgentEnvelopeStateDropped {
		env.DroppedReason = droppedReason
		f.droppedCount++
	}
	return nil
}

func newTestRedis(t *testing.T) (*common.RedisClient, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)

	client, err := common.NewRedisClient(types.RedisConfig{
		Mode:  types.RedisModeSingle,
		Addrs: []string{mr.Addr()},
	})
	require.NoError(t, err)

	cleanup := func() {
		_ = client.Close()
		mr.Close()
	}
	return client, cleanup
}

func TestAcceptAgentCommandAcceptedFirstAndIdempotent(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	svc := NewAgentService(context.Background(), backend, nil, redisClient, nil, nil, "ghcr.io/beam/sandbox:latest")

	params := AgentCommandParams{
		Message:        "hello world",
		SessionID:      "session-1",
		IdempotencyKey: "idem-1",
	}

	envelope, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.False(t, deduped)
	require.NotEmpty(t, envelope.ID)
	require.Equal(t, types.AgentEnvelopeStateQueued, envelope.State)
	require.Nil(t, envelope.TargetRunID, "run should not exist at acceptance time")

	queueLen, err := redisClient.LLen(context.Background(), common.Keys.AgentEnvelopeQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen)

	again, deduped, err := svc.AcceptAgentCommand(context.Background(), 42, params)
	require.NoError(t, err)
	require.True(t, deduped)
	require.Equal(t, envelope.ID, again.ID)

	queueLen, err = redisClient.LLen(context.Background(), common.Keys.AgentEnvelopeQueue()).Result()
	require.NoError(t, err)
	require.EqualValues(t, 1, queueLen, "idempotent replay must not enqueue duplicate work")
}

func TestQueueReshapingDropsOlderFollowupEnvelope(t *testing.T) {
	redisClient, cleanup := newTestRedis(t)
	defer cleanup()

	backend := newFakeBackend()
	store := repository.NewAgentEnvelopeQueueStore(backend, redisClient)
	router := NewEnvelopeQueueRouter(store)
	ctx := context.Background()

	instanceKey := "execclass_test"
	first := &types.AgentTaskEnvelope{
		ID:             uuid.NewString(),
		WorkspaceID:    1,
		Kind:           types.AgentEnvelopeKindRunInput,
		QueueMode:      types.AgentQueueModeFollowup,
		State:          types.AgentEnvelopeStateAccepted,
		IdempotencyKey: "f1",
	}
	second := &types.AgentTaskEnvelope{
		ID:             uuid.NewString(),
		WorkspaceID:    1,
		Kind:           types.AgentEnvelopeKindRunInput,
		QueueMode:      types.AgentQueueModeFollowup,
		State:          types.AgentEnvelopeStateAccepted,
		IdempotencyKey: "f2",
	}

	backend.envelopes[first.ID] = first
	backend.envelopes[second.ID] = second

	require.NoError(t, router.Enqueue(ctx, first, instanceKey))
	require.NoError(t, router.Enqueue(ctx, second, instanceKey))

	require.Equal(t, types.AgentEnvelopeStateDropped, backend.envelopes[first.ID].State)
	require.Equal(t, types.AgentEnvelopeStateQueued, backend.envelopes[second.ID].State)

	token, err := router.Pop(ctx, 0)
	require.NoError(t, err)
	require.NotEmpty(t, token)

	envelopeID, err := router.ResolveEnvelopeID(ctx, token)
	require.NoError(t, err)
	require.Equal(t, second.ID, envelopeID)
}
