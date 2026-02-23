package orchestration

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/beam-cloud/airstore/pkg/types"
)

const (
	dispatchTokenEnvPrefix  = "env:"
	dispatchTokenModePrefix = "mode:"
)

type EnvelopeQueueRouter struct {
	store EnvelopeQueueStore
}

type EnvelopeQueueStore interface {
	UpdateEnvelopeState(ctx context.Context, envelopeID string, state types.AgentEnvelopeState, dropReason *string, targetRunID *string) error
	PushQueueToken(ctx context.Context, token string) error
	PopQueueToken(ctx context.Context, timeout time.Duration) (string, error)
	GetModeEnvelopeID(ctx context.Context, modeKey string) (string, error)
	SetModeEnvelopeID(ctx context.Context, modeKey string, envelopeID string, ttl time.Duration) error
	AddModeKey(ctx context.Context, modeKey string) (bool, error)
	RemoveModeKey(ctx context.Context, modeKey string) error
	GetDelModeEnvelopeID(ctx context.Context, modeKey string) (string, error)
}

func NewEnvelopeQueueRouter(store EnvelopeQueueStore) *EnvelopeQueueRouter {
	return &EnvelopeQueueRouter{
		store: store,
	}
}

func (r *EnvelopeQueueRouter) Enqueue(ctx context.Context, envelope *types.AgentTaskEnvelope, instanceKey string) error {
	if r.store == nil {
		return fmt.Errorf("queue store is required")
	}

	if err := r.store.UpdateEnvelopeState(ctx, envelope.ID, types.AgentEnvelopeStateQueued, nil, envelope.TargetRunID); err != nil {
		return err
	}

	switch envelope.QueueMode {
	case types.AgentQueueModeFollowup, types.AgentQueueModeSteer, types.AgentQueueModeInterrupt:
		return r.enqueueModeKey(ctx, envelope, instanceKey)
	default:
		token := dispatchTokenEnvPrefix + envelope.ID
		return r.store.PushQueueToken(ctx, token)
	}
}

func (r *EnvelopeQueueRouter) enqueueModeKey(ctx context.Context, envelope *types.AgentTaskEnvelope, instanceKey string) error {
	modeKey := fmt.Sprintf("%s:%s", instanceKey, envelope.QueueMode)
	prevID, err := r.store.GetModeEnvelopeID(ctx, modeKey)
	if err != nil {
		return err
	}
	if prevID != "" && prevID != envelope.ID {
		reason := types.AgentEnvelopeDropReasonReshapedByQueueMode
		_ = r.store.UpdateEnvelopeState(ctx, prevID, types.AgentEnvelopeStateDropped, &reason, envelope.TargetRunID)
	}
	if err := r.store.SetModeEnvelopeID(ctx, modeKey, envelope.ID, 15*time.Minute); err != nil {
		return err
	}

	added, err := r.store.AddModeKey(ctx, modeKey)
	if err != nil {
		return err
	}
	if added {
		token := dispatchTokenModePrefix + modeKey
		if err := r.store.PushQueueToken(ctx, token); err != nil {
			return err
		}
	}
	return nil
}

func (r *EnvelopeQueueRouter) Pop(ctx context.Context, timeout time.Duration) (string, error) {
	if r.store == nil {
		return "", fmt.Errorf("queue store is required")
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return r.store.PopQueueToken(ctx, timeout)
}

func (r *EnvelopeQueueRouter) ResolveEnvelopeID(ctx context.Context, token string) (string, error) {
	if r.store == nil {
		return "", fmt.Errorf("queue store is required")
	}

	if strings.HasPrefix(token, dispatchTokenEnvPrefix) {
		return strings.TrimPrefix(token, dispatchTokenEnvPrefix), nil
	}

	if strings.HasPrefix(token, dispatchTokenModePrefix) {
		modeKey := strings.TrimPrefix(token, dispatchTokenModePrefix)
		if err := r.store.RemoveModeKey(ctx, modeKey); err != nil {
			return "", err
		}
		return r.store.GetDelModeEnvelopeID(ctx, modeKey)
	}

	return "", fmt.Errorf("unsupported dispatch token: %s", token)
}

func (r *EnvelopeQueueRouter) RequeueEnvelope(ctx context.Context, envelopeID string) error {
	if r.store == nil {
		return fmt.Errorf("queue store is required")
	}
	if strings.TrimSpace(envelopeID) == "" {
		return fmt.Errorf("envelope_id is required")
	}
	return r.store.PushQueueToken(ctx, dispatchTokenEnvPrefix+envelopeID)
}
