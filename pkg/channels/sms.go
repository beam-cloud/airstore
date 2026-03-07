package channels

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
)

const ChannelTypeSMS ChannelType = "sms"

type SMS struct {
	agents *orchestration.AgentAPI
	repo   repository.BackendRepository
}

func NewSMS(agents *orchestration.AgentAPI, repo repository.BackendRepository) *SMS {
	return &SMS{agents: agents, repo: repo}
}

func (s *SMS) Type() ChannelType { return ChannelTypeSMS }

func (s *SMS) SendToAgent(ctx context.Context, workspaceID uint, agentID string, message Message) (*SendResult, error) {
	return acceptCommand(s.agents, ChannelTypeSMS, ctx, workspaceID, agentID, message)
}

func (s *SMS) SendToRun(ctx context.Context, workspaceID uint, runID string, message Message) (*SendResult, error) {
	return nil, fmt.Errorf("sms channel does not support SendToRun")
}

func (s *SMS) ResolveInbound(ctx context.Context, toNumber string) (workspaceID uint, agentID string, err error) {
	return resolveInbound(s.repo, ChannelTypeSMS, ctx, toNumber)
}
