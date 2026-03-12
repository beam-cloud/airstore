package channels

import (
	"context"
	"fmt"

	"github.com/beam-cloud/airstore/pkg/clients"
	"github.com/beam-cloud/airstore/pkg/orchestration"
	"github.com/beam-cloud/airstore/pkg/repository"
)

const ChannelTypeEmail ChannelType = "email"

type Email struct {
	agents *orchestration.AgentAPI
	repo   repository.BackendRepository
	mail   *clients.AgentMailClient
}

func NewEmail(agents *orchestration.AgentAPI, repo repository.BackendRepository, mail *clients.AgentMailClient) *Email {
	return &Email{agents: agents, repo: repo, mail: mail}
}

func (e *Email) Type() ChannelType { return ChannelTypeEmail }
func (e *Email) Mail() *clients.AgentMailClient { return e.mail }

func (e *Email) SendToAgent(ctx context.Context, workspaceID uint, agentID string, message Message) (*SendResult, error) {
	return acceptCommand(e.agents, ChannelTypeEmail, ctx, workspaceID, agentID, message)
}

func (e *Email) SendToRun(ctx context.Context, workspaceID uint, runID string, message Message) (*SendResult, error) {
	return nil, fmt.Errorf("email channel does not support SendToRun")
}

func (e *Email) ResolveInbound(ctx context.Context, toAddress string) (workspaceID uint, agentID string, err error) {
	return resolveInbound(e.repo, ChannelTypeEmail, ctx, toAddress)
}

func (e *Email) ProvisionInbox(ctx context.Context, key string, displayName string) (string, error) {
	if e.mail == nil {
		return "", fmt.Errorf("agentmail not configured")
	}
	inbox, err := e.mail.CreateOrGetInbox(ctx, clients.CreateInboxParams{
		Username:    key,
		DisplayName: displayName,
	})
	if err != nil {
		return "", err
	}
	return inbox.InboxID, nil
}

func (e *Email) DeprovisionInbox(ctx context.Context, inboxID string) error {
	if e.mail == nil {
		return fmt.Errorf("agentmail not configured")
	}
	return e.mail.DeleteInbox(ctx, inboxID)
}
