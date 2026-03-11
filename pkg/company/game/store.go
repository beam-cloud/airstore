package game

import (
	"context"
	"fmt"
	"sync"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/beam-cloud/airstore/pkg/company"
	"github.com/beam-cloud/airstore/pkg/repository"
)

type Store struct {
	s2      *common.S2Client
	backend repository.BackendRepository

	mu          sync.RWMutex
	checkpoints map[string]*company.CompanyWorldSnapshot
}

func NewStore(s2 *common.S2Client, backend repository.BackendRepository) *Store {
	return &Store{
		s2:          s2,
		backend:     backend,
		checkpoints: make(map[string]*company.CompanyWorldSnapshot),
	}
}

func (s *Store) AppendEvent(ctx context.Context, workspaceID string, event Event) error {
	if s == nil {
		return nil
	}
	if s.s2 == nil || !s.s2.Enabled() {
		return nil
	}
	return s.s2.Append(ctx, companyWorldEventStream(workspaceID), event)
}

func (s *Store) SaveCheckpoint(_ context.Context, workspaceID string, snapshot *company.CompanyWorldSnapshot) error {
	if s == nil || snapshot == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkpoints[workspaceID] = snapshot
	return nil
}

func (s *Store) LoadCheckpoint(_ context.Context, workspaceID string) (*company.CompanyWorldSnapshot, error) {
	if s == nil {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.checkpoints[workspaceID], nil
}

func companyWorldEventStream(workspaceID string) string {
	return fmt.Sprintf("company-world.%s.events", workspaceID)
}
