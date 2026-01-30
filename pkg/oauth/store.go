package oauth

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// SessionStatus represents the state of an OAuth session
type SessionStatus string

const (
	StatusPending  SessionStatus = "pending"
	StatusComplete SessionStatus = "complete"
	StatusError    SessionStatus = "error"

	// DefaultSessionTTL is how long sessions live before cleanup
	DefaultSessionTTL = 10 * time.Minute
)

// Session represents an OAuth session in progress
type Session struct {
	ID              string        `json:"id"`
	State           string        `json:"state"` // Used in OAuth state param, maps back to session
	WorkspaceID     uint          `json:"-"`     // Internal workspace ID
	WorkspaceExt    string        `json:"workspace_id"`
	IntegrationType string        `json:"integration_type"` // gmail, gdrive
	Status          SessionStatus `json:"status"`
	Error           string        `json:"error,omitempty"`
	ConnectionID    string        `json:"connection_id,omitempty"` // Set on success
	ReturnTo        string        `json:"-"`                       // Optional redirect after callback
	CreatedAt       time.Time     `json:"created_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
}

// Store manages OAuth sessions in memory with TTL cleanup
type Store struct {
	mu       sync.RWMutex
	sessions map[string]*Session // keyed by session ID
	byState  map[string]string   // state -> session ID
	ttl      time.Duration
	stopCh   chan struct{}
}

// NewStore creates a new session store with cleanup goroutine
func NewStore(ttl time.Duration) *Store {
	if ttl == 0 {
		ttl = DefaultSessionTTL
	}
	s := &Store{
		sessions: make(map[string]*Session),
		byState:  make(map[string]string),
		ttl:      ttl,
		stopCh:   make(chan struct{}),
	}
	go s.cleanupLoop()
	return s
}

// Create creates a new pending OAuth session
func (s *Store) Create(workspaceID uint, workspaceExt, integrationType, returnTo string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	session := &Session{
		ID:              generateID(),
		State:           generateID(), // Separate random value for OAuth state
		WorkspaceID:     workspaceID,
		WorkspaceExt:    workspaceExt,
		IntegrationType: integrationType,
		Status:          StatusPending,
		ReturnTo:        returnTo,
		CreatedAt:       now,
		ExpiresAt:       now.Add(s.ttl),
	}

	s.sessions[session.ID] = session
	s.byState[session.State] = session.ID
	return session
}

// Get retrieves a session by ID
func (s *Store) Get(id string) *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessions[id]
}

// GetByState retrieves a session by OAuth state parameter
func (s *Store) GetByState(state string) *Session {
	s.mu.RLock()
	defer s.mu.RUnlock()

	id, ok := s.byState[state]
	if !ok {
		return nil
	}
	return s.sessions[id]
}

// Complete marks a session as successfully completed
func (s *Store) Complete(id, connectionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if session, ok := s.sessions[id]; ok {
		session.Status = StatusComplete
		session.ConnectionID = connectionID
	}
}

// Fail marks a session as failed with an error
func (s *Store) Fail(id, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if session, ok := s.sessions[id]; ok {
		session.Status = StatusError
		session.Error = errMsg
	}
}

// Delete removes a session
func (s *Store) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if session, ok := s.sessions[id]; ok {
		delete(s.byState, session.State)
		delete(s.sessions, id)
	}
}

// Stop stops the cleanup goroutine
func (s *Store) Stop() {
	close(s.stopCh)
}

func (s *Store) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.cleanup()
		}
	}
}

func (s *Store) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, session := range s.sessions {
		if now.After(session.ExpiresAt) {
			delete(s.byState, session.State)
			delete(s.sessions, id)
		}
	}
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
