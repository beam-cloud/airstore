package oauth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/beam-cloud/airstore/pkg/common"
	"github.com/redis/go-redis/v9"
)

const (
	keyPrefixSession = "oauth:session:"
	keyPrefixState   = "oauth:state:"

	DefaultSessionTTL = 10 * time.Minute
)

var (
	ErrSessionNotFound = errors.New("session not found")
	ErrSessionExpired  = errors.New("session expired")
	ErrStorageFailed   = errors.New("storage operation failed")
)

type SessionStatus string

const (
	StatusPending  SessionStatus = "pending"
	StatusComplete SessionStatus = "complete"
	StatusError    SessionStatus = "error"
)

// Session represents an OAuth session in progress.
type Session struct {
	ID              string        `json:"id"`
	State           string        `json:"state"`
	ProviderName    string        `json:"provider"`
	WorkspaceID     uint          `json:"workspace_id_internal"`
	WorkspaceExt    string        `json:"workspace_id"`
	IntegrationType string        `json:"integration_type"`
	Status          SessionStatus `json:"status"`
	Error           string        `json:"error,omitempty"`
	ConnectionID    string        `json:"connection_id,omitempty"`
	ReturnTo        string        `json:"return_to,omitempty"`
	CreatedAt       time.Time     `json:"created_at"`
	ExpiresAt       time.Time     `json:"expires_at"`
}

// Backend abstracts session storage.
type Backend interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// Store manages OAuth sessions.
type Store struct {
	backend Backend
	ttl     time.Duration
}

// NewStore creates a session store. Uses Redis if client is non-nil, otherwise falls back to memory.
func NewStore(client *common.RedisClient, ttl time.Duration) *Store {
	if ttl == 0 {
		ttl = DefaultSessionTTL
	}

	var backend Backend
	if client != nil {
		backend = &redisBackend{client: client}
	} else {
		backend = newMemoryBackend()
	}

	return &Store{backend: backend, ttl: ttl}
}

// Create creates a new pending OAuth session.
func (s *Store) Create(providerName string, workspaceID uint, workspaceExt, integrationType, returnTo string) (*Session, error) {
	now := time.Now()
	session := &Session{
		ID:              generateID(),
		State:           generateID(),
		ProviderName:    providerName,
		WorkspaceID:     workspaceID,
		WorkspaceExt:    workspaceExt,
		IntegrationType: integrationType,
		Status:          StatusPending,
		ReturnTo:        returnTo,
		CreatedAt:       now,
		ExpiresAt:       now.Add(s.ttl),
	}

	ctx := context.Background()
	data, err := json.Marshal(session)
	if err != nil {
		return nil, err
	}

	if err := s.backend.Set(ctx, keyPrefixSession+session.ID, data, s.ttl); err != nil {
		return nil, ErrStorageFailed
	}

	if err := s.backend.Set(ctx, keyPrefixState+session.State, []byte(session.ID), s.ttl); err != nil {
		s.backend.Delete(ctx, keyPrefixSession+session.ID)
		return nil, ErrStorageFailed
	}

	return session, nil
}

// Get retrieves a session by ID.
func (s *Store) Get(id string) (*Session, error) {
	data, err := s.backend.Get(context.Background(), keyPrefixSession+id)
	if err != nil {
		return nil, ErrStorageFailed
	}
	if data == nil {
		return nil, ErrSessionNotFound
	}

	var session Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, err
	}

	if time.Now().After(session.ExpiresAt) {
		return nil, ErrSessionExpired
	}

	return &session, nil
}

// GetByState retrieves a session by OAuth state parameter.
func (s *Store) GetByState(state string) (*Session, error) {
	ctx := context.Background()

	idData, err := s.backend.Get(ctx, keyPrefixState+state)
	if err != nil {
		return nil, ErrStorageFailed
	}
	if idData == nil {
		return nil, ErrSessionNotFound
	}

	return s.Get(string(idData))
}

// Complete marks a session as successfully completed and schedules cleanup.
func (s *Store) Complete(id, connectionID string) error {
	session, err := s.Get(id)
	if err != nil {
		return err
	}

	session.Status = StatusComplete
	session.ConnectionID = connectionID

	if err := s.save(session); err != nil {
		return err
	}

	// Schedule cleanup - session data only needed briefly for polling clients
	go s.cleanupAfter(session, 30*time.Second)

	return nil
}

// Fail marks a session as failed with an error and schedules cleanup.
func (s *Store) Fail(id, errMsg string) error {
	session, err := s.Get(id)
	if err != nil {
		return err
	}

	session.Status = StatusError
	session.Error = errMsg

	if err := s.save(session); err != nil {
		return err
	}

	// Keep failed sessions a bit longer for debugging
	go s.cleanupAfter(session, 2*time.Minute)

	return nil
}

// Delete removes a session immediately.
func (s *Store) Delete(id string) error {
	session, err := s.Get(id)
	if err != nil {
		return err
	}

	ctx := context.Background()
	s.backend.Delete(ctx, keyPrefixState+session.State)
	s.backend.Delete(ctx, keyPrefixSession+id)
	return nil
}

func (s *Store) save(session *Session) error {
	ctx := context.Background()
	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	ttl := time.Until(session.ExpiresAt)
	if ttl <= 0 {
		return ErrSessionExpired
	}

	return s.backend.Set(ctx, keyPrefixSession+session.ID, data, ttl)
}

func (s *Store) cleanupAfter(session *Session, delay time.Duration) {
	time.Sleep(delay)

	ctx := context.Background()
	s.backend.Delete(ctx, keyPrefixState+session.State)
	s.backend.Delete(ctx, keyPrefixSession+session.ID)
}

// redisBackend implements Backend using Redis.
type redisBackend struct {
	client *common.RedisClient
}

func (r *redisBackend) Get(ctx context.Context, key string) ([]byte, error) {
	data, err := r.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return data, err
}

func (r *redisBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return r.client.Set(ctx, key, value, ttl).Err()
}

func (r *redisBackend) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// memoryBackend implements Backend using in-memory maps. Used for local development.
type memoryBackend struct {
	mu     sync.RWMutex
	data   map[string][]byte
	expiry map[string]time.Time
}

func newMemoryBackend() *memoryBackend {
	return &memoryBackend{
		data:   make(map[string][]byte),
		expiry: make(map[string]time.Time),
	}
}

func (m *memoryBackend) Get(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	exp, ok := m.expiry[key]
	if !ok || time.Now().After(exp) {
		return nil, nil
	}

	// Return a copy to avoid data races
	src := m.data[key]
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst, nil
}

func (m *memoryBackend) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Store a copy to avoid data races
	dst := make([]byte, len(value))
	copy(dst, value)

	m.data[key] = dst
	m.expiry[key] = time.Now().Add(ttl)
	return nil
}

func (m *memoryBackend) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	delete(m.expiry, key)
	return nil
}

func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
