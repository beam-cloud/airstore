package game

import (
	"sync"

	"github.com/beam-cloud/airstore/pkg/company"
)

type StreamMessage struct {
	Event     string                        `json:"event"`
	Snapshot  *company.CompanyWorldSnapshot `json:"snapshot,omitempty"`
	Delta     *company.CompanyWorldDelta    `json:"delta,omitempty"`
	Timestamp int64                         `json:"ts"`
}

type Fanout struct {
	mu     sync.RWMutex
	nextID int
	subs   map[int]chan StreamMessage
}

func NewFanout() *Fanout {
	return &Fanout{
		subs: make(map[int]chan StreamMessage),
	}
}

func (f *Fanout) Subscribe(buffer int) (<-chan StreamMessage, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()

	id := f.nextID
	f.nextID++
	ch := make(chan StreamMessage, buffer)
	f.subs[id] = ch

	cancel := func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if existing, ok := f.subs[id]; ok {
			delete(f.subs, id)
			close(existing)
		}
	}
	return ch, cancel
}

func (f *Fanout) Publish(msg StreamMessage) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, ch := range f.subs {
		select {
		case ch <- msg:
		default:
			// Drop if a subscriber is lagging; clients can resync from snapshots.
		}
	}
}
