package hooks

import (
	"sync"
	"time"
)

// debouncer coalesces rapid calls on the same key into a single callback.
type debouncer struct {
	delay time.Duration
	mu    sync.Mutex
	state map[string]*debounceEntry
}

type debounceEntry struct {
	timer *time.Timer
	gen   uint64
}

func newDebouncer(delay time.Duration) *debouncer {
	return &debouncer{delay: delay, state: make(map[string]*debounceEntry)}
}

func (d *debouncer) call(key string, fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	e, ok := d.state[key]
	if ok {
		e.timer.Stop()
		e.gen++
	} else {
		e = &debounceEntry{}
		d.state[key] = e
	}

	gen := e.gen
	e.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		// Recheck under lock: if gen changed, a newer call owns this key.
		if e.gen != gen {
			d.mu.Unlock()
			return
		}
		delete(d.state, key)
		d.mu.Unlock()
		fn()
	})
}
