package hooks

import (
	"sync"
	"sync/atomic"
	"time"
)

// debouncer coalesces rapid calls on the same key into a single callback.
// Used to prevent multiple tasks from a burst of write events on the same path.
type debouncer struct {
	delay time.Duration
	mu    sync.Mutex
	state map[string]*debounceEntry
}

type debounceEntry struct {
	timer *time.Timer
	gen   uint64 // generation counter; callback only fires if gen matches
}

func newDebouncer(delay time.Duration) *debouncer {
	return &debouncer{
		delay: delay,
		state: make(map[string]*debounceEntry),
	}
}

// call schedules fn to run after delay. If called again with the same key
// before the delay expires, the timer resets and only the latest fn fires.
func (d *debouncer) call(key string, fn func()) {
	d.mu.Lock()
	defer d.mu.Unlock()

	e, ok := d.state[key]
	if ok {
		e.timer.Stop()
		atomic.AddUint64(&e.gen, 1)
	} else {
		e = &debounceEntry{}
		d.state[key] = e
	}

	gen := atomic.LoadUint64(&e.gen)
	e.timer = time.AfterFunc(d.delay, func() {
		// Only fire if no subsequent call bumped the generation
		if atomic.LoadUint64(&e.gen) != gen {
			return
		}
		d.mu.Lock()
		delete(d.state, key)
		d.mu.Unlock()
		fn()
	})
}
