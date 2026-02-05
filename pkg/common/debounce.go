package common

import (
	"sync"
	"time"
)

// Debouncer coalesces rapid calls on the same key into a single callback.
// Each Call resets the timer for that key. When the timer fires, fn runs.
type Debouncer struct {
	delay  time.Duration
	timers sync.Map // key -> *time.Timer
}

func NewDebouncer(delay time.Duration) *Debouncer {
	return &Debouncer{delay: delay}
}

// Call schedules fn to run after delay. If called again with the same key
// before the delay expires, the timer resets.
func (d *Debouncer) Call(key string, fn func()) {
	if v, ok := d.timers.Load(key); ok {
		v.(*time.Timer).Stop()
	}
	d.timers.Store(key, time.AfterFunc(d.delay, func() {
		d.timers.Delete(key)
		fn()
	}))
}
