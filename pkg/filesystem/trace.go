package filesystem

import (
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// FuseTrace is an opt-in, low-overhead tracer for the FUSE layer.
//
// Enable with:
//   AIRSTORE_FUSE_TRACE=1
//
// Optional:
//   AIRSTORE_FUSE_TRACE_INTERVAL=2s   (default: 2s)
//   AIRSTORE_FUSE_TRACE_SLOW_MS=50    (default: 50ms; 0 disables slow-op logging)
type FuseTrace struct {
	interval      time.Duration
	slowThreshold time.Duration

	// Op counters
	getattrCount atomic.Uint64
	getattrErr   atomic.Uint64
	getattrNanos atomic.Uint64

	readdirCount atomic.Uint64
	readdirErr   atomic.Uint64
	readdirNanos atomic.Uint64

	opendirCount atomic.Uint64
	opendirErr   atomic.Uint64
	opendirNanos atomic.Uint64

	openCount atomic.Uint64
	openErr   atomic.Uint64
	openNanos atomic.Uint64

	readCount atomic.Uint64
	readErr   atomic.Uint64
	readNanos atomic.Uint64

	createCount atomic.Uint64
	createErr   atomic.Uint64
	createNanos atomic.Uint64

	writeCount atomic.Uint64
	writeErr   atomic.Uint64
	writeNanos atomic.Uint64

	mkdirCount atomic.Uint64
	mkdirErr   atomic.Uint64
	mkdirNanos atomic.Uint64
}

func newFuseTraceFromEnv() *FuseTrace {
	if !envBool("AIRSTORE_FUSE_TRACE") {
		return nil
	}

	interval := envDuration("AIRSTORE_FUSE_TRACE_INTERVAL", 2*time.Second)
	if interval <= 0 {
		interval = 2 * time.Second
	}

	slowMs := envInt("AIRSTORE_FUSE_TRACE_SLOW_MS", 50)
	slowThreshold := time.Duration(slowMs) * time.Millisecond
	if slowMs <= 0 {
		slowThreshold = 0
	}

	return &FuseTrace{
		interval:      interval,
		slowThreshold: slowThreshold,
	}
}

func envBool(key string) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return false
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return i
}

func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

type fuseTraceSnapshot struct {
	getattrCount uint64
	getattrErr   uint64
	getattrNanos uint64

	readdirCount uint64
	readdirErr   uint64
	readdirNanos uint64

	opendirCount uint64
	opendirErr   uint64
	opendirNanos uint64

	openCount uint64
	openErr   uint64
	openNanos uint64

	readCount uint64
	readErr   uint64
	readNanos uint64

	createCount uint64
	createErr   uint64
	createNanos uint64

	writeCount uint64
	writeErr   uint64
	writeNanos uint64

	mkdirCount uint64
	mkdirErr   uint64
	mkdirNanos uint64
}

func (t *FuseTrace) snapshot() fuseTraceSnapshot {
	return fuseTraceSnapshot{
		getattrCount: t.getattrCount.Load(),
		getattrErr:   t.getattrErr.Load(),
		getattrNanos: t.getattrNanos.Load(),

		readdirCount: t.readdirCount.Load(),
		readdirErr:   t.readdirErr.Load(),
		readdirNanos: t.readdirNanos.Load(),

		opendirCount: t.opendirCount.Load(),
		opendirErr:   t.opendirErr.Load(),
		opendirNanos: t.opendirNanos.Load(),

		openCount: t.openCount.Load(),
		openErr:   t.openErr.Load(),
		openNanos: t.openNanos.Load(),

		readCount: t.readCount.Load(),
		readErr:   t.readErr.Load(),
		readNanos: t.readNanos.Load(),

		createCount: t.createCount.Load(),
		createErr:   t.createErr.Load(),
		createNanos: t.createNanos.Load(),

		writeCount: t.writeCount.Load(),
		writeErr:   t.writeErr.Load(),
		writeNanos: t.writeNanos.Load(),

		mkdirCount: t.mkdirCount.Load(),
		mkdirErr:   t.mkdirErr.Load(),
		mkdirNanos: t.mkdirNanos.Load(),
	}
}

func (t *FuseTrace) reportLoop(stop <-chan struct{}, mountPoint string) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	prev := t.snapshot()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			now := t.snapshot()
			d := diffFuseTraceSnapshot(prev, now)
			prev = now

			// Only log if something happened during the interval.
			if d.totalOps() == 0 {
				continue
			}

			log.Info().
				Str("mount", mountPoint).
				Uint64("getattr", d.getattrCount).
				Uint64("getattr_err", d.getattrErr).
				Dur("getattr_avg", avgDur(d.getattrNanos, d.getattrCount)).
				Uint64("readdir", d.readdirCount).
				Uint64("readdir_err", d.readdirErr).
				Dur("readdir_avg", avgDur(d.readdirNanos, d.readdirCount)).
				Uint64("opendir", d.opendirCount).
				Uint64("opendir_err", d.opendirErr).
				Dur("opendir_avg", avgDur(d.opendirNanos, d.opendirCount)).
				Uint64("open", d.openCount).
				Uint64("open_err", d.openErr).
				Dur("open_avg", avgDur(d.openNanos, d.openCount)).
				Msg("fuse trace (interval)")
		}
	}
}

func (t *FuseTrace) logSlow(op, path string, dur time.Duration, err error) {
	if t.slowThreshold <= 0 || dur < t.slowThreshold {
		return
	}
	log.Info().Str("op", op).Str("path", path).Dur("dur", dur).Err(err).Msg("fuse slow op")
}

func (t *FuseTrace) recordGetattr(path string, dur time.Duration, err error) {
	t.getattrCount.Add(1)
	if err != nil {
		t.getattrErr.Add(1)
	}
	t.getattrNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("getattr", path, dur, err)
}

func (t *FuseTrace) recordReaddir(path string, dur time.Duration, err error) {
	t.readdirCount.Add(1)
	if err != nil {
		t.readdirErr.Add(1)
	}
	t.readdirNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("readdir", path, dur, err)
}

func (t *FuseTrace) recordOpendir(path string, dur time.Duration, err error) {
	t.opendirCount.Add(1)
	if err != nil {
		t.opendirErr.Add(1)
	}
	t.opendirNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("opendir", path, dur, err)
}

func (t *FuseTrace) recordOpen(path string, dur time.Duration, err error) {
	t.openCount.Add(1)
	if err != nil {
		t.openErr.Add(1)
	}
	t.openNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("open", path, dur, err)
}

func (t *FuseTrace) recordRead(path string, dur time.Duration, err error) {
	t.readCount.Add(1)
	if err != nil {
		t.readErr.Add(1)
	}
	t.readNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("read", path, dur, err)
}

func (t *FuseTrace) recordCreate(path string, dur time.Duration, err error) {
	t.createCount.Add(1)
	if err != nil {
		t.createErr.Add(1)
	}
	t.createNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("create", path, dur, err)
}

func (t *FuseTrace) recordWrite(path string, dur time.Duration, err error) {
	t.writeCount.Add(1)
	if err != nil {
		t.writeErr.Add(1)
	}
	t.writeNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("write", path, dur, err)
}

func (t *FuseTrace) recordMkdir(path string, dur time.Duration, err error) {
	t.mkdirCount.Add(1)
	if err != nil {
		t.mkdirErr.Add(1)
	}
	t.mkdirNanos.Add(uint64(dur.Nanoseconds()))
	t.logSlow("mkdir", path, dur, err)
}

func diffFuseTraceSnapshot(a, b fuseTraceSnapshot) fuseTraceSnapshot {
	return fuseTraceSnapshot{
		getattrCount: b.getattrCount - a.getattrCount,
		getattrErr:   b.getattrErr - a.getattrErr,
		getattrNanos: b.getattrNanos - a.getattrNanos,

		readdirCount: b.readdirCount - a.readdirCount,
		readdirErr:   b.readdirErr - a.readdirErr,
		readdirNanos: b.readdirNanos - a.readdirNanos,

		opendirCount: b.opendirCount - a.opendirCount,
		opendirErr:   b.opendirErr - a.opendirErr,
		opendirNanos: b.opendirNanos - a.opendirNanos,

		openCount: b.openCount - a.openCount,
		openErr:   b.openErr - a.openErr,
		openNanos: b.openNanos - a.openNanos,

		readCount: b.readCount - a.readCount,
		readErr:   b.readErr - a.readErr,
		readNanos: b.readNanos - a.readNanos,

		createCount: b.createCount - a.createCount,
		createErr:   b.createErr - a.createErr,
		createNanos: b.createNanos - a.createNanos,

		writeCount: b.writeCount - a.writeCount,
		writeErr:   b.writeErr - a.writeErr,
		writeNanos: b.writeNanos - a.writeNanos,

		mkdirCount: b.mkdirCount - a.mkdirCount,
		mkdirErr:   b.mkdirErr - a.mkdirErr,
		mkdirNanos: b.mkdirNanos - a.mkdirNanos,
	}
}

func (s fuseTraceSnapshot) totalOps() uint64 {
	return s.getattrCount + s.readdirCount + s.opendirCount + s.openCount + s.readCount + s.createCount + s.writeCount + s.mkdirCount
}

func avgDur(totalNanos uint64, count uint64) time.Duration {
	if count == 0 {
		return 0
	}
	return time.Duration(totalNanos / count)
}

