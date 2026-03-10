package worker

// ringBuffer is a fixed-size circular byte buffer that retains the most
// recent N bytes written to it.  It is not safe for concurrent use.
type ringBuffer struct {
	buf  []byte
	pos  int
	full bool
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{buf: make([]byte, size)}
}

func (r *ringBuffer) Write(p []byte) (int, error) {
	n := len(p)
	if n == 0 {
		return 0, nil
	}
	cap := len(r.buf)
	if n >= cap {
		copy(r.buf, p[n-cap:])
		r.pos = 0
		r.full = true
		return n, nil
	}
	space := cap - r.pos
	if n <= space {
		copy(r.buf[r.pos:], p)
	} else {
		copy(r.buf[r.pos:], p[:space])
		copy(r.buf, p[space:])
	}
	r.pos = (r.pos + n) % cap
	if !r.full && r.pos < n {
		r.full = true
	}
	return n, nil
}

// Bytes returns the buffered content in chronological order.
func (r *ringBuffer) Bytes() []byte {
	if !r.full {
		return append([]byte(nil), r.buf[:r.pos]...)
	}
	out := make([]byte, len(r.buf))
	n := copy(out, r.buf[r.pos:])
	copy(out[n:], r.buf[:r.pos])
	return out
}

// Reset clears the buffer.
func (r *ringBuffer) Reset() {
	r.pos = 0
	r.full = false
}
