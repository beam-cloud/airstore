package vnode

import "bytes"

// mergedWrite holds a coalesced (offset, data) pair that covers two
// potentially non-contiguous byte ranges.
type mergedWrite struct {
	off  int64
	data []byte
}

// mergeWriteBuffer combines two (offset, data) ranges into a single buffer
// that preserves correct byte positions. Gaps between non-contiguous ranges
// are zero-filled so that each chunk lands at its intended offset.
//
// The zero-filled gaps may introduce NULL bytes for sparse/pwrite patterns
// (e.g. Claude Code shell snapshots). These are stripped later by
// compactNulls in flushWriteBuffer for text files (data starting with '#').
// Binary/sparse files keep the zeros, which is the correct POSIX behavior.
func mergeWriteBuffer(oldOff int64, oldData []byte, newOff int64, newData []byte) *mergedWrite {
	if len(oldData) == 0 && len(newData) == 0 {
		return nil
	}

	minOff := oldOff
	if newOff < minOff {
		minOff = newOff
	}

	oldEnd := oldOff + int64(len(oldData))
	newEnd := newOff + int64(len(newData))

	maxEnd := oldEnd
	if newEnd > maxEnd {
		maxEnd = newEnd
	}

	combined := make([]byte, maxEnd-minOff)
	copy(combined[oldOff-minOff:], oldData)
	copy(combined[newOff-minOff:], newData)
	return &mergedWrite{off: minOff, data: combined}
}

// compactNulls replaces runs of consecutive NULL bytes with a single newline
// in data that is confirmed to be a text file. This handles the case where
// the kernel page cache or VFS write-merge buffer coalesces sparse/pwrite
// writes into a single contiguous buffer with zero-filled gaps. Bash 5.x on
// Linux refuses to source files containing NULLs ("cannot execute binary
// file"). Replacing NULL runs with newlines (rather than deleting them)
// preserves structural separation between content sections, avoiding syntax
// errors from token merging.
//
// Guards (all must be true to activate):
//  1. offset is 0 (full file replacement, not a partial update)
//  2. first byte is '#' (shell script / config comment)
//  3. a newline exists within the first 256 bytes (confirms multi-line text;
//     rejects binary data that coincidentally starts with 0x23)
//  4. NULLs are actually present
func compactNulls(off int64, data []byte) (int64, []byte) {
	if off != 0 || len(data) == 0 {
		return off, data
	}
	if data[0] != '#' {
		return off, data
	}

	// Verify a newline exists early in the data, confirming text content.
	// Binary formats that happen to start with '#' (0x23) won't have a
	// newline at a natural text-line boundary.
	limit := 256
	if limit > len(data) {
		limit = len(data)
	}
	if !bytes.ContainsRune(data[:limit], '\n') {
		return off, data
	}

	if !bytes.Contains(data, []byte{0}) {
		return off, data
	}

	compact := make([]byte, 0, len(data))
	for i := 0; i < len(data); i++ {
		if data[i] == 0 {
			compact = append(compact, '\n')
			for i+1 < len(data) && data[i+1] == 0 {
				i++
			}
		} else {
			compact = append(compact, data[i])
		}
	}
	return 0, compact
}
