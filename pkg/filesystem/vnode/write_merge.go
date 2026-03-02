package vnode

import "bytes"

// mergedWrite holds a coalesced (offset, data) pair that covers two
// potentially non-contiguous byte ranges.
type mergedWrite struct {
	off  int64
	data []byte
}

// mergeWriteBuffer combines two (offset, data) ranges into a single buffer.
//
// Instead of zero-filling gaps between non-contiguous ranges, the chunks are
// concatenated directly. This prevents NULL bytes from leaking into files
// written with sparse/pwrite patterns (e.g. Claude Code shell snapshots use
// fixed-offset sections). Bash 5.x refuses to source files containing NULLs
// ("cannot execute binary file"), so compacting is the safer default.
//
// The returned offset is the minimum of the two input offsets; subsequent
// flush logic sends the buffer as a single PutObject at that offset.
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

	// Check if the ranges overlap or are contiguous — no gap to worry about.
	if oldEnd >= newOff && newEnd >= oldOff {
		maxEnd := oldEnd
		if newEnd > maxEnd {
			maxEnd = newEnd
		}
		combined := make([]byte, maxEnd-minOff)
		copy(combined[oldOff-minOff:], oldData)
		copy(combined[newOff-minOff:], newData)
		return &mergedWrite{off: minOff, data: combined}
	}

	// Non-overlapping: concatenate without gap. Order by offset so the
	// resulting byte stream matches the logical write order.
	var combined []byte
	if oldOff <= newOff {
		combined = make([]byte, len(oldData)+len(newData))
		copy(combined, oldData)
		copy(combined[len(oldData):], newData)
	} else {
		combined = make([]byte, len(newData)+len(oldData))
		copy(combined, newData)
		copy(combined[len(newData):], oldData)
	}

	return &mergedWrite{off: minOff, data: combined}
}

// compactNulls strips NULL bytes from data that appears to be a text file.
// This handles the case where the kernel page cache coalesces sparse/pwrite
// writes into a single contiguous buffer with zero-filled gaps. Bash 5.x on
// Linux refuses to source files containing NULLs ("cannot execute binary
// file"), so we strip them for text-like content written at offset 0.
//
// Only activates when: offset is 0 (full file replacement), the data starts
// with '#' (shell script / config comment), and NULLs are present.
func compactNulls(off int64, data []byte) (int64, []byte) {
	if off != 0 || len(data) == 0 {
		return off, data
	}
	if data[0] != '#' {
		return off, data
	}
	if !bytes.Contains(data, []byte{0}) {
		return off, data
	}

	// Remove all NULLs. For text files this simply closes the sparse gaps.
	compact := bytes.ReplaceAll(data, []byte{0}, nil)
	return 0, compact
}
