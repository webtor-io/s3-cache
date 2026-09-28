package services

import (
	"io"
	"os"
	"sync/atomic"
)

// Where the bytes of a served chunk came from (s3cache_chunk_serves_total).
const (
	viaHit    = "hit"    // cache hit: own handle on the cache file
	viaFile   = "file"   // miss, cached by this fetch: own handle on the fresh file
	viaPinned = "pinned" // miss, cached, but the path was evicted before we opened it
	viaBuffer = "buffer" // miss, not cached (cache disabled or Put failed)
)

// chunkFlight is the outcome of one upstream chunk fetch, shared by every
// caller that joined it. Exactly one of {path+pin, buf} is set on success.
//
// A cached chunk is served from the file, never from memory: the download
// buffer is dropped (and its budget returned) right after the cache write.
// pin is the handle the write went through; it keeps the inode alive if
// the evictor unlinks the path before a caller has opened it.
type chunkFlight struct {
	err  error
	size int64
	path string
	pin  *os.File
	buf  []byte
	free func() // runs once, when the last reference is dropped
	refs atomic.Int32
}

func (fl *chunkFlight) unref() {
	if fl.refs.Add(-1) == 0 && fl.free != nil {
		fl.free()
	}
}

// claim turns the flight into this caller's chunkResult and accounts for
// its reference: dropped right away when the caller ends up with nothing
// shared (an error, or a private handle on the cache file), carried by the
// chunkResult — and dropped by its close() — when the caller reads shared
// state (the buffer, or the pinned handle). Every caller that joined the
// flight must claim exactly once.
func (fl *chunkFlight) claim(chunkStart int64) chunkResult {
	if fl.err != nil {
		fl.unref()
		return chunkResult{err: fl.err, chunkStart: chunkStart}
	}
	if fl.buf != nil {
		return chunkResult{data: fl.buf, chunkStart: chunkStart, via: viaBuffer, done: fl.unref}
	}
	// A private handle has its own file offset, which sendfile needs: on
	// Linux it reads from and advances the descriptor's offset, so one
	// handle can't serve concurrent consumers.
	if file, err := os.Open(fl.path); err == nil {
		fl.unref()
		return chunkResult{file: file, fileSize: fl.size, chunkStart: chunkStart, via: viaFile}
	}
	// Evicted (or otherwise unopenable) since the write: read the pinned
	// inode with pread. Correct and heap-free, just not zero-copy.
	return chunkResult{
		section:    io.NewSectionReader(fl.pin, 0, fl.size),
		fileSize:   fl.size,
		chunkStart: chunkStart,
		via:        viaPinned,
		done:       fl.unref,
	}
}

// chunkResult is the fetch→consumer payload. On success exactly one of
// {file, section, data} is set: file is a private handle on a cache file
// (streamed with sendfile), section reads a pinned cache file shared with
// other callers, data is an uncached chunk's buffer shared with other
// callers.
//
// The consumer MUST call close() on every chunkResult it receives, even on
// error paths — otherwise a file handle or a shared buffer's budget leaks.
// A chunkResult is passed by value; close exactly one copy.
type chunkResult struct {
	data       []byte
	file       *os.File
	section    *io.SectionReader
	fileSize   int64
	err        error
	chunkStart int64
	via        string
	done       func() // drops the shared-flight reference, if one is held
}

func (cr *chunkResult) size() int64 {
	if cr.file != nil || cr.section != nil {
		return cr.fileSize
	}
	return int64(len(cr.data))
}

func (cr *chunkResult) close() {
	if cr.file != nil {
		_ = cr.file.Close()
		cr.file = nil
	}
	cr.section = nil
	cr.data = nil
	if cr.done != nil {
		cr.done()
		cr.done = nil
	}
}

// writeSlice writes the portion of the chunk that falls inside
// [reqStart..reqEnd]. A private file handle is Seek'ed and io.CopyN'ed
// (net/http turns that into sendfile); a pinned section is copied through
// a buffer; an in-memory chunk is written directly.
func (cr *chunkResult) writeSlice(w io.Writer, reqStart, reqEnd int64) (int64, error) {
	chunkStart := cr.chunkStart
	chunkEnd := chunkStart + cr.size() - 1
	sliceStart := int64(0)
	if reqStart > chunkStart {
		sliceStart = reqStart - chunkStart
	}
	sliceEnd := cr.size()
	if reqEnd < chunkEnd {
		sliceEnd = reqEnd - chunkStart + 1
	}
	if sliceStart >= sliceEnd {
		return 0, nil
	}
	switch {
	case cr.file != nil:
		if _, err := cr.file.Seek(sliceStart, io.SeekStart); err != nil {
			return 0, err
		}
		return io.CopyN(w, cr.file, sliceEnd-sliceStart)
	case cr.section != nil:
		if _, err := cr.section.Seek(sliceStart, io.SeekStart); err != nil {
			return 0, err
		}
		return io.CopyN(w, cr.section, sliceEnd-sliceStart)
	}
	n, err := w.Write(cr.data[sliceStart:sliceEnd])
	return int64(n), err
}

// cleanupPending closes any chunkResult left in the per-chunk channels.
// Call it only after every producer has finished (wg.Wait), so nothing is
// delivered after the drain.
func cleanupPending(pending []chan chunkResult) {
	for i := range pending {
		select {
		case cr := <-pending[i]:
			cr.close()
		default:
		}
	}
}
