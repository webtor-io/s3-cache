package services

import (
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
)

// writeGuard bounds how long a client that stops reading can keep
// chunk-buffer budget pinned.
//
// An uncached chunk's buffer holds a budget unit until every consumer has
// written it, so a request holds a unit for each buffered chunk delivered
// to its window and not yet written. When its client stops reading (a
// paused player), the consumer blocks in a write — on a buffered chunk, or
// on a cached chunk while buffered ones wait behind it — and those units
// never come back: a few such requests take the whole budget, and every
// other miss on the node waits in the queue until CHUNK_FETCH_TIMEOUT and
// fails with a 502.
//
// So while the request holds buffered chunks, a chunk write and its flush
// run under a write deadline, and a client that can't take the chunk in
// time is cut off; its units go back with the aborted request. The
// deadline is armed when a write starts with buffers held, or when a
// buffered chunk arrives during a write, and cleared after every chunk:
// the server has no WriteTimeout, so nothing else would reset it on a
// kept-alive connection. A request that holds no buffers — every chunk a
// hit or a cached miss — pins no budget and is never cut; paused players
// keep their connections as before.
type writeGuard struct {
	rc      *http.ResponseController
	timeout time.Duration // <= 0 disables the guard

	mu      sync.Mutex
	held    int  // buffered chunks delivered to the request, not yet written
	writing bool // the consumer is inside a chunk write
	armed   bool // a write deadline is set on the connection
}

func newWriteGuard(rc *http.ResponseController, timeout time.Duration) *writeGuard {
	return &writeGuard{rc: rc, timeout: timeout}
}

// hold counts a buffered chunk delivered to the request. The fetch side
// calls it before the hand-off, so the chunk is counted before the
// consumer can write it.
func (g *writeGuard) hold() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.held++
	if g.writing && !g.armed {
		g.arm()
	}
}

func (g *writeGuard) begin() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writing = true
	if g.held > 0 {
		g.arm()
	}
}

// end closes a chunk write; buffered says the chunk was one hold counted.
func (g *writeGuard) end(buffered bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.writing = false
	if buffered {
		g.held--
	}
	if g.armed {
		_ = g.rc.SetWriteDeadline(time.Time{})
		g.armed = false
	}
}

func (g *writeGuard) arm() {
	if g.timeout <= 0 {
		return
	}
	if g.rc.SetWriteDeadline(time.Now().Add(g.timeout)) == nil {
		g.armed = true
	}
}

// writeChunk writes one chunk's slice of [start..end] to the client and
// flushes it, under the guard. The flush is inside: the tail of a write
// can sit in the response's bufio and block there just as well.
func writeChunk(w http.ResponseWriter, g *writeGuard, res *chunkResult, start, end int64) error {
	g.begin()
	defer g.end(res.via == viaBuffer)
	_, err := res.writeSlice(w, start, end)
	if err == nil {
		if ferr := g.rc.Flush(); ferr != nil && !errors.Is(ferr, http.ErrNotSupported) {
			err = ferr
		}
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		stalledWriteCuts.Inc()
	}
	return err
}
