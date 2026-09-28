package services

import (
	"context"
	"sync/atomic"
)

// chunkBudget caps the chunk bytes that live in the heap: upstream
// downloads in progress plus buffers of chunks that could not be cached,
// held until their consumers have written them out. Chunks that reached
// the disk cache are served from the file and hold no budget once the
// write is done.
//
// It is a counting semaphore in whole-chunk units — every buffer is at
// most chunkSize — so the byte bound is units × chunkSize. Waiters are
// served in arrival order: a Go channel's send queue is FIFO, and a
// release hands the freed slot straight to the oldest waiter, so a
// stream of new fetches cannot starve an older one.
type chunkBudget struct {
	units chan struct{}
	inUse atomic.Int64 // bytes allocated under the budget right now
	peak  atomic.Int64 // high-water mark of inUse (tests, diagnostics)
}

func newChunkBudget(units int, unitBytes int64) *chunkBudget {
	chunkBufferBudgetBytes.Set(float64(int64(units) * unitBytes))
	return &chunkBudget{units: make(chan struct{}, units)}
}

// acquire takes one unit, blocking until one is free or ctx is done.
func (b *chunkBudget) acquire(ctx context.Context) error {
	select {
	case b.units <- struct{}{}:
		return nil
	default:
	}
	chunkBudgetWaits.Inc()
	select {
	case b.units <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// alloc allocates an n-byte buffer under a unit taken by acquire.
func (b *chunkBudget) alloc(n int64) []byte {
	buf := make([]byte, n)
	v := b.inUse.Add(n)
	chunkBufferBytes.Add(float64(n))
	for {
		p := b.peak.Load()
		if v <= p || b.peak.CompareAndSwap(p, v) {
			break
		}
	}
	return buf
}

// release returns a unit together with the n bytes alloc'ed under it
// (0 when nothing was allocated). The caller must drop its buffer.
func (b *chunkBudget) release(n int64) {
	if n > 0 {
		b.inUse.Add(-n)
		chunkBufferBytes.Sub(float64(n))
	}
	<-b.units
}
