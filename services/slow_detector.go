package services

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"time"
)

// errSlowAbort is returned (via context cancel propagation) by a chunk read
// that was killed by the slow-detector. Callers may retry on a fresh conn.
var errSlowAbort = errors.New("aborted by slow detector")

// slowDetector tracks bytes/sec on a wrapped io.Reader and fires onSlow
// (typically a context cancel) when throughput stays below floor for window.
// One detector per S3 connection; cheap to allocate.
type slowDetector struct {
	floorBps int64
	window   time.Duration

	bytes  atomic.Int64
	stopCh chan struct{}
	abort  atomic.Bool

	onSlow context.CancelFunc // set by caller after construction
}

func newSlowDetector(floorBps int64, window time.Duration) *slowDetector {
	return &slowDetector{
		floorBps: floorBps,
		window:   window,
		stopCh:   make(chan struct{}),
	}
}

// wrap returns a Reader that counts bytes and starts the background sampler.
// First successful read starts the clock; we don't penalise TLS / TTFB.
func (sd *slowDetector) wrap(ctx context.Context, r io.Reader) io.Reader {
	go sd.sample(ctx)
	return &countingReader{sd: sd, r: r}
}

func (sd *slowDetector) sample(ctx context.Context) {
	// Sample interval ~ 1/4 of window — coarse enough to be cheap, fine enough to react.
	tick := sd.window / 4
	if tick < 100*time.Millisecond {
		tick = 100 * time.Millisecond
	}
	t := time.NewTicker(tick)
	defer t.Stop()

	// Wait until first bytes arrive before judging throughput (skip TTFB).
	for {
		select {
		case <-ctx.Done():
			return
		case <-sd.stopCh:
			return
		case <-t.C:
			if sd.bytes.Load() > 0 {
				goto monitor
			}
		}
	}

monitor:
	// Now monitor: every tick, compare current sample vs previous; if rate stays
	// below floor across the whole window (i.e. 4 consecutive ticks at 1/4 each),
	// fire onSlow.
	var prev int64 = sd.bytes.Load()
	prevAt := time.Now()
	below := 0
	required := int(sd.window / tick)
	if required < 1 {
		required = 1
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-sd.stopCh:
			return
		case <-t.C:
			now := sd.bytes.Load()
			dt := time.Since(prevAt).Seconds()
			delta := now - prev
			prev = now
			prevAt = time.Now()
			if dt <= 0 {
				continue
			}
			bps := int64(float64(delta) / dt)
			if bps < sd.floorBps {
				below++
				if below >= required {
					sd.abort.Store(true)
					if sd.onSlow != nil {
						sd.onSlow()
					}
					return
				}
			} else {
				below = 0
			}
		}
	}
}

func (sd *slowDetector) stop() {
	select {
	case <-sd.stopCh:
	default:
		close(sd.stopCh)
	}
}

func (sd *slowDetector) aborted() bool { return sd.abort.Load() }

type countingReader struct {
	sd *slowDetector
	r  io.Reader
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if n > 0 {
		c.sd.bytes.Add(int64(n))
	}
	return n, err
}
