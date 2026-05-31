package services

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	ReadaheadChunksFlag      = "readahead-chunks"
	ReadaheadConcurrencyFlag = "readahead-concurrency"
	ReadaheadTimeoutFlag     = "readahead-timeout"
)

func RegisterReadaheadFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.IntFlag{
			Name:   ReadaheadChunksFlag,
			Usage:  "number of chunks to prefetch past the served range (0 disables)",
			Value:  4,
			EnvVar: "READAHEAD_CHUNKS",
		},
		cli.IntFlag{
			Name:   ReadaheadConcurrencyFlag,
			Usage:  "max concurrent readahead fetches (process-wide)",
			Value:  8,
			EnvVar: "READAHEAD_CONCURRENCY",
		},
		cli.DurationFlag{
			Name:   ReadaheadTimeoutFlag,
			Usage:  "per-chunk readahead timeout",
			Value:  30 * time.Second,
			EnvVar: "READAHEAD_TIMEOUT",
		},
	)
}

// Readahead prefetches the next K aligned chunks after a served range to
// catch sequential playback / scan patterns. Best-effort: drops requests
// rather than queueing when saturated, so it never crowds out
// foreground traffic.
//
// Dedup is delegated to the fetcher's singleflight — if a foreground
// fetch for the same chunk lands first, the readahead callback joins it.
type Readahead struct {
	chunks      int
	concurrency int
	timeout     time.Duration
	sem         chan struct{}

	mu       sync.Mutex
	inflight map[string]struct{} // dedup against own concurrent kicks
}

func NewReadahead(c *cli.Context) *Readahead {
	conc := c.Int(ReadaheadConcurrencyFlag)
	if conc < 1 {
		conc = 1
	}
	return &Readahead{
		chunks:      c.Int(ReadaheadChunksFlag),
		concurrency: conc,
		timeout:     c.Duration(ReadaheadTimeoutFlag),
		sem:         make(chan struct{}, conc),
		inflight:    make(map[string]struct{}),
	}
}

// Kick schedules readahead for `chunks` aligned chunks starting at
// fromChunkIdx. totalSize must be known (>0); without it we don't know
// where the file ends.
func (r *Readahead) Kick(f *Fetcher, key string, fromChunkIdx, totalSize int64) {
	if r == nil || r.chunks == 0 || totalSize <= 0 {
		return
	}
	chunkSize := f.chunkSize
	lastChunkIdx := (totalSize - 1) / chunkSize
	for i := 0; i < r.chunks; i++ {
		c := fromChunkIdx + int64(i)
		if c > lastChunkIdx {
			return
		}
		r.schedule(f, key, c, totalSize)
	}
}

func (r *Readahead) schedule(f *Fetcher, key string, chunkIdx, totalSize int64) {
	chunkSize := f.chunkSize
	cStart := chunkIdx * chunkSize
	cEnd := cStart + chunkSize - 1
	if cEnd > totalSize-1 {
		cEnd = totalSize - 1
	}

	if data, _ := f.cache.Get(key, cStart); data != nil {
		readaheadKicks.WithLabelValues("already_cached").Inc()
		return
	}

	dedupKey := key + "/" + itoa(cStart)
	r.mu.Lock()
	if _, ok := r.inflight[dedupKey]; ok {
		r.mu.Unlock()
		readaheadKicks.WithLabelValues("already_cached").Inc()
		return
	}
	r.inflight[dedupKey] = struct{}{}
	r.mu.Unlock()

	select {
	case r.sem <- struct{}{}:
	default:
		// Saturated: drop. Foreground request will fetch on demand if
		// the client reaches this chunk.
		r.mu.Lock()
		delete(r.inflight, dedupKey)
		r.mu.Unlock()
		readaheadKicks.WithLabelValues("dropped").Inc()
		return
	}

	readaheadKicks.WithLabelValues("scheduled").Inc()
	go func() {
		defer func() {
			<-r.sem
			r.mu.Lock()
			delete(r.inflight, dedupKey)
			r.mu.Unlock()
		}()
		ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
		defer cancel()
		if _, err := f.fetchChunk(ctx, key, cStart, cEnd, sourceReadahead); err != nil {
			log.WithFields(log.Fields{
				"key": key, "chunk_start": cStart,
			}).WithError(err).Debug("readahead failed")
		}
	}()
}

func itoa(n int64) string {
	// Fast path: avoid strconv import here; the dedup key only needs a
	// stable string form.
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
