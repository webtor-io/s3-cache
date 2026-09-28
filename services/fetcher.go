package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
)

const (
	ChunkSizeFlag         = "chunk-size"
	WorkersFlag           = "workers"
	BucketFlag            = "aws-bucket"
	FetchConcurrencyFlag  = "fetch-concurrency"
	ChunkFetchTimeoutFlag = "chunk-fetch-timeout"
	HeadCacheTTLFlag      = "head-cache-ttl"
)

const (
	sourceForeground = "foreground"
	sourceReadahead  = "readahead"
)

func RegisterFetcherFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.Int64Flag{
			Name:   ChunkSizeFlag,
			Usage:  "chunk size in bytes (also the cache granularity — change requires draining cache)",
			Value:  4 << 20,
			EnvVar: "CHUNK_SIZE",
		},
		cli.IntFlag{
			Name:   WorkersFlag,
			Usage:  "concurrent S3 fetch workers per request",
			Value:  8,
			EnvVar: "WORKERS",
		},
		cli.StringFlag{
			Name:   BucketFlag,
			Usage:  "S3 bucket to serve from (single-tenant: bucket is fixed by config, not in URL)",
			EnvVar: "AWS_BUCKET",
		},
		cli.IntFlag{
			Name:   FetchConcurrencyFlag,
			Usage:  "process-wide cap on chunk buffers in memory: upstream fetches in progress plus uncached chunks awaiting slow consumers (heap bound = this x chunk-size)",
			Value:  32,
			EnvVar: "FETCH_CONCURRENCY",
		},
		cli.DurationFlag{
			Name:   ChunkFetchTimeoutFlag,
			Usage:  "deadline for one detached chunk fetch; generous enough to ride out upstream per-connection throttle windows",
			Value:  90 * time.Second,
			EnvVar: "CHUNK_FETCH_TIMEOUT",
		},
		cli.DurationFlag{
			Name:   HeadCacheTTLFlag,
			Usage:  "TTL for cached HeadObject metadata (objects are immutable; stale entries also serve as fallback during upstream outages)",
			Value:  60 * time.Second,
			EnvVar: "HEAD_CACHE_TTL",
		},
	)
}

type Fetcher struct {
	s3cl              *cs.S3Client
	bucket            string
	chunkSize         int64
	workers           int
	cache             *DiskCache
	sf                *singleflight
	readahead         *Readahead
	budget            *chunkBudget
	chunkFetchTimeout time.Duration
	// writeGuardTimeout bounds one chunk write while the request holds
	// chunk-buffer budget (see writeGuard). Half the fetch deadline: a
	// fetch queued behind a stalled holder still gets a unit with time
	// left to download.
	writeGuardTimeout time.Duration
	heads             *headCache

	flights  atomic.Int64      // published flights not yet released (leak checks in tests)
	afterPut func(path string) // test hook: runs between the cache write and the callers' opens
}

func NewFetcher(c *cli.Context, s3cl *cs.S3Client, cache *DiskCache, readahead *Readahead) *Fetcher {
	bucket := c.String(BucketFlag)
	if bucket == "" {
		log.Fatal("AWS_BUCKET is required")
	}
	chunkSize := c.Int64(ChunkSizeFlag)
	return &Fetcher{
		s3cl:              s3cl,
		bucket:            bucket,
		chunkSize:         chunkSize,
		workers:           c.Int(WorkersFlag),
		cache:             cache,
		sf:                newSingleflight(),
		readahead:         readahead,
		budget:            newChunkBudget(c.Int(FetchConcurrencyFlag), chunkSize),
		chunkFetchTimeout: c.Duration(ChunkFetchTimeoutFlag),
		writeGuardTimeout: c.Duration(ChunkFetchTimeoutFlag) / 2,
		heads:             newHeadCache(c.Duration(HeadCacheTTLFlag)),
	}
}

// headObject returns object metadata through a TTL cache. Objects are
// content-addressed and immutable, so a fresh entry short-circuits the
// upstream HEAD every GET would otherwise pay. When the upstream HEAD
// fails and a stale entry exists, the stale entry is served instead —
// fully-disk-cached content must stay servable through upstream outages
// (the cache exists to decouple serving from the upstream).
func (f *Fetcher) headObject(ctx context.Context, key string) (*s3.HeadObjectOutput, error) {
	if out, fresh := f.heads.get(key); fresh {
		return out, nil
	}
	out, err := f.s3cl.Get().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		if stale, _ := f.heads.get(key); stale != nil {
			log.WithError(err).WithField("key", key).Warn("HEAD failed, serving stale metadata")
			return stale, nil
		}
		return nil, err
	}
	f.heads.put(key, out)
	return out, nil
}

// setObjectHeaders copies Content-Type / ETag / Last-Modified from the
// HeadObject result to w. Players resume interrupted streams with
// If-Range/restart heuristics keyed on the validators — the TWS path
// (http.ServeContent) always emits them, and their absence on the vault
// path made KSPlayer restart from byte 0 instead of resuming (2026-07-06
// end-of-episode drop). Content-Type is normalized: vault uploads carry
// S3's default "binary/octet-stream", which is not a registered MIME type.
func setObjectHeaders(w http.ResponseWriter, out *s3.HeadObjectOutput) {
	ct := "application/octet-stream"
	if out.ContentType != nil && *out.ContentType != "" && *out.ContentType != "binary/octet-stream" {
		ct = *out.ContentType
	}
	w.Header().Set("Content-Type", ct)
	if out.ETag != nil {
		w.Header().Set("ETag", *out.ETag)
	}
	if out.LastModified != nil {
		w.Header().Set("Last-Modified", out.LastModified.UTC().Format(http.TimeFormat))
	}
}

// Head serves object metadata (through the TTL head cache) and copies
// Content-Length / Content-Type / ETag / Last-Modified to w.
func (f *Fetcher) Head(ctx context.Context, w http.ResponseWriter, key string) error {
	out, err := f.headObject(ctx, key)
	if err != nil {
		return err
	}
	if out.ContentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*out.ContentLength, 10))
	}
	setObjectHeaders(w, out)
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	return nil
}

// Get serves a GET (range) request through the aligned-chunk path.
// end == -1 means "to the end"; suffixLen > 0 means the "bytes=-N" suffix
// form (last N bytes). A HEAD always runs first: the object size is needed
// to resolve suffix ranges, clamp end, reject start-past-EOF with a proper
// 416 and emit an honest total in Content-Range. Players (ffmpeg/AVPlayer)
// and thp's retry layer parse `Content-Range: bytes */size` on 416 to tell
// clean EOF from a real error — omitting it broke end-of-file playback of
// vaulted content (see the 2026-07-05 Stremio incident).
// rangeRequested distinguishes a true Range request from a plain GET —
// the former gets 206 + Content-Range, the latter gets 200 + full
// Content-Length. Returning 206 to a client that didn't send Range
// trips up some HTTP clients and reverse-proxies (thp's redirect
// follower notably gags on it) — that mismatch is what caused the
// production 403s we saw on plain downloads.
func (f *Fetcher) Get(ctx context.Context, w http.ResponseWriter, key string, start, end, suffixLen int64, rangeRequested bool) error {
	hd, err := f.headObject(ctx, key)
	if err != nil {
		httpErrorFromS3(w, err)
		return err
	}
	if hd.ContentLength == nil {
		http.Error(w, "upstream missing Content-Length", http.StatusBadGateway)
		return errors.New("upstream missing Content-Length")
	}
	totalSize := *hd.ContentLength

	if suffixLen > 0 {
		// "bytes=-N": last N bytes, clamped to the whole object.
		start = totalSize - suffixLen
		if start < 0 {
			start = 0
		}
		end = totalSize - 1
	}
	if rangeRequested && start >= totalSize {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", totalSize))
		http.Error(w, "range not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return errors.Errorf("range not satisfiable: start=%d size=%d", start, totalSize)
	}
	if end < 0 || end > totalSize-1 {
		end = totalSize - 1
	}

	// Validators go on success responses only — past the 416 gate, so error
	// bodies don't claim ETag/Last-Modified for content that was not served.
	setObjectHeaders(w, hd)

	if totalSize == 0 {
		// Empty object: plain GET gets an empty 200; any Range on it is
		// unsatisfiable and was rejected above.
		w.Header().Set("Content-Length", "0")
		w.Header().Set("Accept-Ranges", "bytes")
		w.WriteHeader(http.StatusOK)
		return nil
	}

	return f.serveAligned(ctx, w, key, start, end, totalSize, rangeRequested)
}

// serveAligned splits [start..end] into chunkSize-aligned chunks
// (absolute offsets, not request-relative), fetches them in parallel,
// and writes the requested span to the client in order. Each chunk goes
// through cache → singleflight → upstream.
//
// Status commit is deferred until chunk 0 resolves: a pre-header
// upstream failure yields a clean 502; failures on later chunks can
// only abort an already-streaming response.
func (f *Fetcher) serveAligned(ctx context.Context, w http.ResponseWriter, key string, start, end, totalSize int64, rangeRequested bool) error {
	chunkSize := f.chunkSize
	firstChunkIdx := start / chunkSize
	lastChunkIdx := end / chunkSize
	nChunks := int(lastChunkIdx - firstChunkIdx + 1)

	pending := make([]chan chunkResult, nChunks)
	for i := range pending {
		pending[i] = make(chan chunkResult, 1)
	}

	// Slot semaphore caps how far fetches can run ahead of the consumer
	// (and thereby the open file handles / buffer references one request
	// holds). A slot is taken per chunk in order and freed once the
	// consumer has written that chunk.
	window := f.workers
	if window < 4 {
		window = 4
	}
	slots := make(chan struct{}, window)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	guard := newWriteGuard(http.NewResponseController(w), f.writeGuardTimeout)

	// The dispatcher starts chunk fetches strictly in order and admits
	// chunk i+1 only once chunk i no longer waits for buffer budget (cache
	// hit, or its fetch holds a unit). Without the ordering a request could
	// hold budget for later chunks — buffers only its own consumer
	// releases — while its head chunk queues for budget; enough such
	// requests exhaust the budget and every one of them stalls until the
	// fetch timeout. With it, a request that holds budget for chunk j has
	// every earlier chunk in hand or in progress, so its consumer always
	// advances and eventually returns the budget.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for idx := 0; idx < nChunks; idx++ {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			// The select picks pseudo-randomly when both cases are ready —
			// don't start new work for a request that's already gone.
			if ctx.Err() != nil {
				return
			}
			absChunkIdx := firstChunkIdx + int64(idx)
			cStart := absChunkIdx * chunkSize
			cEnd := cStart + chunkSize - 1
			if totalSize > 0 && cEnd > totalSize-1 {
				cEnd = totalSize - 1
			}
			admitted := make(chan struct{})
			var once sync.Once
			admit := func() { once.Do(func() { close(admitted) }) }
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				defer admit()
				cr, err := f.fetchChunk(ctx, key, cStart, cEnd, sourceForeground, admit)
				if err != nil {
					cr = chunkResult{err: err}
				}
				cr.chunkStart = cStart
				// A buffered chunk pins budget until it is written: count it
				// before the hand-off (see writeGuard).
				if cr.via == viaBuffer {
					guard.hold()
				}
				pending[idx] <- cr
			}(idx)
			select {
			case <-admitted:
			case <-ctx.Done():
				return
			}
		}
	}()

	abort := func() {
		cancel()
		wg.Wait()
		cleanupPending(pending)
	}

	// Wait for chunk 0 before committing status — earlier failure surfaces
	// as a real HTTP error, not a half-written response.
	var res0 chunkResult
	select {
	case res0 = <-pending[0]:
	case <-ctx.Done():
		abort()
		dropValidatorHeaders(w)
		http.Error(w, "request cancelled", http.StatusBadGateway)
		return ctx.Err()
	}
	if res0.err != nil {
		abort()
		dropValidatorHeaders(w)
		httpErrorFromS3(w, res0.err)
		return res0.err
	}

	contentRange := ""
	status := http.StatusOK
	if rangeRequested {
		contentRange = fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize)
		status = http.StatusPartialContent
	}
	writeRangeHeaders(w, contentRange, "", totalSize, start, end)
	w.WriteHeader(status)

	err := writeChunk(w, guard, &res0, start, end)
	via := res0.via
	res0.close()
	if err != nil {
		abort()
		return err
	}
	chunkServes.WithLabelValues(via).Inc()
	<-slots

	for i := 1; i < nChunks; i++ {
		select {
		case res := <-pending[i]:
			if res.err != nil {
				log.WithFields(log.Fields{
					"key": key, "chunk": i,
				}).WithError(res.err).Warn("chunk failed mid-response")
				abort()
				return res.err
			}
			err := writeChunk(w, guard, &res, start, end)
			via := res.via
			res.close()
			if err != nil {
				abort()
				return err
			}
			chunkServes.WithLabelValues(via).Inc()
			<-slots
		case <-ctx.Done():
			abort()
			return ctx.Err()
		}
	}
	wg.Wait()

	// Sequential readahead kicks past the served tail. Fire-and-forget;
	// schedule() handles dedup / saturation / cache-already-hit.
	if f.readahead != nil {
		f.readahead.Kick(f, key, lastChunkIdx+1, totalSize)
	}
	return nil
}

// fetchChunk pulls a single aligned chunk: cache lookup → singleflight →
// upstream + cache write. `source` labels metrics for foreground vs
// readahead traffic. Callers MUST chunkResult.close() the result.
//
// admit (may be nil) is called exactly once, as soon as this chunk no
// longer waits for buffer budget: on a cache hit, when the fetch it
// leads or joined holds a budget unit, or on any early return.
//
// start MUST be chunkSize-aligned; end is start+chunkSize-1 unless this
// is the last chunk in the object (EOF-clamped). The cache key is keyed
// on start only — the same aligned offset always means the same chunk.
func (f *Fetcher) fetchChunk(ctx context.Context, key string, start, end int64, source string, admit func()) (chunkResult, error) {
	if admit == nil {
		admit = func() {}
	}
	if file, size, err := f.cache.Get(key, start); err != nil {
		cacheLookups.WithLabelValues("error").Inc()
		log.WithError(err).WithFields(log.Fields{
			"key": key, "chunk_start": start,
		}).Warn("cache get failed")
	} else if file != nil {
		cacheLookups.WithLabelValues("hit").Inc()
		cacheBytesServed.Add(float64(size))
		admit()
		return chunkResult{file: file, fileSize: size, chunkStart: start, via: viaHit}, nil
	} else {
		cacheLookups.WithLabelValues("miss").Inc()
	}

	if ctx.Err() != nil {
		admit()
		return chunkResult{}, ctx.Err()
	}

	// The upstream fetch is detached from the request context on purpose:
	// an impatient client (players abort slow tail fetches within ~1s and
	// retry) must not cancel it, or the chunk never lands in cache and every
	// retry starts cold — under upstream per-connection throttling that
	// loops until the player gives up (the 2026-07 credits-drop storms).
	// The WAIT below stays cancellable though: an aborted caller returns
	// immediately while the fetch finishes in the background (bounded by
	// the buffer budget + chunkFetchTimeout) and warms the cache for the
	// next retry.
	sfKey := fmt.Sprintf("%s/%d", key, start)
	c, leader := f.sf.join(sfKey)
	if leader {
		go f.lead(sfKey, key, start, end, source, c)
	} else {
		singleflightShared.Inc()
	}

	// Claim the result in a goroutine that outlives an aborted caller: the
	// flight counted this caller, so its reference must be dropped whether
	// or not anyone is left to read the chunk. The handoff is unbuffered —
	// either the caller receives the result (and owns closing it), or the
	// caller is gone and the result is closed here.
	out := make(chan chunkResult)
	go func() {
		<-c.done
		cr := c.res.claim(start)
		select {
		case out <- cr:
		case <-ctx.Done():
			cr.close()
		}
	}()

	select {
	case <-c.admitted:
		admit()
	case <-ctx.Done():
		admit()
		return chunkResult{}, ctx.Err()
	}
	select {
	case cr := <-out:
		return cr, cr.err
	case <-ctx.Done():
		return chunkResult{}, ctx.Err()
	}
}

// lead runs the upstream fetch for one singleflight call, detached from
// every caller's context (see fetchChunk). It always finishes the call, so
// every caller has a result to claim.
func (f *Fetcher) lead(sfKey, key string, start, end int64, source string, c *sfCall) {
	dctx, cancel := context.WithTimeout(context.Background(), f.chunkFetchTimeout)
	defer cancel()

	size := end - start + 1
	res := &chunkFlight{size: size}
	f.flights.Add(1)
	res.free = func() {
		if res.pin != nil {
			_ = res.pin.Close()
		}
		if res.buf != nil {
			f.budget.release(size)
		}
		f.flights.Add(-1)
	}

	if err := f.budget.acquire(dctx); err != nil {
		res.err = errors.New("chunk fetch queue timeout")
		f.sf.finish(sfKey, c, res)
		return
	}
	c.admit()

	buf := f.budget.alloc(size)
	if err := f.download(dctx, key, start, end, source, buf); err != nil {
		f.budget.release(size)
		res.err = err
		f.sf.finish(sfKey, c, res)
		return
	}

	// Cache failures are logged but don't fail the request — a degraded
	// cache is still better than no response.
	pin, path, err := f.cache.Put(key, start, buf)
	if err != nil {
		cacheWrites.WithLabelValues("error").Inc()
		log.WithError(err).WithFields(log.Fields{
			"key": key, "chunk_start": start,
		}).Warn("cache put failed")
	} else if pin != nil {
		cacheWrites.WithLabelValues("ok").Inc()
	}
	if pin != nil {
		// Cached: every caller reads the file, the buffer goes now. Holding
		// it until slow consumers drained it was the 2026-09-28 heap blowup
		// (~70 slow downloads × up to 8 chunks each, no global cap).
		f.budget.release(size)
		res.path, res.pin = path, pin
		if f.afterPut != nil {
			f.afterPut(path)
		}
	} else {
		// Not cached (cache disabled or Put failed): callers share the
		// buffer, and its budget unit stays taken until the last one is
		// done with it.
		res.buf = buf
	}
	f.sf.finish(sfKey, c, res)
}

// dropValidatorHeaders strips ETag/Last-Modified before an error status is
// written — error bodies must not claim validators for content that was
// never served.
func dropValidatorHeaders(w http.ResponseWriter) {
	w.Header().Del("ETag")
	w.Header().Del("Last-Modified")
}

// download fills buf with bytes [start..end] of key from S3.
func (f *Fetcher) download(ctx context.Context, key string, start, end int64, source string, buf []byte) error {
	t0 := time.Now()
	body, _, _, err := f.openRange(ctx, key, start, end)
	if err != nil {
		return err
	}
	defer body.Close()

	n, err := io.ReadFull(body, buf)
	if err != nil {
		// A short read is an upstream failure, never a valid chunk — the
		// requested span is always clamped to the object end, so anything
		// less than the full buffer must NOT reach the cache (a truncated
		// cached chunk poisons every response over this offset until
		// eviction).
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.Wrapf(err, "read chunk body: got %d of %d bytes", n, len(buf))
	}
	upstreamChunkDuration.WithLabelValues(source).Observe(time.Since(t0).Seconds())
	upstreamBytesFetched.Add(float64(n))
	return nil
}

// openRange opens a single Range GET against S3. Returns body +
// Content-Range + Content-Type.
func (f *Fetcher) openRange(ctx context.Context, key string, start, end int64) (io.ReadCloser, string, string, error) {
	in := &s3.GetObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	out, err := f.s3cl.Get().GetObjectWithContext(ctx, in, func(r *request.Request) {
		r.HTTPRequest.Header.Set("User-Agent", "webtor-s3-cache/0.2")
	})
	if err != nil {
		return nil, "", "", err
	}
	cr := ""
	if out.ContentRange != nil {
		cr = *out.ContentRange
	}
	ct := ""
	if out.ContentType != nil {
		ct = *out.ContentType
	}
	return out.Body, cr, ct, nil
}

// writeRangeHeaders sets response headers for a successful GET.
// contentRange="" signals "no Range was requested" — we omit
// Content-Range entirely and let the WriteHeader caller emit 200.
// Setting Content-Range on a 200 response confuses some HTTP clients.
func writeRangeHeaders(w http.ResponseWriter, contentRange, contentType string, totalSize, start, end int64) {
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	if contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	w.Header().Set("Accept-Ranges", "bytes")
}
