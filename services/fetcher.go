package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
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
	ChunkSizeFlag = "chunk-size"
	WorkersFlag   = "workers"
	BucketFlag    = "aws-bucket"
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
	)
}

type Fetcher struct {
	s3cl      *cs.S3Client
	bucket    string
	chunkSize int64
	workers   int
	cache     *DiskCache
	sf        *singleflight
	readahead *Readahead
}

func NewFetcher(c *cli.Context, s3cl *cs.S3Client, cache *DiskCache, readahead *Readahead) *Fetcher {
	bucket := c.String(BucketFlag)
	if bucket == "" {
		log.Fatal("AWS_BUCKET is required")
	}
	return &Fetcher{
		s3cl:      s3cl,
		bucket:    bucket,
		chunkSize: c.Int64(ChunkSizeFlag),
		workers:   c.Int(WorkersFlag),
		cache:     cache,
		sf:        newSingleflight(),
		readahead: readahead,
	}
}

// Head issues HeadObject and copies Content-Length / Content-Type / ETag to w.
func (f *Fetcher) Head(ctx context.Context, w http.ResponseWriter, key string) error {
	out, err := f.s3cl.Get().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(f.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	if out.ContentLength != nil {
		w.Header().Set("Content-Length", strconv.FormatInt(*out.ContentLength, 10))
	}
	if out.ContentType != nil {
		w.Header().Set("Content-Type", *out.ContentType)
	}
	if out.ETag != nil {
		w.Header().Set("ETag", *out.ETag)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusOK)
	return nil
}

// Get serves a GET (range) request through the aligned-chunk path.
// end == -1 means "to the end" and forces a HEAD to discover totalSize.
// rangeRequested distinguishes a true Range request from a plain GET —
// the former gets 206 + Content-Range, the latter gets 200 + full
// Content-Length. Returning 206 to a client that didn't send Range
// trips up some HTTP clients and reverse-proxies (thp's redirect
// follower notably gags on it) — that mismatch is what caused the
// production 403s we saw on plain downloads.
func (f *Fetcher) Get(ctx context.Context, w http.ResponseWriter, key string, start, end int64, rangeRequested bool) error {
	totalSize := int64(-1)
	if end < 0 {
		hd, err := f.s3cl.Get().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			httpErrorFromS3(w, err)
			return err
		}
		if hd.ContentLength == nil {
			http.Error(w, "upstream missing Content-Length", http.StatusBadGateway)
			return errors.New("upstream missing Content-Length")
		}
		totalSize = *hd.ContentLength
		end = totalSize - 1
		if hd.ContentType != nil {
			w.Header().Set("Content-Type", *hd.ContentType)
		}
	}

	wantLen := end - start + 1
	if wantLen <= 0 {
		http.Error(w, "empty range", http.StatusRequestedRangeNotSatisfiable)
		return errors.New("empty range")
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

	workers := f.workers
	if workers > nChunks {
		workers = nChunks
	}

	pending := make([]chan chunkResult, nChunks)
	for i := range pending {
		pending[i] = make(chan chunkResult, 1)
	}

	jobs := make(chan int, nChunks)
	for i := 0; i < nChunks; i++ {
		jobs <- i
	}
	close(jobs)

	// Slot semaphore caps how far workers can race ahead of the
	// consumer. Without it 250 chunks of a 1 GiB file could all
	// materialize in RAM before the client drains. Cap = workers
	// (one in-flight chunk per worker) — under slow-client load this
	// halves the transient working set vs the old workers*2 default
	// without measurably hurting throughput, since workers re-fill
	// the moment the consumer drains.
	slotCap := workers
	if slotCap < 4 {
		slotCap = 4
	}
	slots := make(chan struct{}, slotCap)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				select {
				case slots <- struct{}{}:
				case <-ctx.Done():
					pending[idx] <- chunkResult{err: ctx.Err()}
					return
				}
				absChunkIdx := firstChunkIdx + int64(idx)
				cStart := absChunkIdx * chunkSize
				cEnd := cStart + chunkSize - 1
				if totalSize > 0 && cEnd > totalSize-1 {
					cEnd = totalSize - 1
				}
				cr, err := f.fetchChunk(ctx, key, cStart, cEnd, sourceForeground)
				if err != nil {
					pending[idx] <- chunkResult{err: err, chunkStart: cStart}
					continue
				}
				cr.chunkStart = cStart
				pending[idx] <- cr
			}
		}()
	}

	// Wait for chunk 0 before committing status — earlier failure surfaces
	// as a real HTTP error, not a half-written response.
	var res0 chunkResult
	select {
	case res0 = <-pending[0]:
	case <-ctx.Done():
		cancel()
		cleanupPending(slots, pending)
		wg.Wait()
		cleanupPending(slots, pending)
		http.Error(w, "request cancelled", http.StatusBadGateway)
		return ctx.Err()
	}
	if res0.err != nil {
		cancel()
		cleanupPending(slots, pending)
		wg.Wait()
		cleanupPending(slots, pending)
		httpErrorFromS3(w, res0.err)
		return res0.err
	}

	contentRange := ""
	status := http.StatusOK
	if rangeRequested {
		contentRange = fmt.Sprintf("bytes %d-%d/%s", start, end, contentLenStr(totalSize))
		status = http.StatusPartialContent
	}
	writeRangeHeaders(w, contentRange, "", totalSize, start, end)
	w.WriteHeader(status)
	flusher, _ := w.(http.Flusher)

	if _, err := res0.writeSlice(w, start, end); err != nil {
		res0.close()
		cancel()
		cleanupPending(slots, pending)
		wg.Wait()
		cleanupPending(slots, pending)
		return err
	}
	res0.close()
	<-slots
	if flusher != nil {
		flusher.Flush()
	}

	for i := 1; i < nChunks; i++ {
		select {
		case res := <-pending[i]:
			if res.err != nil {
				log.WithFields(log.Fields{
					"key": key, "chunk": i,
				}).WithError(res.err).Warn("chunk failed mid-response")
				cancel()
				cleanupPending(slots, pending)
				wg.Wait()
				cleanupPending(slots, pending)
				return res.err
			}
			if _, err := res.writeSlice(w, start, end); err != nil {
				res.close()
				cancel()
				cleanupPending(slots, pending)
				wg.Wait()
				cleanupPending(slots, pending)
				return err
			}
			res.close()
			<-slots
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			cancel()
			cleanupPending(slots, pending)
			wg.Wait()
			cleanupPending(slots, pending)
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

// chunkResult is the worker→consumer payload. Exactly one of {data, file}
// is set on success: a cache hit returns the open *os.File so the
// consumer can stream slices via io.CopyN (no 4 MiB heap alloc per
// hit); a cache miss returns the freshly-fetched bytes (which already
// got cached to disk by fetchUncached).
//
// The consumer MUST call close() on every chunkResult it reads, even
// (especially) on error paths — otherwise the file handle leaks.
type chunkResult struct {
	data       []byte
	file       *os.File
	fileSize   int64
	err        error
	chunkStart int64
}

func (cr *chunkResult) size() int64 {
	if cr.file != nil {
		return cr.fileSize
	}
	return int64(len(cr.data))
}

func (cr *chunkResult) close() {
	if cr.file != nil {
		_ = cr.file.Close()
		cr.file = nil
	}
}

// writeSlice writes the portion of the chunk that falls inside
// [reqStart..reqEnd]. From the hit path it Seeks + io.CopyN'es out
// of the open file; from the miss path it w.Write's the in-memory
// slice.
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
	if cr.file != nil {
		if _, err := cr.file.Seek(sliceStart, io.SeekStart); err != nil {
			return 0, err
		}
		return io.CopyN(w, cr.file, sliceEnd-sliceStart)
	}
	n, err := w.Write(cr.data[sliceStart:sliceEnd])
	return int64(n), err
}

// cleanupPending drains the slot semaphore (so blocked workers can
// proceed and exit) and closes any chunkResult left in the per-chunk
// channels — necessary because hit results carry an open *os.File.
// Safe to call multiple times.
func cleanupPending(slots chan struct{}, pending []chan chunkResult) {
	for {
		select {
		case <-slots:
		default:
			goto pendingDrain
		}
	}
pendingDrain:
	for i := range pending {
		select {
		case cr := <-pending[i]:
			cr.close()
		default:
		}
	}
}

// fetchChunk pulls a single aligned chunk: cache lookup → singleflight →
// upstream + cache write. `source` labels metrics for foreground vs
// readahead traffic. On a hit, the returned chunkResult carries an open
// *os.File; on a miss, it carries the freshly-fetched bytes. Callers
// MUST chunkResult.close() in either case.
//
// start MUST be chunkSize-aligned; end is start+chunkSize-1 unless this
// is the last chunk in the object (EOF-clamped). The cache key is keyed
// on start only — the same aligned offset always means the same chunk.
func (f *Fetcher) fetchChunk(ctx context.Context, key string, start, end int64, source string) (chunkResult, error) {
	if file, size, err := f.cache.Get(key, start); err != nil {
		cacheLookups.WithLabelValues("error").Inc()
		log.WithError(err).WithFields(log.Fields{
			"key": key, "chunk_start": start,
		}).Warn("cache get failed")
	} else if file != nil {
		cacheLookups.WithLabelValues("hit").Inc()
		cacheBytesServed.Add(float64(size))
		return chunkResult{file: file, fileSize: size, chunkStart: start}, nil
	} else {
		cacheLookups.WithLabelValues("miss").Inc()
	}

	sfKey := fmt.Sprintf("%s/%d", key, start)
	data, err, shared := f.sf.Do(sfKey, func() ([]byte, error) {
		return f.fetchUncached(ctx, key, start, end, source)
	})
	if shared {
		singleflightShared.Inc()
	}
	if err != nil {
		return chunkResult{}, err
	}
	return chunkResult{data: data, chunkStart: start}, nil
}

// fetchUncached pulls bytes from S3 and writes them to cache. Cache
// failures are logged but don't fail the request — a degraded cache is
// still better than no response.
func (f *Fetcher) fetchUncached(ctx context.Context, key string, start, end int64, source string) ([]byte, error) {
	t0 := time.Now()
	body, _, _, err := f.openRange(ctx, key, start, end)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	buf := make([]byte, end-start+1)
	n, err := io.ReadFull(body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Wrap(err, "read chunk body")
	}
	data := buf[:n]
	upstreamChunkDuration.WithLabelValues(source).Observe(time.Since(t0).Seconds())
	upstreamBytesFetched.Add(float64(n))

	if err := f.cache.Put(key, start, data); err != nil {
		cacheWrites.WithLabelValues("error").Inc()
		log.WithError(err).WithFields(log.Fields{
			"key": key, "chunk_start": start,
		}).Warn("cache put failed")
	} else if f.cache != nil {
		cacheWrites.WithLabelValues("ok").Inc()
	}
	return data, nil
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

func contentLenStr(total int64) string {
	if total < 0 {
		return "*"
	}
	return strconv.FormatInt(total, 10)
}
