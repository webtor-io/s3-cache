package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
	)
}

type Fetcher struct {
	s3cl      *cs.S3Client
	chunkSize int64
	workers   int
	cache     *DiskCache
	sf        *singleflight
	readahead *Readahead
}

func NewFetcher(c *cli.Context, s3cl *cs.S3Client, cache *DiskCache, readahead *Readahead) *Fetcher {
	return &Fetcher{
		s3cl:      s3cl,
		chunkSize: c.Int64(ChunkSizeFlag),
		workers:   c.Int(WorkersFlag),
		cache:     cache,
		sf:        newSingleflight(),
		readahead: readahead,
	}
}

// Head issues HeadObject and copies Content-Length / Content-Type / ETag to w.
func (f *Fetcher) Head(ctx context.Context, w http.ResponseWriter, bucket, key string) error {
	out, err := f.s3cl.Get().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
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

// Get serves a GET range request through the aligned-chunk path.
// end == -1 means "to the end" and forces a HEAD to discover totalSize.
//
// Unlike MVP-1's split between singleStream and multiRange, every request
// now goes through aligned chunks — the cache keys on aligned offsets,
// so request-relative offsets would defeat reuse across requests. For
// sub-chunk-sized requests this still amounts to one upstream GET (one
// chunk fetched, sliced to the requested span).
func (f *Fetcher) Get(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end int64) error {
	totalSize := int64(-1)
	if end < 0 {
		hd, err := f.s3cl.Get().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
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

	return f.serveAligned(ctx, w, bucket, key, start, end, totalSize)
}

// serveAligned splits [start..end] into chunkSize-aligned chunks
// (absolute offsets, not request-relative), fetches them in parallel,
// and writes the requested span to the client in order. Each chunk goes
// through cache → singleflight → upstream.
//
// Status commit is deferred until chunk 0 resolves: a pre-header
// upstream failure yields a clean 502; failures on later chunks can
// only abort an already-streaming response.
func (f *Fetcher) serveAligned(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end, totalSize int64) error {
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
	// materialize in RAM before the client drains. workers*2 leaves
	// headroom for workers to always have somewhere to write into.
	slotCap := workers * 2
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
				data, err := f.fetchChunk(ctx, bucket, key, cStart, cEnd, sourceForeground)
				pending[idx] <- chunkResult{data: data, err: err, chunkStart: cStart}
			}
		}()
	}

	// Wait for chunk 0 before committing 206 — earlier failure surfaces
	// as a real HTTP error, not a half-written response.
	var res0 chunkResult
	select {
	case res0 = <-pending[0]:
	case <-ctx.Done():
		cancel()
		drainSlots(slots, pending)
		wg.Wait()
		http.Error(w, "request cancelled", http.StatusBadGateway)
		return ctx.Err()
	}
	if res0.err != nil {
		cancel()
		drainSlots(slots, pending)
		wg.Wait()
		httpErrorFromS3(w, res0.err)
		return res0.err
	}

	contentRange := fmt.Sprintf("bytes %d-%d/%s", start, end, contentLenStr(totalSize))
	writeRangeHeaders(w, contentRange, "", totalSize, start, end)
	w.WriteHeader(http.StatusPartialContent)
	flusher, _ := w.(http.Flusher)

	if _, err := writeSliced(w, res0.data, res0.chunkStart, start, end); err != nil {
		cancel()
		drainSlots(slots, pending)
		wg.Wait()
		return err
	}
	<-slots
	if flusher != nil {
		flusher.Flush()
	}

	for i := 1; i < nChunks; i++ {
		select {
		case res := <-pending[i]:
			if res.err != nil {
				log.WithFields(log.Fields{
					"bucket": bucket, "key": key, "chunk": i,
				}).WithError(res.err).Warn("chunk failed mid-response")
				cancel()
				drainSlots(slots, pending)
				wg.Wait()
				return res.err
			}
			if _, err := writeSliced(w, res.data, res.chunkStart, start, end); err != nil {
				cancel()
				drainSlots(slots, pending)
				wg.Wait()
				return err
			}
			<-slots
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			cancel()
			drainSlots(slots, pending)
			wg.Wait()
			return ctx.Err()
		}
	}
	wg.Wait()

	// Sequential readahead kicks past the served tail. Fire-and-forget;
	// schedule() handles dedup / saturation / cache-already-hit.
	if f.readahead != nil {
		f.readahead.Kick(f, bucket, key, lastChunkIdx+1, totalSize)
	}
	return nil
}

// writeSliced writes the portion of `chunk` (whose bytes start at
// chunkStart) that falls inside [reqStart..reqEnd]. Used to trim the
// first/last chunks down to the requested span.
func writeSliced(w io.Writer, chunk []byte, chunkStart, reqStart, reqEnd int64) (int, error) {
	chunkEnd := chunkStart + int64(len(chunk)) - 1
	sliceStart := int64(0)
	if reqStart > chunkStart {
		sliceStart = reqStart - chunkStart
	}
	sliceEnd := int64(len(chunk))
	if reqEnd < chunkEnd {
		sliceEnd = reqEnd - chunkStart + 1
	}
	if sliceStart >= sliceEnd {
		return 0, nil
	}
	return w.Write(chunk[sliceStart:sliceEnd])
}

// drainSlots non-blockingly empties both the slot semaphore and any
// already-produced chunk results so workers can finish their send + exit
// without leaking.
func drainSlots(slots chan struct{}, pending []chan chunkResult) {
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
		case <-pending[i]:
		default:
		}
	}
}

type chunkResult struct {
	data       []byte
	err        error
	chunkStart int64
}

// fetchChunk pulls a single aligned chunk: cache lookup → singleflight →
// upstream + cache write. `source` labels metrics for foreground vs
// readahead traffic.
//
// start MUST be chunkSize-aligned; end is start+chunkSize-1 unless this
// is the last chunk in the object (EOF-clamped). The cache key is keyed
// on start only — the same aligned offset always means the same chunk.
func (f *Fetcher) fetchChunk(ctx context.Context, bucket, key string, start, end int64, source string) ([]byte, error) {
	if data, err := f.cache.Get(bucket, key, start); err != nil {
		cacheLookups.WithLabelValues("error").Inc()
		log.WithError(err).WithFields(log.Fields{
			"bucket": bucket, "key": key, "chunk_start": start,
		}).Warn("cache get failed")
	} else if data != nil {
		cacheLookups.WithLabelValues("hit").Inc()
		cacheBytesServed.Add(float64(len(data)))
		return data, nil
	} else {
		cacheLookups.WithLabelValues("miss").Inc()
	}

	sfKey := fmt.Sprintf("%s/%s/%d", bucket, key, start)
	data, err, shared := f.sf.Do(sfKey, func() ([]byte, error) {
		return f.fetchUncached(ctx, bucket, key, start, end, source)
	})
	if shared {
		singleflightShared.Inc()
	}
	return data, err
}

// fetchUncached pulls bytes from S3 and writes them to cache. Cache
// failures are logged but don't fail the request — a degraded cache is
// still better than no response.
func (f *Fetcher) fetchUncached(ctx context.Context, bucket, key string, start, end int64, source string) ([]byte, error) {
	t0 := time.Now()
	body, _, _, err := f.openRange(ctx, bucket, key, start, end)
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

	if err := f.cache.Put(bucket, key, start, data); err != nil {
		cacheWrites.WithLabelValues("error").Inc()
		log.WithError(err).WithFields(log.Fields{
			"bucket": bucket, "key": key, "chunk_start": start,
		}).Warn("cache put failed")
	} else if f.cache != nil {
		cacheWrites.WithLabelValues("ok").Inc()
	}
	return data, nil
}

// openRange opens a single Range GET against S3. Returns body +
// Content-Range + Content-Type.
func (f *Fetcher) openRange(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, string, string, error) {
	in := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
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

func writeRangeHeaders(w http.ResponseWriter, contentRange, contentType string, totalSize, start, end int64) {
	if contentRange != "" {
		w.Header().Set("Content-Range", contentRange)
	} else if totalSize >= 0 {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
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
