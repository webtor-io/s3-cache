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
	ChunkSizeFlag           = "chunk-size"
	WorkersFlag             = "workers"
	SmallRangeThresholdFlag = "small-range-threshold"
	SlowBpsFlag             = "slow-bps"
	SlowWindowMsFlag        = "slow-window-ms"
	MaxRetriesFlag          = "max-retries"
)

func RegisterFetcherFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.Int64Flag{
			Name:   ChunkSizeFlag,
			Usage:  "chunk size in bytes for multi-range split",
			Value:  4 << 20,
			EnvVar: "CHUNK_SIZE",
		},
		cli.IntFlag{
			Name:   WorkersFlag,
			Usage:  "concurrent S3 fetch workers per request",
			Value:  8,
			EnvVar: "WORKERS",
		},
		cli.Int64Flag{
			Name:   SmallRangeThresholdFlag,
			Usage:  "ranges smaller than this go single-stream pass-through",
			Value:  8 << 20,
			EnvVar: "SMALL_RANGE_THRESHOLD",
		},
		cli.Int64Flag{
			Name:   SlowBpsFlag,
			Usage:  "per-connection bytes/sec floor; below this for slow-window triggers abort",
			Value:  5 << 20,
			EnvVar: "SLOW_BPS",
		},
		cli.IntFlag{
			Name:   SlowWindowMsFlag,
			Usage:  "sustained-below-floor window in ms before aborting a slow conn",
			Value:  2000,
			EnvVar: "SLOW_WINDOW_MS",
		},
		cli.IntFlag{
			Name:   MaxRetriesFlag,
			Usage:  "max retries on slow/failed chunk",
			Value:  3,
			EnvVar: "MAX_RETRIES",
		},
	)
}

type Fetcher struct {
	s3cl                *cs.S3Client
	chunkSize           int64
	workers             int
	smallRangeThreshold int64
	slowBps             int64
	slowWindow          time.Duration
	maxRetries          int
}

func NewFetcher(c *cli.Context, s3cl *cs.S3Client) *Fetcher {
	return &Fetcher{
		s3cl:                s3cl,
		chunkSize:           c.Int64(ChunkSizeFlag),
		workers:             c.Int(WorkersFlag),
		smallRangeThreshold: c.Int64(SmallRangeThresholdFlag),
		slowBps:             c.Int64(SlowBpsFlag),
		slowWindow:          time.Duration(c.Int(SlowWindowMsFlag)) * time.Millisecond,
		maxRetries:          c.Int(MaxRetriesFlag),
	}
}

// Head issues HeadObject and copies Content-Length / Content-Type / Accept-Ranges to w.
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

// Get serves a GET, routing between single-stream pass-through and
// chunked multi-range parallel fetch depending on requested size.
// end == -1 means "to the end".
func (f *Fetcher) Get(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end int64) error {
	// If we don't know end (open-ended range), we need a HEAD to know total size before splitting.
	// For full-file GET we'd HEAD too. Small overhead vs. the win from parallelism.
	totalSize := int64(-1)
	if end < 0 {
		hd, err := f.s3cl.Get().HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return errors.Wrap(err, "HEAD before GET failed")
		}
		if hd.ContentLength == nil {
			return errors.New("upstream missing Content-Length")
		}
		totalSize = *hd.ContentLength
		end = totalSize - 1
		// Carry through Content-Type if S3 returned one
		if hd.ContentType != nil {
			w.Header().Set("Content-Type", *hd.ContentType)
		}
	}

	wantLen := end - start + 1
	if wantLen <= 0 {
		return errors.New("empty range")
	}

	// Decide path
	if wantLen < f.smallRangeThreshold {
		return f.singleStream(ctx, w, bucket, key, start, end, totalSize)
	}
	return f.multiRange(ctx, w, bucket, key, start, end, totalSize)
}

// singleStream proxies one S3 GET with a Range header straight to the client.
// Used for HLS segments, small requests, and any range below the threshold.
func (f *Fetcher) singleStream(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end, totalSize int64) error {
	body, contentRange, contentType, err := f.openRange(ctx, bucket, key, start, end)
	if err != nil {
		httpErrorFromS3(w, err)
		return err
	}
	defer body.Close()

	writeRangeHeaders(w, contentRange, contentType, totalSize, start, end)
	w.WriteHeader(http.StatusPartialContent)

	// Stream with slow-detector wrapped; on slow abort we just bail (single stream cannot retry mid-response).
	sd := newSlowDetector(f.slowBps, f.slowWindow)
	defer sd.stop()
	rdr := sd.wrap(ctx, body)
	_, err = io.Copy(w, rdr)
	return err
}

// multiRange splits [start..end] into chunks, dispatches to N workers
// fetching them in parallel from S3, and writes them to the client in order.
// Each worker can hit slow-detector and retry on a fresh connection.
func (f *Fetcher) multiRange(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end, totalSize int64) error {
	wantLen := end - start + 1
	chunkSize := f.chunkSize
	nChunks := int((wantLen + chunkSize - 1) / chunkSize)
	workers := f.workers
	if workers > nChunks {
		workers = nChunks
	}

	// Write headers up front; we know exactly what we'll serve.
	contentRange := fmt.Sprintf("bytes %d-%d/%s", start, end, contentLenStr(totalSize))
	writeRangeHeaders(w, contentRange, "", totalSize, start, end)
	w.WriteHeader(http.StatusPartialContent)

	// chunkResult holds bytes for one chunk; chunks are produced out of order by workers,
	// consumed in order by the writer goroutine.
	type chunkResult struct {
		idx  int
		data []byte
		err  error
	}

	// pending[i] holds chunk i's result once ready; writer goroutine reads sequentially.
	pending := make([]chan chunkResult, nChunks)
	for i := range pending {
		pending[i] = make(chan chunkResult, 1)
	}

	// jobs queue: worker pulls next chunk index to fetch.
	jobs := make(chan int, nChunks)
	for i := 0; i < nChunks; i++ {
		jobs <- i
	}
	close(jobs)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				cStart := start + int64(idx)*chunkSize
				cEnd := cStart + chunkSize - 1
				if cEnd > end {
					cEnd = end
				}
				data, err := f.fetchChunkWithRetry(ctx, bucket, key, cStart, cEnd)
				pending[idx] <- chunkResult{idx: idx, data: data, err: err}
			}
		}()
	}

	// Writer loop: drains pending[i] in order, flushes each to the client.
	flusher, _ := w.(http.Flusher)
	var writeErr error
	for i := 0; i < nChunks; i++ {
		select {
		case res := <-pending[i]:
			if res.err != nil {
				log.WithFields(log.Fields{
					"bucket": bucket, "key": key, "chunk": i,
				}).WithError(res.err).Error("chunk failed; aborting")
				writeErr = res.err
				cancel()
				goto drain
			}
			if _, err := w.Write(res.data); err != nil {
				writeErr = err
				cancel()
				goto drain
			}
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			writeErr = ctx.Err()
			goto drain
		}
	}

drain:
	cancel()
	// Drain remaining workers so they exit (results dropped).
	go func() {
		for range jobs {
		}
	}()
	// Drain pending channels so workers can send and exit.
	for i := 0; i < nChunks; i++ {
		select {
		case <-pending[i]:
		default:
		}
	}
	wg.Wait()
	return writeErr
}

// fetchChunkWithRetry pulls a single [start..end] range from S3, with
// slow-detector retry-on-fresh-connection. Returns bytes for that chunk.
func (f *Fetcher) fetchChunkWithRetry(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	chunkLen := end - start + 1
	var lastErr error
	for attempt := 0; attempt <= f.maxRetries; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		data, err := f.fetchChunkOnce(ctx, bucket, key, start, end, chunkLen)
		if err == nil {
			return data, nil
		}
		lastErr = err
		kind := "error"
		if errors.Is(err, errSlowAbort) {
			kind = "slow-abort"
		}
		log.WithError(err).WithFields(log.Fields{
			"bucket": bucket, "key": key, "attempt": attempt + 1,
			"chunk_start": start, "chunk_end": end, "kind": kind,
		}).Warn("chunk fetch failed, retrying")
	}
	return nil, errors.Wrapf(lastErr, "chunk %d-%d exhausted retries", start, end)
}

// fetchChunkOnce does one S3 GetObject with Range, reads body with slow-detector.
// Returns errSlowAbort if the slow-detector aborted before completion.
func (f *Fetcher) fetchChunkOnce(ctx context.Context, bucket, key string, start, end, chunkLen int64) ([]byte, error) {
	// Local context so slow-detector can cancel just this attempt's S3 fetch
	// (request + body read) without killing the surrounding multi-range request.
	attemptCtx, attemptCancel := context.WithCancel(ctx)
	defer attemptCancel()

	body, _, _, err := f.openRange(attemptCtx, bucket, key, start, end)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	sd := newSlowDetector(f.slowBps, f.slowWindow)
	defer sd.stop()
	// onSlow cancels the attempt context (causes pending Read to fail) AND
	// closes the body explicitly to break out of any in-flight read that
	// the SDK isn't tying to ctx for whatever reason.
	sd.onSlow = func() {
		attemptCancel()
		_ = body.Close()
	}

	rdr := sd.wrap(attemptCtx, body)
	buf := make([]byte, chunkLen)
	n, readErr := io.ReadFull(rdr, buf)
	if readErr != nil && readErr != io.ErrUnexpectedEOF {
		if sd.aborted() {
			return nil, errSlowAbort
		}
		// If parent ctx is done, propagate cleanly.
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Wrap(readErr, "read chunk body")
	}
	return buf[:n], nil
}

// openRange opens a single Range GET against S3. Returns body + Content-Range + Content-Type.
func (f *Fetcher) openRange(ctx context.Context, bucket, key string, start, end int64) (io.ReadCloser, string, string, error) {
	in := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
	}
	out, err := f.s3cl.Get().GetObjectWithContext(ctx, in, func(r *request.Request) {
		r.HTTPRequest.Header.Set("User-Agent", "webtor-s3-cache/0.1")
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
