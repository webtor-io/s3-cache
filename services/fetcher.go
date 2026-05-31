package services

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"

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
	)
}

type Fetcher struct {
	s3cl                *cs.S3Client
	chunkSize           int64
	workers             int
	smallRangeThreshold int64
}

func NewFetcher(c *cli.Context, s3cl *cs.S3Client) *Fetcher {
	return &Fetcher{
		s3cl:                s3cl,
		chunkSize:           c.Int64(ChunkSizeFlag),
		workers:             c.Int(WorkersFlag),
		smallRangeThreshold: c.Int64(SmallRangeThresholdFlag),
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
	// Open-ended range needs a HEAD to know total size before splitting.
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

	if wantLen < f.smallRangeThreshold {
		return f.singleStream(ctx, w, bucket, key, start, end, totalSize)
	}
	return f.multiRange(ctx, w, bucket, key, start, end, totalSize)
}

// singleStream proxies one S3 GET with Range straight to the client.
// Used for HLS segments and any range below the threshold — small enough
// that parallelisation overhead doesn't pay back.
func (f *Fetcher) singleStream(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end, totalSize int64) error {
	body, contentRange, contentType, err := f.openRange(ctx, bucket, key, start, end)
	if err != nil {
		httpErrorFromS3(w, err)
		return err
	}
	defer body.Close()

	writeRangeHeaders(w, contentRange, contentType, totalSize, start, end)
	w.WriteHeader(http.StatusPartialContent)
	_, err = io.Copy(w, body)
	return err
}

// multiRange splits [start..end] into chunks, dispatches them to N workers
// fetching from S3 in parallel, and writes them to the client in order.
//
// We wait for chunk 0 to resolve before committing to a 206 status — that
// way any upstream failure on the first chunk yields a clean 5xx instead
// of a mid-response abort. Failures on later chunks log + drop the
// connection (we've already committed to a Content-Length).
func (f *Fetcher) multiRange(ctx context.Context, w http.ResponseWriter, bucket, key string, start, end, totalSize int64) error {
	wantLen := end - start + 1
	chunkSize := f.chunkSize
	nChunks := int((wantLen + chunkSize - 1) / chunkSize)
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
				data, err := f.fetchChunk(ctx, bucket, key, cStart, cEnd)
				pending[idx] <- chunkResult{data: data, err: err}
			}
		}()
	}

	// Block until first chunk resolves before declaring 206; any earlier
	// upstream error needs to surface as a real HTTP error, not a torn-off
	// half-response.
	var res0 chunkResult
	select {
	case res0 = <-pending[0]:
	case <-ctx.Done():
		cancel()
		drainPending(pending)
		wg.Wait()
		http.Error(w, "request cancelled", http.StatusBadGateway)
		return ctx.Err()
	}
	if res0.err != nil {
		cancel()
		drainPending(pending)
		wg.Wait()
		httpErrorFromS3(w, res0.err)
		return res0.err
	}

	contentRange := fmt.Sprintf("bytes %d-%d/%s", start, end, contentLenStr(totalSize))
	writeRangeHeaders(w, contentRange, "", totalSize, start, end)
	w.WriteHeader(http.StatusPartialContent)

	flusher, _ := w.(http.Flusher)
	if _, err := w.Write(res0.data); err != nil {
		cancel()
		drainPending(pending)
		wg.Wait()
		return err
	}
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
				drainPending(pending)
				wg.Wait()
				return res.err
			}
			if _, err := w.Write(res.data); err != nil {
				cancel()
				drainPending(pending)
				wg.Wait()
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		case <-ctx.Done():
			cancel()
			drainPending(pending)
			wg.Wait()
			return ctx.Err()
		}
	}
	wg.Wait()
	return nil
}

// drainPending non-blockingly consumes any already-produced chunk results
// so workers can send + exit. Safe to call repeatedly.
func drainPending(pending []chan chunkResult) {
	for i := range pending {
		select {
		case <-pending[i]:
		default:
		}
	}
}

// chunkResult mirrors the type from multiRange; named at package level so
// drainPending can take it as a parameter.
type chunkResult struct {
	data []byte
	err  error
}

// fetchChunk pulls a single [start..end] range from S3. No retry — when
// the upstream is throttling per-source-IP, retrying from the same node
// hits the same shaper. Callers that care about throughput should rely
// on parallelism + (later) a local cache, not on per-request retries.
func (f *Fetcher) fetchChunk(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	chunkLen := end - start + 1
	body, _, _, err := f.openRange(ctx, bucket, key, start, end)
	if err != nil {
		return nil, err
	}
	defer body.Close()

	buf := make([]byte, chunkLen)
	n, err := io.ReadFull(body, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.Wrap(err, "read chunk body")
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
