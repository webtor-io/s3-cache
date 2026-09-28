package services

import (
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
)

const testBucket = "bkt"

var testLastModified = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// objKey names a fake object; its size is encoded in the key so the fake
// upstream needs no state.
func objKey(size int64, name string) string { return fmt.Sprintf("o/%d/%s", size, name) }

func objSize(key string) (int64, bool) {
	parts := strings.SplitN(key, "/", 3)
	if len(parts) != 3 || parts[0] != "o" {
		return 0, false
	}
	n, err := strconv.ParseInt(parts[1], 10, 64)
	return n, err == nil
}

func keySeed(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}

// objBytes returns bytes [start..end] of key's content: a pattern that
// differs between neighbouring offsets and between keys, so a misplaced
// or cross-wired slice can't compare equal.
func objBytes(key string, start, end int64) []byte {
	seed := keySeed(key)
	out := make([]byte, end-start+1)
	for i := range out {
		x := uint32(start+int64(i))*2654435761 ^ seed
		x ^= x >> 13
		out[i] = byte(x ^ x>>8 ^ x>>16)
	}
	return out
}

// fakeS3 speaks the slice of the S3 path-style API the fetcher uses:
// HEAD and ranged GET on /<bucket>/<key>.
type fakeS3 struct {
	gets     atomic.Int64
	heads    atomic.Int64
	getDelay time.Duration
}

func (s *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/"+testBucket+"/")
	size, ok := objSize(key)
	if !ok {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`)
		return
	}
	w.Header().Set("ETag", fmt.Sprintf(`"%08x"`, keySeed(key)))
	w.Header().Set("Last-Modified", testLastModified.Format(http.TimeFormat))
	// S3's default for uploads without a type; the fetcher normalizes it.
	w.Header().Set("Content-Type", "binary/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	switch r.Method {
	case http.MethodHead:
		s.heads.Add(1)
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		s.gets.Add(1)
		if s.getDelay > 0 {
			select {
			case <-time.After(s.getDelay):
			case <-r.Context().Done():
				return
			}
		}
		start, end := int64(0), size-1
		status := http.StatusOK
		if rh := r.Header.Get("Range"); rh != "" {
			if _, err := fmt.Sscanf(strings.TrimPrefix(rh, "bytes="), "%d-%d", &start, &end); err != nil {
				http.Error(w, "bad range "+rh, http.StatusBadRequest)
				return
			}
			if end > size-1 {
				end = size - 1
			}
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, size))
			status = http.StatusPartialContent
		}
		w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
		w.WriteHeader(status)
		_, _ = w.Write(objBytes(key, start, end))
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

type cacheMode int

const (
	cacheOn cacheMode = iota
	cacheOff
	cachePutFails
)

func (m cacheMode) String() string {
	return [...]string{"cache-on", "cache-off", "put-fails"}[m]
}

type envOpts struct {
	chunkSize        int64
	workers          int
	fetchConcurrency int
	readahead        int
	cache            cacheMode
	fetchTimeout     time.Duration
	getDelay         time.Duration
	// smallBuffers shrinks the kernel socket buffers on both ends of the
	// client connections, so a client that stops reading backs up into
	// the handler after a few tens of KiB instead of the OS default.
	smallBuffers bool
}

type testEnv struct {
	t         *testing.T
	f         *Fetcher
	s3        *fakeS3
	url       string
	client    *http.Client
	cacheRoot string
}

const smallSockBuf = 32 << 10

type smallBufListener struct{ net.Listener }

func (l smallBufListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetWriteBuffer(smallSockBuf)
	}
	return c, err
}

func newTestEnv(t *testing.T, o envOpts) *testEnv {
	t.Helper()
	if o.chunkSize == 0 {
		o.chunkSize = 64 << 10
	}
	if o.workers == 0 {
		o.workers = 8
	}
	if o.fetchConcurrency == 0 {
		o.fetchConcurrency = 32
	}
	if o.fetchTimeout == 0 {
		o.fetchTimeout = 30 * time.Second
	}

	fs3 := &fakeS3{getDelay: o.getDelay}
	s3srv := httptest.NewServer(fs3)

	set := flag.NewFlagSet("test", flag.ContinueOnError)
	var flags []cli.Flag
	flags = cs.RegisterS3ClientFlags(flags)
	flags = RegisterFetcherFlags(flags)
	flags = RegisterCacheFlags(flags)
	flags = RegisterReadaheadFlags(flags)
	for _, fl := range flags {
		fl.Apply(set)
	}
	vals := map[string]string{
		"aws-access-key-id":     "test",
		"aws-secret-access-key": "test",
		"aws-endpoint":          s3srv.URL,
		"aws-region":            "us-east-1",
		"aws-no-ssl":            "true",
		BucketFlag:              testBucket,
		ChunkSizeFlag:           strconv.FormatInt(o.chunkSize, 10),
		WorkersFlag:             strconv.Itoa(o.workers),
		FetchConcurrencyFlag:    strconv.Itoa(o.fetchConcurrency),
		ChunkFetchTimeoutFlag:   o.fetchTimeout.String(),
		ReadaheadChunksFlag:     strconv.Itoa(o.readahead),
		CacheEnabledFlag:        "false",
	}
	cacheRoot := ""
	if o.cache == cacheOn || o.cache == cachePutFails {
		cacheRoot = filepath.Join(t.TempDir(), "data1")
		vals[CacheEnabledFlag] = "true"
		vals[CacheDirFlag] = cacheRoot
	}
	if o.cache == cachePutFails {
		// A read-only subdir: lookups are clean misses, every write fails
		// (as on a full or broken disk).
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory permissions; put-fails mode needs a non-root user")
		}
		sub := filepath.Join(cacheRoot, "s3-cache")
		if err := os.MkdirAll(sub, 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(sub, 0555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(sub, 0755) })
	}
	for k, v := range vals {
		if err := set.Set(k, v); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}
	c := cli.NewContext(nil, set, nil)
	cache := NewDiskCache(c)
	f := NewFetcher(c, cs.NewS3Client(c, &http.Client{}), cache, NewReadahead(c))

	web := httptest.NewUnstartedServer(http.HandlerFunc((&Web{fetcher: f}).handle))
	if o.smallBuffers {
		web.Listener = smallBufListener{web.Listener}
	}
	web.Start()

	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if o.smallBuffers {
		dialer.Control = func(network, address string, rc syscall.RawConn) error {
			var serr error
			if err := rc.Control(func(fd uintptr) {
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, smallSockBuf)
			}); err != nil {
				return err
			}
			return serr
		}
	}
	client := &http.Client{Transport: &http.Transport{
		DialContext:         dialer.DialContext,
		MaxIdleConnsPerHost: 256,
		DisableCompression:  true,
	}}

	env := &testEnv{t: t, f: f, s3: fs3, url: web.URL, client: client, cacheRoot: cacheRoot}
	t.Cleanup(func() {
		web.CloseClientConnections()
		web.Close()
		env.assertNoLeaks()
		s3srv.Close()
	})
	return env
}

// assertNoLeaks waits for every detached fetch and every claim to settle
// and fails if a flight, a budget unit or budgeted bytes are still held.
func (e *testEnv) assertNoLeaks() {
	e.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		flights := e.f.flights.Load()
		inflight := e.f.sf.inflight()
		bytes := e.f.budget.inUse.Load()
		units := len(e.f.budget.units)
		if flights == 0 && inflight == 0 && bytes == 0 && units == 0 {
			return
		}
		if time.Now().After(deadline) {
			e.t.Errorf("leak: flights=%d inflight=%d budget bytes=%d units=%d", flights, inflight, bytes, units)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type response struct {
	status int
	header http.Header
	body   []byte
}

// canonical renders everything a client can observe except Date.
func (r response) canonical() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%d\n", r.status)
	keys := make([]string, 0, len(r.header))
	for k := range r.header {
		if k != "Date" {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "%s: %s\n", k, strings.Join(r.header[k], ", "))
	}
	fmt.Fprintf(&b, "body %d bytes fnv %08x\n", len(r.body), keySeed(string(r.body)))
	return b.String()
}

func (e *testEnv) do(method, key, rng string) response {
	e.t.Helper()
	req, err := http.NewRequest(method, e.url+"/"+key, nil)
	if err != nil {
		e.t.Fatal(err)
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s %q: %v", method, key, rng, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatalf("%s %s %q: read body: %v", method, key, rng, err)
	}
	return response{status: resp.StatusCode, header: resp.Header, body: body}
}

// metricValue reads a counter or gauge from the default registry.
func metricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := 0
			for _, lp := range m.GetLabel() {
				if want, ok := labels[lp.GetName()]; ok && want == lp.GetValue() {
					match++
				}
			}
			if match != len(labels) {
				continue
			}
			if c := m.GetCounter(); c != nil {
				return c.GetValue()
			}
			if g := m.GetGauge(); g != nil {
				return g.GetValue()
			}
		}
	}
	return 0
}

func serves(t *testing.T, via string) float64 {
	return metricValue(t, "s3cache_chunk_serves_total", map[string]string{"via": via})
}

func waitFor(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}
