package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type respCase struct {
	name   string
	method string
	rng    string
	status int
	start  int64 // served span, for 200/206
	end    int64
}

// TestResponsesIdenticalAcrossCacheModes pins down that where the bytes
// come from — a cache hit, a fresh cache file, a shared buffer when the
// cache is off or failing — never shows on the wire: status, every header
// but Date, and the body are identical, and match the upstream object.
func TestResponsesIdenticalAcrossCacheModes(t *testing.T) {
	const chunk = 64 << 10
	const size = 5*chunk + 12345 // last chunk partial
	key := objKey(size, "movie.mkv")

	cases := []respCase{
		{"head", http.MethodHead, "", 200, 0, 0},
		{"full no range", http.MethodGet, "", 200, 0, size - 1},
		{"full open range", http.MethodGet, "bytes=0-", 206, 0, size - 1},
		{"head of chunk 0", http.MethodGet, "bytes=0-99", 206, 0, 99},
		{"unaligned inside chunk 0", http.MethodGet, "bytes=1000-2000", 206, 1000, 2000},
		{"across boundary 0/1", http.MethodGet, "bytes=65000-70000", 206, 65000, 70000},
		{"exactly chunk 1", http.MethodGet, "bytes=65536-131071", 206, 65536, 131071},
		{"unaligned multi-chunk", http.MethodGet, "bytes=100000-300000", 206, 100000, 300000},
		{"last chunk open", http.MethodGet, "bytes=327680-", 206, 327680, size - 1},
		{"last bytes", http.MethodGet, fmt.Sprintf("bytes=%d-%d", size-25, size-1), 206, size - 25, size - 1},
		{"suffix inside last chunk", http.MethodGet, "bytes=-5000", 206, size - 5000, size - 1},
		{"suffix across boundary", http.MethodGet, "bytes=-20000", 206, size - 20000, size - 1},
		{"end clamped", http.MethodGet, "bytes=300000-999999999", 206, 300000, size - 1},
		{"start at size", http.MethodGet, fmt.Sprintf("bytes=%d-", size), 416, 0, 0},
		{"start past size", http.MethodGet, "bytes=999999-", 416, 0, 0},
	}
	modes := []struct {
		name  string
		cache cacheMode
		warm  bool
		via   string
	}{
		{"miss", cacheOn, false, viaFile},
		{"hit", cacheOn, true, viaHit},
		{"cache-off", cacheOff, false, viaBuffer},
		{"put-fails", cachePutFails, false, viaBuffer},
	}

	reference := map[string]string{}
	for _, m := range modes {
		for _, c := range cases {
			t.Run(m.name+"/"+c.name, func(t *testing.T) {
				env := newTestEnv(t, envOpts{chunkSize: chunk, cache: m.cache})
				if m.warm {
					env.do(http.MethodGet, key, "")
					env.s3.gets.Store(0)
				}
				viaBefore := serves(t, m.via)
				got := env.do(c.method, key, c.rng)

				if got.status != c.status {
					t.Fatalf("status %d, want %d", got.status, c.status)
				}
				checkHeaders(t, got, c, size)
				if c.status == 206 || (c.status == 200 && c.method == http.MethodGet) {
					want := objBytes(key, c.start, c.end)
					if !bytes.Equal(got.body, want) {
						t.Fatalf("body mismatch: got %d bytes, want %d", len(got.body), len(want))
					}
					if served := serves(t, m.via) - viaBefore; served < 1 {
						t.Fatalf("no chunk served via %q", m.via)
					}
				}
				if m.warm && env.s3.gets.Load() != 0 {
					t.Fatalf("warm cache still went upstream %d times", env.s3.gets.Load())
				}

				canon := got.canonical()
				if ref, ok := reference[c.name]; !ok {
					reference[c.name] = canon
				} else if ref != canon {
					t.Fatalf("response differs from mode %q:\n--- %s\n%s--- %s\n%s", modes[0].name, modes[0].name, ref, m.name, canon)
				}
			})
		}
	}
}

func checkHeaders(t *testing.T, r response, c respCase, size int64) {
	t.Helper()
	h := r.header
	switch c.status {
	case 416:
		if got, want := h.Get("Content-Range"), fmt.Sprintf("bytes */%d", size); got != want {
			t.Fatalf("Content-Range %q, want %q", got, want)
		}
		if h.Get("ETag") != "" || h.Get("Last-Modified") != "" {
			t.Fatalf("validators on an error response: %v", h)
		}
		return
	case 200, 206:
	default:
		t.Fatalf("unexpected status %d", c.status)
	}
	wantLen := c.end - c.start + 1
	if c.method == http.MethodHead {
		wantLen = size
	}
	if got := h.Get("Content-Length"); got != strconv.FormatInt(wantLen, 10) {
		t.Fatalf("Content-Length %q, want %d", got, wantLen)
	}
	wantCR := ""
	if c.status == 206 {
		wantCR = fmt.Sprintf("bytes %d-%d/%d", c.start, c.end, size)
	}
	if got := h.Get("Content-Range"); got != wantCR {
		t.Fatalf("Content-Range %q, want %q", got, wantCR)
	}
	for k, want := range map[string]string{
		"Content-Type":  "application/octet-stream",
		"Accept-Ranges": "bytes",
		"ETag":          fmt.Sprintf(`"%08x"`, keySeed(objKey(size, "movie.mkv"))),
		"Last-Modified": testLastModified.Format(http.TimeFormat),
	} {
		if got := h.Get(k); got != want {
			t.Fatalf("%s %q, want %q", k, got, want)
		}
	}
}

// stalledClients starts n GETs (one object each) that read nothing past
// the response headers until release is closed, then read everything and
// check it. It is the 2026-09-28 shape: many downloads whose clients read
// far slower than the upstream delivers.
type stalledClients struct {
	headers atomic.Int64
	release chan struct{}
	errs    chan error
}

func startStalledClients(env *testEnv, keys []string, size int64) *stalledClients {
	sc := &stalledClients{release: make(chan struct{}), errs: make(chan error, len(keys))}
	for _, key := range keys {
		go func(key string) {
			resp, err := env.client.Get(env.url + "/" + key)
			if err != nil {
				sc.errs <- err
				return
			}
			defer resp.Body.Close()
			sc.headers.Add(1)
			<-sc.release
			body, err := io.ReadAll(resp.Body)
			switch {
			case err != nil:
				sc.errs <- fmt.Errorf("%s: %v", key, err)
			case resp.StatusCode != http.StatusOK:
				sc.errs <- fmt.Errorf("%s: status %d", key, resp.StatusCode)
			case !bytes.Equal(body, objBytes(key, 0, size-1)):
				sc.errs <- fmt.Errorf("%s: body mismatch (%d bytes)", key, len(body))
			default:
				sc.errs <- nil
			}
		}(key)
	}
	return sc
}

func (sc *stalledClients) finish(t *testing.T, n int, timeout time.Duration) {
	t.Helper()
	close(sc.release)
	deadline := time.After(timeout)
	for i := 0; i < n; i++ {
		select {
		case err := <-sc.errs:
			if err != nil {
				t.Error(err)
			}
		case <-deadline:
			t.Fatalf("%d of %d clients still unfinished after %v", n-i, n, timeout)
		}
	}
}

func liveHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

const (
	slowChunk   = 256 << 10
	slowChunks  = 8 // = the per-request window, so each request fetches its whole object
	slowClients = 64
	slowBudget  = 4 // FETCH_CONCURRENCY → budget of 4 chunks = 1 MiB
	// Live heap the stalled clients may add. Holding the window in memory
	// (the pre-fix behaviour) costs slowClients × slowChunks × slowChunk =
	// 128 MiB here.
	slowHeapLimit = 24 << 20
)

func slowKeys(prefix string) []string {
	keys := make([]string, slowClients)
	for i := range keys {
		keys[i] = objKey(slowChunks*slowChunk, fmt.Sprintf("%s-%d", prefix, i))
	}
	return keys
}

// TestSlowClientsServedFromCacheFiles: a cached miss is served from the
// cache file, so stalled clients hold file handles, not heap — every
// request gets its whole window fetched and its headers, while the
// buffer budget (4 chunks) is never exceeded and ends up empty.
func TestSlowClientsServedFromCacheFiles(t *testing.T) {
	env := newTestEnv(t, envOpts{
		chunkSize: slowChunk, workers: 8, fetchConcurrency: slowBudget,
		cache: cacheOn, smallBuffers: true,
	})
	keys := slowKeys("cached")
	base := liveHeap()
	fileBefore := serves(t, viaFile)

	sc := startStalledClients(env, keys, slowChunks*slowChunk)
	wantGets := int64(slowClients * slowChunks)
	if !waitFor(20*time.Second, func() bool {
		return sc.headers.Load() == slowClients && env.s3.gets.Load() == wantGets && env.f.flights.Load() == 0
	}) {
		t.Fatalf("stalled clients: headers %d/%d, upstream GETs %d/%d, open flights %d, budget bytes %d",
			sc.headers.Load(), slowClients, env.s3.gets.Load(), wantGets, env.f.flights.Load(), env.f.budget.inUse.Load())
	}
	heap := int64(liveHeap()) - int64(base)
	t.Logf("live heap +%.1f MiB with %d stalled clients, budget peak %d KiB",
		float64(heap)/(1<<20), slowClients, env.f.budget.peak.Load()>>10)
	if heap > slowHeapLimit {
		t.Errorf("live heap grew by %d MiB, limit %d MiB", heap>>20, slowHeapLimit>>20)
	}
	if peak := env.f.budget.peak.Load(); peak > slowBudget*slowChunk {
		t.Errorf("buffer budget peak %d bytes, cap %d", peak, slowBudget*slowChunk)
	}
	if b := env.f.budget.inUse.Load(); b != 0 {
		t.Errorf("%d buffer bytes held while every chunk is cached", b)
	}
	if g := metricValue(t, "s3cache_chunk_buffer_bytes", nil); g != 0 {
		t.Errorf("s3cache_chunk_buffer_bytes = %v while every chunk is cached", g)
	}

	sc.finish(t, slowClients, 60*time.Second)
	if got := serves(t, viaFile) - fileBefore; got != float64(wantGets) {
		t.Errorf("served via file: %v chunks, want %d", got, wantGets)
	}
}

// TestSlowClientsBufferBudget: when chunks can't be cached they are held
// in memory for the consumer — under the global budget, so stalled
// clients can't grow the heap past it; they queue instead, and all finish
// once they read.
func TestSlowClientsBufferBudget(t *testing.T) {
	env := newTestEnv(t, envOpts{
		chunkSize: slowChunk, workers: 8, fetchConcurrency: slowBudget,
		cache: cachePutFails, smallBuffers: true,
	})
	keys := slowKeys("uncached")
	base := liveHeap()
	waitsBefore := metricValue(t, "s3cache_chunk_budget_waits_total", nil)

	sc := startStalledClients(env, keys, slowChunks*slowChunk)
	// Let the handlers take whatever they can: wait until upstream GETs
	// stop growing.
	last, stableSince := int64(-1), time.Now()
	waitFor(20*time.Second, func() bool {
		if g := env.s3.gets.Load(); g != last {
			last, stableSince = g, time.Now()
		}
		return time.Since(stableSince) > 500*time.Millisecond
	})
	heap := int64(liveHeap()) - int64(base)
	t.Logf("live heap +%.1f MiB, %d clients got headers, %d upstream GETs, budget peak %d KiB",
		float64(heap)/(1<<20), sc.headers.Load(), env.s3.gets.Load(), env.f.budget.peak.Load()>>10)
	if peak := env.f.budget.peak.Load(); peak > slowBudget*slowChunk {
		t.Errorf("buffer budget peak %d bytes, cap %d", peak, slowBudget*slowChunk)
	}
	if heap > slowHeapLimit {
		t.Errorf("live heap grew by %d MiB, limit %d MiB", heap>>20, slowHeapLimit>>20)
	}
	if waits := metricValue(t, "s3cache_chunk_budget_waits_total", nil) - waitsBefore; waits == 0 {
		t.Errorf("no budget waits recorded with %d stalled clients over a %d-chunk budget", slowClients, slowBudget)
	}

	sc.finish(t, slowClients, 60*time.Second)
}

// TestBudgetAdmissionInChunkOrder: a budget far smaller than the
// per-request window, chunks that can't be cached, many concurrent
// requests. Chunks must acquire budget in order per request; otherwise
// later chunks of a request can hold the whole budget while its head
// chunk waits for it, and the requests stall until the fetch timeout.
func TestBudgetAdmissionInChunkOrder(t *testing.T) {
	const chunk = 64 << 10
	const nChunks = 8
	const size = nChunks * chunk
	const clients = 24
	env := newTestEnv(t, envOpts{
		chunkSize: chunk, workers: 8, fetchConcurrency: 2,
		cache: cachePutFails, fetchTimeout: 3 * time.Second,
		getDelay: 2 * time.Millisecond,
	})
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(size, fmt.Sprintf("order-%d", i))
			resp, err := env.client.Get(env.url + "/" + key)
			if err != nil {
				errs <- err
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || resp.StatusCode != 200 || !bytes.Equal(body, objBytes(key, 0, size-1)) {
				errs <- fmt.Errorf("%s: status %d, %d bytes, err %v", key, resp.StatusCode, len(body), err)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("requests still running after 30s")
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestEvictedBetweenPutAndOpen: the evictor may unlink a chunk file right
// after the fetch wrote it, before the callers open it. They must still
// get the bytes (from the pinned handle), not an error or a torn body.
func TestEvictedBetweenPutAndOpen(t *testing.T) {
	const chunk = 64 << 10
	const size = 5*chunk + 777
	key := objKey(size, "evicted.mkv")
	env := newTestEnv(t, envOpts{chunkSize: chunk, cache: cacheOn})
	env.f.afterPut = func(path string) {
		if err := os.Remove(path); err != nil {
			t.Errorf("remove %s: %v", path, err)
		}
	}
	pinnedBefore := serves(t, viaPinned)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rng, start, end := "", int64(0), int64(size-1)
			if i%2 == 1 {
				start, end = int64(1000+i), int64(3*chunk+i)
				rng = fmt.Sprintf("bytes=%d-%d", start, end)
			}
			req, _ := http.NewRequest(http.MethodGet, env.url+"/"+key, nil)
			if rng != "" {
				req.Header.Set("Range", rng)
			}
			resp, err := env.client.Do(req)
			if err != nil {
				t.Error(err)
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil || !bytes.Equal(body, objBytes(key, start, end)) {
				t.Errorf("request %d (%q): status %d, %d bytes, err %v", i, rng, resp.StatusCode, len(body), err)
			}
		}(i)
	}
	wg.Wait()
	if got := serves(t, viaPinned) - pinnedBefore; got < 6 {
		t.Errorf("served via pinned handle: %v chunks, want at least 6 (one per chunk)", got)
	}
}

// TestEvictorChurnDuringServe runs the real evictor with a 1-byte cap in
// a tight loop — every chunk file is unlinked as soon as it's seen, at
// any point between write, open and send — while clients stream. Every
// response must still be complete and correct.
func TestEvictorChurnDuringServe(t *testing.T) {
	const chunk = 64 << 10
	const size = 9*chunk + 4321
	env := newTestEnv(t, envOpts{chunkSize: chunk, cache: cacheOn, readahead: 2})
	ev := &Evictor{cache: env.f.cache, maxBytes: 1, interval: time.Millisecond}
	freedBefore := metricValue(t, "s3cache_eviction_bytes_freed_total", nil)
	pinnedBefore := serves(t, viaPinned)
	stop := make(chan struct{})
	evDone := make(chan struct{})
	go func() {
		defer close(evDone)
		for {
			select {
			case <-stop:
				return
			default:
				ev.runOnce()
			}
		}
	}()
	defer func() { close(stop); <-evDone }()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(size, fmt.Sprintf("churn-%d", i%4))
			for j := 0; j < 5; j++ {
				start := int64((i*7919 + j*104729) % size)
				end := start + int64(3*chunk)
				if end > size-1 {
					end = size - 1
				}
				req, _ := http.NewRequest(http.MethodGet, env.url+"/"+key, nil)
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
				resp, err := env.client.Do(req)
				if err != nil {
					t.Error(err)
					return
				}
				body, err := io.ReadAll(resp.Body)
				resp.Body.Close()
				if err != nil || resp.StatusCode != 206 || !bytes.Equal(body, objBytes(key, start, end)) {
					t.Errorf("%s %d-%d: status %d, %d bytes, err %v", key, start, end, resp.StatusCode, len(body), err)
				}
			}
		}(i)
	}
	wg.Wait()
	freed := metricValue(t, "s3cache_eviction_bytes_freed_total", nil) - freedBefore
	t.Logf("evicted %d KiB during the run, %v chunks served via pinned handle",
		int64(freed)>>10, serves(t, viaPinned)-pinnedBefore)
	if freed == 0 {
		t.Fatal("the evictor removed nothing: the run did not race anything")
	}
}

// TestAbortedRequestReleasesBuffers: a client that gives up while its
// chunks are still downloading leaves the detached fetch running; when it
// finishes, the uncached buffer must be released although nobody reads
// it. assertNoLeaks (cleanup) checks flights, budget units and bytes.
func TestAbortedRequestReleasesBuffers(t *testing.T) {
	for _, mode := range []cacheMode{cachePutFails, cacheOn} {
		t.Run(mode.String(), func(t *testing.T) {
			const chunk = 64 << 10
			env := newTestEnv(t, envOpts{chunkSize: chunk, cache: mode, getDelay: 300 * time.Millisecond})
			key := objKey(4*chunk, "aborted.mkv")
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.url+"/"+key, nil)
			if resp, err := env.client.Do(req); err == nil {
				resp.Body.Close()
				t.Fatal("request finished before the client gave up; raise getDelay")
			}
			if !waitFor(10*time.Second, func() bool { return env.s3.gets.Load() >= 4 }) {
				t.Fatalf("detached fetches did not run: %d upstream GETs", env.s3.gets.Load())
			}
			env.assertNoLeaks()
		})
	}
}

// TestReadaheadWarmsCacheWithoutHoldingBuffers: readahead fetches past
// the served range land in the cache and return their budget.
func TestReadaheadWarmsCacheWithoutHoldingBuffers(t *testing.T) {
	const chunk = 64 << 10
	env := newTestEnv(t, envOpts{chunkSize: chunk, cache: cacheOn, readahead: 4})
	key := objKey(8*chunk, "readahead.mkv")
	if r := env.do(http.MethodGet, key, fmt.Sprintf("bytes=0-%d", chunk-1)); r.status != 206 {
		t.Fatalf("status %d", r.status)
	}
	if !waitFor(10*time.Second, func() bool {
		return env.s3.gets.Load() == 5 && env.f.flights.Load() == 0 && env.f.budget.inUse.Load() == 0
	}) {
		t.Fatalf("readahead: %d upstream GETs (want 5), flights %d, budget bytes %d",
			env.s3.gets.Load(), env.f.flights.Load(), env.f.budget.inUse.Load())
	}
	hitsBefore := serves(t, viaHit)
	r := env.do(http.MethodGet, key, fmt.Sprintf("bytes=%d-%d", chunk, 5*chunk-1))
	if r.status != 206 || !bytes.Equal(r.body, objBytes(key, chunk, 5*chunk-1)) {
		t.Fatalf("status %d, %d bytes", r.status, len(r.body))
	}
	if got := serves(t, viaHit) - hitsBefore; got != 4 {
		t.Errorf("%v chunks served from cache after readahead, want 4", got)
	}
}
