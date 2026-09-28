package services

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// seedChunk puts key's chunk at off (size bytes) into a put-fails cache:
// lookups of that chunk hit, every write still fails (the dirs on its path
// are read-only too).
func (e *testEnv) seedChunk(key string, off, size int64) {
	e.t.Helper()
	p, err := e.f.cache.path(key, off)
	if err != nil {
		e.t.Fatal(err)
	}
	keyDir := filepath.Dir(p)
	dirs := []string{filepath.Join(e.cacheRoot, "s3-cache"), filepath.Dir(keyDir), keyDir}
	for _, d := range dirs {
		if err := os.Chmod(d, 0755); err != nil && !os.IsNotExist(err) {
			e.t.Fatal(err)
		}
	}
	if err := os.MkdirAll(keyDir, 0755); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(p, objBytes(key, off, off+size-1), 0644); err != nil {
		e.t.Fatal(err)
	}
	for _, d := range dirs {
		if err := os.Chmod(d, 0555); err != nil {
			e.t.Fatal(err)
		}
	}
	e.t.Cleanup(func() {
		for _, d := range dirs {
			_ = os.Chmod(d, 0755)
		}
	})
}

// TestPausedClientReleasesBudget: with chunks that can't be cached, a
// client that stops reading pins the buffer budget its window holds. The
// write guard cuts it off, so another client's miss gets budget before its
// fetch deadline instead of failing with a 502 — whether the paused
// consumer is stuck on a buffered chunk, or on a cached chunk that it was
// already writing when the buffered ones behind it arrived (upstream GETs
// are delayed so they land after the consumer blocked).
func TestPausedClientReleasesBudget(t *testing.T) {
	for _, c := range []struct {
		name        string
		cachedHead  int64 // leading chunks already in the cache
		wantBlocked string
	}{
		{"on an uncached chunk", 0, viaBuffer},
		// 3 cached chunks = 768 KiB, more than a paused loopback
		// connection takes in (256–512 KiB on macOS, less on Linux).
		{"on a cached chunk ahead of uncached ones", 3, viaHit},
	} {
		t.Run(c.name, func(t *testing.T) {
			const fetchTimeout = 4 * time.Second
			env := newTestEnv(t, envOpts{
				chunkSize: slowChunk, workers: 8, fetchConcurrency: slowBudget,
				cache: cachePutFails, smallBuffers: true, fetchTimeout: fetchTimeout,
				getDelay: 200 * time.Millisecond,
			})
			pausedKey := objKey(slowChunks*slowChunk, "paused")
			for i := int64(0); i < c.cachedHead; i++ {
				env.seedChunk(pausedKey, i*slowChunk, slowChunk)
			}
			hitsBefore := serves(t, viaHit)
			cutsBefore := metricValue(t, "s3cache_stalled_writes_cut_total", nil)

			resp, err := env.client.Get(env.url + "/" + pausedKey)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if !waitFor(5*time.Second, func() bool {
				return env.f.budget.inUse.Load() == slowBudget*slowChunk
			}) {
				t.Fatalf("paused client holds %d budget bytes, want the whole budget %d", env.f.budget.inUse.Load(), slowBudget*slowChunk)
			}
			// Where the paused consumer is stuck: still inside the cached
			// head, or past it.
			if hits := serves(t, viaHit) - hitsBefore; (hits < float64(c.cachedHead)) != (c.wantBlocked == viaHit) {
				t.Fatalf("%v of %d cached head chunks written, want the consumer blocked on a %s chunk", hits, c.cachedHead, c.wantBlocked)
			}

			key := objKey(2*slowChunk, "after-paused")
			t0 := time.Now()
			r := env.do(http.MethodGet, key, "")
			took := time.Since(t0)
			if r.status != http.StatusOK || !bytes.Equal(r.body, objBytes(key, 0, 2*slowChunk-1)) {
				t.Fatalf("miss behind a paused client: status %d, %d bytes after %v", r.status, len(r.body), took)
			}
			if took >= fetchTimeout {
				t.Fatalf("miss behind a paused client took %v, fetch timeout %v", took, fetchTimeout)
			}
			if cuts := metricValue(t, "s3cache_stalled_writes_cut_total", nil) - cutsBefore; cuts < 1 {
				t.Errorf("s3cache_stalled_writes_cut_total grew by %v, want the paused client counted", cuts)
			}
			t.Logf("miss behind a paused client served in %v", took)
		})
	}
}

// TestPausedClientKeptWithoutBudget: a request that holds no budget —
// every chunk cached — is never cut, however long its client pauses.
func TestPausedClientKeptWithoutBudget(t *testing.T) {
	const fetchTimeout = 2 * time.Second // write guard timeout: 1 s
	env := newTestEnv(t, envOpts{
		chunkSize: slowChunk, workers: 8, fetchConcurrency: slowBudget,
		cache: cacheOn, smallBuffers: true, fetchTimeout: fetchTimeout,
	})
	key := objKey(slowChunks*slowChunk, "paused-cached")
	cutsBefore := metricValue(t, "s3cache_stalled_writes_cut_total", nil)
	resp, err := env.client.Get(env.url + "/" + key)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	time.Sleep(3 * env.f.writeGuardTimeout)
	body, err := io.ReadAll(resp.Body)
	if err != nil || !bytes.Equal(body, objBytes(key, 0, slowChunks*slowChunk-1)) {
		t.Fatalf("paused client of a cached object: %d bytes, err %v", len(body), err)
	}
	if cuts := metricValue(t, "s3cache_stalled_writes_cut_total", nil) - cutsBefore; cuts != 0 {
		t.Errorf("s3cache_stalled_writes_cut_total grew by %v", cuts)
	}
}

// deadlineRecorder is a ResponseWriter that records the write deadline in
// force at each Write and flush. With block set, the first Write signals
// inWrite and waits for block to close.
type deadlineRecorder struct {
	hdr     http.Header
	block   chan struct{}
	inWrite chan struct{}

	mu       sync.Mutex
	deadline time.Time
	atWrite  []time.Time
	atFlush  []time.Time
}

func newDeadlineRecorder() *deadlineRecorder { return &deadlineRecorder{hdr: http.Header{}} }

func (d *deadlineRecorder) Header() http.Header { return d.hdr }
func (d *deadlineRecorder) WriteHeader(int)     {}

func (d *deadlineRecorder) Write(p []byte) (int, error) {
	d.mu.Lock()
	d.atWrite = append(d.atWrite, d.deadline)
	first := len(d.atWrite) == 1
	d.mu.Unlock()
	if first && d.block != nil {
		close(d.inWrite)
		<-d.block
	}
	return len(p), nil
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.deadline = t
	return nil
}

func (d *deadlineRecorder) FlushError() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.atFlush = append(d.atFlush, d.deadline)
	return nil
}

func (d *deadlineRecorder) current() time.Time {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.deadline
}

// allSet reports whether every recorded deadline is set (want true) or
// every one is zero (want false).
func allSet(ts []time.Time, want bool) bool {
	if len(ts) == 0 {
		return false
	}
	for _, t := range ts {
		if t.IsZero() == want {
			return false
		}
	}
	return true
}

func bufferedChunk(n int) chunkResult {
	return chunkResult{data: make([]byte, n), via: viaBuffer}
}

func fileChunk(t *testing.T, n int) chunkResult {
	t.Helper()
	p := filepath.Join(t.TempDir(), "chunk")
	if err := os.WriteFile(p, make([]byte, n), 0644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return chunkResult{file: f, fileSize: int64(n), via: viaFile}
}

// TestWriteGuardDeadlines: a chunk write — and the flush of its tail,
// which can block in the response's bufio just as well — runs under a
// deadline exactly while the request holds buffered chunks, and the
// deadline is gone after every chunk (nothing else resets it on a
// kept-alive connection).
func TestWriteGuardDeadlines(t *testing.T) {
	const n = 100
	steps := []struct {
		name     string
		holds    int // hold() calls before this write (buffered chunks delivered)
		buffered bool
		want     bool // deadline in force during the write and the flush
	}{
		{"cached chunk, no buffers held", 0, false, false},
		{"buffered chunk", 1, true, true},
		{"cached chunk, buffered chunk held behind it", 1, false, true},
		{"the held buffered chunk", 0, true, true},
		{"cached chunk after the buffers were written", 0, false, false},
	}
	g := newWriteGuard(nil, time.Minute)
	for _, s := range steps {
		rec := newDeadlineRecorder()
		g.rc = http.NewResponseController(rec)
		for i := 0; i < s.holds; i++ {
			g.hold()
		}
		res := fileChunk(t, n)
		if s.buffered {
			res = bufferedChunk(n)
		}
		if err := writeChunk(rec, g, &res, 0, n-1); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		if !allSet(rec.atWrite, s.want) {
			t.Errorf("%s: deadlines at write %v, want set: %v", s.name, rec.atWrite, s.want)
		}
		if len(rec.atFlush) != 1 || !allSet(rec.atFlush, s.want) {
			t.Errorf("%s: deadlines at flush %v, want one flush, set: %v", s.name, rec.atFlush, s.want)
		}
		if d := rec.current(); !d.IsZero() {
			t.Errorf("%s: write deadline %v left set after the chunk", s.name, d)
		}
	}
}

// TestWriteGuardArmsMidWrite: a consumer already blocked writing a cached
// chunk when a buffered chunk arrives in its window now pins budget — the
// deadline must be armed on the write in progress.
func TestWriteGuardArmsMidWrite(t *testing.T) {
	rec := newDeadlineRecorder()
	rec.block, rec.inWrite = make(chan struct{}), make(chan struct{})
	g := newWriteGuard(http.NewResponseController(rec), time.Minute)
	res := fileChunk(t, 100)
	done := make(chan error, 1)
	go func() { done <- writeChunk(rec, g, &res, 0, 99) }()

	<-rec.inWrite
	if d := rec.current(); !d.IsZero() {
		t.Fatalf("deadline %v on a cached chunk before any buffer is held", d)
	}
	g.hold()
	if d := rec.current(); d.IsZero() {
		t.Error("no deadline on the write in progress after a buffered chunk arrived")
	}
	close(rec.block)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if d := rec.current(); !d.IsZero() {
		t.Errorf("write deadline %v left set after the chunk", d)
	}
}
