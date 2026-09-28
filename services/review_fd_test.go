package services

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// cacheFDs lists this process's open descriptors on files under root (lsof).
func cacheFDs(t *testing.T, root string) []string {
	t.Helper()
	real, err := filepath.EvalSymlinks(root)
	if err != nil {
		real = root
	}
	if ents, err := os.ReadDir("/proc/self/fd"); err == nil {
		var fds []string
		for _, e := range ents {
			if l, err := os.Readlink("/proc/self/fd/" + e.Name()); err == nil && (strings.HasPrefix(l, real) || strings.HasPrefix(l, root)) {
				fds = append(fds, l)
			}
		}
		return fds
	}
	out, err := exec.Command("lsof", "-n", "-P", "-p", fmt.Sprint(os.Getpid()), "-Fn").Output()
	if err != nil && len(out) == 0 {
		t.Fatalf("lsof: %v", err)
	}
	var fds []string
	for _, ln := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(ln, "n") && (strings.HasPrefix(ln[1:], real) || strings.HasPrefix(ln[1:], root)) {
			fds = append(fds, ln[1:])
		}
	}
	return fds
}

// TestReviewNoFDLeakCacheOn: in cache-on mode every miss now holds a file
// handle instead of a buffer. Clients that disconnect mid-stream, clients
// that give up before headers, and requests that fail mid-response must
// leave no cache-file descriptor open once the detached fetches settle.
func TestReviewNoFDLeakCacheOn(t *testing.T) {
	const chunk = 64 << 10
	const nChunks = 16
	const size = nChunks * chunk
	env := newTestEnv(t, envOpts{chunkSize: chunk, cache: cacheOn, smallBuffers: true, readahead: 4})
	env.s3.failFrom = 3 * chunk

	var wg sync.WaitGroup
	var peak int
	var peakMu sync.Mutex
	errs := make(chan error, 256)

	// 1) mid-stream disconnects after 100 KiB
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(size, fmt.Sprintf("disc-%d", i%8))
			resp, err := env.client.Get(env.url + "/" + key)
			if err != nil {
				errs <- err
				return
			}
			buf := make([]byte, 100<<10)
			_, _ = io.ReadFull(resp.Body, buf)
			time.Sleep(50 * time.Millisecond) // let the window fill with open handles
			peakMu.Lock()
			if n := len(cacheFDs(t, env.cacheRoot)); n > peak {
				peak = n
			}
			peakMu.Unlock()
			resp.Body.Close() // unread body → Transport closes the conn
		}(i)
	}
	// 2) requests failing mid-response (chunk 3 onward refused upstream)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(size, fmt.Sprintf("fail-%d", i))
			resp, err := env.client.Get(env.url + "/" + key)
			if err != nil {
				errs <- err
				return
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err == nil {
				errs <- fmt.Errorf("%s: expected a truncated body, got %d bytes status %d", key, len(body), resp.StatusCode)
				return
			}
			if !bytes.Equal(body, objBytes(key, 0, int64(len(body))-1)) {
				errs <- fmt.Errorf("%s: wrong bytes before truncation", key)
			}
		}(i)
	}
	// 3) ranges that start in the refused area → clean error status
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(size, fmt.Sprintf("fail-r-%d", i))
			r := env.do(http.MethodGet, key, fmt.Sprintf("bytes=%d-", 5*chunk))
			if r.status != http.StatusForbidden {
				errs <- fmt.Errorf("%s: status %d, want 403", key, r.status)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	if !waitFor(15*time.Second, func() bool {
		return env.f.flights.Load() == 0 && env.f.sf.inflight() == 0 && len(cacheFDs(t, env.cacheRoot)) == 0
	}) {
		t.Fatalf("cache fds still open after settle: %d (flights %d): %v",
			len(cacheFDs(t, env.cacheRoot)), env.f.flights.Load(), cacheFDs(t, env.cacheRoot))
	}
	t.Logf("peak cache fds observed mid-run: %d", peak)
	if peak == 0 {
		t.Fatal("never observed an open cache fd: the test did not exercise the file path")
	}
}

// TestReviewAbortBeforeHeadersCacheOnFDs: clients that give up while the
// detached fetches are still downloading — the claim goroutines must close
// the handles they open for callers that are gone.
func TestReviewAbortBeforeHeadersCacheOnFDs(t *testing.T) {
	const chunk = 64 << 10
	env := newTestEnv(t, envOpts{chunkSize: chunk, cache: cacheOn, getDelay: 300 * time.Millisecond})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(8*chunk, fmt.Sprintf("early-%d", i%4))
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, env.url+"/"+key, nil)
			if resp, err := env.client.Do(req); err == nil {
				resp.Body.Close()
			}
		}(i)
	}
	wg.Wait()
	if !waitFor(15*time.Second, func() bool {
		return env.s3.gets.Load() >= 4*8 && env.f.flights.Load() == 0 && len(cacheFDs(t, env.cacheRoot)) == 0
	}) {
		t.Fatalf("gets %d flights %d cache fds %v", env.s3.gets.Load(), env.f.flights.Load(), cacheFDs(t, env.cacheRoot))
	}
}

// TestReviewCacheRootRemovedMidServe: the whole cache root disappears while
// clients stream misses. Responses must stay byte-exact; the cache must
// recover (dirs recreated) for later requests.
func TestReviewCacheRootRemovedMidServe(t *testing.T) {
	const chunk = 64 << 10
	const size = 12*chunk + 99
	env := newTestEnv(t, envOpts{chunkSize: chunk, cache: cacheOn, smallBuffers: true})
	stop := make(chan struct{})
	rmDone := make(chan struct{})
	go func() {
		defer close(rmDone)
		for {
			select {
			case <-stop:
				return
			default:
				_ = os.RemoveAll(env.cacheRoot)
				time.Sleep(time.Millisecond)
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := objKey(size, fmt.Sprintf("rm-%d", i%6))
			for j := 0; j < 3; j++ {
				start := int64((i*31337 + j*7777) % size)
				r := env.do(http.MethodGet, key, fmt.Sprintf("bytes=%d-", start))
				if r.status != 206 || !bytes.Equal(r.body, objBytes(key, start, size-1)) {
					t.Errorf("%s from %d: status %d, %d bytes", key, start, r.status, len(r.body))
				}
			}
		}(i)
	}
	wg.Wait()
	close(stop)
	<-rmDone
	t.Logf("serves: file=%v pinned=%v buffer=%v", serves(t, viaFile), serves(t, viaPinned), serves(t, viaBuffer))
	// After the directory is gone for good, the next miss recreates it and caches.
	key := objKey(size, "after-rm")
	okBefore := metricValue(t, "s3cache_cache_writes_total", map[string]string{"result": "ok"})
	if r := env.do(http.MethodGet, key, ""); r.status != 200 || !bytes.Equal(r.body, objBytes(key, 0, size-1)) {
		t.Fatalf("after removal: status %d", r.status)
	}
	if got := metricValue(t, "s3cache_cache_writes_total", map[string]string{"result": "ok"}) - okBefore; got != 13 {
		t.Fatalf("cache writes after removal: %v, want 13", got)
	}
}
