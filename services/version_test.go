package services

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
)

func testDiskCache(t *testing.T) *DiskCache {
	t.Helper()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "s1"), 0755); err != nil {
		t.Fatal(err)
	}
	return &DiskCache{location: filepath.Join(root, "s*"), subdir: "s3cache"}
}

func readAll(t *testing.T, f *os.File) []byte {
	t.Helper()
	if f == nil {
		return nil
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Chunks are named by the object's version: a re-uploaded object (vault
// re-stores a corrupt file under the same key) never hits its old bytes.
func TestDiskCache_ChunksAreVersioned(t *testing.T) {
	c := testDiskCache(t)
	v1, v2 := objVersion{etag: `"e1"`}, objVersion{etag: `"e2"`}
	pin, _, err := c.Put("k", v1, 0, []byte("old"))
	if err != nil {
		t.Fatal(err)
	}
	pin.Close()
	f, _, _ := c.Get("k", v1, 0)
	if got := readAll(t, f); string(got) != "old" {
		t.Fatalf("same version: got %q", got)
	}
	if f, _, _ := c.Get("k", v2, 0); f != nil {
		f.Close()
		t.Fatal("a new version hit the old version's chunk")
	}
}

// Chunks written before versioning carry no version. They stay valid for an
// object unchanged since then (the cache stays warm), never for one written
// after this shard started versioning.
func TestDiskCache_UnversionedChunksOnlyForOlderObjects(t *testing.T) {
	c := testDiskCache(t)
	pin, _, err := c.Put("k", objVersion{}, 0, []byte("legacy"))
	if err != nil {
		t.Fatal(err)
	}
	pin.Close()
	since, err := c.versionedSince("k")
	if err != nil {
		t.Fatal(err)
	}
	f, _, _ := c.Get("k", objVersion{etag: `"e1"`, modified: since.Add(-time.Hour)}, 0)
	if got := readAll(t, f); string(got) != "legacy" {
		t.Fatalf("object older than versioning: got %q, want the legacy chunk", got)
	}
	if f, _, _ := c.Get("k", objVersion{etag: `"e2"`, modified: since.Add(time.Second)}, 0); f != nil {
		f.Close()
		t.Fatal("an object written after versioning began hit a legacy chunk")
	}
	again, _ := (&DiskCache{location: c.location, subdir: c.subdir}).versionedSince("k")
	if !again.Equal(since) {
		t.Fatalf("marker not persisted: %v then %v", since, again)
	}
}

// The evictor never removes the marker: without it every legacy chunk would
// look valid again for a re-uploaded object.
func TestEvictor_KeepsVersionMarker(t *testing.T) {
	c := testDiskCache(t)
	if _, err := c.versionedSince("k"); err != nil {
		t.Fatal(err)
	}
	pin, _, _ := c.Put("k", objVersion{etag: "e"}, 0, bytes.Repeat([]byte("x"), 4096))
	pin.Close()
	roots, _ := c.CacheRoots()
	(&Evictor{cache: c, maxBytes: 1}).sweepShard(roots[0])
	if _, err := os.Stat(filepath.Join(roots[0], versionMarker)); err != nil {
		t.Fatalf("marker gone after eviction: %v", err)
	}
}

// ---- end to end through Fetcher.Get with a fake S3 ----

type fakeObjectStore struct {
	mu       sync.Mutex
	data     []byte
	etag     string
	modified time.Time
}

func (s *fakeObjectStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	data, etag, mod := s.data, s.etag, s.modified
	s.mu.Unlock()
	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", mod.UTC().Format(http.TimeFormat))
	if m := r.Header.Get("If-Match"); m != "" && m != etag {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}
	start, end := int64(0), int64(len(data))-1
	if rg := r.Header.Get("Range"); rg != "" {
		fmt.Sscanf(rg, "bytes=%d-%d", &start, &end)
		end = min(end, int64(len(data))-1)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	}
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	if r.Method == http.MethodHead {
		return
	}
	if r.Header.Get("Range") != "" {
		w.WriteHeader(http.StatusPartialContent)
	}
	_, _ = w.Write(data[start : end+1])
}

func (s *fakeObjectStore) put(data []byte, etag string, modified time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data, s.etag, s.modified = data, etag, modified
}

func testFetcher(t *testing.T, endpoint string, cache *DiskCache, headTTL ...time.Duration) *Fetcher {
	t.Helper()
	app := cli.NewApp()
	app.Flags = cs.RegisterS3ClientFlags(nil)
	set := flag.NewFlagSet("t", flag.ContinueOnError)
	for _, f := range app.Flags {
		f.Apply(set)
	}
	if err := set.Parse([]string{"--aws-access-key-id=k", "--aws-secret-access-key=s", "--aws-region=de",
		"--aws-endpoint=" + endpoint, "--aws-no-ssl"}); err != nil {
		t.Fatal(err)
	}
	const chunk = 1024
	return &Fetcher{
		s3cl: cs.NewS3Client(cli.NewContext(app, set, nil), http.DefaultClient), bucket: "b",
		chunkSize: chunk, workers: 2, cache: cache, sf: newSingleflight(),
		budget: newChunkBudget(4, chunk), chunkFetchTimeout: 5 * time.Second,
		writeGuardTimeout: 2500 * time.Millisecond, heads: newHeadCache(append(headTTL, 0)[0]),
	}
}

func get(t *testing.T, f *Fetcher) string {
	t.Helper()
	rec := httptest.NewRecorder()
	if err := f.Get(context.Background(), rec, "obj", 0, -1, 0, false); err != nil {
		t.Fatal(err)
	}
	return rec.Body.String()
}

// After vault re-uploads an object under the same key, s3-cache serves the new
// bytes once its HEAD refreshes; before, it kept serving the cached old chunks.
func TestFetcher_ServesReUploadedObject(t *testing.T) {
	store := &fakeObjectStore{}
	store.put([]byte(strings.Repeat("old-", 700)), `"v1"`, time.Now().Add(-time.Minute))
	srv := httptest.NewServer(store)
	defer srv.Close()
	f := testFetcher(t, srv.URL, testDiskCache(t))
	if got := get(t, f); !strings.HasPrefix(got, "old-") {
		t.Fatalf("first read: %.8q", got)
	}
	store.put([]byte(strings.Repeat("new-", 700)), `"v2"`, time.Now().Add(time.Minute))
	if got := get(t, f); !strings.HasPrefix(got, "new-") || strings.Contains(got, "old-") {
		t.Fatalf("after re-upload: served %.8q…, want only the new bytes", got)
	}
}

// Within the HEAD TTL after a re-upload the cached ETag is old. A chunk missed
// then must not be fetched from the new object and filed under the old
// version: one response would mix old and new bytes. Chunk GETs are
// conditional on the version; a mismatch drops the stale HEAD instead.
func TestFetcher_NoMixedVersionsWithinHeadTTL(t *testing.T) {
	store := &fakeObjectStore{}
	store.put([]byte(strings.Repeat("old-", 700)), `"v1"`, time.Now().Add(-time.Minute))
	srv := httptest.NewServer(store)
	defer srv.Close()
	f := testFetcher(t, srv.URL, testDiskCache(t), time.Hour)
	rec := httptest.NewRecorder()
	if err := f.Get(context.Background(), rec, "obj", 0, 1023, 0, true); err != nil { // caches chunk 0 only
		t.Fatal(err)
	}
	store.put([]byte(strings.Repeat("new-", 700)), `"v2"`, time.Now().Add(time.Minute))
	rec = httptest.NewRecorder()
	_ = f.Get(context.Background(), rec, "obj", 0, -1, 0, false)
	if body := rec.Body.String(); strings.Contains(body, "old-") && strings.Contains(body, "new-") {
		t.Fatal("one response mixed the old and the new object")
	}
	if got := get(t, f); strings.Contains(got, "old-") {
		t.Fatalf("after the version mismatch: served %.8q…, want the new bytes", got)
	}
}

// A marker that cannot be read does not stop caching: versioned chunks need no
// marker, and old ones are then trusted, as before versioning.
func TestDiskCache_BadMarkerKeepsCaching(t *testing.T) {
	c := testDiskCache(t)
	roots, _ := c.CacheRoots()
	if err := os.MkdirAll(roots[0], 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(roots[0], versionMarker), nil, 0644); err != nil { // torn write
		t.Fatal(err)
	}
	pin, _, err := c.Put("k", objVersion{etag: "e"}, 0, []byte("new"))
	if err != nil {
		t.Fatalf("put with a bad marker: %v", err)
	}
	pin.Close()
	if f, _, _ := c.Get("k", objVersion{etag: "e"}, 0); f == nil {
		t.Fatal("versioned chunk not served")
	} else {
		f.Close()
	}
}

// Markers exist from startup: an object re-uploaded between the deploy and
// the first use of a shard must not have its old chunks trusted.
func TestDiskCache_MarkersCreatedAtStart(t *testing.T) {
	c := testDiskCache(t)
	c.ensureMarkers()
	roots, _ := c.CacheRoots()
	for _, r := range roots {
		if _, err := os.Stat(filepath.Join(r, versionMarker)); err != nil {
			t.Fatalf("%s: %v", r, err)
		}
	}
}
