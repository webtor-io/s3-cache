package services

import (
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"testing"
)

// TestReviewRandomRangesAcrossModes: edge sizes × edge/random ranges, every
// cache mode; responses must match each other and the object bytes.
func TestReviewRandomRangesAcrossModes(t *testing.T) {
	const chunk = 4096
	sizes := []int64{0, 1, 2, chunk - 1, chunk, chunk + 1, 3 * chunk, 3*chunk + 1}
	type rq struct {
		method, rng string
	}
	build := func(size int64, rnd *rand.Rand) []rq {
		out := []rq{{http.MethodHead, ""}, {http.MethodGet, ""}, {http.MethodGet, "bytes=0-"}, {http.MethodGet, "bytes=0-0"},
			{http.MethodGet, "bytes=-1"}, {http.MethodGet, fmt.Sprintf("bytes=-%d", size+10)},
			{http.MethodGet, fmt.Sprintf("bytes=%d-", size)}, {http.MethodGet, fmt.Sprintf("bytes=%d-%d", chunk-1, chunk)},
			{http.MethodGet, fmt.Sprintf("bytes=%d-%d", chunk, 2*chunk-1)}, {http.MethodGet, fmt.Sprintf("bytes=%d-", size-1)}}
		for i := 0; i < 12 && size > 0; i++ {
			s := rnd.Int63n(size)
			e := s + rnd.Int63n(size-s+chunk)
			out = append(out, rq{http.MethodGet, fmt.Sprintf("bytes=%d-%d", s, e)})
		}
		return out
	}
	modes := []struct {
		name string
		mode cacheMode
		warm bool
	}{{"miss", cacheOn, false}, {"hit", cacheOn, true}, {"off", cacheOff, false}, {"putfail", cachePutFails, false}}
	ref := map[string]string{}
	for _, m := range modes {
		env := newTestEnv(t, envOpts{chunkSize: chunk, cache: m.mode})
		for _, size := range sizes {
			key := objKey(size, "edge")
			rnd := rand.New(rand.NewSource(size + 7))
			reqs := build(size, rnd)
			if m.warm {
				env.do(http.MethodGet, key, "")
			}
			for _, r := range reqs {
				if !m.warm && m.mode == cacheOn {
					// fresh key per request so every request is a real miss
					key = objKey(size, fmt.Sprintf("edge-%s-%s", r.method, r.rng))
				}
				got := env.do(r.method, key, r.rng)
				id := fmt.Sprintf("%d %s %q", size, r.method, r.rng)
				// body correctness against the object
				if got.status == 200 && r.method == http.MethodGet {
					if !bytes.Equal(got.body, objBytes(key, 0, size-1)) && size > 0 {
						t.Errorf("%s %s: 200 body mismatch", m.name, id)
					}
				}
				if got.status == 206 {
					var s, e, total int64
					if _, err := fmt.Sscanf(got.header.Get("Content-Range"), "bytes %d-%d/%d", &s, &e, &total); err != nil {
						t.Errorf("%s %s: bad Content-Range %q", m.name, id, got.header.Get("Content-Range"))
					} else if !bytes.Equal(got.body, objBytes(key, s, e)) {
						t.Errorf("%s %s: 206 body mismatch", m.name, id)
					}
				}
				// cross-mode identity (ETag differs per key → drop it and the body hash key dependence)
				got.header.Del("ETag")
				canon := fmt.Sprintf("%d|%s|%s|%d", got.status, got.header.Get("Content-Range"), got.header.Get("Content-Length"), len(got.body))
				if prev, ok := ref[id]; !ok {
					ref[id] = canon
				} else if prev != canon {
					t.Errorf("%s %s: %s differs from miss mode %s", m.name, id, canon, prev)
				}
			}
		}
	}
}
