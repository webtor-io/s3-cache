package services

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

// headCache is a TTL map of HeadObject results keyed by object key.
// Entries past the TTL are not evicted eagerly: a stale entry is kept as an
// outage fallback (see Fetcher.headObject) until the size cap pushes it out.
type headCache struct {
	mu  sync.Mutex
	m   map[string]headEntry
	ttl time.Duration
	cap int
}

type headEntry struct {
	out *s3.HeadObjectOutput
	at  time.Time
}

const headCacheCap = 8192

func newHeadCache(ttl time.Duration) *headCache {
	return &headCache{
		m:   make(map[string]headEntry),
		ttl: ttl,
		cap: headCacheCap,
	}
}

// get returns the entry (nil if absent) and whether it is still fresh.
func (h *headCache) get(key string) (*s3.HeadObjectOutput, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.m[key]
	if !ok {
		return nil, false
	}
	return e.out, time.Since(e.at) < h.ttl
}

func (h *headCache) put(key string, out *s3.HeadObjectOutput) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.m) >= h.cap {
		// Shed expired entries first; if everything is fresh (cap-sized hot
		// set), drop arbitrary entries — correctness is unaffected, the next
		// miss just re-HEADs.
		for k, e := range h.m {
			if time.Since(e.at) >= h.ttl {
				delete(h.m, k)
			}
		}
		for k := range h.m {
			if len(h.m) < h.cap {
				break
			}
			delete(h.m, k)
		}
	}
	h.m[key] = headEntry{out: out, at: time.Now()}
}
