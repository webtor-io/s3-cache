package services

import "sync"

// singleflight collapses concurrent fetches of the same chunk into one
// upstream GET. Unlike x/sync/singleflight the result is a shared
// resource (an open cache file, or a budgeted buffer), so the group counts
// the callers that joined and publishes the result with that many
// references; every caller drops exactly one (chunkFlight.claim).
type singleflight struct {
	mu sync.Mutex
	m  map[string]*sfCall
}

type sfCall struct {
	// admitted is closed once the fetch no longer waits for buffer budget
	// — the leader holds a unit, or gave up. Callers admit their next chunk
	// only after this one (see serveAligned).
	admitted  chan struct{}
	admitOnce sync.Once
	done      chan struct{} // closed once res is set
	res       *chunkFlight
	waiters   int32 // guarded by singleflight.mu until finish
}

func (c *sfCall) admit() { c.admitOnce.Do(func() { close(c.admitted) }) }

func newSingleflight() *singleflight {
	return &singleflight{m: make(map[string]*sfCall)}
}

// join returns the in-flight call for key, or registers a new one and
// reports leader == true: the caller must then run the fetch and finish.
func (g *singleflight) join(key string) (c *sfCall, leader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if c, ok := g.m[key]; ok {
		c.waiters++
		return c, false
	}
	c = &sfCall{admitted: make(chan struct{}), done: make(chan struct{}), waiters: 1}
	g.m[key] = c
	return c, true
}

// finish publishes res to every caller. Joining ends here: the key is
// removed under the lock, so the caller count — and with it res's
// reference count — is final.
func (g *singleflight) finish(key string, c *sfCall, res *chunkFlight) {
	g.mu.Lock()
	delete(g.m, key)
	res.refs.Store(c.waiters)
	g.mu.Unlock()
	c.admit()
	c.res = res
	close(c.done)
}

func (g *singleflight) inflight() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.m)
}
