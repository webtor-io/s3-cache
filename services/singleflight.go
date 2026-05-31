package services

import "sync"

// singleflight collapses concurrent calls with the same key to a single
// execution. Lighter than golang.org/x/sync/singleflight and avoids the
// extra module dep — we only need the shared-result case, no Forget /
// DoChan API.
//
// The `shared` return tells the caller whether their work was deduped
// onto someone else's call (used for metrics).
type singleflight struct {
	mu sync.Mutex
	m  map[string]*sfCall
}

type sfCall struct {
	wg   sync.WaitGroup
	data []byte
	err  error
}

func newSingleflight() *singleflight {
	return &singleflight{m: make(map[string]*sfCall)}
}

func (g *singleflight) Do(key string, fn func() ([]byte, error)) (data []byte, err error, shared bool) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.data, c.err, true
	}
	c := &sfCall{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.data, c.err = fn()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	c.wg.Done()
	return c.data, c.err, false
}
