package services

import (
	"bytes"
	"flag"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/urfave/cli"
)

// exitListener records the connections it accepts, so the test can cut them
// the way the process exit does once Close has returned.
type exitListener struct {
	net.Listener
	mu    sync.Mutex
	conns []net.Conn
}

func (l *exitListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns = append(l.conns, c)
		l.mu.Unlock()
	}
	return c, err
}

func (l *exitListener) exit() {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.conns {
		_ = c.Close()
	}
}

// A Range stream in flight when SIGTERM lands is delivered whole: Close
// returns only after it, and the process exit that follows cuts nothing.
// New connections are refused as soon as the drain starts. The Web comes
// from NewWeb over RegisterWebFlags, so the WEB_SHUTDOWN_TIMEOUT flag must be
// registered there too: without it the timeout is 0 and the stream is cut.
func TestWebCloseDrainsInFlightStream(t *testing.T) {
	env := newTestEnv(t, envOpts{smallBuffers: true})
	set := flag.NewFlagSet("web", flag.ContinueOnError)
	for _, fl := range RegisterWebFlags(nil) {
		fl.Apply(set)
	}
	web := NewWeb(cli.NewContext(nil, set, nil), env.f)

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln := &exitListener{Listener: smallBufListener{inner}}
	go func() { _ = web.serve(ln) }()
	addr := inner.Addr().String()

	const size = 2 << 20
	key := objKey(size, "drain")
	req, err := http.NewRequest(http.MethodGet, "http://"+addr+"/"+key, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-")
	resp, err := env.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status %d", resp.StatusCode)
	}
	got := make([]byte, 64<<10)
	if _, err := io.ReadFull(resp.Body, got); err != nil {
		t.Fatal(err)
	}

	closed := make(chan time.Duration, 1)
	t0 := time.Now()
	go func() {
		web.Close()
		closed <- time.Since(t0)
	}()

	refused := waitFor(2*time.Second, func() bool {
		c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err != nil {
			return true
		}
		_ = c.Close()
		return false
	})
	if !refused {
		t.Fatal("new connections still accepted 2 s after Close started")
	}

	// The client reads the rest slowly (~0.6 s at 32 KiB / 10 ms): the
	// handler stays blocked on the small socket buffers until then.
	type result struct {
		body []byte
		err  error
	}
	rest := make(chan result, 1)
	go func() {
		var b bytes.Buffer
		buf := make([]byte, 32<<10)
		for {
			n, err := resp.Body.Read(buf)
			b.Write(buf[:n])
			if err != nil {
				if err == io.EOF {
					err = nil
				}
				rest <- result{b.Bytes(), err}
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}()

	select {
	case d := <-closed:
		t.Logf("Close returned after %v", d)
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return within 10 s")
	}
	ln.exit()

	r := <-rest
	got = append(got, r.body...)
	if r.err != nil || !bytes.Equal(got, objBytes(key, 0, size-1)) {
		t.Fatalf("stream cut: got %d of %d bytes, err %v", len(got), size, r.err)
	}
}
