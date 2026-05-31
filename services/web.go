package services

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	WebHostFlag = "host"
	WebPortFlag = "port"
)

type Web struct {
	host    string
	port    int
	ln      net.Listener
	fetcher *Fetcher
}

func RegisterWebFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{
			Name:   WebHostFlag,
			Usage:  "listening host",
			Value:  "",
			EnvVar: "WEB_HOST",
		},
		cli.IntFlag{
			Name:   WebPortFlag,
			Usage:  "http listening port",
			Value:  8080,
			EnvVar: "WEB_PORT",
		},
	)
}

func NewWeb(c *cli.Context, fetcher *Fetcher) *Web {
	return &Web{
		host:    c.String(WebHostFlag),
		port:    c.Int(WebPortFlag),
		fetcher: fetcher,
	}
}

// parseRange parses "bytes=start-end" or "bytes=start-". Returns -1 for missing end.
// Only supports single byte range (multi-range not supported — fine for our use case).
func parseRange(h string) (start, end int64, err error) {
	if h == "" {
		return 0, -1, nil
	}
	if !strings.HasPrefix(h, "bytes=") {
		return 0, -1, errors.New("only bytes= ranges supported")
	}
	spec := strings.TrimPrefix(h, "bytes=")
	if strings.Contains(spec, ",") {
		return 0, -1, errors.New("multi-range not supported")
	}
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, -1, errors.New("bad range")
	}
	if parts[0] == "" {
		// suffix range "bytes=-N" — last N bytes; we don't support this without HEAD first
		return 0, -1, errors.New("suffix range not supported")
	}
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, -1, errors.Wrap(err, "bad range start")
	}
	if parts[1] == "" {
		return start, -1, nil
	}
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, -1, errors.Wrap(err, "bad range end")
	}
	if end < start {
		return 0, -1, errors.New("end before start")
	}
	return start, end, nil
}

// parseKey returns the S3 object key from the URL path. The bucket is
// fixed per-deploy via AWS_BUCKET, so the URL carries only the key.
// We accept the entire path after the leading "/" as the key — S3 keys
// can contain "/" and we just pass it through verbatim.
func parseKey(p string) (string, bool) {
	p = strings.TrimPrefix(p, "/")
	if p == "" {
		return "", false
	}
	return p, true
}

func (s *Web) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key, ok := parseKey(r.URL.Path)
	if !ok {
		http.Error(w, "expected /{key}", http.StatusBadRequest)
		return
	}

	start, end, err := parseRange(r.Header.Get("Range"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	logger := log.WithFields(log.Fields{
		"key":    key,
		"range":  r.Header.Get("Range"),
		"method": r.Method,
	})

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	// HEAD: just pull metadata from S3 (single HeadObject), return headers.
	if r.Method == http.MethodHead {
		if err := s.fetcher.Head(ctx, w, key); err != nil {
			logger.WithError(err).Warn("HEAD failed")
			httpErrorFromS3(w, err)
			requestsTotal.WithLabelValues("HEAD", "error").Inc()
			return
		}
		requestsTotal.WithLabelValues("HEAD", "ok").Inc()
		return
	}

	t0 := time.Now()
	if err := s.fetcher.Get(ctx, w, key, start, end); err != nil {
		// If we've already written some bytes, just log; cannot reset response.
		logger.WithError(err).WithField("elapsed", time.Since(t0)).Warn("GET failed")
		requestsTotal.WithLabelValues("GET", "error").Inc()
		return
	}
	logger.WithField("elapsed", time.Since(t0)).Debug("GET served")
	requestsTotal.WithLabelValues("GET", "ok").Inc()
}

func httpErrorFromS3(w http.ResponseWriter, err error) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "NoSuchKey"), strings.Contains(msg, "NotFound"), strings.Contains(msg, "status code: 404"):
		http.Error(w, "not found", http.StatusNotFound)
	case strings.Contains(msg, "Forbidden"), strings.Contains(msg, "status code: 403"):
		http.Error(w, "forbidden", http.StatusForbidden)
	default:
		http.Error(w, "upstream error", http.StatusBadGateway)
	}
}

func (s *Web) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Wrap(err, "failed to listen")
	}
	s.ln = ln
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handle)
	log.Infof("serving s3-cache at %v", addr)
	srv := &http.Server{Handler: mux}
	return srv.Serve(ln)
}

func (s *Web) Close() {
	if s.ln != nil {
		_ = s.ln.Close()
	}
}
