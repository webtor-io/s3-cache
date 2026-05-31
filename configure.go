package main

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	cs "github.com/webtor-io/common-services"
	s "github.com/webtor-io/s3-cache/services"
)

func configure(app *cli.App) {
	app.Flags = []cli.Flag{}
	app.Flags = cs.RegisterProbeFlags(app.Flags)
	app.Flags = cs.RegisterS3ClientFlags(app.Flags)
	app.Flags = s.RegisterWebFlags(app.Flags)
	app.Flags = s.RegisterFetcherFlags(app.Flags)
	app.Action = run
}

func run(c *cli.Context) error {
	var servers []cs.Servable

	probe := cs.NewProbe(c)
	if probe != nil {
		servers = append(servers, probe)
		defer probe.Close()
	}

	httpCl := &http.Client{
		Timeout: 0, // no overall deadline; body reads can be long
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
			// Force HTTP/1.1 only — under HTTP/2 all chunk streams share one TCP
			// connection, so a per-host slowdown stalls every parallel worker at
			// once and ResponseHeaderTimeout doesn't apply to H2 streams.
			// One TCP per chunk gives independent congestion control + fresh dice
			// odds against any per-connection rate variance.
			ForceAttemptHTTP2: false,
			TLSNextProto:      map[string]func(string, *tls.Conn) http.RoundTripper{},
			// Bound TTFB: if S3 doesn't return response headers in 3s, fail fast
			// and let the chunk retry path kick in. Body reads are unaffected
			// (slow-detector handles those).
			ResponseHeaderTimeout: 3 * time.Second,
			// Bound TCP+TLS handshake similarly.
			DialContext: (&net.Dialer{
				Timeout:   3 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			TLSHandshakeTimeout: 3 * time.Second,
		},
	}

	s3cl := cs.NewS3Client(c, httpCl)
	if s3cl == nil {
		log.Fatal("S3 client not configured — set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_ENDPOINT/AWS_REGION")
	}

	fetcher := s.NewFetcher(c, s3cl)
	web := s.NewWeb(c, fetcher)
	defer web.Close()
	servers = append(servers, web)

	serve := cs.NewServe(servers...)
	if err := serve.Serve(); err != nil {
		log.WithError(err).Error("got server error")
		return err
	}
	return nil
}
