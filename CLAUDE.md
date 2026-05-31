# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Transparent S3 proxy for the [webtor.io](https://webtor.io) platform. Sits in
front of S3-compatible object storage to improve single-connection throughput
via parallel range fetches and an abort-on-slow strategy.

Designed to be a drop-in target for redirects from `vault` (webtor's persistent
storage layer): vault still resolves a `Resource` to a stored object hash, but
instead of redirecting clients to a presigned upstream URL it redirects them
here. Same path semantics (`/{bucket}/{key}`), HTTP-Range supported, opaque
to thp's `redirectFollowingTransport` which just follows the 302.

## Build & Run

```bash
# Build
go build -o server

# Run locally (requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_ENDPOINT / AWS_REGION)
./server

# Docker (scratch-based, no shell in final image)
docker build -t s3-cache .
```

There are no tests yet.

## Architecture

**Entry point:** `main.go` → `configure.go` → `services.Web.Serve()`

| File | Role |
|------|------|
| `main.go` | urfave/cli app boot, logrus formatter |
| `configure.go` | Wires `cs.Probe`, `cs.S3Client`, `Fetcher`, `Web`; builds an `*http.Client` with tight TTFB/handshake timeouts |
| `services/web.go` | HTTP server, `/{bucket}/{key}` handler, Range parsing, dispatch to single-stream or multi-range |
| `services/fetcher.go` | The core: `Head`, `Get` (single-stream pass-through), `multiRange` (chunked parallel fetch with ordered output), `fetchChunkWithRetry`, `fetchChunkOnce` |
| `services/slow_detector.go` | Reader wrapper + background sampler, fires onSlow (typically a `context.CancelFunc`) when throughput stays below floor for window |

### Request flow

```
GET /{bucket}/{key}  Range: bytes=start-end
        │
        ▼
  Parse path + Range
        │
   HEAD?  ─yes→  HeadObject → copy Content-Length / Type / ETag, 200
        │
   wantLen < SMALL_RANGE_THRESHOLD?  ─yes→  single-stream:
        │                                    one GetObject with Range header
        │                                    slow-detector wrapped reader
        │                                    io.Copy to client (no retry; can't reset response)
        │
        └→ multi-range:
              split [start..end] into chunkSize-aligned ranges
              jobs := chan int (chunk indices)
              N workers (default 8) pull jobs, call fetchChunkWithRetry
              pending[i] := chan chunkResult (one slot per chunk)
              writer loop drains pending[0..N-1] in order, flushes each chunk
```

### Slow-detector

Each S3 GET wrapped reader counts bytes. A goroutine samples every `window/4`:
- Skip until first bytes arrive (don't penalise TTFB).
- After that: per tick, compute `(delta_bytes / dt)`. If `<floor` for `4` consecutive ticks (= one full window), fire `onSlow()`.
- `onSlow` is typically a per-attempt `context.CancelFunc`. The SDK Body read then errors, fetcher recognises it via `sd.aborted()` and returns `errSlowAbort`.

### Retry policy

`fetchChunkWithRetry` retries any failed chunk up to `MAX_RETRIES` times. Both
slow-aborts and other errors (5xx, dial fail) feed the same retry path, since
on a retry we get a fresh connection that has fresh dice odds against the
provider's per-connection variance.

### Why these bounds

- `ResponseHeaderTimeout: 5s` / `DialContext.Timeout: 5s` / `TLSHandshakeTimeout: 5s` in `configure.go` — stuck connections fail fast (slow-detector only acts after first byte arrives, so TTFB-hangs need a separate ceiling).
- `CHUNK_SIZE: 4 MiB` — small enough that a single throttle event doesn't cost the whole request, large enough that per-chunk SigV4 + handshake overhead is amortised.
- `WORKERS: 8` — with ~10% per-conn variance, P(all 8 slow) ≈ 0.1^8 ≈ 0. Aggregate throughput dominated by fast workers.
- `SMALL_RANGE_THRESHOLD: 8 MiB` — HLS segments (1-10 MB) skip parallelisation. The player parallelises across segments itself.

## Configuration

All settings via CLI flags or env vars (urfave/cli). Key knobs in
`services/fetcher.go` (`RegisterFetcherFlags`). See `README.md` for the full
table.

Common-services wiring:
- `cs.RegisterProbeFlags` — `/liveness`, `/readiness` on `PROBE_PORT` (default 8081)
- `cs.RegisterS3ClientFlags` — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_REGION`, `AWS_NO_SSL`

## Dependencies

- **HTTP:** stdlib `net/http`
- **S3 SDK:** `aws/aws-sdk-go` (v1, matches the rest of webtor)
- **CLI:** `urfave/cli`
- **Logging:** `sirupsen/logrus`
- **Errors:** `pkg/errors`
- **Shared infra:** `webtor-io/common-services` (Probe, S3Client, Serve)

## Deployment

GHCR via GitHub Actions on push to `main` or version tags (`v*`). Helm chart
in `infra/helmfile/charts/s3-cache/`, values in `infra/helmfile/values/s3-cache.yaml.gotmpl`,
both symlinked into this repo at `chart/` and `s3-cache.yaml.gotmpl`.

Kubernetes shape: **DaemonSet** on `webtor.io/worker-pool` nodes,
`Service` with `internalTrafficPolicy: Local` so co-located callers (vault, thp)
always hit the local-node pod with no cross-node hop.

### Integration with vault

`vault` reads `S3_CACHE_URL`. When set, its `/webseed/{id}/{path}` handler
redirects clients to `${S3_CACHE_URL}/{bucket}/{key}` instead of presigning
upstream. Empty value falls back to the legacy direct path — rollback is a
single env unset, no code revert.

## Future phases (not in MVP)

- **Disk-backed chunk cache** with shard-by-hash distribution (mirroring the
  pattern in `torrent-web-seeder` / `content-transcoder` — see `GetDir` in
  `content-transcoder/services/common.go`). Hot chunks served from local
  NVMe at LAN latency, miss-only goes to upstream.
- **Prometheus metrics** on `:8083`: per-request fetch latency histogram,
  slow-abort counter, retry counter, range-size distribution.
- **Singleflight dedup** so concurrent identical chunk fetches share one
  upstream GET.
