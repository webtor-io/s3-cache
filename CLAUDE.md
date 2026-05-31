# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Transparent S3 proxy for the [webtor.io](https://webtor.io) platform.

Designed as the redirect target for `vault`: vault still resolves a
`Resource` to a stored object hash, but instead of redirecting clients to a
presigned upstream URL it redirects them here. Same path semantics
(`/{bucket}/{key}`), HTTP-Range supported, opaque to thp's
`redirectFollowingTransport` which just follows the 302.

**Current state — MVP-1 (no cache yet):**

- HEAD + GET with Range
- Single-stream pass-through for ranges below threshold
- Multi-range parallel fetch for larger ranges, ordered output via per-chunk
  channels, status committed only after chunk 0 lands
- HTTP/1.1 forced upstream so workers each get their own TCP

**Roadmap — MVP-2:**

- On-disk shard cache (hostPath shards, `sha1(key) % N`, chunk files by
  aligned offset)
- Sequential readahead prefetch
- Singleflight dedup
- Prometheus metrics

The reason for the staged rollout: we empirically confirmed that the
upstream's throughput cap is **per-source-IP**, not per-connection.
Multi-range alone doesn't escape that — retries from the same node hit
the same shaper. The cache is what actually decouples us. Multi-range
remains as the architectural seam where cache hit/miss check will sit
in front of each `fetchChunk`.

## Build & Run

```bash
# Build
go build -o server

# Run (requires AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_ENDPOINT / AWS_REGION)
./server

# Docker (scratch-based)
docker build -t s3-cache .
```

No tests yet.

## Architecture

**Entry point:** `main.go` → `configure.go` → `services.Web.Serve()`

| File | Role |
|------|------|
| `main.go` | urfave/cli app boot, logrus formatter |
| `configure.go` | Wires `cs.Probe`, `cs.S3Client`, `Fetcher`, `Web`; builds an `*http.Client` with HTTP/2 disabled and tight TTFB/handshake timeouts |
| `services/web.go` | HTTP server, `/{bucket}/{key}` handler, Range parsing, dispatch |
| `services/fetcher.go` | `Head`, `Get`, `singleStream`, `multiRange`, `fetchChunk`, `openRange` |

### Request flow

```
GET /{bucket}/{key}  Range: bytes=start-end
        │
        ▼
  parsePath + parseRange
        │
   HEAD?  ─yes→  HeadObject → copy Content-Length / Type / ETag, 200
        │
   end<0 (open range)? ─yes→  HEAD first to learn total size
        │
   wantLen < SMALL_RANGE_THRESHOLD?  ─yes→  singleStream:
        │                                    one GetObject with Range
        │                                    write 206 + headers
        │                                    io.Copy to client
        │
        └→ multiRange:
              split [start..end] into chunkSize-aligned ranges
              jobs := chan int (chunk indices)
              N workers (default 8) pull jobs, call fetchChunk
              pending[i] := chan chunkResult
              wait for pending[0]:
                 err → cancel + httpErrorFromS3 → return err (502)
                 ok  → write 206 + headers + chunk 0 bytes
              for i in 1..N-1:
                 select pending[i] | ctx.Done
                 err → log warn, cancel, return (connection closed)
                 ok  → Write + Flush
```

### Why these design choices

- **Status not committed until chunk 0 ready** — pre-header failures yield
  a clean `502 Bad Gateway`. Once we write 206 we're locked into a
  Content-Length, so failures on chunks 1..N can only abort the connection.
- **HTTP/1.1 forced** (`ForceAttemptHTTP2: false` + `TLSNextProto:
  map[]{}`) — under HTTP/2 all parallel chunk streams would share one TCP
  socket, so a slowdown would stall every worker at once and
  `ResponseHeaderTimeout` doesn't apply to H2 streams. One TCP per chunk
  gives independent congestion control.
- **TTFB / dial / TLS timeouts of 3s each** — stuck connections fail fast,
  the upstream then errors out rather than hanging the chunk forever.
- **No slow-detector, no retries** — empirically the upstream's
  rate-limit is per-source-IP. Aborting a "slow" connection and reopening
  another from the same node hits the same shaper. Cache will fix this;
  retry won't.
- **`CHUNK_SIZE: 4 MiB`** — small enough to amortise, large enough that
  per-chunk handshake + SigV4 overhead is negligible.
- **`SMALL_RANGE_THRESHOLD: 8 MiB`** — HLS segments (1-10 MB) skip
  parallelisation, since the player parallelises across segments itself.

## Configuration

All settings via CLI flags or env vars (`urfave/cli`).

Common-services wiring:

- `cs.RegisterProbeFlags` — `/liveness`, `/readiness` on `PROBE_PORT` (default 8081)
- `cs.RegisterS3ClientFlags` — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_REGION`, `AWS_NO_SSL`

Fetcher tunables (see `services/fetcher.go`'s `RegisterFetcherFlags`):

- `CHUNK_SIZE` (4 MiB default)
- `WORKERS` (8 default)
- `SMALL_RANGE_THRESHOLD` (8 MiB default)

## Dependencies

- **HTTP:** stdlib `net/http`
- **S3 SDK:** `aws/aws-sdk-go` (v1, matches the rest of webtor)
- **CLI:** `urfave/cli`
- **Logging:** `sirupsen/logrus`
- **Errors:** `pkg/errors`
- **Shared infra:** `webtor-io/common-services` (Probe, S3Client, Serve)

## Deployment

GHCR via GitHub Actions on push to `main` or version tags (`v*`). Helm
chart in `infra/helmfile/charts/s3-cache/`, values in
`infra/helmfile/values/s3-cache.yaml.gotmpl`, both symlinked into this
repo at `chart/` and `s3-cache.yaml.gotmpl`.

Kubernetes shape: **DaemonSet** on `webtor.io/worker-pool` nodes,
`Service` with `internalTrafficPolicy: Local` so co-located callers
(vault, thp) always hit the local-node pod with no cross-node hop.

### Integration with vault

`vault` reads `S3_CACHE_URL`. When set, its `/webseed/{id}/{path}` handler
redirects clients to `${S3_CACHE_URL}/{bucket}/{key}` instead of presigning
upstream. Empty value falls back to the legacy direct path — rollback is a
single env unset, no code revert.

## Where to insert the cache (MVP-2)

The natural seam is `Fetcher.fetchChunk` in `services/fetcher.go`. Today
it directly calls `openRange` and reads the whole chunk into a `[]byte`.

The next iteration wants:

```go
func (f *Fetcher) fetchChunk(ctx, bucket, key string, start, end int64) ([]byte, error) {
    // 1. cache lookup by sha1(bucket+key) + aligned offset
    //    hit → mmap or read from disk shard, return
    //    miss → fall through
    // 2. singleflight: collapse concurrent identical misses to one upstream GET
    // 3. openRange + ReadFull as today
    // 4. write-back into cache (tmp file + rename, update meta)
    // 5. return bytes
}
```

The shard layout to copy from `content-transcoder`:

- Mount N hostPath volumes at `/cache/1`, `/cache/2`, ...
- `CACHE_DIR: /cache/*` (wildcard syntax `GetDir` from content-transcoder understands)
- `GetDir("/cache/*", sha1(bucketKey))` returns `/cache/N/<sha1>`
- Chunk file: `<shardDir>/<sha1[:2]>/<sha1>/chunk_<offset:0>10>.bin`

Eviction: background goroutine walks shards by mtime, deletes oldest until
total size below cap.

Sequential readahead: when a Range request arrives, check whether the
preceding chunks are recent in cache. If so, prefetch the next K aligned
chunks in the background.

## What was tried and rolled back

For posterity (and to not relitigate):

- **Slow-detector** (`services/slow_detector.go`, removed) — counted
  bytes/sec on each chunk's body reader, aborted reads below a floor and
  triggered a retry. Worked correctly (verified via deployed logs) but
  delivered nothing because the upstream throttle is per-source-IP: every
  retry from the node hit the same shaper. After max-retries the request
  would tear off mid-response, which was worse than just letting it ride
  out slowly.
- **HTTP/2 to upstream** (`ForceAttemptHTTP2: true`, reverted) — all
  parallel chunks shared one TCP via H2 streams, so a single upstream
  slowdown stalled every worker. Per-stream timeouts don't apply the same
  way as `ResponseHeaderTimeout` on HTTP/1.1.
