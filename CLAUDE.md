# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Transparent S3 proxy for the [webtor.io](https://webtor.io) platform.

Designed as the redirect target for `vault`: vault still resolves a
`Resource` to a stored object key, but instead of redirecting clients
to a presigned upstream URL it redirects them here. Single-tenant —
the bucket is fixed per s3-cache deploy via `AWS_BUCKET`, so URLs are
`/{key}` (no bucket segment). HTTP-Range supported, opaque to thp's
`redirectFollowingTransport` which just follows the 302.

**Current state — MVP-2:**

- HEAD + GET with Range
- Aligned-chunk path for **all** requests (4 MiB granularity by default)
- Per-chunk on-disk cache (`hostPath`, `sha1(bucket+"/"+key)` sharded)
- LRU eviction (`os.Chtimes` on hit, oldest-mtime-first sweep), **per-shard**
  size cap
- Singleflight dedup of concurrent identical chunk misses
- Sequential readahead (K aligned chunks past served range)
- HTTP/1.1 forced upstream so workers each get their own TCP
- Status committed only after chunk 0 lands → clean 502 on first-chunk fail
- Prometheus metrics, pprof endpoint

**Roadmap — MVP-3 (if needed):**

- Admission filter to defeat scan pollution (cache only on 2nd miss /
  small bloom filter). Decide after observing prod hit-ratio.
- Per-shard bandwidth quota on readahead (today: global concurrency cap)

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
| `configure.go` | Wires `cs.Probe`, `cs.Prom`, `cs.Pprof`, `cs.S3Client`, `DiskCache`, `Readahead`, `Evictor`, `Fetcher`, `Web`; builds an `*http.Client` with HTTP/2 disabled and tight TTFB/handshake timeouts |
| `services/web.go` | HTTP server, `/{key}` handler, Range parsing, dispatch |
| `services/fetcher.go` | `Head`, `Get`, `serveAligned`, `fetchChunk`, `fetchUncached`, `openRange` |
| `services/cache.go` | `DiskCache.Get/Put` + sha1 shard distribution (`getDir`, `distributeByHash`) |
| `services/eviction.go` | Bg per-shard LRU sweep (Servable) |
| `services/readahead.go` | Best-effort sequential prefetch with bounded concurrency |
| `services/singleflight.go` | Minimal in-process dedup for chunk fetches |
| `services/metrics.go` | Prometheus counters / histograms / gauges (`promauto`) |

### Request flow

```
GET /{key}  Range: bytes=start-end
        │
        ▼
  parsePath + parseRange
        │
   HEAD?  ─yes→  HeadObject → copy Content-Length / Type / ETag, 200
        │
   end<0 (open range)? ─yes→  HEAD first to learn total size
        │
        ▼
   serveAligned:
     firstChunkIdx = start / chunkSize  (absolute alignment, not request-relative)
     lastChunkIdx  = end   / chunkSize
     pending[]chan, jobs<-, N workers (default 8)
     slot semaphore (workers*2) caps in-flight chunks → bounded RAM
     workers call fetchChunk(absChunkIdx * chunkSize, ...)
     wait for pending[0]:
        err → cancel + httpErrorFromS3 → return err (502)
        ok  → write {200 if no Range else 206} + headers + sliced chunk 0 bytes
     for i in 1..N-1:
        select pending[i] | ctx.Done
        err → log warn, cancel, return (connection closed)
        ok  → writeSliced + Flush, release slot
     readahead.Kick(bucket, key, lastChunkIdx+1, totalSize)
        │
        ▼
   fetchChunk(start, end, source):
     cache.Get → hit ⇒ return data (mtime touched for LRU)
                 miss ⇒ fall through, metric counted
     singleflight.Do("$key/$start"):
        openRange + ReadFull
        cache.Put (tmp file + atomic rename)
        return data
     return data
```

### Why these design choices

- **Aligned chunks for everything** — cache keys on absolute aligned
  offsets so the same (key, offset) tuple maps to the same disk file
  across requests. A request-relative split (MVP-1's `multiRange`)
  would only cache-hit on bit-exact replays. The cost is materialising
  one full chunk even for a sub-chunk-sized request, which is ~negligible
  compared to dialing upstream.
- **Status code mirrors RFC 7233** — Range request → 206 + Content-Range,
  plain GET → 200 + Content-Length (no Content-Range). Threading
  `rangeRequested` from `web.go` into `serveAligned` isn't cosmetic:
  thp's `redirectFollowingTransport` returns **403 to the client** if it
  sees a 206 on a non-Range request. Production incident 2026-05-31 —
  see `feedback_206_only_for_range_requests.md` in memory.
- **Status not committed until chunk 0 ready** — pre-header failures yield
  a clean `502 Bad Gateway`. Once we write the status (200 or 206) we're
  locked into a Content-Length, so failures on chunks 1..N can only
  abort the connection.
- **HTTP/1.1 forced** (`ForceAttemptHTTP2: false` + `TLSNextProto:
  map[]{}`) — under HTTP/2 all parallel chunk streams would share one TCP
  socket, so a slowdown would stall every worker at once and
  `ResponseHeaderTimeout` doesn't apply to H2 streams. One TCP per chunk
  gives independent congestion control.
- **TTFB / dial / TLS timeouts of 3s each** — stuck connections fail fast,
  the upstream then errors out rather than hanging the chunk forever.
- **No slow-detector, no retries** — empirically the upstream's
  rate-limit is per-source-IP. Aborting a "slow" connection and reopening
  another from the same node hits the same shaper. Cache is what
  decouples us; retry won't.
- **`CHUNK_SIZE: 4 MiB`** — small enough to amortise, large enough that
  per-chunk handshake + SigV4 overhead is negligible. **Also the cache
  granularity** — changing it after a cache exists requires draining
  the cache dirs.
- **Slot semaphore at `workers*2`** — caps how far workers can race ahead
  of the consumer. Without it, a 1 GiB file fanned out at 4 MiB chunks
  could materialise ~250 chunks in RAM before the client drains. With
  it: ≤ `workers*2 * chunkSize` transient (≈ 64 MiB at defaults).
- **Per-shard size cap, not global** — one hot key family can't starve
  evenly-distributed traffic across other shards. Sweep deletes oldest-
  mtime first; `cache.Get` does `os.Chtimes(now)` so mtime tracks
  access, not creation → genuine LRU.
- **Singleflight in-process only** — multi-pod dedup would need a
  shared lock; DaemonSet + `internalTrafficPolicy: Local` means each
  node's pod is the sole consumer of its own cache, so in-process is
  enough.

## Configuration

All settings via CLI flags or env vars (`urfave/cli`).

Common-services wiring:

- `cs.RegisterProbeFlags` — `/liveness`, `/readiness` on `PROBE_PORT` (default 8081)
- `cs.RegisterPprofFlags` — pprof endpoints on `PPROF_PORT` (default 8082, `USE_PPROF=true` default)
- `cs.RegisterPromFlags` — `/metrics` on `PROM_PORT` (default 8083, `USE_PROM=true` default)
- `cs.RegisterS3ClientFlags` — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT`, `AWS_REGION`, `AWS_NO_SSL`

Local tunables:

- Fetcher: `CHUNK_SIZE` (4 MiB), `WORKERS` (8), `AWS_BUCKET` (required — bucket is fixed per deploy)
- Cache: `CACHE_ENABLED` (off by default — chart enables), `CACHE_DIR` (`/webtor/data*` — reuses TWS shard topology), `CACHE_SHARD_SUBDIR` (`s3-cache`)
- Eviction: `EVICTION_MAX_BYTES` (10 GiB / shard), `EVICTION_INTERVAL` (1m)
- Readahead: `READAHEAD_CHUNKS` (4), `READAHEAD_CONCURRENCY` (8), `READAHEAD_TIMEOUT` (30s)

## Dependencies

- **HTTP:** stdlib `net/http`
- **S3 SDK:** `aws/aws-sdk-go` (v1, matches the rest of webtor)
- **CLI:** `urfave/cli`
- **Logging:** `sirupsen/logrus`
- **Errors:** `pkg/errors`
- **Metrics:** `prometheus/client_golang` (`promauto`)
- **Shared infra:** `webtor-io/common-services` (Probe, Prom, Pprof, S3Client, Serve)

## Deployment

GHCR via GitHub Actions on push to `main` or version tags (`v*`). Helm
chart in `infra/helmfile/charts/s3-cache/`, values in
`infra/helmfile/values/s3-cache.yaml.gotmpl`, both symlinked into this
repo at `chart/` and `s3-cache.yaml.gotmpl`.

Kubernetes shape: **DaemonSet** on `webtor.io/worker-pool` nodes,
`Service` with `internalTrafficPolicy: Local` so co-located callers
(vault, thp) always hit the local-node pod with no cross-node hop.

Cache hostPath uses the **same `/webtor` mount as `torrent-web-seeder`
and `content-transcoder`** (one disk allocation per node serves all
three). We piggyback on TWS's `/webtor/data*` shard topology and
own only the `s3-cache/` subdir inside each shard — eviction is
scoped to that subdir, so TWS torrent data sitting next to us
under `data1/` is off-limits.

### Integration with vault

`vault` reads `S3_CACHE_URL`. When set, its `/webseed/{id}/{path}` handler
redirects clients to `${S3_CACHE_URL}/{key}` instead of presigning
upstream. Empty value falls back to the legacy direct path — rollback is a
single env unset, no code revert.

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
- **`SMALL_RANGE_THRESHOLD` + `singleStream` path** (MVP-1, removed in
  MVP-2) — sub-threshold requests bypassed the multi-range fan-out. With
  caching now required to key on absolute aligned offsets, a separate
  request-relative path would defeat reuse. Unified aligned-chunk path
  costs at most one extra chunk of upstream traffic per small request
  on cold miss, and zero on hit.
- **Per-shard cap considered as "total cap"** — briefly tempted to use
  one global byte budget for simpler ops. Reverted because a single hot
  key family on one shard could then evict everything from other shards.
  Per-shard is the correct knob.
- **LFU / TinyLFU library route** — considered `ristretto` /
  `hashicorp/golang-lru`. Both are in-memory; bolting a disk store
  underneath means an in-memory index that desyncs on pod restart and a
  lot of plumbing. Stuck with disk-mtime LRU; if scan pollution shows
  up in metrics, the cheap upgrade is an admission filter, not a full
  LFU rewrite.
