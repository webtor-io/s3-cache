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

```bash
# Tests
go test ./services/
```

## Architecture

**Entry point:** `main.go` → `configure.go` → `services.Web.Serve()`

| File | Role |
|------|------|
| `main.go` | urfave/cli app boot, logrus formatter |
| `configure.go` | Wires `cs.Probe`, `cs.Prom`, `cs.Pprof`, `cs.S3Client`, `DiskCache`, `Readahead`, `Evictor`, `Fetcher`, `Web`; builds an `*http.Client` with HTTP/2 disabled and tight TTFB/handshake timeouts |
| `services/web.go` | HTTP server, `/{key}` handler, Range parsing, dispatch |
| `services/fetcher.go` | `Head`, `Get`, `serveAligned` (in-order chunk dispatcher), `fetchChunk`, `lead` (detached upstream fetch + cache write), `download`, `openRange` |
| `services/chunk.go` | `chunkFlight` (shared fetch outcome, refcounted), `chunkResult` (per-consumer handle / section / buffer), `writeSlice` |
| `services/budget.go` | `chunkBudget` — global FIFO cap on chunk buffers in memory (`FETCH_CONCURRENCY` chunks) |
| `services/cache.go` | `DiskCache.Get/Put` + sha1 shard distribution (`getDir`, `distributeByHash`) |
| `services/eviction.go` | Bg per-shard LRU sweep (Servable) |
| `services/readahead.go` | Best-effort sequential prefetch with bounded concurrency |
| `services/singleflight.go` | In-process dedup for chunk fetches; publishes the result with one reference per caller |
| `services/metrics.go` | Prometheus counters / histograms / gauges (`promauto`) |

### Request flow

```
GET /{key}  Range: bytes=start-end | bytes=start- | bytes=-N (suffix)
        │
        ▼
  parsePath + parseRange (suffix form resolved later against object size)
        │
   HEAD?  ─yes→  headObject (TTL cache) → copy Content-Length / Type / ETag / Last-Modified, 200
        │
        ▼
   headObject ALWAYS runs (TTL cache, default 60s; stale entry served as
   fallback if the upstream HEAD fails — cached content stays servable
   through upstream outages):
     resolve suffix → absolute range, clamp end to size-1,
     start >= size → 416 + "Content-Range: bytes */size"
     validators (ETag/Last-Modified/normalized Content-Type) set on
     success paths only, stripped before error statuses
        │
        ▼
   serveAligned:
     firstChunkIdx = start / chunkSize  (absolute alignment, not request-relative)
     lastChunkIdx  = end   / chunkSize
     dispatcher, chunks strictly in order:
        take a slot (window = workers, min 4; freed when the chunk is written)
        re-check ctx (no new work post-abort)
        go fetchChunk(absChunkIdx * chunkSize, ..., admit) → pending[idx]
        wait until that chunk is ADMITTED (hit, or its fetch holds a budget
        unit) before starting the next one
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
   fetchChunk(start, end, source, admit):
     cache.Get → hit ⇒ admit, return own *os.File (mtime touched for LRU)
                 miss ⇒ fall through, metric counted
     singleflight.join("$key/$start"); the leader spawns lead():
        detached ctx (CHUNK_FETCH_TIMEOUT, default 90s) — NOT the request ctx
        acquire a budget unit (global FIFO, FETCH_CONCURRENCY units) → admitted
        openRange + ReadFull into a budgeted buffer (short read = error, never cached)
        cache.Put (tmp file + atomic rename; returns the still-open handle)
          ok   ⇒ drop the buffer, release the unit; result = path + pinned handle
          fail ⇒ result = the buffer; unit released when the last reader is done
        finish: result published with refs = number of callers that joined
     claim goroutine (outlives the caller): per caller —
        cached ⇒ os.Open(path) own handle (sendfile), drop ref
                 open failed (evicted) ⇒ SectionReader over the pinned handle
        buffer ⇒ share it, ref dropped by chunkResult.close()
        hand over to the caller, or close it here if the caller is gone
     select { admitted | ctx.Done }, then { result | ctx.Done } — the wait is
     cancellable; an aborted caller returns immediately while the fetch
     finishes in the background and warms the cache for the next retry
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
- **Detached chunk fetches (do NOT re-attach them to the request ctx)** —
  the S3 fetch inside singleflight runs under its own `CHUNK_FETCH_TIMEOUT`
  context, not the request's. Impatient players abort slow tail fetches
  within ~1s and retry; with request-ctx fetches the abort cancelled the
  fetch, the chunk never reached the cache, and every retry started cold —
  under upstream per-connection throttling that looped until the player
  gave up (the 2026-07 credits-drop storms). The caller's WAIT on the
  result stays cancellable (select on ctx), so aborted requests return
  immediately while the orphaned fetch completes, caches, and warms the
  next retry. Buildup is bounded by the chunk-buffer budget
  (`FETCH_CONCURRENCY`).
- **Short chunk reads are errors, never cached** — the requested span is
  always clamped to the object end, so a short body means upstream failure;
  caching it would poison every response over that offset until eviction.
- **HeadObject through a TTL cache with stale fallback** — object metadata
  is immutable (content-addressed keys), so HEADs are cached
  (`HEAD_CACHE_TTL`); when a HEAD fails and a stale entry exists it is
  served instead, keeping fully-disk-cached content servable through
  upstream outages.
- **Validators on success responses only** — ETag/Last-Modified/normalized
  Content-Type are emitted for players' resume heuristics (If-Range /
  restart-from-zero avoidance) and stripped before error statuses.
- **No slow-detector, no retries** — empirically the upstream's
  rate-limit is per-source-IP. Aborting a "slow" connection and reopening
  another from the same node hits the same shaper. Cache is what
  decouples us; retry won't.
- **`CHUNK_SIZE: 4 MiB`** — small enough to amortise, large enough that
  per-chunk handshake + SigV4 overhead is negligible. **Also the cache
  granularity** — changing it after a cache exists requires draining
  the cache dirs.
- **Cache misses are served from the cache file, not from memory** — after
  a successful `Put` the download buffer is dropped and every caller opens
  its own handle on the file, exactly like a hit. Production incident
  2026-09-28 (worker62): misses kept each 4 MiB chunk as `[]byte` until the
  consumer drained it, up to a window of 8 chunks per request; ~70 slow
  downloads (5 Mbit/s multi-connection downloaders, vault tar archives)
  held 1.5 GB of heap (98.9% in the fetch path), GC thrashed at the 2-core
  limit, 1 s probes timed out, 4 kills incl. one OOMKill. A local replay
  (70 clients at 5 Mbit/s, 4 MiB chunks, FETCH_CONCURRENCY 32): live heap
  1690 MiB before, 13 MiB after.
- **Own handle per consumer, pinned handle as fallback** — Linux sendfile
  reads from and advances the descriptor's own file offset, so a shared
  (or dup'ed) descriptor can't serve concurrent consumers. `Put` returns
  the handle it wrote through; it keeps the inode alive if the evictor
  unlinks the path before a consumer's `os.Open` — that consumer reads the
  pinned handle through a `SectionReader` (pread; no sendfile, no heap).
- **Global chunk-buffer budget (`FETCH_CONCURRENCY` chunks)** — every chunk
  buffer, download in progress or uncached chunk awaiting its consumer,
  takes a unit; heap in chunk buffers ≤ `FETCH_CONCURRENCY × CHUNK_SIZE`
  (128 MiB at defaults) regardless of the number of clients. It replaced
  `fetchSem`, which counted downloads only. Waiters are FIFO (Go channel
  send queue), no barging.
- **Budget admission in chunk order per request** — the dispatcher starts
  chunk i+1 only once chunk i is admitted. Otherwise later chunks of a
  request can hold the whole budget (buffers only its own consumer
  releases) while its head chunk queues for it; in the test with a
  2-chunk budget and 24 requests that deadlocked every request until the
  fetch timeout (24 × 502). With the ordering, whoever holds budget has
  every earlier chunk in hand or in progress, so its consumer advances.
- **Trade-off of the budget when chunks can't be cached** (cache disabled,
  `Put` failing, e.g. a full disk): buffers wait for their consumers, so
  clients that stop reading (paused players behind thp) hold units — up to
  the window (8) each; 4 stalled clients take the default budget and other
  misses queue (FIFO) until someone reads or `CHUNK_FETCH_TIMEOUT` → 502.
  Hits and cached misses are unaffected. Before, the same situation grew
  the heap without bound. Watch `s3cache_chunk_serves_total{via="buffer"}`
  and `s3cache_chunk_budget_waits_total`.
- **Slot window = `workers` (min 4)** — caps how far fetches run ahead of
  the consumer (file handles, and buffers when uncached) per request.
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

- Fetcher: `CHUNK_SIZE` (4 MiB), `WORKERS` (8), `AWS_BUCKET` (required — bucket is fixed per deploy), `FETCH_CONCURRENCY` (32, process-wide cap on chunk buffers in memory — downloads in progress plus uncached chunks awaiting consumers; heap bound = this × `CHUNK_SIZE`), `CHUNK_FETCH_TIMEOUT` (90s, detached fetch deadline), `HEAD_CACHE_TTL` (60s)
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
