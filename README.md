# s3-cache

Transparent S3 proxy for the [webtor.io](https://webtor.io) platform. Sits
between vault and S3-compatible object storage, signing requests itself
(SigV4), fan-out-fetching large range requests across parallel HTTP/1.1
connections, and caching aligned 4 MiB chunks on local NVMe so per-source-IP
upstream rate limits stop dominating cold traffic.

## Why this exists, plainly

Two problems we want to solve:

1. **Per-source-IP rate caps on object storage.** Some providers throttle
   bursty single-IP traffic via a token bucket. A single TCP stream from one
   node sees the full effect; once depleted, requests crawl until the bucket
   refills. **Multi-range alone doesn't fix this** — opening more connections
   from the same IP shares the same shaper. The proper fix is a local cache
   so we stop hitting the upstream for hot ranges.
2. **TLS / SigV4 / presigning overhead in vault.** vault currently mints a
   presigned URL per webseed request. Pushing signing into a dedicated
   service lets vault stay stateless about S3 specifics.

MVP-2 delivers both: chunked multi-range upstream fetch + on-disk LRU cache
+ sequential readahead + singleflight dedup + Prometheus metrics.

## Architecture

```
client → vault (302 with stable URL) → s3-cache → object storage
                                          │
                                          ├─ HEAD pass-through
                                          └─ GET  (aligned chunks, 4 MiB)
                                               ├─ disk cache lookup
                                               │    hit → serve from /webtor/s3-cache*
                                               │    miss → singleflight → upstream
                                               ├─ N=8 workers per request (own TCP each)
                                               ├─ wait for chunk 0 → 206
                                               ├─ stream chunks 1..N in order
                                               └─ kick readahead (K aligned chunks)
```

Calling convention: `GET /{key}` with `Range: bytes=start-end`. HEAD
returns metadata. The bucket is fixed per-deploy via `AWS_BUCKET` —
single-tenant, so URLs don't carry it.

Every request goes through the **same aligned-chunk path**, including
sub-chunk-sized HLS-segment reads — the cache keys on *absolute* aligned
offsets, so request-relative offsets would defeat reuse across requests.
For a < 4 MiB request this still amounts to one upstream GET that gets
sliced down to the requested span.

The status code is **only committed after chunk 0 lands**, so any upstream
failure on the first chunk yields a clean `502 Bad Gateway` instead of a
torn-off mid-response.

Workers force HTTP/1.1 (`ForceAttemptHTTP2: false` + empty `TLSNextProto`)
so each chunk gets an independent TCP connection — under HTTP/2 they'd all
share one socket and stall together.

There are no slow-detector aborts and no per-chunk retries. We tried them;
empirically the upstream's throttle is per-source-IP, so retries from the
same node hit the same shaper. The cache is what decouples us.

## Disk cache

- **Pod-local** (`hostPath` on the DaemonSet, same `/webtor` mount as
  `torrent-web-seeder` and `content-transcoder` so one disk allocation
  per node serves all three).
- **Shard topology piggybacks on TWS** — we read `/webtor/data*` to
  pick a shard (same wildcard convention as content-transcoder's
  `GetDir`), then own `<shard>/s3-cache/` for our own chunks. This
  way s3-cache automatically follows whatever multi-disk layout the
  node admin gave TWS, and eviction stays scoped to our subdir so
  torrent data sitting next to us under `data1/` is never touched.
- **Chunk file** at `/webtor/dataN/s3-cache/<keyHash[:2]>/<keyHash>/<chunkHash>`
  where `keyHash = sha1(key)` (bucket fixed per deploy) and
  `chunkHash = sha1(key + ":" + offset)`. Uniform 40-char hex names,
  no prefix/suffix — matches TWS's naming style. Atomic writes via
  tmp file + rename — never serves a torn chunk.
- **LRU eviction**, **per-shard** size cap. `os.Chtimes` bumps mtime on
  every cache hit, and the evictor sweeps oldest-mtime files until each
  shard is below ~90% of the cap. Per-shard (not global) so one hot key
  family can't starve evenly-distributed traffic.
- **Singleflight dedup** — concurrent identical chunk misses collapse to
  one upstream GET. Typical for HLS where N viewers want the same
  segment simultaneously.
- **Sequential readahead** — after the served range, prefetch K aligned
  chunks past the tail. Best-effort: drops kicks when the readahead
  worker pool is saturated, never blocks foreground.

Cache content is assumed immutable per key (typical for webtor storage).
There is no ETag invalidation — bust the cache by deleting the shard
files or by changing the key.

## Build & run

```bash
go build -o server
./server
```

Local probe:

```bash
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_ENDPOINT=https://... \
AWS_REGION=... \
AWS_BUCKET=webtor-vault \
CACHE_ENABLED=true \
CACHE_DIR=/tmp/s3-cache/* \
./server

curl -I http://localhost:8080/<key>
curl -r 0-52428800 http://localhost:8080/<key> -o /dev/null
```

## Configuration

All settings via env vars (or matching CLI flags).

| Env | Default | Purpose |
|---|---|---|
| `WEB_PORT` | 8080 | HTTP listen port |
| `PROBE_PORT` | 8081 | Liveness/readiness port |
| `PPROF_PORT` | 8082 | pprof `/debug/pprof`-equivalent endpoints (`USE_PPROF=true` default) |
| `PROM_PORT` | 8083 | Prometheus `/metrics` port |
| `AWS_ACCESS_KEY_ID` | — | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | — | S3 secret key |
| `AWS_ENDPOINT` | — | S3-compatible endpoint |
| `AWS_REGION` | — | S3 region |
| `AWS_NO_SSL` | `false` | Disable TLS to upstream |
| `AWS_BUCKET` | — | S3 bucket (required; single-tenant — same bucket for every request) |
| `CHUNK_SIZE` | 4194304 (4 MiB) | Chunk granularity (also cache granularity) |
| `WORKERS` | 8 | Concurrent S3 fetches per request |
| `CACHE_ENABLED` | `false` | Enable on-disk chunk cache |
| `CACHE_DIR` | `/webtor/data*` | Cache shard roots (wildcard); piggybacks on TWS shards |
| `CACHE_SHARD_SUBDIR` | `s3-cache` | Subdirectory inside each shard we own |
| `EVICTION_MAX_BYTES` | 10737418240 (10 GiB) | Per-shard size cap (0 disables) |
| `EVICTION_INTERVAL` | `1m` | Eviction sweep interval |
| `READAHEAD_CHUNKS` | 4 | Chunks to prefetch past served range (0 disables) |
| `READAHEAD_CONCURRENCY` | 8 | Max concurrent readahead fetches (process-wide) |
| `READAHEAD_TIMEOUT` | `30s` | Per-chunk readahead timeout |

## Metrics

Scrape `:8083/metrics`. Key series:

- `s3cache_cache_lookups_total{result="hit|miss|error"}`
- `s3cache_cache_writes_total{result="ok|error"}`
- `s3cache_cache_bytes_served_total` — bytes served from disk
- `s3cache_upstream_bytes_fetched_total`
- `s3cache_upstream_chunk_seconds{source="foreground|readahead"}` (histogram)
- `s3cache_singleflight_shared_total` — fetches that joined an in-flight call
- `s3cache_readahead_kicks_total{result="scheduled|dropped|already_cached"}`
- `s3cache_eviction_runs_total`, `s3cache_eviction_bytes_freed_total`
- `s3cache_shard_bytes{shard="..."}` — current shard size (gauge)
- `s3cache_requests_total{method,status}`

Hit ratio: `rate(s3cache_cache_lookups_total{result="hit"}[5m]) /
ignoring(result) sum(rate(s3cache_cache_lookups_total[5m]))`.

## Deployment

Docker multi-stage build (scratch base). Published to GHCR via GitHub
Actions on push to `main`. Helm chart lives in `infra/helmfile/` and is
symlinked at `chart/`.

Deployed as a **DaemonSet** on the worker pool with
`internalTrafficPolicy: Local` so vault/thp on each node always hit the
local s3-cache pod, no cross-node hops. The chart mounts `/webtor` from
the host (same convention as TWS / content-transcoder).

## Integration with vault

When `S3_CACHE_URL` is set in vault, its `/webseed/{id}/{path}` handler
redirects clients to `${S3_CACHE_URL}/{key}` instead of generating
a presigned upstream URL. Empty env disables — falls back to direct
presigned upstream, so rollback is a single env unset.
