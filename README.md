# s3-cache

Transparent S3 proxy for the [webtor.io](https://webtor.io) platform. Sits
between vault and S3-compatible object storage, signing requests itself
(SigV4) and fan-out-fetching large range requests across parallel HTTP/1.1
connections.

Currently a stateless proxy; on-disk chunk caching is the next phase.

## Why this exists, plainly

Two problems we want to solve eventually:

1. **Per-source-IP rate caps on object storage.** Some providers throttle
   bursty single-IP traffic via a token bucket. A single TCP stream from one
   node sees the full effect; once depleted, requests crawl until the bucket
   refills. **Multi-range alone doesn't fix this** — opening more connections
   from the same IP shares the same shaper. The proper fix is a local cache
   so we stop hitting the upstream for hot ranges.
2. **TLS / SigV4 / presigning overhead in vault.** vault currently mints a
   presigned URL per webseed request. Pushing signing into a dedicated
   service lets vault stay stateless about S3 specifics.

This MVP delivers (2) and lays the seam for (1). The cache itself is the
next milestone.

## Architecture

```
client → vault (302 with stable URL) → s3-cache → object storage
                                          │
                                          ├─ HEAD pass-through
                                          ├─ Range < 8 MiB: single-stream
                                          └─ Range ≥ 8 MiB: chunked multi-range
                                               ├─ split into 4 MiB chunks
                                               ├─ N=8 workers (own TCP each)
                                               ├─ wait for chunk 0 → 206
                                               └─ stream chunks 1..N in order
```

Calling convention is S3-compatible: `GET /{bucket}/{key}` with
`Range: bytes=start-end`. HEAD returns metadata.

The status code is **only committed after chunk 0 lands**, so any upstream
failure on the first chunk yields a clean `502 Bad Gateway` instead of a
torn-off mid-response.

Workers force HTTP/1.1 (`ForceAttemptHTTP2: false` + empty `TLSNextProto`)
so each chunk gets an independent TCP connection — under HTTP/2 they'd all
share one socket and stall together.

There are no slow-detector aborts and no per-chunk retries. We tried them;
empirically the upstream's throttle is per-source-IP, so retries from the
same node hit the same shaper. The plan is a local cache, not smarter
retry logic.

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
./server

curl -I http://localhost:8080/<bucket>/<key>
curl -r 0-52428800 http://localhost:8080/<bucket>/<key> -o /dev/null
```

## Configuration

All settings via env vars (or matching CLI flags).

| Env | Default | Purpose |
|---|---|---|
| `WEB_PORT` | 8080 | HTTP listen port |
| `AWS_ACCESS_KEY_ID` | — | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | — | S3 secret key |
| `AWS_ENDPOINT` | — | S3-compatible endpoint |
| `AWS_REGION` | — | S3 region |
| `AWS_NO_SSL` | `false` | Disable TLS to upstream |
| `CHUNK_SIZE` | 4194304 (4 MiB) | Bytes per chunk in multi-range split |
| `WORKERS` | 8 | Concurrent S3 fetches per request |
| `SMALL_RANGE_THRESHOLD` | 8388608 (8 MiB) | Below this, single-stream pass-through |

## Deployment

Docker multi-stage build (scratch base). Published to GHCR via GitHub
Actions on push to `main`. Helm chart lives in `infra/helmfile/` and is
symlinked at `chart/`.

Deployed as a **DaemonSet** on the worker pool with
`internalTrafficPolicy: Local` so vault/thp on each node always hit the
local s3-cache pod, no cross-node hops.

## Integration with vault

When `S3_CACHE_URL` is set in vault, its `/webseed/{id}/{path}` handler
redirects clients to `${S3_CACHE_URL}/{bucket}/{key}` instead of generating
a presigned upstream URL. Empty env disables — falls back to direct
presigned upstream, so rollback is a single env unset.

## Roadmap

- **On-disk chunk cache with shard-by-hash distribution** (mirroring the
  pattern in `torrent-web-seeder` and `content-transcoder` —
  see `GetDir` in `content-transcoder/services/common.go`). Mount N
  hostPath volumes at `/cache/1`, `/cache/2`, etc; pick a shard via
  `sha1(s3_path) % N`. Stores 4 MiB chunks as files named by aligned
  offset. Hot chunks served from local NVMe; cold chunks fall through to
  upstream + write-back to cache. Eviction by LRU mtime.
- **Sequential readahead.** When a client makes a Range request that looks
  like sequential playback or download, prefetch the next several aligned
  chunks into cache in the background so subsequent requests are warm.
- **Singleflight dedup.** Concurrent identical chunk fetches collapse to a
  single upstream GET (typical for HLS where N viewers want the same
  segment simultaneously).
- **Prometheus metrics:** hit/miss ratio, fetch latency histogram, range
  size distribution, per-shard disk usage.
