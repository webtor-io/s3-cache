# s3-cache

Transparent S3 proxy for the [webtor.io](https://webtor.io) platform. Replaces
direct presigned-URL redirects from vault with a multi-range parallel fetcher
that improves single-connection throughput against S3-compatible object stores.

## Why

A single TCP connection to object storage doesn't always reach line rate —
per-connection rate variance is observed regardless of source IP or path. A
single-stream download is then bottlenecked by whichever connection is slow.

This proxy splits each request into chunks fetched in parallel — if one chunk
is slow, the other workers keep aggregate throughput high. A slow-detector
aborts visibly-stalled connections within seconds and retries on a fresh one.

In local benchmarks the proxy delivers **~2.3× the throughput** of single-stream
direct S3 with roughly **4× lower variance** across 30 sequential 80 MB pulls.

## Architecture

```
client → vault (302 with stable URL) → s3-cache → object storage
                                          │
                                          ├─ split range into 4 MB chunks
                                          ├─ N=8 parallel workers fetch via SigV4
                                          ├─ slow-detector aborts <5 MB/s sustained 2s
                                          ├─ retry up to 3× on fresh connection
                                          └─ ordered stream back to client
```

URLs are S3-compatible: `GET /{bucket}/{key}` with `Range: bytes=start-end`.
HEAD returns metadata. Small ranges (< 8 MB) pass through single-stream — no
parallelisation overhead for HLS segments.

## Build & run

```bash
go build -o server
./server
```

Run locally:

```bash
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_ENDPOINT=https://... \
AWS_REGION=... \
./server
```

Then probe:

```bash
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
| `SLOW_BPS` | 5242880 (5 MiB/s) | Per-conn bytes/sec floor before abort |
| `SLOW_WINDOW_MS` | 2000 | Window sustained below floor before abort fires |
| `MAX_RETRIES` | 3 | Per-chunk retries on slow/error |

## Deployment

Docker multi-stage build (scratch base, no shell). Published to GHCR via
GitHub Actions on push to `main`. Helm chart lives in `infra/helmfile/`
and is symlinked at `chart/`.

Deployed as a DaemonSet on the worker pool with `internalTrafficPolicy: Local`
so callers on each node always hit the s3-cache on the same node, no
cross-node hops.

## Integration with vault

When `S3_CACHE_URL` is set in vault, its `/webseed/{id}/{path}` handler
redirects clients to `${S3_CACHE_URL}/{bucket}/{key}` instead of generating a
presigned upstream URL. Empty env disables — falls back to direct presigned
upstream, making rollback a single env unset.
