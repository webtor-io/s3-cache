package services

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metric labels intentionally avoid bucket/key — high-cardinality on a
// torrent platform — and stick to coarse states (hit/miss, source, etc.).

var (
	cacheLookups = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3cache_cache_lookups_total",
		Help: "Cache lookup outcomes per chunk.",
	}, []string{"result"}) // hit | miss | error

	cacheWrites = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3cache_cache_writes_total",
		Help: "Cache write outcomes per chunk.",
	}, []string{"result"}) // ok | error

	cacheBytesServed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3cache_cache_bytes_served_total",
		Help: "Bytes served from cache (chunk granularity).",
	})

	upstreamBytesFetched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3cache_upstream_bytes_fetched_total",
		Help: "Bytes fetched from upstream S3 (excluding readahead — counted separately).",
	})

	upstreamChunkDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "s3cache_upstream_chunk_seconds",
		Help:    "Upstream chunk fetch duration (full body read).",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12), // 10ms .. ~40s
	}, []string{"source"}) // foreground | readahead

	singleflightShared = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3cache_singleflight_shared_total",
		Help: "Chunk fetches that joined an in-flight fetch instead of dialing upstream.",
	})

	readaheadKicks = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3cache_readahead_kicks_total",
		Help: "Readahead outcomes.",
	}, []string{"result"}) // scheduled | dropped | already_cached

	requestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "s3cache_requests_total",
		Help: "HTTP requests by method and outcome class.",
	}, []string{"method", "status"})

	evictionRuns = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3cache_eviction_runs_total",
		Help: "Eviction sweeps completed.",
	})

	evictionBytesFreed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "s3cache_eviction_bytes_freed_total",
		Help: "Bytes deleted by the evictor.",
	})

	cacheSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "s3cache_shard_bytes",
		Help: "Current cache size per shard (as observed by the last eviction sweep).",
	}, []string{"shard"})
)
