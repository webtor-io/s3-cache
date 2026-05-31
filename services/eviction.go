package services

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	EvictionMaxBytesPerShardFlag = "eviction-max-bytes"
	EvictionIntervalFlag         = "eviction-interval"
)

func RegisterEvictionFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.Int64Flag{
			Name:   EvictionMaxBytesPerShardFlag,
			Usage:  "per-shard size cap in bytes (0 disables eviction)",
			Value:  10 << 30, // 10 GiB / shard default
			EnvVar: "EVICTION_MAX_BYTES",
		},
		cli.DurationFlag{
			Name:   EvictionIntervalFlag,
			Usage:  "eviction sweep interval",
			Value:  1 * time.Minute,
			EnvVar: "EVICTION_INTERVAL",
		},
	)
}

// Evictor walks each shard, sums file sizes, and if a shard is over its
// per-shard cap deletes oldest-mtime files until it's back under. Runs
// as a Servable so it gets the same signal-handling as the HTTP servers.
//
// We deliberately keep per-shard caps (not a global cap) so a single
// hot key family on one shard can't starve evenly-distributed traffic.
//
// Tombstone files left behind by interrupted Puts (".tmp_chunk_*") are
// swept by the same pass — anything older than tmpMaxAge gets removed
// regardless of cap status.
type Evictor struct {
	cache    *DiskCache
	maxBytes int64
	interval time.Duration
}

const tmpMaxAge = 10 * time.Minute

func NewEvictor(c *cli.Context, cache *DiskCache) *Evictor {
	if cache == nil {
		return nil
	}
	maxBytes := c.Int64(EvictionMaxBytesPerShardFlag)
	if maxBytes <= 0 {
		return nil
	}
	return &Evictor{
		cache:    cache,
		maxBytes: maxBytes,
		interval: c.Duration(EvictionIntervalFlag),
	}
}

func (e *Evictor) Serve() error {
	log.WithFields(log.Fields{
		"max_bytes_per_shard": e.maxBytes,
		"interval":            e.interval,
		"location":            e.cache.Location(),
	}).Info("starting cache evictor")
	// First sweep runs immediately so an over-cap cache from a previous
	// pod incarnation gets trimmed before serving heats up.
	e.runOnce()
	t := time.NewTicker(e.interval)
	defer t.Stop()
	for range t.C {
		e.runOnce()
	}
	return nil
}

func (e *Evictor) Close() {}

func (e *Evictor) runOnce() {
	shards, err := listShards(e.cache.Location())
	if err != nil {
		log.WithError(err).Warn("eviction: list shards failed")
		return
	}
	if len(shards) == 0 {
		// /cache/*  with no provisioned dirs — nothing to evict.
		return
	}
	for _, sh := range shards {
		e.sweepShard(sh)
	}
	evictionRuns.Inc()
}

type fileEntry struct {
	path  string
	size  int64
	mtime time.Time
}

func (e *Evictor) sweepShard(shard string) {
	var entries []fileEntry
	var total int64
	now := time.Now()

	err := filepath.WalkDir(shard, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		name := d.Name()
		// Stale tmp file from a crashed Put: nuke unconditionally.
		if len(name) > 5 && name[:5] == ".tmp_" {
			if now.Sub(info.ModTime()) > tmpMaxAge {
				_ = os.Remove(p)
			}
			return nil
		}
		total += info.Size()
		entries = append(entries, fileEntry{p, info.Size(), info.ModTime()})
		return nil
	})
	if err != nil {
		log.WithError(err).WithField("shard", shard).Warn("eviction: walk failed")
		return
	}

	cacheSize.WithLabelValues(shard).Set(float64(total))

	if total <= e.maxBytes {
		return
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].mtime.Before(entries[j].mtime) })
	target := e.maxBytes - e.maxBytes/10 // shave to 90% of cap to avoid thrash
	var freed int64
	for _, en := range entries {
		if total <= target {
			break
		}
		if err := os.Remove(en.path); err != nil {
			continue
		}
		total -= en.size
		freed += en.size
		// Tidy: try removing parent dirs if empty.
		removeIfEmpty(filepath.Dir(en.path))
	}
	if freed > 0 {
		evictionBytesFreed.Add(float64(freed))
		log.WithFields(log.Fields{
			"shard": shard, "freed_bytes": freed, "size_after": total,
		}).Info("eviction sweep complete")
	}
	cacheSize.WithLabelValues(shard).Set(float64(total))
}

// removeIfEmpty walks up at most two levels (the <sha1[:2]>/<sha1>
// hierarchy) deleting empty dirs. Best-effort: stops on any error.
func removeIfEmpty(dir string) {
	for i := 0; i < 2; i++ {
		entries, err := os.ReadDir(dir)
		if err != nil || len(entries) > 0 {
			return
		}
		if err := os.Remove(dir); err != nil {
			return
		}
		dir = filepath.Dir(dir)
	}
}
