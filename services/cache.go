package services

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/urfave/cli"
)

const (
	CacheDirFlag         = "cache-dir"
	CacheEnabledFlag     = "cache-enabled"
	CacheShardSubdirFlag = "cache-shard-subdir"
)

func RegisterCacheFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.BoolFlag{
			Name:   CacheEnabledFlag,
			Usage:  "enable on-disk chunk cache",
			EnvVar: "CACHE_ENABLED",
		},
		cli.StringFlag{
			Name:   CacheDirFlag,
			Usage:  "cache root; trailing /* expands to all matching shards (sha1 distributed)",
			Value:  "/webtor/data*",
			EnvVar: "CACHE_DIR",
		},
		cli.StringFlag{
			Name: CacheShardSubdirFlag,
			Usage: "subdirectory inside each shard for our chunks; isolates eviction from " +
				"sibling tenants (e.g. TWS torrent data under the same /webtor mount)",
			Value:  "s3-cache",
			EnvVar: "CACHE_SHARD_SUBDIR",
		},
	)
}

// DiskCache stores fixed-size aligned chunks on local disk. Pod-local
// (hostPath) — each DaemonSet pod owns its node's cache.
//
// Keying: sha1(bucket+"/"+key) chooses a shard via wildcard expansion of
// `location` (e.g. /cache/* → /cache/1, /cache/2, ...). Within a shard,
// chunks live at <shard>/<sha1[:2]>/<sha1>/chunk_<aligned_offset>.bin.
//
// Writes go via tmp file + rename so crash mid-write leaves either a
// complete chunk or no chunk — never a torn read.
type DiskCache struct {
	location string
	subdir   string
}

// NewDiskCache returns nil when caching is disabled; callers must treat
// (*DiskCache)(nil) as a no-op (Get/Put short-circuit on nil receiver).
func NewDiskCache(c *cli.Context) *DiskCache {
	if !c.Bool(CacheEnabledFlag) {
		return nil
	}
	return &DiskCache{
		location: c.String(CacheDirFlag),
		subdir:   c.String(CacheShardSubdirFlag),
	}
}

// CacheRoots returns the per-shard directories we own (i.e. shardDir +
// subdir). The evictor walks exactly these — never the bare shardDir —
// so sibling tenants under the same /webtor mount (TWS data, etc.)
// are off-limits to eviction.
func (c *DiskCache) CacheRoots() ([]string, error) {
	if c == nil {
		return nil, nil
	}
	shards, err := listShards(c.location)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(shards))
	for _, s := range shards {
		out = append(out, filepath.Join(s, c.subdir))
	}
	return out, nil
}

func (c *DiskCache) path(key string, alignedOffset int64) (string, error) {
	sum := sha1.Sum([]byte(key))
	h := hex.EncodeToString(sum[:])
	shard, err := getDir(c.location, h)
	if err != nil {
		return "", err
	}
	// Chunk filename = sha1(key + ":" + offset). Uniform 40-char hex,
	// no prefix/suffix — matches TWS's naming style. Offset is no
	// longer human-readable from the filename; that's fine since the
	// only callers are this process and the evictor (by mtime).
	chunkSum := sha1.Sum([]byte(key + ":" + strconv.FormatInt(alignedOffset, 10)))
	chunk := hex.EncodeToString(chunkSum[:])
	return filepath.Join(shard, c.subdir, h[:2], h, chunk), nil
}

// Get returns (data, nil) on hit, (nil, nil) on clean miss, or (nil, err)
// on I/O error. Errors are non-fatal — callers should log + fall through
// to upstream fetch.
func (c *DiskCache) Get(key string, alignedOffset int64) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	p, err := c.path(key, alignedOffset)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	// LRU touch: bump mtime to "now" on every hit so the evictor's
	// oldest-mtime-first policy is access-ordered, not creation-ordered.
	// One extra syscall per hit; cheap compared to the cache read itself.
	now := time.Now()
	_ = os.Chtimes(p, now, now)
	return data, nil
}

// Put writes data to <path> via tmp+rename. Idempotent — concurrent Puts
// for the same key race on the rename and the loser silently overwrites.
func (c *DiskCache) Put(key string, alignedOffset int64, data []byte) error {
	if c == nil {
		return nil
	}
	p, err := c.path(key, alignedOffset)
	if err != nil {
		return err
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp_chunk_*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// getDir resolves a shard for a hash under a wildcard location like /cache/*.
// Mirrors content-transcoder's GetDir so cache topology is consistent
// across the platform.
func getDir(location, hash string) (string, error) {
	if !strings.HasSuffix(location, "*") {
		return location, nil
	}
	shards, err := listShards(location)
	if err != nil {
		return "", err
	}
	if len(shards) == 0 {
		prefix := strings.TrimSuffix(location, "*")
		sh := prefix + "1"
		if err := os.MkdirAll(sh, 0755); err != nil {
			return "", err
		}
		return sh, nil
	}
	if len(shards) == 1 {
		return shards[0], nil
	}
	return distributeByHash(shards, hash)
}

// listShards enumerates directories matching the wildcard prefix.
func listShards(location string) ([]string, error) {
	prefix := strings.TrimSuffix(location, "*")
	dir, lp := path.Split(prefix)
	if dir == "" {
		dir = "."
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), lp) {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out, nil
}

// distributeByHash picks one shard from a sorted list by hashing `hash`
// into a fixed-width integer space. Same algorithm as content-transcoder
// so cache placement is reproducible across services.
func distributeByHash(dirs []string, hash string) (string, error) {
	sort.Strings(dirs)
	h := fmt.Sprintf("%x", sha1.Sum([]byte(hash)))[0:5]
	num64, err := strconv.ParseInt(h, 16, 64)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse shard hash")
	}
	num := int(num64 * 1000)
	total := 1048575 * 1000
	interval := total / len(dirs)
	for i := range dirs {
		if num < (i+1)*interval {
			return dirs[i], nil
		}
	}
	return dirs[len(dirs)-1], nil
}
