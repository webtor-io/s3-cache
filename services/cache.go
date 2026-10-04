package services

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	mu    sync.Mutex
	since map[string]time.Time // shard root → versionMarker time
}

// objVersion identifies the bytes of an object. Keys are not immutable: vault
// re-uploads a corrupt file under the same key, so a chunk is named by the
// object's ETag as well, and a re-uploaded object never hits its old bytes.
type objVersion struct {
	etag     string
	modified time.Time
}

// versionMarker, in each shard root, records when that shard started naming
// chunks by version. Chunks from before carry none: they are trusted only for
// objects last modified before then (unchanged since they were cached), which
// keeps the existing cache warm. The evictor never removes it.
const versionMarker = ".versioned-since"

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

// root is the shard root (shard dir + subdir) that holds key's chunks.
func (c *DiskCache) root(key string) (string, string, error) {
	sum := sha1.Sum([]byte(key))
	h := hex.EncodeToString(sum[:])
	shard, err := getDir(c.location, h)
	if err != nil {
		return "", "", err
	}
	return filepath.Join(shard, c.subdir), h, nil
}

// path is a chunk's file. Filename = sha1(key + ":" + offset [+ ":" + etag]).
// Uniform 40-char hex, no prefix/suffix — matches TWS's naming style. An
// empty etag gives the pre-versioning name.
func (c *DiskCache) path(key string, v objVersion, alignedOffset int64) (string, error) {
	root, h, err := c.root(key)
	if err != nil {
		return "", err
	}
	name := key + ":" + strconv.FormatInt(alignedOffset, 10)
	if v.etag != "" {
		name += ":" + v.etag
	}
	chunkSum := sha1.Sum([]byte(name))
	return filepath.Join(root, h[:2], h, hex.EncodeToString(chunkSum[:])), nil
}

// versionedSince returns (creating it on first use) the versionMarker time of
// key's shard root.
func (c *DiskCache) versionedSince(key string) (time.Time, error) {
	root, _, err := c.root(key)
	if err != nil {
		return time.Time{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t, ok := c.since[root]; ok {
		return t, nil
	}
	marker := filepath.Join(root, versionMarker)
	b, err := os.ReadFile(marker)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(root, 0755); err != nil {
			return time.Time{}, err
		}
		b = []byte(time.Now().UTC().Format(time.RFC3339Nano))
		tmp := marker + ".new"
		if err := os.WriteFile(tmp, b, 0644); err != nil {
			return time.Time{}, err
		}
		if err := os.Rename(tmp, marker); err != nil {
			return time.Time{}, err
		}
	} else if err != nil {
		return time.Time{}, err
	}
	t, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b)))
	if err != nil {
		return time.Time{}, err
	}
	if c.since == nil {
		c.since = map[string]time.Time{}
	}
	c.since[root] = t
	return t, nil
}

// Get returns an open *os.File handle + size on hit, (nil, 0, nil) on
// clean miss, or (nil, 0, err) on I/O error. The caller MUST Close the
// file when done. We deliberately return the handle (not bytes) so the
// hit-path can stream via io.CopyN instead of materialising a 4 MiB
// buffer per chunk in RAM — under load that allocation was the
// dominant heap pressure in pprof.
func (c *DiskCache) Get(key string, v objVersion, alignedOffset int64) (*os.File, int64, error) {
	if c == nil {
		return nil, 0, nil
	}
	p, err := c.path(key, v, alignedOffset)
	if err != nil {
		return nil, 0, err
	}
	f, err := os.Open(p)
	if os.IsNotExist(err) && v.etag != "" {
		// A chunk cached before versioning: valid only if the object has
		// not changed since this shard started versioning. A shard that
		// cannot get its marker cannot write chunks either (read-only),
		// so all it holds predates versioning: trusted, as before.
		since, sErr := c.versionedSince(key)
		if sErr != nil || v.modified.IsZero() || v.modified.Before(since) {
			if p, err = c.path(key, objVersion{}, alignedOffset); err != nil {
				return nil, 0, err
			}
			f, err = os.Open(p)
		}
	}
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, nil
		}
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	// LRU touch: bump mtime to "now" on every hit so the evictor's
	// oldest-mtime-first policy is access-ordered, not creation-ordered.
	// One extra syscall per hit; cheap compared to the read itself.
	now := time.Now()
	_ = os.Chtimes(p, now, now)
	return f, info.Size(), nil
}

// Put writes data to the chunk's path via tmp+rename and returns an open
// handle on the written file plus its path. The handle pins the inode: if
// the evictor unlinks the path right after the rename, the bytes stay
// readable through it until it is closed. The caller MUST close it.
// Returns (nil, "", nil) on a nil (disabled) cache. Idempotent —
// concurrent Puts for the same key race on the rename and the loser
// silently overwrites (same bytes: chunks are immutable).
func (c *DiskCache) Put(key string, v objVersion, alignedOffset int64, data []byte) (*os.File, string, error) {
	if c == nil {
		return nil, "", nil
	}
	if v.etag != "" {
		// The marker must predate every versioned chunk of this shard.
		if _, err := c.versionedSince(key); err != nil {
			return nil, "", err
		}
	}
	p, err := c.path(key, v, alignedOffset)
	if err != nil {
		return nil, "", err
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", err
	}
	tmp, err := os.CreateTemp(dir, ".tmp_chunk_*")
	if err != nil {
		return nil, "", err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return nil, "", err
	}
	if err := os.Rename(tmpName, p); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return nil, "", err
	}
	return tmp, p, nil
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
