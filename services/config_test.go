package services

import (
	"errors"
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/urfave/cli"
)

// TestValidateFetcherConfig: settings under which no miss can be served
// are refused at startup instead of failing every request.
func TestValidateFetcherConfig(t *testing.T) {
	const chunk, conc, timeout = 4 << 20, 32, 90 * time.Second
	for _, c := range []struct {
		name    string
		chunk   int64
		conc    int
		timeout time.Duration
		ok      bool
	}{
		{"defaults", chunk, conc, timeout, true},
		{"one buffer", chunk, 1, timeout, true},
		{"zero FETCH_CONCURRENCY", chunk, 0, timeout, false},
		{"negative FETCH_CONCURRENCY", chunk, -1, timeout, false},
		{"zero CHUNK_FETCH_TIMEOUT", chunk, conc, 0, false},
		{"zero CHUNK_SIZE", 0, conc, timeout, false},
	} {
		if err := validateFetcherConfig(c.chunk, c.conc, c.timeout); (err == nil) != c.ok {
			t.Errorf("%s: err %v, want ok: %v", c.name, err, c.ok)
		}
	}
}

// TestNewFetcherRefusesZeroBudget: the service exits at startup, naming
// the setting, rather than coming up with a budget no miss can get.
func TestNewFetcherRefusesZeroBudget(t *testing.T) {
	const helperEnv = "S3CACHE_NEW_FETCHER_HELPER"
	if os.Getenv(helperEnv) == "1" {
		set := flag.NewFlagSet("helper", flag.ContinueOnError)
		for _, fl := range RegisterFetcherFlags(nil) {
			fl.Apply(set)
		}
		_ = set.Set(BucketFlag, testBucket)
		_ = set.Set(FetchConcurrencyFlag, "0")
		NewFetcher(cli.NewContext(nil, set, nil), nil, nil, nil)
		os.Exit(0) // reached only if the setting was accepted
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestNewFetcherRefusesZeroBudget$")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() == 0 {
		t.Fatalf("NewFetcher with FETCH_CONCURRENCY=0 did not exit non-zero (err %v):\n%s", err, out)
	}
	if !strings.Contains(string(out), "FETCH_CONCURRENCY must be at least 1") {
		t.Fatalf("exit message does not name the setting:\n%s", out)
	}
}
