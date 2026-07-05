package services

import "testing"

func TestParseRange(t *testing.T) {
	cases := []struct {
		name      string
		header    string
		start     int64
		end       int64
		suffixLen int64
		wantErr   bool
	}{
		{"empty", "", 0, -1, 0, false},
		{"open", "bytes=100-", 100, -1, 0, false},
		{"bounded", "bytes=100-199", 100, 199, 0, false},
		{"zero start", "bytes=0-", 0, -1, 0, false},
		{"suffix", "bytes=-28321", 0, -1, 28321, false},
		{"suffix zero", "bytes=-0", 0, 0, 0, true},
		{"suffix garbage", "bytes=-abc", 0, 0, 0, true},
		{"end before start", "bytes=200-100", 0, 0, 0, true},
		{"multi", "bytes=0-1,5-6", 0, 0, 0, true},
		{"not bytes", "items=0-1", 0, 0, 0, true},
		{"garbage", "bytes=abc-", 0, 0, 0, true},
		{"bare dash", "bytes=-", 0, 0, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			start, end, suffixLen, err := parseRange(c.header)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got start=%d end=%d suffixLen=%d", start, end, suffixLen)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if start != c.start || end != c.end || suffixLen != c.suffixLen {
				t.Fatalf("got start=%d end=%d suffixLen=%d, want start=%d end=%d suffixLen=%d",
					start, end, suffixLen, c.start, c.end, c.suffixLen)
			}
		})
	}
}
