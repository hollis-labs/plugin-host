package pluginhost_test

import (
	"context"
	"errors"
	pluginhost "github.com/hollis-labs/plugin-host"
	"sync"
	"testing"
)

func TestSemanticVersionOrdering(t *testing.T) {
	for _, tc := range []struct {
		a, b string
		want int
	}{
		{"1.9.0", "1.10.0", -1}, {"0.1.0", "0.1.0", 0}, {"1.0.0+build.1", "1.0.0+other", 0},
		{"1.0.0-alpha", "1.0.0-alpha.1", -1}, {"1.0.0-alpha.1", "1.0.0-alpha.beta", -1},
		{"1.0.0-beta.2", "1.0.0-beta.11", -1}, {"1.0.0-rc.1", "1.0.0", -1},
		{"999999999999999999999.0.0", "1000000000000000000000.0.0", -1},
	} {
		n, err := pluginhost.CompareVersions(tc.a, tc.b)
		if err != nil || n != tc.want {
			t.Fatalf("compare(%s,%s)=%d,%v", tc.a, tc.b, n, err)
		}
		n, err = pluginhost.CompareVersions(tc.b, tc.a)
		if err != nil || n != -tc.want {
			t.Fatal(n, err)
		}
	}
	for _, v := range []string{"1", "1.0", "v1.0.0", "01.0.0", "1.0.0-01", "1.0.0-", "1.0.0+", "1.0.0+meta+meta", "1.0.0-a..b", " 1.0.0"} {
		if _, err := pluginhost.CompareVersions(v, "1.0.0"); err == nil {
			t.Fatal("accepted invalid version", v)
		}
	}
}
func TestTransientClassification(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, pluginhost.ErrProtocolMismatch, pluginhost.ErrNoPluginID, pluginhost.ErrIdentityMismatch, pluginhost.ErrVersionMismatch} {
		if pluginhost.IsTransient(&pluginhost.TransientError{Code: "temporary", Cause: cause}) {
			t.Fatal("retried permanent failure", cause)
		}
	}
	if pluginhost.IsTransient(errors.New("unknown")) {
		t.Fatal("unknown error retried")
	}
}

func TestGenerationStoreIssuesUniqueTokensAcrossConcurrentControllers(t *testing.T) {
	var store pluginhost.MemoryGenerationStore
	var wg sync.WaitGroup
	tokens := make(chan uint64, 64)
	for range 64 {
		wg.Go(func() {
			n, err := store.Next(context.Background(), "epoch", "plugin")
			if err != nil {
				t.Error(err)
				return
			}
			tokens <- n
		})
	}
	wg.Wait()
	close(tokens)
	seen := make(map[uint64]bool)
	for n := range tokens {
		if n == 0 || seen[n] {
			t.Fatal("generation reused", n)
		}
		seen[n] = true
	}
	next, err := store.Next(context.Background(), "epoch", "plugin")
	if err != nil || next != 65 {
		t.Fatal(next, err)
	}
}
