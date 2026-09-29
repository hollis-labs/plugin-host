package pluginhost

import (
	"strings"
	"testing"
)

func TestRedactEdgeFragments(t *testing.T) {
	const secret = "hunter2-secret"
	cases := []struct {
		name, in, want string
	}{
		{"whole", "a " + secret + " b", "a [redacted] b"},
		{"twice", secret + secret, "[redacted]"},
		{"leading fragment cut by the window", secret[6:] + " tail", "[redacted] tail"},
		{"trailing fragment cut by the window", "head " + secret[:5], "head [redacted]"},
		{"none", "nothing here", "nothing here"},
	}
	for _, c := range cases {
		if got := Redact(c.in, []string{secret}); got != c.want {
			t.Errorf("%s: Redact(%q) = %q, want %q", c.name, c.in, got, c.want)
		}
	}
	if got := Redact("abc", []string{""}); got != "abc" {
		t.Errorf("empty secret must be ignored, got %q", got)
	}
	if got := Redact("xxAByy", []string{"AB", "BY"}); strings.Contains(got, "A") && strings.Contains(got, "Y") {
		t.Errorf("overlapping secrets left residue: %q", got)
	}
}

func TestTailKeepsTheNewestBytesAndRedactsOnRead(t *testing.T) {
	tail := &Tail{Bytes: 32, Secrets: []string{"topsecret"}}
	for i := 0; i < 1000; i++ {
		_, _ = tail.Write([]byte("noise noise noise topsecret noise\n"))
	}
	got := tail.String()
	if len(got) > 32 {
		t.Fatalf("len = %d, want <= 32", len(got))
	}
	if strings.Contains(got, "topsecret") {
		t.Fatalf("secret leaked: %q", got)
	}
	// A secret split across two writes is caught once both halves landed.
	split := &Tail{Bytes: 64, Secrets: []string{"topsecret"}}
	_, _ = split.Write([]byte("x top"))
	_, _ = split.Write([]byte("secret y"))
	if got := split.String(); strings.Contains(got, "topsecret") || !strings.Contains(got, "[redacted]") {
		t.Fatalf("split secret: %q", got)
	}
}

func TestTailZeroValueIsUsable(t *testing.T) {
	var tail Tail
	_, _ = tail.Write([]byte("hello"))
	if tail.String() != "hello" {
		t.Fatal(tail.String())
	}
}
