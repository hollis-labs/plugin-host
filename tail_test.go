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
	if len(got) > 32 || got == "" || !strings.Contains(got, "[redacted]") {
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

func TestTruncatedTailDropsLeadingFragmentBeforeHostRedaction(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		tail := &Tail{Bytes: 32}
		text := "PASSWORD=secret" + strings.Repeat("x", 32) + "\nCAUSE: schema failed\n"
		if chunked {
			for _, b := range []byte(text) {
				_, _ = tail.Write([]byte{b})
			}
		} else {
			_, _ = tail.Write([]byte(text))
		}
		p := &Process{tail: tail, spec: Spec{Redact: func(raw string) string {
			if raw != "CAUSE: schema failed\n" {
				t.Fatalf("redactor saw partial key: %q", raw)
			}
			return raw
		}}}
		if got := p.Diagnostics(); got != "CAUSE: schema failed\n" {
			t.Fatal(got)
		}
	}
}

func TestTruncatedTailWithoutNewlineRemainsBoundedAndIntact(t *testing.T) {
	tail := &Tail{Bytes: 16}
	_, _ = tail.Write([]byte("a long single line without a boundary"))
	if got := tail.String(); got != "thout a boundary" {
		t.Fatalf("unexpected retained window: %q", got)
	}
	if len(tail.String()) != 16 {
		t.Fatal("single-line tail dropped")
	}
}

func TestExactlyFullTailKeepsCompleteLeadingLine(t *testing.T) {
	const text = "CAUSE: first\nlast"
	tail := &Tail{Bytes: len(text)}
	_, _ = tail.Write([]byte(text))
	if got := tail.String(); got != text {
		t.Fatal("untruncated full window lost first line", got)
	}
}

func TestTruncatedLongLineWithTrailingNewlineIsRetained(t *testing.T) {
	tail := &Tail{Bytes: 32}
	text := strings.Repeat("x", 5000) + "\n"
	_, _ = tail.Write([]byte(text))
	if got := tail.String(); got != strings.Repeat("x", 31)+"\n" {
		t.Fatalf("terminal newline erased single-line diagnostic: %q", got)
	}
}

func TestTruncatedLongLineKeepsPartialSecondLine(t *testing.T) {
	tail := &Tail{Bytes: 32}
	_, _ = tail.Write([]byte(strings.Repeat("x", 5000) + "\nSECOND CAUSE: partial"))
	if got := tail.String(); got != "SECOND CAUSE: partial" {
		t.Fatalf("second-line diagnostic lost: %q", got)
	}
}

func TestTailTruncationNeighbourTable(t *testing.T) {
	long := strings.Repeat("x", 5000)
	cases := []struct {
		name         string
		limit        int
		stream, want string
	}{
		{"LF", 32, long + "\n", strings.Repeat("x", 31) + "\n"},
		{"CRLF", 32, long + "\r\n", strings.Repeat("x", 30) + "\r\n"},
		{"only LF", 1, "abc\n", "\n"},
		{"blank LF", 32, long + "\n\n", strings.Repeat("x", 30) + "\n\n"},
		{"three LF", 32, long + "\n\n\n", strings.Repeat("x", 29) + "\n\n\n"},
		{"blank spaces", 32, long + "\n   ", strings.Repeat("x", 28) + "\n   "},
		{"partial then complete", 18, "PASSWORD=secretxx\nCAUSE: a\n", "CAUSE: a\n"},
		{"partial second", 32, long + "\nCAU", "CAU"},
		{"boundary one line", 12, "first\nsecond line\n", "second line\n"},
		{"boundary two lines", 10, "AAAA\nBBBB\nCCCC\n", "CCCC\n"},
		{"boundary then partial", 7, "AAAA\nBBBB\nCC", "CC"},
		{"CR only accepted limitation", 18, "PASSWORD=aaaa\rCAUSE: x\r", "ORD=aaaa\rCAUSE: x\r"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, chunked := range []bool{false, true} {
				tail := &Tail{Bytes: tc.limit}
				if chunked {
					for _, b := range []byte(tc.stream) {
						_, _ = tail.Write([]byte{b})
					}
				} else {
					_, _ = tail.Write([]byte(tc.stream))
				}
				if got := tail.String(); got != tc.want {
					t.Fatalf("chunked=%v got=%q want=%q", chunked, got, tc.want)
				}
			}
		})
	}
}

func TestTailFinalTrimDropsPartialLineBeforeHostRedaction(t *testing.T) {
	for _, truncated := range []bool{false, true} {
		tail := &Tail{Bytes: 20, Secrets: []string{"x"}}
		stream := "PASSWORD=hunter2\nx\n"
		if truncated {
			stream = strings.Repeat("q", 100) + "\n" + stream
		}
		_, _ = tail.Write([]byte(stream))
		var seen string
		process := &Process{tail: tail, spec: Spec{Redact: func(raw string) string { seen = raw; return raw }}}
		if got := process.Diagnostics(); got != "[redacted]\n" || seen != got {
			t.Fatalf("truncated=%v redactor saw %q result %q", truncated, seen, got)
		}
	}
}

func TestTailDropsLeadingFragmentBeforeConfiguredSecretRedaction(t *testing.T) {
	// The leading partial line contains one secret, and the complete next line
	// contains another. Dropping after redaction instead changes the byte trim.
	tail := &Tail{Bytes: 28, Secrets: []string{"x"}}
	_, _ = tail.Write([]byte(strings.Repeat("q", 100) + "\npartialxx\nCAUSE: x failed\n"))
	if got := tail.String(); got != "CAUSE: [redacted] failed\n" {
		t.Fatalf("drop/redaction order: %q", got)
	}
}
