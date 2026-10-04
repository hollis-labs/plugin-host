package pluginhost

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func TestMismatchDetailsAndSentinels(t *testing.T) {
	for _, tc := range []struct {
		field            string
		sentinel         error
		expected, actual string
	}{
		{"id", ErrIdentityMismatch, "expected.plugin", "actual.plugin"},
		{"version", ErrVersionMismatch, "1.0.0", "2.0.0"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			p := &Process{spec: Spec{ID: "expected.plugin", ExpectedID: "expected.plugin", ExpectedVersion: "1.0.0", Init: subprocess.InitParams{PluginDir: "/plugin", DataDir: "/data", CacheDir: "/cache", Config: map[string]string{}, LogLevel: "info", HostInfo: subprocess.HostInfo{Protocol: 2, Version: "1.0.0"}, CapabilityContract: 1, Incarnation: capability.RuntimeIdentity{HostInstance: "epoch", OwnerID: "expected.plugin", OwnerGeneration: 1}, Grants: capability.GrantSet{}}}}
			result := subprocess.InitResult{ID: "expected.plugin", Name: "Plugin", Version: "1.0.0", Protocol: 2, CapabilityContract: 1}
			if tc.field == "id" {
				result.ID = tc.actual
			} else {
				result.Version = tc.actual
			}
			err := p.verify(result)
			var detail *MismatchError
			if !errors.Is(err, tc.sentinel) || !errors.As(err, &detail) {
				t.Fatalf("lost mismatch: %v", err)
			}
			if detail.Field != tc.field || detail.Expected != tc.expected || detail.Actual != tc.actual {
				t.Fatalf("wrong detail: %+v", detail)
			}
			l := &Lifecycle{id: "expected.plugin"}
			failure := l.failure(StageLoad, "handshake", 1, err)
			if !errors.As(failure, &detail) || !strings.Contains(failure.Error(), tc.actual) || !errors.Is(failure, tc.sentinel) {
				t.Fatalf("lost nested detail: %v", failure)
			}
		})
	}
}

func TestMismatchSanitizesUntrustedStrings(t *testing.T) {
	raw := "\n\x00\x1b\u202e\u2066\u200b" + strings.Repeat("界", 200)
	detail := mismatch("id", raw, raw)
	for _, value := range []string{detail.Expected, detail.Actual, printableValue(raw)} {
		if !utf8.ValidString(value) || utf8.RuneCountInString(value) > 128 {
			t.Fatal("unbounded or invalid UTF8")
		}
		for _, r := range value {
			if !unicode.IsPrint(r) || unicode.Is(unicode.Cf, r) {
				t.Fatalf("unsafe rune %U", r)
			}
		}
	}
	direct := (&MismatchError{Field: "id", Expected: raw, Actual: raw}).Error()
	if strings.ContainsAny(direct, "\n\x00\x1b\u202e\u2066\u200b") {
		t.Fatal("direct construction escaped sanitizer")
	}
}

func TestLifecycleMismatchKeepsHostRedactionAndNoUnrelatedPayload(t *testing.T) {
	detail := mismatch("version", "1.0.0", "2.0.0")
	cause := processFailure(Spec{ID: "plugin", Init: subprocess.InitParams{Config: map[string]string{"secret": "never-emit"}}, Redact: func(s string) string { return strings.ReplaceAll(s, "2.0.0", "[hidden]") }}, "version", detail)
	failure := (&Lifecycle{id: "plugin"}).failure(StageLoad, "handshake", 1, cause)
	if strings.Contains(failure.Error(), "2.0.0") || strings.Contains(failure.Error(), "never-emit") || !strings.Contains(failure.Error(), "[hidden]") {
		t.Fatal(failure)
	}
	var got *MismatchError
	if !errors.As(failure, &got) || !errors.Is(failure, ErrVersionMismatch) {
		t.Fatal("lost typed cause")
	}
}

func TestInvalidPlannedVersionHasSafeActualValue(t *testing.T) {
	l, err := NewLifecycle("plugin", LifecycleOptions{HostInstance: "epoch", Generations: &MemoryGenerationStore{}, Callbacks: LifecycleCallbacks{Plan: func(context.Context) (Plan, error) { return Plan{Spec: Spec{ExpectedVersion: "dev"}}, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	err = l.Enable(context.Background())
	var detail *MismatchError
	if !errors.As(err, &detail) || detail.Actual != "dev" || detail.Expected != "strict SemVer" {
		t.Fatal(err)
	}
}
