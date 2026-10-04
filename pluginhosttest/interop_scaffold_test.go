package pluginhosttest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const interopSDKBaseCommit = "d04ab2149506a96e8c54b58829f58ee480e0de41"

type interopRuntime struct {
	Name    string `json:"runtime"`
	Version string `json:"version,omitempty"`
	Status  string `json:"status"`
	Owner   string `json:"pending_owner,omitempty"`
	Reason  string `json:"reason,omitempty"`
}

type interopVersionOutput struct{ bytes.Buffer }

func (b *interopVersionOutput) Write(p []byte) (int, error) {
	n := len(p)
	if remaining := 4096 - b.Len(); remaining > 0 {
		_, _ = b.Buffer.Write(p[:min(remaining, n)])
	}
	return n, nil
}

func interopRuntimeVersion(name string) interopRuntime {
	item := interopRuntime{Name: name, Status: "unavailable", Owner: interopAdapterOwner}
	program, err := exec.LookPath(name)
	if err != nil {
		item.Reason = "Runtime executable unavailable"
		return item
	}
	arg := "--version"
	if name == "go" {
		arg = "version"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Version probes run trusted local tools, never a plugin child. Fixture
	// child execution later requires the library's independent lifetime owner.
	cmd := exec.CommandContext(ctx, program, arg) //nolint:gosec // fixed go/node/deno names resolved through the local PATH
	var output interopVersionOutput
	cmd.Stdout, cmd.Stderr = &output, &output
	if err = cmd.Run(); err != nil {
		item.Reason = "Runtime version probe failed"
		return item
	}
	item.Status, item.Owner, item.Version = "available", "", strings.TrimSpace(output.String())
	return item
}

// This opt-in inventories obligations and a compiled child. It deliberately
// runs no SDK fake host and makes no claim about real host interoperability.
func TestSDKInteropScaffold(t *testing.T) {
	source, child, reportPath := os.Getenv("INTEROP_SDK_SOURCE"), os.Getenv("INTEROP_GO_CHILD"), os.Getenv("INTEROP_REPORT")
	if source == "" && child == "" && reportPath == "" {
		t.Skip("opt in with scripts/interop-scaffold.sh")
	}
	if source == "" || child == "" || reportPath == "" {
		t.Fatal("source, compiled child and report path are required together")
	}
	commit, err := os.ReadFile(filepath.Join(source, ".interop-source-commit")) //nolint:gosec // caller-owned immutable SDK archive, opt-in test only
	if err != nil || strings.TrimSpace(string(commit)) != interopSDKSourceCommit {
		t.Fatal("SDK archive pin does not match the designated source", err)
	}
	if os.Getenv("INTEROP_SDK_BASE") != interopSDKBaseCommit {
		t.Fatal("SDK base commit is missing or different")
	}
	info, err := os.Stat(child) //nolint:gosec // explicit opt-in caller-owned fixture path, never served to a remote caller
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		t.Fatal("compiled Go fixture is unavailable", err)
	}
	receipt, err := os.ReadFile(filepath.Join(filepath.Dir(child), "build-receipt")) //nolint:gosec // test-owned sibling of compiled fixture
	if err != nil || !strings.HasPrefix(string(receipt), interopSDKSourceCommit+"|") {
		t.Fatal("compiled fixture receipt does not match the designated source", err)
	}
	manifest, cases, err := inventoryInterop(os.DirFS(source))
	if err != nil {
		t.Fatal(err)
	}
	runtimes := []interopRuntime{interopRuntimeVersion("go"), interopRuntimeVersion("node"), interopRuntimeVersion("deno")}
	var observations []interopCase
	for _, runtime := range runtimes {
		for _, item := range cases {
			item.Runtime = runtime.Name
			if runtime.Status == "unavailable" && item.Status != "unavailable" {
				item.Status, item.Owner, item.Reason = "unavailable", runtime.Owner, runtime.Reason
			}
			observations = append(observations, item)
		}
	}
	rawManifest, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child.json")) //nolint:gosec // fixed manifest inside the designated source archive
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(rawManifest)
	report := struct {
		Mode         string           `json:"mode"`
		SDKCommit    string           `json:"sdk_commit"`
		SDKBase      string           `json:"sdk_base_commit"`
		HostCommit   string           `json:"host_commit"`
		HostDirty    bool             `json:"host_dirty"`
		Corpus       int              `json:"corpus_version"`
		ManifestHash string           `json:"manifest_sha256"`
		Coverage     string           `json:"sdk_coverage"`
		BuildReceipt string           `json:"go_child_build_receipt"`
		Runtimes     []interopRuntime `json:"runtimes"`
		Cases        []interopCase    `json:"cases"`
	}{"scaffold-only-no-replay", interopSDKSourceCommit, interopSDKBaseCommit, os.Getenv("INTEROP_HOST_COMMIT"), os.Getenv("INTEROP_HOST_DIRTY") == "true", manifest.CorpusVersion, hex.EncodeToString(digest[:]), manifest.Coverage, strings.TrimSpace(string(receipt)), runtimes, observations}
	if report.HostCommit == "" {
		t.Fatal("host commit is required")
	}
	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(reportPath, append(raw, '\n'), 0600); err != nil { //nolint:gosec // explicit opt-in scratch report destination
		t.Fatal(err)
	}
	for _, runtime := range runtimes {
		t.Logf("%s: %s %s", runtime.Name, runtime.Status, runtime.Version)
	}
	t.Logf("inventoried %d obligations across three runtimes; zero replay passes claimed", len(cases))
}
