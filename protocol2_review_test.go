//go:build unix

package pluginhost_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// fake child: records every stdin line, answers init with $RESULT (raw JSON
// for the "result" member, or a full raw line when RAWLINE=1), answers every
// later request with {} under the same id.
const scriptedInitChild = `echo $$ > "$DIR/pid"
IFS= read -r line || exit 0
printf '%s\n' "$line" >> "$DIR/lines"
if [ "$RAWLINE" = "1" ]; then printf '%s\n' "$RESULT"; else printf '{"jsonrpc":"2.0","id":1,"result":%s}\n' "$RESULT"; fi
while IFS= read -r l; do
  printf '%s\n' "$l" >> "$DIR/lines"
  id=$(printf '%s' "$l" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p')
  if [ -n "$id" ]; then printf '{"jsonrpc":"2.0","id":%s,"result":{"ok":true}}\n' "$id"; fi
done
`

func scriptedInitSpec(t *testing.T, result string, raw bool) (pluginhost.Spec, string) {
	dir := t.TempDir()
	t.Cleanup(func() {
		data, err := os.ReadFile(filepath.Join(dir, "pid")) // #nosec G304 -- host-owned test directory.
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(data)))
			if parseErr == nil && pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
	})
	rl := "0"
	if raw {
		rl = "1"
	}
	return pluginhost.Spec{
		ID: "probe", ExpectedID: "probe", ExpectedVersion: "1.0.0",
		Command: "/bin/sh", Args: []string{"-c", scriptedInitChild},
		Env:              []string{"PATH=/usr/bin:/bin", "DIR=" + dir, "RESULT=" + result, "RAWLINE=" + rl},
		Init:             pluginhosttest.FixtureInit(dir, filepath.Join(dir, "cache")),
		HandshakeTimeout: 5 * time.Second, UnloadTimeout: time.Second, ReapTimeout: 2 * time.Second,
	}, dir
}

func recordedInitLines(dir string) []string {
	b, _ := os.ReadFile(filepath.Join(dir, "lines")) // #nosec G304 -- host-owned test directory.
	s := strings.TrimSpace(string(b))
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func recordedInitMethods(dir string) []string {
	var out []string
	for _, l := range recordedInitLines(dir) {
		var m struct {
			Method string `json:"method"`
		}
		_ = json.Unmarshal([]byte(l), &m)
		out = append(out, m.Method)
	}
	return out
}

func initChildState(dir string) string {
	b, err := os.ReadFile(filepath.Join(dir, "pid")) // #nosec G304 -- host-owned test directory.
	if err != nil {
		return "no-pid-file"
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(b)))
	if syscall.Kill(pid, 0) == nil {
		return "ALIVE"
	}
	return "gone"
}

func TestVerifyFailureReapsBeforeLoad(t *testing.T) {
	for _, tc := range []struct{ name, id, version, step string }{
		{"wrong_id", "other", "1.0.0", "identity"},
		{"id_space", "probe ", "1.0.0", "identity"},
		{"id_case", "Probe", "1.0.0", "identity"},
		{"version_build", "probe", "1.0.0+b", "version"},
		{"version_space", "probe", "1.0.0 ", "version"},
		{"version_case", "probe", "1.0.0+BUILD", "version"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, _ := json.Marshal(subprocess.InitResult{ID: tc.id, Name: "P", Version: tc.version, Description: "", Protocol: 2, CapabilityContract: 1})
			spec, dir := scriptedInitSpec(t, string(result), false)
			if tc.name == "version_case" {
				spec.ExpectedVersion = "1.0.0+build"
			}
			_, err := pluginhost.Start(context.Background(), spec)
			var failure *pluginhost.Failure
			if !errors.As(err, &failure) || failure.Stage != pluginhost.StageLoad || failure.Step != tc.step {
				t.Fatalf("%v", err)
			}
			if got := recordedInitMethods(dir); len(got) != 1 || got[0] != subprocess.MethodInit {
				t.Fatalf("load sent on failed verification: %v", got)
			}
			if initChildState(dir) != "gone" {
				t.Fatal("failed child not reaped")
			}
		})
	}
}

func TestExpectedIDDefaultsToOwner(t *testing.T) {
	spec, dir := scriptedInitSpec(t, `{"id":"other","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	spec.ExpectedID = ""
	_, err := pluginhost.Start(context.Background(), spec)
	if !errors.Is(err, pluginhost.ErrIdentityMismatch) || initChildState(dir) != "gone" || len(recordedInitMethods(dir)) != 1 {
		t.Fatalf("identity check missing: %v", err)
	}
}

func TestPluginInitRejectionsRemainTyped(t *testing.T) {
	for _, tc := range []struct {
		code subprocess.InitFailureCode
		step string
	}{
		{subprocess.InitInvalid, "init"}, {subprocess.InitProtocolMismatch, "protocol"}, {subprocess.InitCapabilityContractMismatch, "capability_contract"}, {subprocess.InitProfileMismatch, "profile"},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			rpc := subprocess.RPCResponse{JSONRPC: "2.0", ID: 1, Error: &subprocess.RPCError{Code: -32602, Message: "init refused", Data: (&subprocess.InitError{Code: tc.code, Field: "host_info.protocol", Expected: 1, Received: 2}).RPCData()}}
			wire, err := json.Marshal(rpc)
			if err != nil {
				t.Fatal(err)
			}
			spec, dir := scriptedInitSpec(t, string(wire), true)
			_, err = pluginhost.Start(context.Background(), spec)
			var f *pluginhost.Failure
			var typed *subprocess.InitError
			var transport *subprocess.RPCError
			if !errors.As(err, &f) || f.Step != tc.step || !errors.As(err, &typed) || typed.Code != tc.code || !errors.As(err, &transport) {
				t.Fatalf("causes lost: %v", err)
			}
			if tc.code == subprocess.InitProtocolMismatch && !errors.Is(err, pluginhost.ErrProtocolMismatch) {
				t.Fatal(err)
			}
			if initChildState(dir) != "gone" || len(recordedInitMethods(dir)) != 1 {
				t.Fatal("failed init reached load or left child alive")
			}
		})
	}
}

func TestManualInitVerifiesResult(t *testing.T) {
	spec, _ := scriptedInitSpec(t, `{"id":"probe","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":2}`, false)
	process, err := pluginhost.Spawn(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = process.Kill(); _ = process.Stop(context.Background()) })
	_, err = process.Client().Init(context.Background(), spec.Init)
	var typed *subprocess.InitError
	if !errors.As(err, &typed) || typed.Code != subprocess.InitCapabilityContractMismatch {
		t.Fatalf("manual init accepted bad result: %v", err)
	}
}

func TestSupervisorWithoutInitFactoryIsTerminal(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	ev := newEvents()
	sup := pluginhost.Supervise(spec, ev.options(fastPolicy(3)))
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	_ = sup.Current().Kill()
	if err := awaitResult(t, ev.giveUp); !errors.Is(err, pluginhost.ErrInitFactoryRequired) {
		t.Fatal(err)
	}
	if sup.Restarts() != 0 || ev.starts.Load() != 1 {
		t.Fatal("unauthorized restart")
	}
}

func TestSupervisorRefusesReusedIncarnation(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	ev := newEvents()
	options := ev.options(fastPolicy(3))
	options.InitFactory = func(context.Context, uint64) (subprocess.InitParams, error) { return spec.Init, nil }
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	_ = sup.Current().Kill()
	var typed *subprocess.InitError
	if err := awaitResult(t, ev.giveUp); !errors.As(err, &typed) || typed.Field != "incarnation" {
		t.Fatal(err)
	}
	if ev.starts.Load() != 1 {
		t.Fatal("reused incarnation activated")
	}
}

func TestGrantScopeSnapshotSurvivesHostMutation(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	grant := testGrant()
	grant.Scope = json.RawMessage(`{"name":"a"}`)
	spec.Init.Grants = capability.GrantSet{grant}
	spec.BeforeSpawn = func(context.Context) error {
		for i, b := range grant.Scope {
			if b == 'a' && i == 9 {
				grant.Scope[i] = 'b'
			}
		}
		return nil
	}
	p, err := pluginhost.Start(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Stop(context.Background()) })
	got := toolAs[subprocess.InitParams](t, p, "init", nil)
	if len(got.Grants) != 1 || string(got.Grants[0].Scope) != `{"name":"a"}` || string(grant.Scope) != `{"name":"b"}` {
		t.Fatal("scope snapshot shares host memory", got.Grants, string(grant.Scope))
	}
}

func TestPreSpawnFailureStepSurvivesStartAndLifecycle(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.ExpectedVersion = "1.0.0"
	spec.Init.HostInfo.Protocol = 1
	spawned := false
	spec.BeforeSpawn = func(context.Context) error { spawned = true; return nil }
	check := func(err error) {
		t.Helper()
		var f *pluginhost.Failure
		if !errors.As(err, &f) || f.Stage != pluginhost.StageLoad || f.Step != "protocol" || !errors.Is(err, pluginhost.ErrProtocolMismatch) {
			t.Fatalf("step lost: %v", err)
		}
	}
	_, err := pluginhost.Start(context.Background(), spec)
	check(err)
	options := lifecycleOptions(t)
	options.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return pluginhost.Plan{Spec: spec}, nil }
	lifecycle := newController(t, options)
	check(lifecycle.Enable(context.Background()))
	if spawned {
		t.Fatal("invalid Init spawned child")
	}
}

func TestSupervisorFactoryIssuesFreshGrants(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	ev := newEvents()
	options := ev.options(fastPolicy(1))
	options.InitFactory = func(_ context.Context, attempt uint64) (subprocess.InitParams, error) {
		params := spec.Init
		params.Incarnation.OwnerGeneration = attempt
		grant := testGrant()
		grant.OwnerGeneration = attempt
		grant.GrantID = "grant-" + strconv.FormatUint(attempt, 10)
		params.Grants = capability.GrantSet{grant}
		return params, nil
	}
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	first := sup.Current()
	before := toolAs[subprocess.InitParams](t, first, "init", nil)
	_ = first.Kill()
	second := awaitNewProcess(t, sup, first)
	after := toolAs[subprocess.InitParams](t, second, "init", nil)
	if before.Incarnation.OwnerGeneration != 1 || after.Incarnation.OwnerGeneration != 2 || after.Grants[0].GrantID == before.Grants[0].GrantID || after.Grants[0].OwnerGeneration != 2 {
		t.Fatal("factory authority reused", before, after)
	}
}

func TestManualInitRefusesUnofferedProfile(t *testing.T) {
	spec, _ := scriptedInitSpec(t, `{"id":"probe","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1,"reverse_rpc_version":1}`, false)
	p, err := pluginhost.Spawn(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(); _ = p.Stop(context.Background()) })
	_, err = p.Client().Init(context.Background(), spec.Init)
	var typed *subprocess.InitError
	if !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch {
		t.Fatalf("profile accepted: %v", err)
	}
}

func TestBlankExpectedIdentityIsRefusedBeforeSpawn(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.ExpectedID = " "
	spawned := false
	spec.BeforeSpawn = func(context.Context) error { spawned = true; return nil }
	_, err := pluginhost.Start(context.Background(), spec)
	var f *pluginhost.Failure
	if spawned || !errors.As(err, &f) || f.Step != "identity" {
		t.Fatalf("blank expectation: spawned=%v err=%v", spawned, err)
	}
}

func TestManualInitRejectsInvalidParamsWithoutSending(t *testing.T) {
	spec, dir := scriptedInitSpec(t, `{"id":"probe","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	p, err := pluginhost.Spawn(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(); _ = p.Stop(context.Background()) })
	params := spec.Init
	params.HostInfo.Protocol = 1
	_, err = p.Client().Init(context.Background(), params)
	var typed *subprocess.InitError
	if !errors.As(err, &typed) || typed.Code != subprocess.InitProtocolMismatch || len(recordedInitMethods(dir)) != 0 {
		t.Fatalf("invalid manual Init sent: %v", err)
	}
}

func TestNoPluginIDKeepsSentinel(t *testing.T) {
	spec, dir := scriptedInitSpec(t, `{"id":"","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	_, err := pluginhost.Start(context.Background(), spec)
	var f *pluginhost.Failure
	if !errors.As(err, &f) || f.Step != "identity" || !errors.Is(err, pluginhost.ErrNoPluginID) || initChildState(dir) != "gone" {
		t.Fatalf("empty id classification: %v", err)
	}
}

func TestManualInitRefusesOffersWithoutSending(t *testing.T) {
	spec, dir := scriptedInitSpec(t, `{"id":"probe","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	p, err := pluginhost.Spawn(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill(); _ = p.Stop(context.Background()) })
	params := spec.Init
	params.HooksProfile = &subprocess.HooksProfile{HooksProfileVersion: 1}
	_, err = p.Client().Init(context.Background(), params)
	var typed *subprocess.InitError
	if !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch || len(recordedInitMethods(dir)) != 0 {
		t.Fatalf("unsupported offer sent: %v", err)
	}
}
