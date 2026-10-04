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
	params := pluginhosttest.FixtureInit(dir, filepath.Join(dir, "cache"))
	params.Incarnation.OwnerID = "probe"
	rl := "0"
	if raw {
		rl = "1"
	}
	return pluginhost.Spec{
		ID: "probe", ExpectedID: "probe", ExpectedVersion: "1.0.0",
		Command: "/bin/sh", Args: []string{"-c", scriptedInitChild},
		Env:              []string{"PATH=/usr/bin:/bin", "DIR=" + dir, "RESULT=" + result, "RAWLINE=" + rl},
		Init:             params,
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
	spec.Init.Incarnation.HostInstance = spec.Init.DataDir
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
	spec.Init.Incarnation.HostInstance = spec.Init.DataDir
	ev := newEvents()
	options := ev.options(fastPolicy(1))
	options.InitFactory = func(_ context.Context, attempt uint64) (subprocess.InitParams, error) {
		params := spec.Init
		params.Incarnation.OwnerGeneration = attempt
		grant := testGrant()
		grant.HostInstance = params.Incarnation.HostInstance
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

func TestSupervisorGenerationLedgerSurvivesRecreation(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	options := freshInitOptions(spec, pluginhost.SuperviseOptions{})
	first := pluginhost.Supervise(spec, options)
	if err := first.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := pluginhost.Supervise(spec, options)
	if err := second.Start(context.Background()); !errors.Is(err, pluginhost.ErrInvalidGeneration) {
		t.Fatalf("recreated supervisor reused generation: %v", err)
	}
	_ = second.Stop(context.Background())
}

func TestSupervisorAndLifecycleShareGenerationLedger(t *testing.T) {
	for _, supervisorFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(supervisorFirst), func(t *testing.T) {
			spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
			spec.ExpectedVersion = "1.0.0"
			options := lifecycleOptions(t)
			epoch := options.HostInstance
			options.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return pluginhost.Plan{Spec: spec}, nil }
			lifecycle := newController(t, options)
			factory := pluginhost.SuperviseOptions{InitFactory: func(context.Context, uint64) (subprocess.InitParams, error) {
				p := spec.Init
				p.Incarnation.HostInstance = epoch
				return p, nil
			}}
			sup := pluginhost.Supervise(spec, factory)
			t.Cleanup(func() { _ = sup.Stop(context.Background()) })
			if supervisorFirst {
				if err := sup.Start(context.Background()); err != nil {
					t.Fatal(err)
				}
				if err := lifecycle.Enable(context.Background()); !errors.Is(err, pluginhost.ErrInvalidGeneration) {
					t.Fatalf("lifecycle reused live supervisor tuple: %v", err)
				}
				if err := sup.Stop(context.Background()); err != nil {
					t.Fatal(err)
				}
				found := false
				for _, report := range lifecycle.Status().Disposals {
					if report.Owner.OwnerGeneration == 1 && !report.Incomplete && report.ID != "" {
						found = true
					}
				}
				if !found {
					t.Fatal("stopped supervisor left no completed disposal record")
				}
				// Completed disposal clears Active but never lowers the generation watermark.
				store := &pluginhost.MemoryLifecycleStateStore{}
				options.StateStore = store
				next := newController(t, options)
				if err := next.Enable(context.Background()); err != nil {
					t.Fatalf("released reservation quarantined lifecycle: %v", err)
				}
				if next.Status().Owner.OwnerGeneration != 2 {
					t.Fatal("generation watermark lost")
				}
			} else {
				enableController(t, lifecycle)
				if err := sup.Start(context.Background()); !errors.Is(err, pluginhost.ErrInvalidGeneration) {
					t.Fatalf("supervisor reused live lifecycle tuple: %v", err)
				}
			}
		})
	}
}

func TestSupervisorFactoryRefusesHostAndOwnerChanges(t *testing.T) {
	for _, host := range []bool{false, true} {
		t.Run(strconv.FormatBool(host), func(t *testing.T) {
			spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
			ev := newEvents()
			options := freshInitOptions(spec, ev.options(fastPolicy(1)))
			original := options.InitFactory
			options.InitFactory = func(ctx context.Context, attempt uint64) (subprocess.InitParams, error) {
				p, err := original(ctx, attempt)
				if attempt > 1 {
					if host {
						p.Incarnation.HostInstance = "other-epoch"
					} else {
						p.Incarnation.OwnerID = "other-owner"
					}
				}
				return p, err
			}
			sup := pluginhost.Supervise(spec, options)
			if err := sup.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sup.Stop(context.Background()) })
			_ = sup.Current().Kill()
			if err := awaitResult(t, ev.giveUp); err == nil {
				t.Fatal("foreign incarnation accepted")
			}
			if ev.starts.Load() != 1 || sup.Current() != nil {
				t.Fatal("foreign incarnation spawned replacement")
			}
		})
	}
}

func TestPluginProtocolRejectionDiagnosticDirection(t *testing.T) {
	wire := `{"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"init rejected","data":{"contract":"plugin-init/2","code":"protocol_mismatch","field":"host_info.protocol","expected":3,"received":2}}}`
	spec, _ := scriptedInitSpec(t, wire, true)
	_, err := pluginhost.Start(context.Background(), spec)
	if !errors.Is(err, pluginhost.ErrProtocolMismatch) || !strings.Contains(err.Error(), "host requests protocol 2, plugin requires 3") {
		t.Fatalf("diagnostic reversed: %v", err)
	}
}

func TestSupervisorNoFactoryPreservesClassifierCause(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	reason := errors.New("host classified exit")
	ev := newEvents()
	options := ev.options(fastPolicy(1))
	options.ClassifyExit = func(pluginhost.ExitInfo) error { return reason }
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	_ = sup.Current().Kill()
	err := awaitResult(t, ev.giveUp)
	if !errors.Is(err, reason) || !errors.Is(err, pluginhost.ErrInitFactoryRequired) {
		t.Fatalf("give-up cause lost: %v", err)
	}
}

func TestSupervisorNoFactoryPreservesHealthKill(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	ev := newEvents()
	options := ev.options(fastPolicy(1))
	options.HealthInterval = 50 * time.Millisecond
	options.KillAfterUnhealthy = 1
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sup.Stop(context.Background()) })
	tool(t, sup.Current(), "set_health", map[string]any{"ok": false})
	err := awaitResult(t, ev.giveUp)
	if !errors.Is(err, pluginhost.ErrUnhealthy) || !errors.Is(err, pluginhost.ErrInitFactoryRequired) {
		t.Fatalf("health-kill cause lost: %v", err)
	}
}

func TestSupervisorBlockingFactoryIsTrackedAndBounded(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.HandshakeTimeout = 40 * time.Millisecond
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	calls := 0
	options := pluginhost.SuperviseOptions{InitFactory: func(context.Context, uint64) (subprocess.InitParams, error) {
		calls++
		close(entered)
		<-release
		return spec.Init, nil
	}}
	sup := pluginhost.Supervise(spec, options)
	result := make(chan error, 1)
	go func() { result <- sup.Start(context.Background()) }()
	<-entered
	err := awaitResult(t, result)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, pluginhost.ErrInitFactoryPending) || !sup.PendingFactory() {
		t.Fatalf("untracked callback: %v", err)
	}
	if err := sup.Start(context.Background()); !errors.Is(err, pluginhost.ErrInitFactoryPending) {
		t.Fatalf("pending factory reentered: %v", err)
	}
	if calls != 1 {
		t.Fatal("callback repeated before completion")
	}
	if err := sup.Stop(context.Background()); !errors.Is(err, pluginhost.ErrInitFactoryPending) {
		t.Fatalf("Stop hid pending work: %v", err)
	}
	close(release)
	eventually(t, time.Second, "factory completion", func() bool { return !sup.PendingFactory() })
	if sup.Current() != nil {
		t.Fatal("late result spawned a child")
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatalf("completed factory still pending: %v", err)
	}

}

func TestSupervisorPendingRestartFactoryReachesGiveUp(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	spec.HandshakeTimeout = 200 * time.Millisecond
	ev := newEvents()
	options := freshInitOptions(spec, ev.options(fastPolicy(1)))
	original := options.InitFactory
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	options.InitFactory = func(ctx context.Context, attempt uint64) (subprocess.InitParams, error) {
		if attempt > 1 {
			close(entered)
			<-release
		}
		return original(ctx, attempt)
	}
	sup := pluginhost.Supervise(spec, options)
	if err := sup.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_ = sup.Current().Kill()
	<-entered
	err := awaitResult(t, ev.giveUp)
	if !errors.Is(err, pluginhost.ErrInitFactoryPending) || !sup.PendingFactory() || sup.Current() != nil {
		t.Fatalf("give-up hid factory: %v", err)
	}
	if err := sup.Stop(context.Background()); !errors.Is(err, pluginhost.ErrInitFactoryPending) {
		t.Fatal(err)
	}
	close(release)
	eventually(t, time.Second, "restart factory completion", func() bool { return !sup.PendingFactory() })
	if sup.Current() != nil {
		t.Fatal("late restart activated")
	}

}

func TestSupervisorFactoryPanicIsContained(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
	sup := pluginhost.Supervise(spec, pluginhost.SuperviseOptions{InitFactory: func(context.Context, uint64) (subprocess.InitParams, error) { panic("secret factory panic") }})
	err := sup.Start(context.Background())
	if !errors.Is(err, pluginhost.ErrCallbackPanic) || strings.Contains(err.Error(), "secret factory panic") || sup.PendingFactory() {
		t.Fatalf("panic not contained: %v", err)
	}
	if err := sup.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestExpectedIdentityMustMatchIncarnation(t *testing.T) {
	spec, dir := scriptedInitSpec(t, `{"id":"other","name":"P","version":"1.0.0","description":"","protocol":2,"capability_contract":1}`, false)
	spec.ExpectedID = "other"
	_, err := pluginhost.Start(context.Background(), spec)
	if !errors.Is(err, pluginhost.ErrIdentityMismatch) || initChildState(dir) != "no-pid-file" {
		t.Fatalf("foreign expected id reached spawn: %v", err)
	}
}
