//go:build unix

package pluginhost

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// These tests run normal public SDK Serve at the fixture source pin recorded
// by the build script/CI. No child correlation/client/grant injection occurs.
// Shared manifest replay and its runtime receipts remain a separate gate.
func sdkReverseChildSpec(t *testing.T, mode string, services HostServices) Spec {
	t.Helper()
	runtime := os.Getenv("REVERSE_CHILD_RUNTIME")
	child := os.Getenv("REVERSE_GO_CHILD")
	if runtime == "node" {
		child = os.Getenv("REVERSE_NODE_WORKER")
	}
	if child == "" {
		t.Skip("pinned normal-Serve child not built; run scripts/interop-scaffold.sh and set REVERSE_GO_CHILD")
	}
	s := reverseTestSpec(t, services)
	control := filepath.Join(t.TempDir(), "control")
	// Test-only fd3 bridge. The bounded regular file supplies an explicit release
	// for SDK's gated Load in non-lifecycle modes; it never changes SDK stdout.
	if err := os.WriteFile(control, []byte("{\"seq\":1,\"op\":\"release\",\"gate\":\"request-2\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	s.Command = "/bin/sh"
	s.Args = []string{"-c", `exec 3<"$CONTROL"; exec "$CHILD" '-test.run=^TestNegotiatedFixtureChild$'`}
	s.Env = []string{"CHILD=" + child, "CONTROL=" + control, "SDK_FIXTURE_CHILD=negotiated:" + mode, "GORACE=atexit_sleep_ms=0"}
	if runtime == "node" {
		node, err := exec.LookPath("node")
		if err != nil {
			t.Fatal(err)
		}
		s.Args = []string{"-c", `exec 3<"$CONTROL"; exec "$NODE" "$WORKER" "$PROFILE"`}
		s.Env = []string{"NODE=" + node, "WORKER=" + child, "PROFILE=negotiated:" + mode, "CONTROL=" + control}
	}
	s.StderrBytes = 64 << 10
	return s
}
func TestReverseSDKChildLifecycleLogAndExit(t *testing.T) {
	var mu sync.Mutex
	var parents []uint64
	var connections []string
	s := sdkReverseChildSpec(t, "lifecycle", HostServices{Log: func(_ context.Context, call *HostCall, _ subprocess.LogParams) (subprocess.LogResult, error) {
		if err := call.CheckCommit(); err != nil {
			return subprocess.LogResult{}, err
		}
		a := call.Authority()
		mu.Lock()
		parents = append(parents, a.Parent.ID)
		connections = append(connections, a.ConnectionInstance)
		mu.Unlock()
		return subprocess.LogResult{Accepted: true}, nil
	}})
	p, err := Start(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill() })
	if p.Info().ReverseRPCVersion == nil {
		t.Fatal("normal Serve did not acknowledge")
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.unloadErr != nil {
		t.Fatal(p.unloadErr)
	}
	select {
	case <-p.Exited():
	case <-time.After(time.Second):
		t.Fatal("child not reaped")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(parents) != 3 || parents[0] != 1 || parents[1] != 2 || parents[2] != 3 {
		t.Fatalf("actual lifecycle parents %v", parents)
	}
	if connections[0] != connections[1] || connections[1] == connections[2] {
		t.Fatal("Unload did not use separate cleanup authority")
	}
	if !strings.Contains(p.Diagnostics(), `"kind":"finished"`) {
		t.Fatal("missing actual SDK completion observation", p.Diagnostics())
	}
}
func TestReverseSDKChildBoundHelperAndRetirement(t *testing.T) {
	observed := make(chan *HostCall, 1)
	s := sdkReverseChildSpec(t, "get", HostServices{StorageGet: func(_ context.Context, call *HostCall, p subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		if p.Key != "read" {
			t.Error(p.Key)
		}
		observed <- call
		return subprocess.StorageGetResult{Found: false}, nil
	}})
	p, err := Start(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill() })
	ctx := WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)})
	health, err := p.Client().Health(ctx)
	if err != nil || !health.OK {
		t.Fatal(health, err, p.Diagnostics())
	}
	var received *HostCall
	select {
	case received = <-observed:
	default:
	}
	if received == nil {
		t.Fatal("typed backend never called")
	}
	if received.Authority().Parent.ID != 3 {
		t.Fatal("not actual forward parent")
	}
	if err := received.CheckCommit(); err == nil {
		t.Fatal("completed parent retained authority")
	}
	if err := p.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestReverseSDKChildDeclineFailedInitAndDisconnect(t *testing.T) {
	for _, mode := range []string{"no-opt-in", "init-error", "init-panic", "wait-init"} {
		t.Run(mode, func(t *testing.T) {
			s := sdkReverseChildSpec(t, mode, HostServices{})
			s.HandshakeTimeout = 150 * time.Millisecond
			p, err := Start(context.Background(), s)
			if err == nil {
				_ = p.Kill()
				t.Fatal("failed/declined Init activated")
			}
			var failure *Failure
			if !errors.As(err, &failure) {
				t.Fatal("untyped failure", err)
			}
		})
	}
	s := sdkReverseChildSpec(t, "lifecycle", HostServices{Log: func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error) {
		return subprocess.LogResult{Accepted: true}, nil
	}})
	p, err := Start(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Kill() })
	if err := p.conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.Exited():
	case <-time.After(time.Second):
		t.Fatal("EOF/disconnect failed to exit", p.Diagnostics())
	}
	p.conn.reverse.business.mu.Lock()
	closed := p.conn.reverse.business.closed
	p.conn.reverse.business.mu.Unlock()
	if !closed {
		t.Fatal("disconnect kept business authority")
	}
}

func TestReverseSDKNodeNormalServe(t *testing.T) {
	if os.Getenv("REVERSE_NODE_WORKER") == "" {
		t.Skip("pinned TS fixture not built")
	}
	t.Setenv("REVERSE_CHILD_RUNTIME", "node")
	t.Run("lifecycle", TestReverseSDKChildLifecycleLogAndExit)
	t.Run("bound-helper", TestReverseSDKChildBoundHelperAndRetirement)
	t.Run("failed-init-and-disconnect", TestReverseSDKChildDeclineFailedInitAndDisconnect)
}

func TestReverseSDKLifecycleFencesBeforeHostCleanupAndReloadsFresh(t *testing.T) {
	epoch, err := NewHostInstance()
	if err != nil {
		t.Fatal(err)
	}
	s := sdkReverseChildSpec(t, "lifecycle", HostServices{Log: func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error) {
		return subprocess.LogResult{Accepted: true}, nil
	}})
	entered := make(chan Owner, 2)
	release := make(chan struct{})
	options := LifecycleOptions{HostInstance: epoch, Generations: &MemoryGenerationStore{}, CleanupTimeout: time.Second, Callbacks: LifecycleCallbacks{
		Plan: func(context.Context) (Plan, error) { return Plan{Spec: s}, nil },
		PrepareScope: func(_ context.Context, o Owner, p Plan) (Spec, error) {
			v := snapshotSpec(p.Spec)
			identity := v.Init.Incarnation
			identity.HostInstance = o.HostInstance
			identity.OwnerID = o.OwnerID
			identity.OwnerGeneration = o.OwnerGeneration
			v.Init.Incarnation = identity
			v.Init.HostServices.Incarnation = identity
			for i := range v.Init.Grants {
				v.Init.Grants[i].HostInstance = o.HostInstance
				v.Init.Grants[i].OwnerID = o.OwnerID
				v.Init.Grants[i].OwnerGeneration = o.OwnerGeneration
			}
			return v, nil
		},
		Revoke: func(_ context.Context, o Owner) error { entered <- o; <-release; return nil },
	}}
	l, err := NewLifecycle("fixture", options)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := l.Current()
	if first == nil {
		t.Fatal("no current child")
	}
	t.Cleanup(func() { _ = first.Kill() })
	oldOwner := l.Status().Owner
	if err := l.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	disabled := make(chan error, 1)
	go func() { disabled <- l.Disable(context.Background()) }()
	deadline := time.Now().Add(time.Second)
	for l.Status().DesiredEnabled {
		if time.Now().After(deadline) {
			l.release()
			t.Fatal("disable did not fence intent")
		}
		time.Sleep(time.Millisecond)
	}
	first.conn.mu.Lock()
	fenced := first.conn.reverse.fenced
	first.conn.mu.Unlock()
	l.release()
	if !fenced {
		t.Fatal("reverse authority fence waited for lifecycle gate")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("host cleanup not reached")
	}
	if err := first.conn.ActivateHostServices(); err == nil {
		t.Fatal("business reopened while cleanup blocked")
	}
	if _, err := first.Client().Health(WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)})); err == nil {
		t.Fatal("disable kept dispatch authority")
	}
	close(release)
	if err := <-disabled; err != nil {
		t.Fatal(err)
	}
	if first.unloadErr != nil {
		t.Fatal("separate Unload log failed", first.unloadErr)
	}
	if err := l.Enable(context.Background()); err != nil {
		t.Fatal(err)
	}
	second := l.Current()
	if second == nil || second == first {
		t.Fatal("not fresh child")
	}
	t.Cleanup(func() { _ = second.Kill() })
	newOwner := l.Status().Owner
	if newOwner.OwnerGeneration <= oldOwner.OwnerGeneration {
		t.Fatal("generation reused")
	}
	if first.conn.reverse.business.connection == second.conn.reverse.business.connection {
		t.Fatal("connection identity reused")
	}
	if _, err := first.Client().Health(context.Background()); err == nil {
		t.Fatal("old connection revived")
	}
	if err := l.Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	third := l.Current()
	if third == nil || third == second {
		t.Fatal("reload did not replace")
	}
	t.Cleanup(func() { _ = third.Kill() })
	if l.Status().Owner.OwnerGeneration <= newOwner.OwnerGeneration {
		t.Fatal("reload generation reused")
	}
	if err := l.Disable(context.Background()); err != nil {
		t.Fatal(err)
	}
}
