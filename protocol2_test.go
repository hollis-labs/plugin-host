//go:build unix

package pluginhost_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func TestInvalidInitIsRejectedBeforeSpawn(t *testing.T) {
	cases := map[string]func(*subprocess.InitParams){
		"oversized_offer": func(p *subprocess.InitParams) {
			p.HostServices = &subprocess.HostServices{ReverseRPCVersion: 1, Incarnation: p.Incarnation, Methods: []string{}, Limits: subprocess.HostServiceLimits{HostToPluginInflight: 1, PluginToHostInflight: 1, HostGlobalInflight: 1, ControlSlots: 2, MaxFrameBytes: (8 << 20) + 1, MaxQueuedWriteBytes: (8 << 20) + 1, WriteTimeoutMS: 100, MaxDepth: 1, MethodTimeoutMS: map[string]uint32{}}}
		},
		"protocol":           func(p *subprocess.InitParams) { p.HostInfo.Protocol = 1 },
		"contract":           func(p *subprocess.InitParams) { p.CapabilityContract = 2 },
		"runtime":            func(p *subprocess.InitParams) { p.Incarnation.OwnerGeneration = 0 },
		"required_directory": func(p *subprocess.InitParams) { p.DataDir = "" },
		"foreign_grant": func(p *subprocess.InitParams) {
			g := testGrant()
			g.OwnerGeneration++
			p.Grants = capability.GrantSet{g}
		},
		"duplicate_grant": func(p *subprocess.InitParams) { p.Grants = capability.GrantSet{testGrant(), testGrant()} },
		"scope_null": func(p *subprocess.InitParams) {
			g := testGrant()
			g.Scope = json.RawMessage(`null`)
			p.Grants = capability.GrantSet{g}
		},
		"identity_null": func(p *subprocess.InitParams) { p.Identity = json.RawMessage(`null`) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			spec, dir := fixtureSpec(t, pluginhosttest.BehaviourEcho)
			mutate(&spec.Init)
			checked := false
			spec.BeforeSpawn = func(context.Context) error { checked = true; return nil }
			_, err := pluginhost.Start(context.Background(), spec)
			var typed *subprocess.InitError
			var failure *pluginhost.Failure
			if !errors.As(err, &typed) || !errors.As(err, &failure) || failure.Stage != pluginhost.StageLoad {
				t.Fatal("untyped init refusal", err)
			}
			if checked {
				t.Fatal("execution admission ran for invalid Init")
			}
			if _, err = os.Stat(filepath.Join(dir, "pid")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid Init spawned a child", err)
			}
		})
	}
}
func TestProtocol2AcknowledgementsFailBeforeActivation(t *testing.T) {
	for _, tc := range []struct {
		behavior string
		code     subprocess.InitFailureCode
		step     string
	}{
		{pluginhosttest.BehaviourBadProtocol, subprocess.InitProtocolMismatch, "protocol"},
		{pluginhosttest.BehaviourBadContract, subprocess.InitCapabilityContractMismatch, "capability_contract"},
		{pluginhosttest.BehaviourProfileAck, subprocess.InitProfileMismatch, "profile"},
		{pluginhosttest.BehaviourDuplicateInit, subprocess.InitInvalid, "init"},
	} {
		t.Run(tc.behavior, func(t *testing.T) {
			o := lifecycleOptions(t)
			spec, _ := fixtureSpec(t, tc.behavior)
			spec.ExpectedVersion = "1.0.0"
			o.Callbacks.Plan = func(context.Context) (pluginhost.Plan, error) { return pluginhost.Plan{Spec: spec}, nil }
			activated := false
			o.Callbacks.Activate = func(context.Context, pluginhost.Owner, *pluginhost.Process) error { activated = true; return nil }
			l := newController(t, o)
			err := l.Enable(context.Background())
			var typed *subprocess.InitError
			var failure *pluginhost.Failure
			if !errors.As(err, &typed) || typed.Code != tc.code || !errors.As(err, &failure) || failure.Stage != pluginhost.StageLoad || failure.Step != tc.step {
				t.Fatal("acknowledgement classification", err, typed, failure)
			}
			if activated || l.Current() != nil {
				t.Fatal("invalid acknowledgement activated")
			}
			if tc.code == subprocess.InitProtocolMismatch && !errors.Is(err, pluginhost.ErrProtocolMismatch) {
				t.Fatal("legacy protocol sentinel lost", err)
			}
		})
	}
}
func TestLifecycleInitUsesFreshSDKIncarnationAndGrants(t *testing.T) {
	o := lifecycleOptions(t)
	o.Callbacks.PrepareScope = func(_ context.Context, owner pluginhost.Owner, p pluginhost.Plan) (pluginhost.Spec, error) {
		g := testGrant()
		g.HostInstance = owner.HostInstance
		g.OwnerID = owner.OwnerID
		g.OwnerGeneration = owner.OwnerGeneration
		p.Spec.Init.Grants = capability.GrantSet{g}
		p.Spec.Init.Identity = json.RawMessage(`{"caller":"opaque-courier"}`)
		return p.Spec, nil
	}
	l := newController(t, o)
	enableController(t, l)
	for attempt := 0; attempt < 2; attempt++ {
		got := toolAs[subprocess.InitParams](t, l.Current(), "init", nil)
		owner := l.Status().Owner
		if got.Incarnation != (capability.RuntimeIdentity{HostInstance: owner.HostInstance, OwnerID: owner.OwnerID, OwnerGeneration: owner.OwnerGeneration}) || len(got.Grants) != 1 || got.Grants[0].OwnerGeneration != owner.OwnerGeneration || string(got.Identity) != `{"caller":"opaque-courier"}` {
			t.Fatal("fresh canonical SDK payload lost", got)
		}
		if attempt == 0 {
			if err := l.Reload(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
}
func TestLifecycleRejectsPreparedForeignIncarnation(t *testing.T) {
	o := lifecycleOptions(t)
	o.Callbacks.PrepareScope = func(_ context.Context, _ pluginhost.Owner, p pluginhost.Plan) (pluginhost.Spec, error) {
		p.Spec.Init.Incarnation.OwnerGeneration++
		return p.Spec, nil
	}
	l := newController(t, o)
	var typed *subprocess.InitError
	if err := l.Enable(context.Background()); !errors.As(err, &typed) || typed.Field != "incarnation" {
		t.Fatal("foreign prepared incarnation accepted", err)
	}
}
func TestOptionalProfilesAreRefusedBeforeSpawn(t *testing.T) {
	for _, hooks := range []bool{false, true} {
		spec, _ := fixtureSpec(t, pluginhosttest.BehaviourEcho)
		if hooks {
			spec.Init.HooksProfile = &subprocess.HooksProfile{HooksProfileVersion: 1}
		} else {
			spec.Init.HostServices = &subprocess.HostServices{ReverseRPCVersion: 1, Incarnation: spec.Init.Incarnation, Methods: []string{}, Limits: subprocess.HostServiceLimits{HostToPluginInflight: 1, PluginToHostInflight: 1, HostGlobalInflight: 1, ControlSlots: 2, MaxFrameBytes: 8 << 20, MaxQueuedWriteBytes: 8 << 20, WriteTimeoutMS: 100, MaxDepth: 1, MethodTimeoutMS: map[string]uint32{}}}
		}
		spawned := false
		spec.BeforeSpawn = func(context.Context) error { spawned = true; return nil }
		_, err := pluginhost.Start(context.Background(), spec)
		var typed *subprocess.InitError
		if spawned || !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch {
			t.Fatalf("spawned=%v err=%v", spawned, err)
		}
	}
}

func TestAcknowledgedOfferedProfileIsStillUnsupported(t *testing.T) {
	spec, _ := fixtureSpec(t, pluginhosttest.BehaviourProfileAck)
	spec.Init.HostServices = &subprocess.HostServices{ReverseRPCVersion: 1, Incarnation: spec.Init.Incarnation, Methods: []string{}, Limits: subprocess.HostServiceLimits{HostToPluginInflight: 1, PluginToHostInflight: 1, HostGlobalInflight: 1, ControlSlots: 2, MaxFrameBytes: 1024, MaxQueuedWriteBytes: 1024, WriteTimeoutMS: 100, MaxDepth: 1, MethodTimeoutMS: map[string]uint32{}}}
	_, err := pluginhost.Start(context.Background(), spec)
	var typed *subprocess.InitError
	if !errors.As(err, &typed) || typed.Code != subprocess.InitProfileMismatch || typed.Field != "unsupported_profile" {
		t.Fatal("unsupported offered profile accepted", err)
	}
}
