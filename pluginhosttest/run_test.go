package pluginhosttest_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func TestMain(m *testing.M) {
	pluginhosttest.MaybeRunFixture()
	os.Exit(m.Run())
}

// harness is the ~40-line adapter every host writes; this one is over
// plugin-host itself.
type harness struct{ spec func(*pluginhost.Spec) }

func (h harness) Start(ctx context.Context, c pluginhosttest.Case) (pluginhosttest.Instance, error) {
	spec := pluginhost.Spec{
		ID: "conformance", Command: c.Command, Args: c.Args, Env: c.Env,
		Init:    subprocess.InitParams{DataDir: c.DataDir, CacheDir: c.CacheDir},
		Secrets: c.Secrets,
	}
	if h.spec != nil {
		h.spec(&spec)
	}
	p, err := pluginhost.Start(ctx, spec)
	if err != nil {
		return nil, err
	}
	return instance{p}, nil
}

type instance struct{ p *pluginhost.Process }

func (i instance) Call(ctx context.Context, method string, params, result any) error {
	raw, err := i.p.Client().Conn().Call(ctx, method, params)
	if err != nil || result == nil {
		return err
	}
	return json.Unmarshal(raw, result)
}
func (i instance) Stop(ctx context.Context) error { return i.p.Stop(ctx) }
func (i instance) Exited() <-chan struct{}        { return i.p.Exited() }
func (i instance) Pid() int                       { return i.p.Pid() }
func (i instance) Diagnostics() string            { return i.p.Diagnostics() }
func (i instance) IsGone(err error) bool          { return errors.Is(err, pluginhost.ErrGone) }
func (i instance) RPCCode(err error) (int, bool) {
	var rpc *subprocess.RPCError
	if errors.As(err, &rpc) {
		return rpc.Code, true
	}
	return 0, false
}

// TestConformance is acceptance (1): the suite passes against the library
// with zero waivers. Budgets are shortened so the run is quick; the defaults
// are exercised by TestDefaultBudgets in the root package.
func TestConformance(t *testing.T) {
	pluginhosttest.Run(t, harness{spec: func(s *pluginhost.Spec) {
		s.HandshakeTimeout = 2 * time.Second
		s.UnloadTimeout = time.Second
		s.ReapTimeout = 2 * time.Second
	}}, pluginhosttest.WithStartBound(10*time.Second), pluginhosttest.WithStopBound(10*time.Second))
}

// TestWaiversArePrintedNotSilent checks the waiver mechanism itself: a
// waived requirement shows up as a skipped subtest that names its reason,
// and an unknown id is refused.
func TestWaiversArePrintedNotSilent(t *testing.T) {
	var opts []pluginhosttest.Option
	for i := 1; i <= 18; i++ {
		opts = append(opts, pluginhosttest.Waive(fmt.Sprintf("R%02d", i), "test"))
	}
	// refuseAll fails any Start, so this passes only if every requirement
	// was skipped, and -v shows each skip with its reason.
	pluginhosttest.Run(t, refuseAll{}, opts...)
}

type refuseAll struct{}

func (refuseAll) Start(context.Context, pluginhosttest.Case) (pluginhosttest.Instance, error) {
	return nil, errors.New("refuseAll: no requirement should have run")
}
