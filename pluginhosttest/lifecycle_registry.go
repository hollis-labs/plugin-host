package pluginhosttest

import (
	"context"
	"encoding/json"
	"errors"
	pluginhost "github.com/hollis-labs/plugin-host"
	"reflect"
	"sync"
	"testing"
	"time"
)

type scopeRegistry struct {
	mu     sync.Mutex
	scopes map[pluginhost.Owner]bool
}

func (r *scopeRegistry) prepare(o pluginhost.Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.scopes == nil {
		r.scopes = make(map[pluginhost.Owner]bool)
	}
	r.scopes[o] = false
}
func (r *scopeRegistry) activate(o pluginhost.Owner) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, active := range r.scopes {
		if active {
			return errors.New("overlapping registrations")
		}
	}
	r.scopes[o] = true
	return nil
}
func (r *scopeRegistry) revoke(o pluginhost.Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.scopes[o] = false
}
func (r *scopeRegistry) dispose(o pluginhost.Owner) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.scopes, o)
}
func (r *scopeRegistry) empty() bool { r.mu.Lock(); defer r.mu.Unlock(); return len(r.scopes) == 0 }
func (r *scopeRegistry) admit(o pluginhost.Owner) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.scopes[o]
}
func (r *scopeRegistry) callbacks(o *pluginhost.LifecycleOptions) {
	o.Callbacks.PrepareScope = func(_ context.Context, owner pluginhost.Owner, p pluginhost.Plan) (pluginhost.Spec, error) {
		r.prepare(owner)
		return p.Spec, nil
	}
	o.Callbacks.Activate = func(_ context.Context, owner pluginhost.Owner, _ *pluginhost.Process) error { return r.activate(owner) }
	o.Callbacks.Revoke = func(_ context.Context, owner pluginhost.Owner) error { r.revoke(owner); return nil }
	o.Callbacks.Dispose = func(_ context.Context, owner pluginhost.Owner) error { r.dispose(owner); return nil }
}
func (e *lifecycleEnv) activationOrder() {
	for _, behavior := range []string{BehaviourEcho, BehaviourWrongID, BehaviourBadProtocol, BehaviourInitError, BehaviourLoadError} {
		p := e.plan(behavior)
		o := e.options(p)
		activated := false
		o.Callbacks.Activate = func(ctx context.Context, _ pluginhost.Owner, child *pluginhost.Process) error {
			activated = true
			if child.Info().ID != "fixture" {
				return errors.New("activation preceded identity verification")
			}
			reply, err := child.Client().Conn().Call(ctx, "mcp/call_tool", map[string]any{"tool_name": "trace", "arguments": map[string]any{}})
			if err != nil {
				return err
			}
			var result struct{ Content []string }
			if err = json.Unmarshal(reply, &result); err != nil {
				return err
			}
			if !reflect.DeepEqual(result.Content, []string{"plugin/init", "plugin/load"}) {
				return errors.New("activation preceded successful load")
			}
			return nil
		}
		l := e.new(o)
		err := l.Enable(context.Background())
		if behavior == BehaviourEcho {
			if err != nil || !activated {
				e.t.Fatalf("successful activation order: %v", err)
			}
		} else if err == nil || activated {
			e.t.Fatalf("failed handshake was activated: %s %v", behavior, err)
		}
	}
}

// isolate helpers keep assertions in the test goroutine, never in a callback
// that can outlive its budget.
func (e *lifecycleEnv) retryGeneration() {
	p := e.plan(BehaviourEcho)
	o := e.options(p)
	o.Retry = pluginhost.RetryPolicy{MaxAttempts: 2, Backoff: time.Millisecond}
	registry := &scopeRegistry{}
	registry.callbacks(&o)
	var owners []pluginhost.Owner
	var children []*pluginhost.Process
	// Scope for the retry already exists inactive; inspect previous owner's
	// absence rather than treating the candidate itself as leaked state.
	o.Callbacks.Activate = func(_ context.Context, owner pluginhost.Owner, child *pluginhost.Process) error {
		owners = append(owners, owner)
		children = append(children, child)
		if len(owners) == 1 {
			return &pluginhost.TransientError{Code: "temporary_activation"}
		}
		registry.mu.Lock()
		_, oldPresent := registry.scopes[owners[0]]
		registry.mu.Unlock()
		if owners[0] == owner || oldPresent {
			return errors.New("retry retained old scope/token")
		}
		return registry.activate(owner)
	}
	l := e.new(o)
	e.enable(l)
	if len(owners) != 2 || owners[1].OwnerGeneration <= owners[0].OwnerGeneration || l.IsCurrent(owners[0]) {
		e.t.Fatal("failed load retried without fresh authority")
	}
	e.gone(children[0])
}
func (e *lifecycleEnv) capturedCallback(l LifecycleDriver, registry *scopeRegistry, owner pluginhost.Owner) func() error {
	return func() error {
		if !l.IsCurrent(owner) || !registry.admit(owner) {
			return pluginhost.ErrDisabled
		}
		return nil
	}
}
func (e *lifecycleEnv) unclassifiedExit() {
	p := e.plan(BehaviourEcho)
	o := e.options(p)
	o.Retry = pluginhost.RetryPolicy{MaxAttempts: 3, Backoff: time.Millisecond}
	var mu sync.Mutex
	starts := 0
	o.Callbacks.Activate = func(context.Context, pluginhost.Owner, *pluginhost.Process) error {
		mu.Lock()
		starts++
		mu.Unlock()
		return nil
	}
	l := e.new(o)
	e.enable(l)
	_ = l.Current().Kill()
	e.await("terminal unclassified exit", func() bool { return l.Status().State == pluginhost.StateFailed })
	// Default backoff is short, so a wrong transient classification would have
	// replaced the process instead of entering terminal failure.
	mu.Lock()
	defer mu.Unlock()
	if starts != 1 || l.Current() != nil || l.Status().RetryAttempts != 0 {
		e.t.Fatal("unclassified exit restarted")
	}
}

func (e *lifecycleEnv) uncooperativeCallbacks() {
	for _, step := range []string{"revoke", "dispose", "activate"} {
		e.t.Run("uncooperative_"+step, func(t *testing.T) {
			child := &lifecycleEnv{t: t, h: e.h}
			p := child.plan(BehaviourEcho)
			o := child.options(p)
			o.CallbackTimeout = 50 * time.Millisecond
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			entered := make(chan struct{})
			var process *pluginhost.Process
			switch step {
			case "revoke":
				o.Callbacks.Revoke = func(context.Context, pluginhost.Owner) error { close(entered); <-release; return nil }
			case "dispose":
				o.Callbacks.Dispose = func(context.Context, pluginhost.Owner) error { close(entered); <-release; return nil }
			case "activate":
				o.Callbacks.Activate = func(_ context.Context, _ pluginhost.Owner, p *pluginhost.Process) error {
					process = p
					close(entered)
					<-release
					return nil
				}
			}
			l := child.new(o)
			started := make(chan error, 1)
			if step == "activate" {
				go func() { started <- l.Enable(context.Background()) }()
				<-entered
			} else {
				child.enable(l)
				process = l.Current()
			}
			stopped := make(chan error, 1)
			go func() { stopped <- l.Disable(context.Background()) }()
			select {
			case err := <-stopped:
				var report pluginhost.DisposalReport
				if !errors.As(err, &report) || !report.Incomplete {
					t.Fatal("uncooperative callback did not report quarantine", err)
				}
			case <-time.After(time.Second):
				unblock()
				<-stopped
				t.Fatal("uncooperative callback blocked disposal")
			}
			if step == "activate" {
				if err := <-started; err == nil {
					t.Fatal("late activation succeeded")
				}
			}
			child.gone(process)
			if !errors.Is(l.Enable(context.Background()), pluginhost.ErrQuarantined) {
				t.Fatal("pending callback permitted replacement")
			}
			unblock()
			child.await("callback reconciliation", func() bool { return l.AcknowledgeDisposal(context.Background(), l.Status().Owner) == nil })
		})
	}
}
