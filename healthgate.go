package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

const defaultHealthTTL = time.Second

// HealthVerdict is one plugin/health outcome.
type HealthVerdict struct {
	// OK is the plugin's own answer.
	OK bool
	// Message is the plugin's message, or the probe error's text when the
	// plugin could not be asked.
	Message string
	// Reachable is false when the probe itself failed (the plugin is gone,
	// timed out, or answered garbage), true when the plugin said what OK
	// reports. "Unreachable" and "the plugin said unhealthy" are different
	// facts and callers may want to treat them differently.
	Reachable bool
	// Checked is when the verdict was made.
	Checked time.Time
}

// HealthGate is an on-demand health check with a cached verdict (Tangent's
// design): nothing runs in the background, [HealthGate.Probe] asks the
// plugin and caches the answer for the TTL, and [HealthGate.Check] refuses on
// the last cached verdict without a round trip. Dispatch paths call Check;
// only an explicit probe advances the verdict, so an unhealthy plugin can
// always be probed back to healthy.
//
// The SDK answers plugin/health {ok:true} for a plugin that never
// implemented HealthChecker, so "healthy" and "never implemented" cannot be
// told apart here.
type HealthGate struct {
	probe func(context.Context) (subprocess.HealthResult, error)
	ttl   time.Duration
	now   func() time.Time

	mu   sync.Mutex
	last HealthVerdict
}

// NewHealthGate returns a gate over probe, typically (*Client).Health. ttl
// is how long a verdict is served from cache; zero means one second, and a
// negative value disables caching.
func NewHealthGate(probe func(context.Context) (subprocess.HealthResult, error), ttl time.Duration) *HealthGate {
	if ttl == 0 {
		ttl = defaultHealthTTL
	}
	return &HealthGate{probe: probe, ttl: ttl, now: time.Now}
}

// Probe returns the cached verdict if it is younger than the TTL, else asks
// the plugin and caches the answer. The round trip holds no lock, so a slow
// probe never blocks Check; two callers racing past the cache may each send
// one plugin/health, which is bounded and cheaper than serializing them.
func (g *HealthGate) Probe(ctx context.Context) HealthVerdict {
	g.mu.Lock()
	if g.ttl > 0 && !g.last.Checked.IsZero() && g.now().Sub(g.last.Checked) < g.ttl {
		cached := g.last
		g.mu.Unlock()
		return cached
	}
	g.mu.Unlock()

	var verdict HealthVerdict
	result, err := g.probe(ctx)
	if err != nil {
		var rpcErr *subprocess.RPCError
		verdict = HealthVerdict{Message: err.Error(), Reachable: errors.As(err, &rpcErr)}
	} else {
		verdict = HealthVerdict{OK: result.OK, Message: result.Message, Reachable: true}
	}
	verdict.Checked = g.now()

	g.mu.Lock()
	g.last = verdict
	g.mu.Unlock()
	return verdict
}

// Check returns nil until a probe has produced a bad verdict, then an error
// wrapping [ErrUnhealthy] for as long as that verdict stands (an unreachable
// plugin counts as bad). It never probes. A gate that has never probed, or
// was just reset, reports healthy: unknown is not a refusal.
func (g *HealthGate) Check() error {
	g.mu.Lock()
	last := g.last
	g.mu.Unlock()
	if last.Checked.IsZero() || last.OK {
		return nil
	}
	if !last.Reachable {
		return fmt.Errorf("%w: could not be asked: %s", ErrUnhealthy, last.Message)
	}
	return fmt.Errorf("%w: %s", ErrUnhealthy, last.Message)
}

// Reset forgets the verdict. Call it after a restart: the last verdict was
// about a process that no longer exists.
func (g *HealthGate) Reset() {
	g.mu.Lock()
	g.last = HealthVerdict{}
	g.mu.Unlock()
}
