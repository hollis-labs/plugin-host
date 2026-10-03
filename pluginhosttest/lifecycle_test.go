package pluginhosttest_test

import (
	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"testing"
)

type lifecycleHarness struct{}

func (lifecycleHarness) NewLifecycle(id string, o pluginhost.LifecycleOptions) (pluginhosttest.LifecycleDriver, error) {
	return pluginhost.NewLifecycle(id, o)
}
func TestLifecycleConformance(t *testing.T) { pluginhosttest.RunLifecycle(t, lifecycleHarness{}) }

func (lifecycleHarness) HostAdapterCases(t *testing.T, p pluginhost.Plan, o pluginhost.LifecycleOptions) []pluginhosttest.LifecycleAdapterCase {
	return pluginhosttest.SyntheticHostAdapterCases(t, p, o)
}
func TestLifecycleHostWaiversArePrinted(t *testing.T) {
	var opts []pluginhosttest.LifecycleOption
	for _, id := range []string{"R19", "R20", "R21", "R22", "R23", "R24", "R25", "R26", "R27"} {
		opts = append(opts, pluginhosttest.LifecycleWaive(id, "explicit host test exception"))
	}
	pluginhosttest.RunLifecycle(t, lifecycleHarness{}, opts...)
}
