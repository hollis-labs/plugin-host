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
