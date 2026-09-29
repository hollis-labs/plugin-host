package pluginhost_test

import (
	"os"
	"testing"

	"github.com/hollis-labs/plugin-host/pluginhosttest"
)

func TestMain(m *testing.M) {
	pluginhosttest.MaybeRunFixture()
	os.Exit(m.Run())
}
