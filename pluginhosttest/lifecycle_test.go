package pluginhosttest_test

import (
	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-host/pluginhosttest"
	"os"
	"os/exec"
	"strings"
	"testing"
)

type lifecycleHarness struct{}

func (lifecycleHarness) NewLifecycle(id string, o pluginhost.LifecycleOptions) (pluginhosttest.LifecycleDriver, error) {
	return pluginhost.NewLifecycle(id, o)
}
func TestLifecycleConformance(t *testing.T) {
	report := pluginhosttest.RunLifecycle(t, lifecycleHarness{})
	if len(report.Waived) != 0 || len(report.Executed) != 9 {
		t.Fatalf("library conformance requires every requirement without waivers: %+v", report)
	}
}

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

func TestBlankLifecycleWaiverIsRejected(t *testing.T) {
	command, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(command, "-test.run=^TestBlankLifecycleWaiverChild$") //nolint:gosec // G204: reexecute this test binary only
	cmd.Env = append(os.Environ(), "PLUGINHOSTTEST_BLANK_WAIVER=1")
	output, err := cmd.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "invalid lifecycle waiver") {
		t.Fatalf("blank waiver accepted: %v %s", err, output)
	}
}
func TestBlankLifecycleWaiverChild(t *testing.T) {
	if os.Getenv("PLUGINHOSTTEST_BLANK_WAIVER") != "1" {
		t.Skip("subprocess helper")
	}
	var opts []pluginhosttest.LifecycleOption
	for _, id := range []string{"R19", "R20", "R21", "R22", "R23", "R24", "R25", "R26", "R27"} {
		reason := "host exception"
		if id == "R23" {
			reason = " "
		}
		opts = append(opts, pluginhosttest.LifecycleWaive(id, reason))
	}
	pluginhosttest.RunLifecycle(t, lifecycleHarness{}, opts...)
}
