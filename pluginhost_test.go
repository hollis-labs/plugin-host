package pluginhost

import "testing"

func TestHello(t *testing.T) {
	got := Hello()
	want := "hello from pluginhost"
	if got != want {
		t.Errorf("Hello() = %q, want %q", got, want)
	}
}
