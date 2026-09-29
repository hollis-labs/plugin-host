package pluginhost_test

import (
	"fmt"

	pluginhost "github.com/hollis-labs/plugin-host"
)

func ExampleHello() {
	fmt.Println(pluginhost.Hello())
	// Output: hello from pluginhost
}
