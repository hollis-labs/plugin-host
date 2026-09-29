# plugin-host

Host-side driver for [plugin-sdk](https://github.com/hollis-labs/plugin-sdk)'s stdio JSON-RPC protocol: spawn a plugin, handshake, make id-correlated calls, restart it when it crashes, and stop it within bounded time.

## Status

**Pre-release.** This project is unreleased, not deployed, and has no outside consumers. It's being built in the open: the code, the docs, and this README describe what exists today, not a pitch for what's planned. Interfaces and behavior change without notice, and there are no compatibility guarantees yet.

## Install

```sh
go get github.com/hollis-labs/plugin-host
```

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	pluginhost "github.com/hollis-labs/plugin-host"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func main() {
	ctx := context.Background()

	// The plugin is any executable that calls subprocess.Serve.
	p, err := pluginhost.Start(ctx, pluginhost.Spec{
		ID:      "hello",
		Command: os.Args[1],
		Env:     pluginhost.InheritEnv(), // the child's environment is exact; inheriting is opt-in
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = p.Stop(stopCtx)
	}()

	res, err := p.Client().MCPCallTool(ctx, subprocess.MCPCallRequest{
		ToolName:  "echo",
		Arguments: map[string]any{"message": "hi"},
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", res.Content)
}
```

`Start` spawns the process, runs `plugin/init` then `plugin/load`, and returns a `*Process`. `Spawn` is the same without the handshake, for a host that builds its own `plugin/init` (for example with resolved secrets in `Config`). `Supervise` wraps a `Spec` and restarts the plugin through a full handshake with backoff. `guard.Guarded` runs an in-process plugin call under a panic and budget guard.

## Compatibility

The wire protocol is plugin-sdk's `subprocess.ProtocolVersion`, currently 1, and the handshake is exact: a plugin answering another protocol fails `Start`. A plugin-sdk `ProtocolVersion` bump is a major version change here. The module requires `go 1.26.6`, so a consumer must be at Go 1.26.6 or newer, and depends on plugin-sdk v0.5.0 and the standard library, nothing else. The process-group kill is unix-only; other platforms fall back to killing the one process, and a plugin that forks leaves its helpers behind there.

The exported API is pre-1.0 and unreleased; see [CHANGELOG.md](./CHANGELOG.md).

## Out of scope

- Registry, trust tiers, signature verification, catalogs and installers.
- Manifests and `plugin.yaml`, in any dialect.
- Capability vocabularies and grants: `Spec.Init.Granted` is passed through untouched.
- Secret resolution and secret stores: `Spec.Env` and `Spec.Init.Config` are the host's.
- Hosting compiled-in plugins (only the small `guard` package for panic and budget containment).
- CRUD-to-HTTP glue and any typed `mcp/list_tools`: plugin-sdk's `Serve` does not answer that method, so the library exposes only the raw `Conn.Call`.
- The plugin side of the protocol: that is plugin-sdk.

## Development

```sh
gofmt -l .
GOWORK=off go vet ./...
GOWORK=off go test -race -count=1 ./...
```

The tests start real child processes over the real wire: the test binary re-executes itself as the fixture plugin, so no `go build` is involved. Any host can check its own client against the same requirements (R01-R18) with `pluginhosttest.Run`; see the `pluginhosttest` package documentation.

CI (`.github/workflows/check.yml`) is the full gate.

## License

MIT — see [LICENSE](./LICENSE).
