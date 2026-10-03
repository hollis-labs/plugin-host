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
	"github.com/hollis-labs/plugin-sdk/capability"
)

func main() {
	ctx := context.Background()
	hostEpoch, err := pluginhost.NewHostInstance()
	if err != nil { log.Fatal(err) }
	generations := &pluginhost.MemoryGenerationStore{}
	generation, err := generations.Next(ctx, hostEpoch, "hello")
	if err != nil { log.Fatal(err) }

	// Issue hostEpoch once per host process and persist a fresh generation
	// before this spawn. The plugin executable calls subprocess.Serve.
	p, err := pluginhost.Start(ctx, pluginhost.Spec{
		ID:      "hello",
		Command: os.Args[1],
		Env:     pluginhost.InheritEnv(), // inheriting is opt-in
		Init: subprocess.InitParams{
			PluginDir: "/plugins/hello", DataDir: "/data/hello", CacheDir: "/cache/hello",
			HostInfo: subprocess.HostInfo{Version: "1.0.0", Protocol: subprocess.ProtocolVersion},
			Incarnation: capability.RuntimeIdentity{HostInstance: hostEpoch, OwnerID: "hello", OwnerGeneration: generation},
			Grants: capability.GrantSet{},
		},
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

`Start` spawns the process, runs `plugin/init` then `plugin/load`, and returns a `*Process`. `Spawn` validates the complete SDK Init payload before starting a child, leaving the handshake to the host. Required directories, host version and incarnation must be provided. Zero protocol/contract default to 2/1, config defaults to `{}`, and nil grants encode as `[]`. `Supervise` wraps a `Spec` and restarts explicitly classified transient exits through a full handshake with backoff. `Lifecycle` adds staged planning, generation-owned enable/disable/reload and cleanup callbacks; see [the lifecycle contract](docs/lifecycle.md). `guard.Guarded` runs an in-process plugin call under a panic and budget guard.

## Compatibility

The wire protocol is plugin-sdk's `subprocess.ProtocolVersion`, currently 2, and the handshake is exact: a plugin answering another protocol fails `Start`. Protocol-1 plugins fail with a typed load-stage protocol failure; there is no fallback. Init acknowledges capability contract 1 and exact identity/version before load. Optional host services/hooks are unimplemented; valid supplied offers can be visibly declined, but positive acknowledgements fail before load. A plugin-sdk `ProtocolVersion` bump is a major version change here. The module requires `go 1.26.6`, so a consumer must be at Go 1.26.6 or newer, and depends on plugin-sdk and the standard library. The process-group kill is unix-only; other platforms fall back to killing the one process, and a plugin that forks leaves its helpers behind there.

The exported API is pre-1.0 and unreleased; see [CHANGELOG.md](./CHANGELOG.md).

## Out of scope

- Registry, trust tiers, signature verification, catalogs and installers.
- Manifests and `plugin.yaml`, in any dialect.
- Capability policy and enforcement: SDK `Spec.Init.Grants` is structurally validated and passed through; discovery grants do not authorize operations.
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

The tests start real child processes over the real wire: the test binary re-executes itself as the fixture plugin, so no `go build` is involved. Any host can check its own client against the same requirements (R01-R18) with `pluginhosttest.Run`, and its lifecycle adapter with `pluginhosttest.RunLifecycle` (R19-R27); see the `pluginhosttest` package documentation.

CI (`.github/workflows/check.yml`) is the full gate.

## License

MIT — see [LICENSE](./LICENSE).

Frames default to 8 MiB including LF in both directions. Oversized incoming
lines are discarded without losing the connection; offers cannot advertise a
larger frame. Lifecycle methods send absent or empty params, never null. The
SDK is pinned by pseudo-version until the first plugin-mcp tag.
