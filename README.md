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
		stopCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
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

`Start` spawns the process, runs `plugin/init` then `plugin/load`, and returns a `*Process`. `Spawn` validates the complete SDK Init payload before starting a child, leaving the handshake to the host. Required directories, host version and incarnation must be provided. Zero protocol/contract default to 2/1, config defaults to `{}`, and nil grants encode as `[]`. `Supervise` requires an `InitFactory` for restarts and wraps a `Spec` to restart explicitly classified transient exits through a full handshake with backoff. `Lifecycle` adds staged planning, generation-owned enable/disable/reload and cleanup callbacks; see [the lifecycle contract](docs/lifecycle.md). `guard.Guarded` runs an in-process plugin call under a panic and budget guard.

Calls to SDK forward methods carry `context.timeout_ms` from the remaining local
call budget. An existing shorter DTO budget narrows the call; writer wait consumes
it. `WithForwardBinding` carries a host-issued reference without creating authority.
With no deadline and a disabled connection default, no wire deadline is invented.
Caller cancellation attempts an absent-ID `rpc/cancel` with `request_owner: host`.
It is best effort: a busy writer or a stream without enforceable write deadlines
skips the control and increments `Conn.CancelDropped`, preserving other calls.
Control writes have a 100 ms bound and never close the connection on failure.
Notifications retain supplied context metadata but acquire no wire timeout from
their local write budget. Terminal unload never sends cancellation control.
Deadline replies retain `*subprocess.RPCError` and its effect state while also
matching `context.DeadlineExceeded` through `errors.Is`.
Outbound IDs are positive safe integers and fail at exhaustion instead of wrapping;
incoming tagged IDs keep zero, integers and strings distinct.

`plugin/unload` is terminal: SDK Serve drains and invokes cleanup once before
replying and exiting. The default graceful stop budget is six seconds, allowing
its five-second shutdown budget plus margin. A shorter caller context or explicit
budget can force teardown sooner. Pre-Init unload refusal is tolerated during
failed-start cleanup. An authored internal Health failure is unhealthy with its
RPC cause retained. Invalid request/method/params and malformed Health results
are protocol failures. SDK rate limits and deadlines return
`*HealthInconclusiveError`: neither healthy nor unhealthy, never counted toward
`KillAfterUnhealthy`. They break the supervisor's consecutive failure streak.
`HealthGate` retries these probes immediately and does not refuse dispatch on
an inconclusive verdict. The SDK shares 16 execution slots across ordinary calls
and Health; excess concurrent calls receive typed `rate_limited` failures.

## Compatibility

The wire protocol is plugin-sdk's `subprocess.ProtocolVersion`, currently 2, and the handshake is exact: a plugin answering another protocol fails `Start`. Protocol-1 plugins fail with a typed load-stage protocol failure; there is no fallback. Init acknowledges capability contract 1 and exact identity/version before load. Optional host services/hooks are unimplemented; supplied offers are refused before spawn, and positive acknowledgements fail before load. A plugin-sdk `ProtocolVersion` bump is a major version change here. The module requires `go 1.26.6`, so a consumer must be at Go 1.26.6 or newer, and depends on plugin-sdk and the standard library. The process-group kill is unix-only; other platforms fall back to killing the one process, and a plugin that forks leaves its helpers behind there.

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

Frames default to 8 MiB including LF in both directions. Oversized incoming
lines are discarded without losing the connection; host-services and hooks offers are refused before spawn. Lifecycle methods with a deadline send `{"context":{"timeout_ms":N}}`; without a deadline they send absent or empty params, never null. The
SDK is pinned by pseudo-version until a tagged plugin-sdk release carries protocol 2.

Standalone `Supervise` callers must supply `SuperviseOptions.InitFactory` for
restarts. It runs on each attempt (including the first), with a monotonically
increasing attempt number; the host issues a fresh generation and grants.
Without a factory an unexpected exit is terminal. `Lifecycle` owns and
replaces `Spec.Init.Incarnation` with its issued tuple.


## License

MIT — see [LICENSE](./LICENSE).
