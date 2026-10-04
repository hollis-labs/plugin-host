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
Calls enter bounded admission before encoding: 16 ordinary calls and two
lifecycle calls; a full admission or writer lane returns `ErrAdmissionFull`
without publication. One writer owns two FIFO lanes, each capped at 32 frames
and 8 MiB, plus one active whole frame. Lifecycle requests and controls use the
control lane; at most four controls overtake an eligible ordinary frame.
Each admitted call reserves capacity for its absent-ID `rpc/cancel` with
`request_owner: host`. Ordinary contention cannot drop it. A call canceled
before writer selection consumes no ID and sends no control. Terminal unload
never sends cancellation control. A selected cancellation has a fresh 100 ms
write bound. Zero-byte failures increment `Conn.CancelDropped` and preserve the
connection; partial frames retire it. Native write deadlines are preferred;
otherwise a closeable stream is closed on expiry. Custom non-closeable writers
must bound their own writes, and receive no cancellation controls when they
cannot enforce write deadlines. Ordinary and terminal writes have a five-second
ceiling, shortened by the call deadline.
Notifications retain supplied context metadata but acquire no wire timeout from
their local write budget. Terminal unload never sends cancellation control.
Deadline replies retain `*subprocess.RPCError` and its effect state while also
matching `context.DeadlineExceeded` through `errors.Is`. An `unknown_outcome`
reply at the local deadline (within 3 ms for wire rounding and timer skew) also
matches that sentinel without losing its typed effect state.
Outbound IDs are positive safe integers issued in physical publication order.
Pending correlation is registered before the first byte is written, and IDs
fail at exhaustion instead of wrapping. Incoming tagged IDs keep zero, integers and strings distinct. Typed-nil forward
params are rejected locally: forward params must be a non-null object. Encoding
reserves a ten-byte space-padded numeric timeout slot so publication can update
the remaining budget without re-encoding the opaque payload.

The single reader classifies method-bearing frames independently of outgoing
reply IDs. An inbound request can share an outgoing ID without answering that
call; it receives a bounded method-not-found refusal while profiles remain
unimplemented. Inbound notifications have no effect. Duplicate active inbound
IDs retire the connection; retired positive numeric IDs are rejected using a
single high-water mark, without a growing lifetime ID set. Inbound string request
IDs over 128 UTF-8 bytes retire the connection to keep refusals within terminal
credit; tagged reply IDs remain distinct. Replies require exact-case `jsonrpc: "2.0"`, an ID,
and exactly one `result` or `error`, with no extra envelope members or duplicate
keys (including nested keys). Invalid UTF-8 and unpaired surrogates are refused.
Successful replies are validated against the pending method's SDK result type;
a mismatched result returns `ErrProtocolMismatch` to that caller. Junk, malformed
envelopes and late replies are dropped without stopping the reader. An invalid
result in an otherwise unambiguous envelope fails its correlated call, preserving
typed Init failures.

`plugin/unload` is terminal: SDK Serve drains and invokes cleanup once before
replying and exiting. The default graceful stop budget is six seconds, allowing
its five-second shutdown budget plus margin. A shorter caller context or explicit
budget can force teardown sooner. Pre-Init unload refusal is tolerated during
failed-start cleanup. An authored internal Health failure is unhealthy with its
RPC cause retained. Invalid request/method/params and malformed Health results
are protocol failures. Local `ErrAdmissionFull` and SDK replies carrying `rate_limited` or
`deadline_exceeded` return
`*HealthInconclusiveError`: neither healthy nor unhealthy, never counted toward
`KillAfterUnhealthy`. They break the supervisor's consecutive failure streak.
`HealthGate` retries these probes immediately and does not refuse dispatch on
an inconclusive verdict. The SDK shares 16 execution slots across ordinary calls
and Health; excess concurrent calls receive typed `rate_limited` failures.
A local health timeout with no reply is unhealthy and counts toward the kill
threshold. A host-cancelled probe provides neither verdict and leaves the cached
health verdict unchanged.

`Supervisor.Status()` and `Lifecycle.Status()` provide copied, typed snapshots
for host health reports. `Exhausted` means an explicitly retryable failure used
up its restart budget; permanent failures do not set it. `LastFailure` preserves
the terminal cause, while `LastExit` identifies the last failed, reaped child's `Owner`,
`ExitInfo` (exit code, signal and error) and `StderrTail`. The tail uses existing
`Tail` secret redaction and `Spec.Redact`, then is capped to `Spec.StderrBytes`
(default 4096 bytes), including when the host redactor expands its output. Status
and formatted process errors own bounded copies, so discarded redactor output
does not remain allocated through a clipped string.
Automatic recovery retains the last exit; explicit Lifecycle Enable/Reload
starts a new attempt cycle and clears it. Status summaries include “restart
attempts exhausted” and the exit code/signal; hosts can display `StderrTail`
directly and use the typed fields without parsing those summaries.

Hosts are responsible for child cleanup if they die without unloading: a stuck
plugin can ignore closed stdin and remain orphaned.

## Compatibility

The wire protocol is plugin-sdk's `subprocess.ProtocolVersion`, currently 2, and the handshake is exact: a plugin answering another protocol fails `Start`. Protocol-1 plugins fail with a typed load-stage protocol failure; there is no fallback. Init acknowledges capability contract 1 and exact identity/version before load. Optional reverse RPC requires explicit `Spec.Reverse` plus a valid `Init.HostServices` offer and shared typed runtime. Nil keeps offer refusal; hooks remain refused. Typed entry points enforce these gates, including raw `Conn.Call`/`Notify` Init profile offers and
positive profile acknowledgements. A plugin-sdk `ProtocolVersion` bump is a major version change here. The module requires `go 1.26.6`, so a consumer must be at Go 1.26.6 or newer, and depends on plugin-sdk and the standard library. The process-group kill is unix-only; other platforms fall back to killing the one process, and a plugin that forks leaves its helpers behind there.

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
lines are discarded without losing the connection; reverse offers require explicit opt-in; hooks offers are refused before spawn. Lifecycle methods with a deadline send `{"context":{"timeout_ms":N}}`; without a deadline they send absent or empty params, never null. The
SDK is pinned by pseudo-version until a tagged plugin-sdk release carries protocol 2.

Standalone `Supervise` callers must supply `SuperviseOptions.InitFactory` for
restarts. It runs on each attempt (including the first), with a monotonically
increasing attempt number; the host issues a fresh generation and grants.
Without a factory an unexpected exit is terminal. `Lifecycle` owns and
replaces `Spec.Init.Incarnation` with its issued tuple.


## License

MIT — see [LICENSE](./LICENSE).

## Optional reverse RPC

Set `Spec.Reverse = &pluginhost.ReverseProfile{Runtime: runtime, Required: true}`
and supply an exact `Init.HostServices` offer. Share one `HostServiceRuntime`
across the host; offered business methods require their typed callbacks. The
module SDK pin remains unchanged; negotiated plugin runtimes must implement the
separately pinned optional profile. Nil is the default and refuses offers.
Optional decline keeps base protocol 2; required decline fails before activation.

Pass trusted narrowing with `WithHostBinding(ctx, HostBinding{GrantID: ...,
Scope: ...})` for a forward invocation. Leave Parent unset: the writer assigns
and registers its real selected ID before the first byte. Metadata/entropy/JSON
preparation happens outside Conn locks. Completion, cancellation and failed
publication retire parent authority immediately. The host still validates
scope/caller/backend policy and couples `HostCall.CheckCommit` to its effect
transaction; transport does not provide durable receipts or automatic retries.
Reverse request and method budgets start at local monotonic frame receipt;
parsing, dispatch and authority/admission waits consume that same deadline.
Expiry before backend entry returns `deadline_exceeded` with `not_started`.

`ReverseProfile.LifecycleBinding` supplies an explicitly granted `log.write`
narrowing for Init/Load and a separate finite Unload cleanup lease. Business
revocation is irreversible; cleanup never reopens that session. Lifecycle fences
at Disable/detach before host cleanup, and marks ownership ready after Activate.
Standalone `Start` marks it ready after Load; callers using `Spawn`/`Handshake`
or raw Conn call `ActivateHostServices` after successful host activation.
`RevokeHostServices` immediately fences business; Close/crash fences all leases.

Admission/credit limits currently have fixed floors: 16 forward, 8 reverse,
64 shared host, 2 control, 8 MiB frame and bytes per writer lane, depth 8. Offers
below these supported floors are refused rather than widened. Larger offers
clip to local capacities; positive whole-write timeouts clip to local 5s (the
SDK fixture's 1000ms works), with cancellation at min(100ms, write ceiling).
Host-service params/results remain capped at 1 MiB. A reader dispatches bounded
workers without running policy/backends; terminal metadata and credits transfer
to the writer before queue visibility and remain owned until physical outcome.

Focused normal-Serve Go/Node child tests build source
`ea8ec0dca862d0c7284cc6a130a4b27fb812ed21` (base
`d04ab2149506a96e8c54b58829f58ee480e0de41`) separately from the module pin.
Their private fd3 bridge uses bounded preseeded releases and actual Spawn/Conn/
cancel/Stop/Lifecycle paths. These tests are separate from manifest replay:
[isolated manifest adapter](scripts/interop-scaffold.md) records genuine
Go/Node/Deno execution receipts separately from its complete pending inventory.
`Conn.CallCorrelation` opts only CommandExecute/Health into transport correlation
without reverse authority. Its explicit finite observer is independent of the
remote wire budget; missing wire context stays missing despite defaults. The
raw deadline/absence replay preserves the literal non-authorizing selector and
records actual remote timer cause, selected IDs, wire, physical receipts and
natural exit. Historical `60d0b318` prepublication refusals and failed derived
timeouts remain separate evidence. Finite authority/lifecycle APIs are unchanged.
Raw repeated/absent-context Load capacity cases remain pending/incompatible;
writer/control and other mandatory obligations also remain open.
These receipts establish no full 0189/0172 gate or unqualified profile
conformance. Hook clients/composition remain unavailable pending SDK per-item
scope support. No consumer activation or release follows from these tests.
