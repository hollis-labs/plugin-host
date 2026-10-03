# plugin-host

Host-side driver for plugin-sdk's stdio JSON-RPC protocol: spawn, handshake, id-correlated calls, classified transient restart, bounded shutdown.

It is not a plugin registry, a trust model or an installer. Anything that decides which plugins exist, what they may do or what secrets they hold belongs to the host that imports this; the library carries `Spec.Env`, `Spec.Init.Config` and `Spec.Init.Granted` through untouched.

## Start Here

- `doc.go` — the package documentation and the list of contracts; read it before changing behavior.
- `conn.go`, `client.go` — `Conn` (framing, ids, correlation, `ErrGone`, frame cap) and the typed `Client`.
- `process.go`, `spec.go`, `pgroup_unix.go` — `Spawn`/`Start`/`Handshake`/`Stop`/`Kill`, the `Spec` defaults, and the process-group code (`pgroup_other.go` is the non-unix fallback).
- `supervisor.go`, `restart.go`, `healthgate.go` — classified transient restart with backoff, the budget, health kill, and the on-demand cached verdict.
- `tail.go`, `exit.go` — stderr `Tail`/`Redact` and exit classification, both copied from go-mcp's `supervise` (attribution in the file comments). Do not import go-mcp: its module requires the MCP SDK.
- `guard/` — `Guarded`, panic and budget containment for in-process plugin calls. It imports nothing.
- `pluginhosttest/` — the conformance suite (`Run`, `Harness`, `Waive`, requirements R01-R18 in `suite.go`) and the re-exec fixture plugin (`fixture.go`). `MaybeRunFixture()` must be the first line of `TestMain` in every test binary that uses it.
- `.github/workflows/check.yml` — the full CI gate; `release.yml` refuses a tag with no CHANGELOG heading.

## Commands

```sh
gofmt -l .
GOWORK=off go vet ./...
GOWORK=off go test -race -count=1 ./...
golangci-lint run --allow-parallel-runners
```

Always `GOWORK=off`: a parent `go.work` would hide a missing or wrong dependency. The full suite takes about 30 seconds because it runs real processes and the default budgets; `-short` skips the default-budget test (`TestDefaultBudgets`).

## Boundaries

- No `replace` directive in `go.mod` and no committed `go.work`: consumers cannot resolve either.
- Dependencies are the standard library and `github.com/hollis-labs/plugin-sdk` (pinned tag) only, and every dependency's `go` line must stay at or below this module's `go 1.26.6`. plugin-sdk must never import this module.
- No exported identifier names an application (Tangent, Nanite, Cerberus, Tachyon, Station). Those hosts adapt this library; the library does not know them.
- Never use `exec.CommandContext` for a plugin. It ties the plugin's life to the context that started it, which is usually a boot context, and kills a healthy plugin when it ends. `TestCancellingTheStartContextDoesNotKillThePlugin` and conformance R16 guard it.
- The child's environment is exactly `Spec.Env`; nil means empty, never inherit. `TestSpecEnvIsExact`, `TestNilEnvMeansEmptyNotInherit`.
- The child runs in its own process group (`Setpgid`) and is killed with `kill(-pid, SIGKILL)`, so grandchildren die too. R14 and R15 use a fixture that ignores EOF and SIGTERM and forks a grandchild holding the pipes. Do not weaken the fixture so a change passes.
- The child's stdout is an `os.Pipe`: the write end goes to `cmd.Stdout` and is closed in the parent right after `Start`, so a concurrent `cmd.Wait` cannot close the read side under a frame in flight. Do not switch to `cmd.StdoutPipe`. `TestAFinalFrameWrittenBeforeExitIsDelivered`, R11.
- The reader assembles lines from `bufio.Reader.ReadSlice('\n')` under an inbound cap (`WithMaxInboundFrame`, default 64 MiB: above the outbound 8 MiB because `Serve` writes responses uncapped, and 12 MiB responses are legal). An over-cap line is discarded as it streams in, never buffered, and counted by `Conn.InboundDropped`; the connection stays up. Junk, `null`, id 0, invalid UTF-8 and unknown ids are dropped, never fatal. `bufio.Scanner` and `json.Decoder` both end the read loop on bad input, and a plain `ReadBytes` lets a newline-less flood grow host memory without bound. `TestConnInboundMemoryStaysBoundedByTheCapNotTheLine`, `TestConnDropsFramesThatAreNotAnswers`, R10, R12.
- Every write is bounded: `Call` uses its context's deadline (or the default timeout) and `Notify` uses the default timeout as its write deadline, so a plugin that stops reading stdin cannot hold the write lock forever. `TestNotifyToAPluginThatStoppedReadingIsBoundedAndDoesNotWedgeCalls`.
- On EOF every waiter fails at once with `ErrGone`; nothing waits out a timeout for "the process is gone". A reply that raced the EOF is taken first (`TestConnDeliversTheFinalFrameWrittenBeforeEOF`).
- Outbound frames are capped at 8 MiB and refused with `ErrFrameTooLarge` before any write, because plugin-sdk's `Serve` exits on a longer line. Ids start at 1: id 0 is a notification and gets no reply.
- Stopping a plugin means closing its stdin. `plugin/unload` over RPC does not end `Serve`, and `Serve` calls the plugin's `Unload` again on exit, so a plugin's `Unload` runs twice on a clean stop. `Stop` is bounded by `UnloadTimeout + ReapTimeout` whatever the context says (`TestStopContextCancellationShortensButNeverLengthens`), and `UnloadTimeout` is one budget shared by the unload call and the wait for exit.
- A failed `Start` leaves no child: it is killed and reaped, and the error carries the redacted stderr tail. Stderr is read into a bounded `Tail`, never passed through, and secrets are scrubbed from every text built from it.
- A `Supervisor` never installs a child spawned while `Stop` was running (`TestStopDuringBackoffNeverResurrectsTheChild`, `TestStopDuringARestartHandshakeStopsTheNewChild`), and `Stop` must return even when it races `Start`'s first handshake: every path that will not run the loop calls `finish()` (`TestStopDuringTheFirstHandshakeReturnsAndLeavesNothingBehind`). Stop it through the `Supervisor`; a process stopped behind its back is restarted.
- Race-instrumented binaries sleep one second at exit unless `GORACE=atexit_sleep_ms=0`; `FixtureCommand` sets it. Without it every "graceful" stop looks slow under `-race`.
- A bare `select {}` in a fixture trips the runtime's deadlock detector and kills the process; use `sleepForever`. A wedge that dies on its own proves nothing.
- The conformance suite is the binding deliverable. This repo's own run (`TestConformance`) uses zero waivers; `Waive` is for hosts, and a waiver names its reason. Do not add a waiver, or loosen a requirement, to make this repo's run pass.
- `mcp/list_tools` has no typed method on purpose: the SDK declares it but `Serve` cannot answer it.
