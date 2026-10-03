# Plugin lifecycle

A `Lifecycle` manages one plugin ID, separate desired intent and actual state,
serialized enable/disable/reload, and generation-owned resources. It uses
individual `Process` instances and owns its one retry loop. It never nests a
`Supervisor`. Process-only hosts can keep using `Start` or `Supervise`.

This implementation consumes the SDK's protocol-2 `subprocess.InitParams` /
`InitResult` and `capability.GrantSet` directly. Capability contract 1 and the
issued incarnation are validated before any child starts. Protocol 1 has no
fallback. Reverse services and hooks are unimplemented: the controller adds no
profile offers, and a positive acknowledgement is refused before load. A valid
host-supplied optional offer may be visibly declined (omitted acknowledgement).

## Host adapter

Create one random epoch with `NewHostInstance` per host process. Share that
string and one `GenerationStore` across controllers. `Next` must atomically
persist a strictly increasing generation for `(host_instance, owner_id)`;
controller recreation must not reset it. The zero-value `MemoryGenerationStore`
is suitable within one process; a process restart requires a new epoch.
Values must be positive and at most `MaxOwnerGeneration` (2^53 - 1). The
library retains a per-owner high-water mark across controller recreation and
rejects constant, decreasing or overflowing store results.
Persistence failures stop before scope preparation or spawn. A consumed number
is never reused, including after a failed load.

```go
// hostEpoch and generations are shared, process-owned values.
controller, err := pluginhost.NewLifecycle("example", pluginhost.LifecycleOptions{
    HostInstance: hostEpoch,
    Generations: generations,
    Callbacks: pluginhost.LifecycleCallbacks{
        Plan: hostPlan,
        Resolve: hostResolve,
        CheckCompatibility: hostCompatibility,
        PrepareScope: hostPrepare,
        Activate: hostActivate,
        Revoke: hostRevoke,
        Dispose: hostDispose,
    },
})
```

Callbacks consume immutable normalized `Plan` values; the controller snapshots
slices and config maps before load. Do not mutate returned maps/slices concurrently.
Host callbacks choose reviewed/pinned artifacts, configuration, dependency order,
capabilities, installation trust and exact environment. The library imports no
manifest or registry. `BeforeSpawn` rechecks reviewed bytes immediately before
execution, or hosts can select an immutable verified snapshot.

Ordered stages are `plan`, `resolve`, `compat`, `load`. Plan/resolve/compat cannot
spawn a child. During load:

1. Persist a fresh generation and form `Owner`'s canonical tuple.
2. `PrepareScope(ctx, owner, plan)` creates an inactive scope and returns the
   generation-bound `Spec`. It can construct fresh host-owned Init/config/env
   after generation issuance. The plan passed to PrepareScope already carries
   the canonical SDK `Incarnation`; the returned Spec must preserve it. Every
   grant must use that same tuple, including fresh generation on retry/reload.
   Opaque `Scope` JSON and verified `Identity` stay separate from authority.
   It must preserve reviewed identity/version/artifact.
3. Validate and encode the complete SDK Init before spawn, initialize, verify
   actual protocol 2/capability contract 1 and expected identity/version, then
   load. `Spec.ExpectedID` and `ExpectedVersion` enforce wire checks before load;
   `Spec.ID` is the diagnostic label. Lifecycle defaults ExpectedID to its ID, rejects a different planned ID, and
   requires a strict expected plugin version during compatibility preflight.
   Wire versions must match exactly, including build metadata. Scope preparation
   cannot overwrite the reviewed identity/version expectations.
4. `Activate` publishes the prepared registrations. A canceled/superseded
   activation is disposed, never installed as the callable generation.

`Failure` identifies plugin, generation, stage, step and safe code, with `Unwrap`
for `errors.Is`/`errors.As`. Process/start/handshake and Supervisor messages include bounded redacted cause
text; process-only errors omit a generation label. Other lifecycle callback
messages expose safe labels, with causes available through Unwrap. Handshake
failures retain the bounded redacted stderr tail. Hosts
must keep identifier/code labels safe and redact underlying causes before display.
Cleanup reports never replace the original load failure.

## Operations and admission

Enable is idempotent when running. Failure leaves desired enabled but actual
failed. Hosts persist durable intent themselves, before acknowledging it to
operators. Plan can enforce durable enable preference; `BeforeDisable` and
`BeforeReload` can enforce dependency and persistence refusal using
`ErrDependency` or another wrapped error. A refusal leaves the live generation
and desired intent untouched. No automatic dependency inference/cascade occurs.

Disable fences the controller immediately after its host preflight, cancels
pending start/backoff, then serializes teardown. Its monotonic fence survives
a later Enable; that Enable waits for accepted teardown and starts a new tuple.
A canceled queued Enable leaves desired intent unchanged. Disable wins over a handshake
or activation completing late. Repeated disable returns the stored report,
without repeating cleanup or starting another child. Accepted teardown completes
with independent bounded cleanup contexts even if the caller context expires.
BeforeDisable is an admission preflight that can overlap a pending load; make
that callback concurrency-safe. Controller operations serialize; an isolated timed-out callback may still run
while teardown proceeds. Host callbacks must be tuple-bound and safe to overlap
Revoke/Dispose with timed-out preparation/activation and each other.

Reload preflights plan/resolve/compat while the old generation serves. A preflight
failure preserves it. After successful preflight, revoke/dispose the old generation
before loading a fresh one: callable generations do not overlap. Post-teardown
failure leaves unavailable; restoration is an explicit new operation.

`Status` exposes intent, actual state, owner, last/origin failure, retry attempts,
exhaustion, the latest disposal report and bounded `Disposals` history. Unresolved
incomplete reports remain until acknowledgement; only the latest 32 completed
or acknowledged reports are retained. Persisted input/output is capped at 128
reports; oversized or invalid records fail closed with `ErrInvalidLifecycleRecord`.
Successful activation clears originating failure; cleanup preserves each old
report when a replacement also fails. `Current` only returns a running generation.
Captured callbacks must check `IsCurrent(owner)` at actual dispatch and use the
host's own tuple-bound admission/draining gate. Checking only when capturing a
callback is insufficient; no Go library can undo an already committed effect.
Never route an old tuple to a replacement with the same plugin ID.

Load-side callbacks run without the state mutex with bounded waits
(`CallbackTimeout`, default 10s); accepted Disable cancels the wait immediately.
Late Plan/Resolve/Prepare results are discarded. Timed-out callbacks are recorded
as incomplete, and any spawned child is stopped before replacement.
Callbacks may read Status/IsCurrent. Do not
synchronously call Enable/Disable/Reload from resource callbacks: operations
serialize and a recursive operation cannot complete until its caller returns.
Unrelated controllers run independently. The host must enforce one active
controller per plugin ID; the generation store prevents token reuse, not two
controllers publishing that ID concurrently.

## Disposal and recovery

Fence library admission, invoke Revoke, cancel the generation lifetime, stop the
child through bounded unload/close/kill/reap, then Dispose. Revoke closes host
admission, detaches published entries, revokes credentials and cancels or drains
admitted host work within its deadline before returning. Dispose removes the
remaining resources after process shutdown, in reverse acquisition order. Both callbacks must be idempotent and target only the given tuple.
Host shutdown calls Disable in reverse dependency order. Secrets/settings/user
records survive unless a separately authorized uninstall policy removes them.

Disposal runs once per prepared incarnation, including partial preparation,
failed init/load/activation, crash, reload and disable. An unload error does not
skip host cleanup. Error/panic/timeout in a cleanup callback also does not skip
later steps. `DisposalReport.Unwrap` exposes ordered cleanup errors. Failed host
cleanup or unreaped children mark the report incomplete and quarantine replacement;
a failed unload with successful reaping and host cleanup remains safely fenced.
Repeated disable preserves the quarantine/report.

Cleanup callbacks run in isolated goroutines with bounded waits (default 2s
each). Revoke and Dispose must tolerate concurrent execution after a timeout.
A callback ignoring context cannot delay child shutdown or remaining
cleanup. It can continue running after timeout; the report stays incomplete
and quarantine prevents replacement. `AcknowledgeDisposal(ctx, owner)` requires
the exact report tuple and refuses while any timed-out callback is still running.
Pre-generation callback failures have generation zero and an independent
`DisposalReport.ID`; reconcile them with `AcknowledgeReport(ctx, id)` instead of
an authority tuple. Report IDs are never authority tokens.
After host reconciliation, acknowledgement clears that report's quarantine while
retaining its historical failures; it does not enable the plugin.

The library retains reports and quarantine across controller recreation within
the host process. `LifecycleOptions.StateStore` optionally persists the
`LifecycleRecord` high-water mark and disposal history, keyed by stable owner ID.
Quarantine survives a new host epoch; the generation watermark is restored only
for its recorded epoch. An active-incarnation checkpoint is saved before scope
preparation, so a host restart without completed disposal also restores quarantine.
Implementations must atomically reject older revisions so a late save cannot undo newer state.
Acknowledgement must persist successfully before clearing quarantine. Its Save
is bounded by CleanupTimeout and does not hold the operation gate. A reserved
revision prevents a late save from overwriting newer disposal/reservation state;
concurrent state changes return `ErrLifecycleStateChanged` for host retry. The
host still permits only one active controller per owner; acknowledgement never
authorizes concurrent controllers. Hung store callbacks remain tracked and
prevent repeated reads/writes until they return.
Use `MemoryLifecycleStateStore` for process-local adapters; durable hosts supply
their own store. A new controller is never a cleanup repair.

Retries are off by default. `RetryPolicy.MaxAttempts > 1` gives a finite budget,
including initial attempt and crash replacements; explicit enable/reload resets
it, successful recovery does not. Backoff defaults to 100ms. Only a typed
`TransientError` with a safe code can authorize retry; timeouts, cancellation,
protocol/identity/version mismatch and unknown errors remain terminal. Every
retry completes disposal and issues a new tuple first. Incomplete cleanup blocks
retry. `ClassifyExit` explicitly classifies unexpected exits; absent classification
is terminal. Disable cancels backoff and no intentional stop retries.

Standalone `Supervisor` now also requires a typed-transient `ClassifyExit` result
for crash restart. A failed replacement handshake is retried only if its own
cause is explicitly transient. Its `RestartPolicy` still supplies budget/backoff,
but a policy alone no longer opts unknown failures into restarting. Never wrap
Lifecycle in Supervisor or add a second host retry loop. Classifier panics
become `ErrCallbackPanic`; `ExitInfo.SupervisorInitiatedKill` distinguishes health
monitor kills from spontaneous exits. Terminal diagnostics preserve the
classification reason.

## Version gates and conformance

`VersionRequirement` accepts a resolved host contract or engine version and
inclusive `VersionBounds`. Missing one side is unbounded; missing both, invalid
semantic versions, min greater than max, unresolved versions and out-of-range
values fail. Numeric/prerelease order follows SemVer; build metadata does not
change precedence. Prerelease acceptance requires explicit `AllowPrerelease`.
Local/dev and 0.x plugins use the same checks. Bounds are directly convertible
from a manifest's inclusive min/max pair. Host CheckCompatibility rejects
unknown required host/engine names; the library does not invent an engines
manifest field or execute plugin code to obtain a runtime version. A host
adapting the SDK execution contract also checks exactly one engine matching
the selected runtime and supplies a separate Node `Min: "22.0.0"` requirement
independent of the declared range. A max-only declaration cannot bypass this
baseline. Native binary versions describe the host's native-runner contract,
not the plugin version or compiler. The controller verifies actual protocol 2; the host still validates its manifest
and resolves actual engine/host versions before supplying a Plan.

`pluginhosttest.Run` retains R01–R18 for process drivers. `RunLifecycle` adds a
a separate harness with real children. Each requirement declares its library
or host owner; `LifecycleWaive(id, reason)` prints and skips a named host waiver.
The library runs all requirements with zero waivers:

| Requirement | Coverage |
| --- | --- |
| R19 | Ordered stages, typed stage/step/cause, pre-spawn refusals, wire identity/version |
| R20 | Inclusive bounds, numeric/prerelease ordering, invalid/dev/0.x versions |
| R21 | Unknown/permanent refusal once, finite transient attempts, canceled backoff |
| R22 | Partial scope and real child rollback after init/load/activation failure |
| R23 | Tuple dispatch fence, canceled in-flight calls, idempotent disable |
| R24 | Preflight retains old, fresh reload token, post-teardown failure unavailable |
| R25 | Unload error/wedge, revoke panic/error/timeout, remaining cleanup, quarantine |
| R26 | Crash disposal/replacement, finite crash budget, late activation fence |
| R27 | Host-provided review/digest/persistence refusal adapters, changed bytes, controller recreation |

Hosts supply R27 cases through `LifecycleHarness.HostAdapterCases`; the
library explicitly supplies `SyntheticHostAdapterCases` to certify its seam.
R22/R23 use staged registration rollback and a captured callback dispatch gate.
Each application must test its
actual registrations, review, durable preference and credential implementation.
This change includes no application adoption, reverse RPC, hook dispatch, release
or tag. Hosts requiring a service profile must refuse the visible decline before
activation; neither profile is available from this driver.

Cancelled callbacks have a 25ms grace to return before being tracked as still
running. Late results never activate a generation. A failed reload preflight
retains the serving generation and records LastFailure; pending candidate work
blocks another reload while serving dispatch and idempotent Enable remain valid.
BeforeDisable is bounded by CallbackTimeout and refuses before changing intent.
Persisted revisions are bounded by MaxLifecycleRevision; invalid or exhausted
ordering fails closed instead of wrapping. Supervisor exit classification is
bounded by ClassifyTimeout (default 10s) and cancelled by Stop.

Hosts running lifecycle conformance must additionally test exact report-ID
acknowledgement with multiple unresolved reports, durable quarantine and active
checkpoints across host epochs, acknowledgement Save failures/timeouts, and
revision conflicts with concurrent disposal. Use the actual storage adapter;
synthetic persistence cases cannot certify these durable guarantees.


The base frame default is 8 MiB including LF both ways; host service offers
may advertise at most that size. Lifecycle calls omit params or encode {}.
Process conformance R12 checks a 7 MiB response, rejects a 9 MiB response
and request, and verifies the connection remains usable. Tagged RPC identifiers
and duplex transport belong to a later SDK adoption.
