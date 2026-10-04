# Changelog

All notable changes to plugin-host are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Write the entry for a release here BEFORE cutting its tag: the release workflow
refuses a tag whose CHANGELOG has no heading for it.

## Unreleased

### Fixed

- Status snapshots and formatted terminal errors retain bounded copies of stderr
  after host redaction, so clipped strings cannot retain an expanded allocation.
  Lifecycle recovery tests read the current process once per replacement poll.
- Supervisor retains the first health-kill cause and stops health polling after
  killing a child; a buffered probe cannot replace a timeout with ErrGone.

- Truncated stderr tails discard their possibly partial leading line only when
  non-blank text remains after the first LF, before secret and host redaction.
  Re-apply this rule after a final byte trim if secret redaction grows the text.
  Exact full windows that never truncated retain their first line unless that
  final trim is needed. Windows without an LF followed by non-blank text stay
  intact within the cap; partial key names can remain. CR-only separators are
  not line boundaries, and exact-boundary cuts may drop a complete leading line.
  Plugins must not log credentials.

### Added

- `Supervisor.Status`, `SupervisorStatus`, and shared `ExitStatus` expose typed
  restart exhaustion, terminal failure and the last reaped child's owner, exit
  information and bounded stderr tail. `LifecycleStatus.LastExit` retains the
  same diagnostics across automatic recovery; explicit attempt cycles reset it.
  Tails apply secret and host redaction and remain bounded after host redaction.
  Status summaries surface restart exhaustion and exit code/signal. Document
  host responsibility for children orphaned after host death without unload.

- Add a dormant typed host-service seam, authenticated bounded binding ledger,
  narrow renewal, shared execution budgets and cancellation/fencing. Reverse
  offers remain refused; negotiated transport/lifecycle hookup and child replay
  are subsequent gates (CW-20261003-0189 slice 3).

- Publication-ordered positive JS-safe IDs, strict direction-aware reply
  demultiplexing and pending-method result validation. Pending correlation is
  registered before the first byte; canceled unselected calls consume no ID.
- Two bounded writer lanes (32 frames/8 MiB each), one whole active write,
  four-control fairness and reserved cancellation capacity. Ordinary contention
  no longer drops `rpc/cancel`; controls get a fresh 100 ms write budget.
- `ErrAdmissionFull` reports immediate effect-free refusal at 16 ordinary/two
  lifecycle calls or a full writer lane. Whole writes have a five-second ceiling;
  closeable streams without native deadlines are interrupted on expiry.
- Raw Init profile offers and positive acknowledgements are refused, alongside
  typed Init/handshake refusal. Reverse execution and negotiation stay disabled.
- Manifest-generic SDK interop inventory scaffold with exact source/runtime/build
  receipts and pending owners; it records obligations, not replay successes.

- `MismatchError` carries bounded printable expected/actual identity and version
  metadata while retaining mismatch sentinels. Lifecycle failures preserve the
  nested redacted mismatch diagnostic and typed cause.
- `WithForwardBinding` carries an invocation's host-issued binding reference.
- Positive safe outbound IDs fail with `ErrRequestIDExhausted` instead of wrapping.
- Forward calls transmit remaining budgets and send host-owned cancellation controls.
- `ErrHealthInconclusive`, `HealthInconclusiveError`, and
  `HealthVerdict.Inconclusive` expose health replies without a verdict.
- `Conn.CancelDropped` counts cancellation controls not published completely.


- `Supervisor.PendingFactory` and `ErrInitFactoryPending` expose host factory
  work still running after cancellation, including through bounded Stop.
- Per-plugin `Lifecycle` with staged planning, compatibility checks, persisted
  generation issuance, enable/disable/reload, scope callbacks, dispatch fencing,
  finite classified retries and deterministic disposal reports/quarantine.
- `Stage`, `Failure`, `TransientError`, `DisposalReport`, and pure inclusive
  semantic-version bounds with explicit prerelease policy.
- Expected wire identity/version verification between init and load.
- Separate real-process lifecycle conformance harness, R19–R27, with declared
  owners, named host waivers and host-provided review/persistence adapters. The
  library run has no waivers.
- Exact-tuple disposal acknowledgement, optional lifecycle state persistence,
  per-generation cleanup history and safe-integer generation validation.

### Changed

- Pin the SDK's bounded admission/cancellation runtime. Tagged RPC IDs preserve
  string and numeric identity; only an absent ID denotes a notification.
- Unload is terminal and invokes cleanup once; default graceful stop allows six
  seconds for the SDK's five-second shutdown budget plus margin. Failed-start
  pre-Init unload refusal does not mark teardown incomplete.
- Health maps authored internal failures to unhealthy, malformed/protocol failures
  to protocol mismatch, and only SDK rate-limit/deadline replies to typed
  inconclusive probes. Silent local health timeouts count as unhealthy;
  host-cancelled probes leave the cached verdict unchanged. Inconclusive probes
  are retried by HealthGate and never trigger health kills. The SDK shares 16 execution slots; excess calls receive rate_limited.
- Deadline responses preserve their typed RPC cause/effect state and also match
  context.DeadlineExceeded, including unknown outcomes at the local deadline
  (3 ms tolerance for wire rounding and timer skew). Caller cancellation controls
  are best effort, bounded to 100 ms, and counted when dropped. Zero-byte drops
  leave the connection up; partial control writes retire it. Writers without
  SetWriteDeadline receive no cancel controls. Terminal unload sends
  no cancellation control. Notifications acquire no implicit forward deadline.
- Payload encoding precedes writer acquisition; remaining timeout is updated at
  publication in a ten-byte space-padded timeout slot without re-encoding opaque
  payloads under the writer. Typed-nil forward params are rejected locally.
- Optional hooks and reverse offers remain refused; duplex activation is deferred.
  Raw Conn.Call for plugin/init bypasses the typed entry-point refusal.


- Supervisor factory tuples share Lifecycle's generation ledger across controller
  recreation; completed disposal releases the active checkpoint without reusing
  a generation. Explicit ExpectedID must match incarnation.owner_id.
- Pin protocol 2 to the SDK pseudo-version until a tagged plugin-sdk release carries protocol 2.
- Default frames are 8 MiB including LF in both directions; profile offers
  are refused until host services and hooks are implemented. Lifecycle methods with deadlines send a context object with remaining timeout_ms; otherwise params are absent or empty, never null.

- Cooperative cancellation receives a bounded grace before pending quarantine;
  reload preflight timeouts preserve the serving generation and failure status.
- BeforeDisable and Supervisor classification are bounded; snapshot revisions
  fail closed at exhaustion. Exact report acknowledgement and revision conflicts
  have dedicated regressions and documented host storage requirements.
- Adopt SDK protocol 2 Init/Grant types directly. Validate/encode the complete
  Init before spawn, bind lifecycle incarnation/grants to each issued tuple, and
  validate acknowledgements before load. Protocol 1 has no fallback; optional
  reverse/hooks profile acknowledgements are refused until implemented.

- Unexpected exits are terminal unless explicitly classified transient;
  standalone Supervisor requires `ClassifyExit`. Failed restart handshakes
  only retry explicitly transient errors. Lifecycle owns its own single loop.
- Load-side and cleanup waits are bounded even for callbacks that ignore cancellation;
  late results are discarded, and cleanup/child reaping continue while quarantine
  blocks reuse. Report IDs support reconciliation before generation issuance.
- Disposal history retains all unresolved reports and a bounded completed tail;
  owner-keyed persistence preserves quarantine across host epochs, including an
  active checkpoint without completed teardown. Acknowledgement saves are bounded
  and release the operation gate.
- Disable fences survive queued Enable calls; replacement requires a fresh tuple.
- Process and Supervisor errors retain bounded redacted cause text and wrapped
  causes; process-only diagnostics omit generation zero. Classifier panics are
  contained, and exit classification identifies supervisor health kills.


- Breaking: Init uses Grants instead of Granted; Spawn requires complete Init
  authority and reports typed failures. The R12 response cap is 8 MiB.
- Standalone supervised restarts require InitFactory to issue fresh incarnation
  tuples and grants; a missing factory ends supervision. Client.Init validates
  results before returning them.
- Add FixtureInit and invalid-contract, profile-ack and duplicate-init fixture
  behaviors for protocol conformance tests.


## v0.1.2 — 2026-10-02

### Added

- `Spec.BeforeSpawn` allows a host to verify its approved bundle before every
  child launch, including supervised restart attempts. A refused check spawns
  nothing, preserves the error chain, and receives the cancellable launch context.

## v0.1.1 — 2026-10-01

### Added

- `Process.LoadInfo` preserves the acknowledgment from a successful handshake,
  including skipped registrations, without requiring a second load call.
  Returned registration slices are copied so callers cannot mutate stored state.
- `pluginhosttest.BehaviourLoadSkips` exercises a plugin's load-time opt-outs.

## v0.1.0 — 2026-09-29

### Added

- `Conn`: id-correlated JSON-RPC over plugin-sdk's newline framing, with one
  reader goroutine, ids from 1, `Notify`, a default timeout applied only when
  the context has no deadline, an outbound frame cap (`ErrFrameTooLarge`), and
  `ErrGone` for every waiter the moment the pipe ends. `Client` is the typed
  layer over plugin-sdk's structs.
- `Process`: `Spawn`, `Start`, `Handshake`, `Stop`, `Kill`, with an exact child
  environment (`InheritEnv` is the opt-in), a fresh process group killed as a
  group, a bounded redacted stderr tail, and a stop bounded by
  `UnloadTimeout + ReapTimeout`.
- `Supervise`: restart through a full handshake with backoff and a budget
  (`RestartPolicy`, `StableFor`), optional health-based kill, and no child
  installed after `Stop`.
- `HealthGate`: on-demand health with a cached verdict.
- `Tail` and `Redact`, copied from go-mcp's `supervise` package.
- `guard.Guarded`: panic, budget, caller-cancel and shutdown containment.
- `pluginhosttest`: the conformance suite (`Run`, `Harness`, `Waive`,
  requirements R01-R18) and the re-exec fixture plugin.

### Fixed

- Inbound lines are capped (`WithMaxInboundFrame`, default 64 MiB). A line over
  the cap is discarded while streaming in, without being buffered, and counted
  by `Conn.InboundDropped`; the connection survives. Previously a plugin
  writing a huge line with no newline grew host memory without bound.
- `Supervisor.Stop` no longer blocks when it races `Start`'s first handshake:
  the handshake is cancelled and every `Start` path that will not run the
  supervision loop releases `Stop`.
- `Conn.Notify` is bounded by the default call timeout as a write deadline, so
  a plugin that stops reading stdin cannot hold the write lock indefinitely.
