# Typed host services and negotiated transport (0189 slices 3–4)

Host-owned authority bookkeeping supplies twelve fixed typed operations.
Explicit ReverseProfile now attaches this seam to Conn with an optional offer;
nil retains reverse refusal and hooks remain refused. Direct executor tests are
ledger tests; targeted normal-Serve child tests are distinct from full negotiated
manifest replay and SDK interoperability certification.

The existing SDK pin `5c663e7ce74c40ceceb95133c439396316850858` is retained.
Compared with the approved planning inputs SDK #56
`d04ab2149506a96e8c54b58829f58ee480e0de41` and #57
`90adf1f02ddffde708fa3784f0063bab5171462b`, the required
`subprocess/hostrpc_types.go`, `hostrpc_validate.go`, `hostrpc_errors.go` and
`capability/` sources have no diff. Those two SDK branches are open drafts;
this PR does not merge them or adopt their activation machinery.

## Host authority

Share one `HostServiceRuntime` across a host to enforce the 64 execution limit.
`OpenSession` requires a canonical owner, a host policy callback, explicit grants
and finite method ceilings. It generates a fresh connection identity. Empty
grants and missing service functions deny access. Eight callbacks may execute
per session. Logical cancellation retains a permit until the handler returns.
The host policy callback must cooperate with its finite context.

Host code registers the actual selected positive invocation ID and deadline,
then issues a binding narrowing the selected grant, verified caller/root and
host-held scope. Bindings contain at least 256 bits of randomness, cannot cross
connections/generations, and are bounded to 256 retained entries per session.
Registration has 18 live parent slots and one monotonic high-water mark; bounded
retired binding records preserve terminal classification until expiry. At most
32 terminal reverse-call records support own-tool terminal cancellation; older
unknown IDs receive no fabricated actor/type information. No lifetime ID sets.

No plugin DTO creates a parent, binding, caller, scope or ancestry. A mismatched
parent receives the uniform `parent_invalid` detail; only a terminal parent of
the authenticated same binding receives `parent_terminal`. Depth and deadlines
come from the ledger. The host supplies reviewed upstream origin owners; a
same-owner cycle or depth overflow refuses issuance. Method adapters must reject
additional indirect target/provider cycles against that verified origin chain.

`HostSession.OwnerReady` records host-owned lifecycle state only. Before it,
and under live Init/Load/explicit-Unload parents, only explicitly granted
`host/log` can execute. EOF/signal cleanup has no fabricated log parent. The host
must retire parents on completion, cancellation and failed publication, revoke
leases on scope loss, revoke the session before disable/reload/crash teardown,
and close/dispose it on disconnect. A fenced session cannot be made ready again.
These are host-controlled bookkeeping calls; explicit profile wiring now performs lifecycle/Conn hookup; nil remains inert.

Renewal keeps the same binding, shared remaining quotas (including zero), and
clips its lease to five minutes, root/parent and current grant limits. Grant
replacement fences changed authority even when a revision label was reused.
Revocation cancels descendants immediately and never waits for a backend.

## Correlation-only forward calls

`Conn.CallCorrelation(observer, method, params)` is an explicit opt-in for
`command/execute` and `plugin/health` after negotiated host activation. The
passed observer must have a positive finite deadline. Constructor/default
timeouts supply neither that observer nor remote wire context. An absent
`params.context` stays absent; a supplied positive timeout starts a separate
monotonic send budget before encoding/admission and clips at publication.
After a complete write, its expiry does not cancel the local observer: the
receiver owns its remote deadline terminal. Caller cancel and observer expiry
instead send actual directional controls with `caller_cancelled` and
`deadline_exceeded`, respectively.

These bounded transport records are not `HostSession` parents and mint no
bindings, grants, root, ancestry or depth. Syntactically valid opaque selectors
are transported unchanged, including fixture literals, but helper admission
still requires a genuine connection/binding/actual authority parent. An inert
selector can expose an SDK client object while every attempted host effect
remains unauthorized. Explicit `WithHostBinding`/`WithForwardBinding` conflicts
refuse. Configured lifecycle binding is never injected into this path.

Conn rechecks its host-owned incarnation/business fence at selected-ID
registration before bytes. Revoke/disable cancels queued/published correlation
calls; crash/close retires their transport receipts. Genuine finite authority
parents and isolated finite Unload log authority are unchanged. Incomplete
write deadline is the minimum observer/send budget/negotiated write bound:
proven zero bytes means no publication, partial writes fence with uncertainty,
and complete writes retain their physical receipt independently of local return.
Local errors never manufacture remote terminals or imply rollback; no automatic
retry follows a lost result. `CorrelationCallError` reports only closed local
validation refusals.

Every host reverse request and authority-bearing parent remains finite under
caller/root/parent/binding/grant/method/received minima. The explicit exception
is non-authority forward correlation with absent wire context, not an infinite
lease or a widened profile. The isolated adapter records exact raw deadline
and absence assertions separately from observer-cancellation and inert-selector
negative probes. Remaining mandatory lifecycle saturation/counter, fairness,
security/application and hook obligations keep full 0189/0172 acceptance open.

## Backend contract

`HostServices` has ten typed business callbacks and two typed optional vetoes
for ledger-owned renewal/cancellation. All Params/Results are SDK-owned. There
is no public method-string execution API or arbitrary registration. Policy gets
private copies of verified authority. Every method-specific backend validates
its targets, narrowed scope, payload/resource/tool schema and effect ceiling.
The opaque narrowing in `HostBinding.Scope` is supplied by trusted host code;
plugin-host cannot infer a descriptor's resource policy from JSON alone.

`HostCall.CheckCommit` checks current ledger/grant/deadline and calls current
host policy. An effect owner must serialize this guard with its actual commit
and policy/revocation transaction. The library does not supply a backend
transaction or claim that a check alone makes a later write atomic. Use
`HostCall.Charge` to reserve aggregate effects/tokens/decoded-byte use; encoded
params/results charge bytes automatically. Dimensions are shared across
concurrent children, and spent work is not refunded by cancellation or renewal.

Business receipts belong to services, keyed by owner ID, method, effective target
and operation key. Generation is provenance. They must serialize same-key
attempts, return the original same-input result, reject changed input, and
revalidate authority before exposing a receipt. Transport does not promise
exactly-once effects or automatic retries. Method-specific egress redirect/DNS/
proxy checks, secret custody, storage CAS/tombstones, event catalogs and MCP
schemas/tool receipts remain host-adapter responsibilities.

The private executor reserves a terminal reply before host code, validates
closed SDK DTOs and method-selected results, enforces 1 MiB params/results,
checks receipt/query echoes, and emits bounded sanitized SDK errors. A possibly
started mutation whose outcome is lost is unknown; cancel is not rollback.
A full terminal queue prevents backend execution. The future transport caller
must fence if it cannot publish a bounded correlated refusal/terminal reply.

## Negotiated transport ownership

`Spec.Reverse` plus `Init.HostServices` opts in explicitly; standalone Conn uses
`WithReverseProfile`. Required decline fails before activation; optional decline
retains base protocol. Offered methods require implemented typed callbacks (the
two ledger methods retain optional veto callbacks). Provisional Init admits only
offered log under the actual live Init parent. Hook offers remain refused.

`WithHostBinding` carries copied trusted host metadata for an ordinary call.
Preparation validates bounded scope/budgets and obtains a random reference
outside Conn locks. Writer selection attaches the actual parent ID and pending
correlation before first bytes, with rollback on failure. Parent completion and
cancellation retire authority before call admission release; stream failures
fence sessions. No fake ID is reserved and plugin ancestry never issues a lease.
Lock order is Conn -> HostSession -> frameQueue. Session execution drops its lock
before publishing through Conn; no host callbacks, entropy or JSON encoding run
under writer locks.

Business fencing is irreversible. A distinct connection-authenticated cleanup
session contains only log grants/ceilings and binds to actual selected Unload.
It cannot become business-ready; its finite lease closes on Unload terminal reply
or disconnect. EOF/SIGTERM have no invented cleanup parent. Lifecycle fences
before teardown waits and owns activation after its callback/current-generation
check. Standalone Start activates after handshake; raw/Spawn callers explicitly
call ActivateHostServices after host activation.

The reader reserves bounded terminal credit and installs cancellation before
starting a bounded worker. Ordinary saturation cannot block reader replies or
control; handlers retain their shared execution permits until actual return.
Terminal metadata/reservation handoff is atomic before queue visibility under
Conn's lock. Writer owns the receipt/inbound ID until physical success, failure
or fence; once-only retirement balances credit. Partial writes fence the stream.

Fixed admission/byte/depth capacities refuse lower offered floors (16/8/64/2,
8MiB frame/per lane, depth8), never widen. Larger offers clip to local capacities;
write timeout narrows to positive offered min local5s. Params/results remain
1MiB. This is an explicit implementation subset; smaller SDK-local policies do
not imply a host supports smaller offers.

Targeted pinned-source Go/Node normal-Serve tests cover actual selected IDs,
lifecycle logging, separate cleanup, typed bound helpers, parent retirement,
failed/declined Init, timeout/disconnect and lifecycle generation/fence paths.
Their test-only fd3 bridge has bounded preseeded release input. CI builds these
assets independently from the module pin. Shared manifest-driven Go/Node/Deno
replay remains separate: no internal fixture bypass, generic inventory or
compiler receipt counts as an interoperability pass. Host-specific scope,
commit transactions, security adapters and durable receipts remain host-owned.
