# Typed host service and binding seam (0189 slice 3)

This slice supplies host-owned authority bookkeeping and twelve fixed typed
operations. It does not enable reverse RPC. `Conn`, `Spec` and Init still refuse
all reverse/hooks offers. The private execution tests exercise this seam directly;
they are not real child, negotiated duplex or SDK interoperability passes.

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
These are host-controlled bookkeeping calls; automatic lifecycle/Conn hookup
remains a slice 4 requirement.

Renewal keeps the same binding, shared remaining quotas (including zero), and
clips its lease to five minutes, root/parent and current grant limits. Grant
replacement fences changed authority even when a revision label was reused.
Revocation cancels descendants immediately and never waits for a backend.

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

## Slice 4 acceptance dependency

Keep profile refusal until the next slice wires negotiated activation and a
bounded worker router. The reader must never invoke `executeHost` or host policy
synchronously or wait on permits. Writer selection must register the actual
parent ID and correlation before the first byte; failed/partial write,
cancellation and completion must retire it. Add real lifecycle integration for
provisional Init/Load/explicit-Unload log scopes, activated ownership and
revoke/disable/reload/crash/close disposal. Only then may negotiated children
exercise this path. Slice 5's manifest-driven Go/Node/Deno replay remains a
separate gate, with exact SDK heads and runtime versions in its report.

Concrete transport APIs remain refinements for slice 4, after this seam is
reviewed. `IssueBinding` currently needs a registered selected parent and does
entropy/JSON work; it must not simply be called under writer locks. Prepare a
bounded host-owned reference and authority snapshot outside Conn locks, then
atomically attach the actual selected ID/parent and patch fixed-width fields
before the first byte, with a fixed lock order and rollback on publication
failure. No fake parent ID is reserved for preparation.

Full `Revoke` irreversibly fences the business session. Explicit Unload needs a
separate finite log-only cleanup lease/state under its actual live Unload
parent; never reopen the revoked business session with `OwnerReady`. Finally,
`executeHost`'s current queue terminal return does not establish physical-write
receipt ownership: install Conn receipt metadata/reservation before queue
visibility, and let the writer retire/release it once on success, failure or
fence. Refuse unsupported smaller negotiated minima rather than silently
widening them. These three refinements are dependencies of slice 4, not features
claimed by this slice 3 candidate.
