# Isolated interop evidence

Run from the plugin-host worktree with `TMPDIR` set to owned scratch:

```sh
INTEROP_SDK_REPO=/path/to/plugin-sdk ./scripts/interop-scaffold.sh
INTEROP_SDK_REPO=/path/to/plugin-sdk ./scripts/interop-replay.sh
```

Both scripts archive exact SDK fixture source
`ea8ec0dca862d0c7284cc6a130a4b27fb812ed21` with base
`d04ab2149506a96e8c54b58829f58ee480e0de41`. Corpus version 1 and authored
selector version 1 have manifest SHA256
`30cb07b87060dc88e9cb6ac5414f7b9cb7bb19c7a97c058e284d96e1e59ff4a9`.
The module dependency stays at `5c663e7`; these are separate test assets.
Source-keyed receipts reuse one Go child build and the exact TS workspace
assets. Go commands use heavytest, GOMAXPROCS=4, GOFLAGS=-p=2, GOWORK=off.
Historical `90adf1f` receipts remain in their own source-keyed directory.

The inventory records source obligations, both internal and negotiated modes,
unknown groups, and authored proposed owners/reasons. It executes no child and
claims zero replay passes. The generic selector validates all referenced rows
before execution, preserves source order, and applies authored scenario
exclusions to observed and proposed rows. Unknown operators, versions, modes,
references and requested names are refused. It has no case-name allowlist.

The replay uses genuine Go/Node/Deno normal-Serve workers with actual Process,
Conn, HostSession, typed callbacks, host-issued bindings, and finite parents.
A test-only bridge forwards protocol bytes unchanged. Private sequenced fd3
controls (Go/Node) or atomic control files (Deno) carry actual acknowledgments
and bounded events. Required runtime, control, observer, acknowledgment or EOF
failures fail the harness. Success requires matching worker and owned Process
exit receipts, graceful reaping, one cleanup and observer EOF; forced cleanup
cannot establish success. Reports retain bounded physical wire witnesses,
raw small frames or hashes for larger frames, backend authority/effect evidence,
source/build/runtime/command provenance, and pending handlers. No inventory
count is a pass denominator. `INTEROP_TEST_PATTERN` permits focused author
runs; excluded tests remain pending, never passed.

Expanded replay follows the authored driver method ceilings of 10000ms, with
actual Init/Load lifecycle IDs and real host-issued bindings instead of the
SDK fake host's literal `binding-example`. Parent-descendant cancellation uses
the authored notification through Conn.Notify. Hung cleanup uses a finite
300ms Unload parent, keeps the authored SDK 200ms drain and requires natural
code 1 with one cleanup and transport failure. A separate derived host witness
holds authority admission 80ms against a 20ms mutating method ceiling. It
records SDK local unknown/unknown separately from the physical host
`deadline_exceeded/not_started` reply and zero backend execution. A second
derived witness keeps the authored offer intact and narrows only the private
test host policy ceiling to 20ms, proving that host receipt-clock expiry
independently prevents backend entry and delivers the definite typed refusal.
Neither derived witness is the FULL shared deadline vector.

**Current boundary:** approved OptionB adds explicit public `CallCorrelation`
for CommandExecute/Health with a finite local observer independent of authored
wire context. The raw `arrival-deadline-and-absent-context` handler preserves
remote300 and literal non-authorizing `binding-example`, then truly absent
hold/Health context. It requires actual published-ID+command event matching,
SDK deadline cause, unknown/unknown terminal, returned0 until explicit release,
interleaved Health, no backend authority/effects, physical publication, natural
exit/cleanup/reap and strict EOF accounting for Go/Node/Deno. Actual execution
rows, not implementation or inventory, determine its result. Separate observer
expiry probes prove transport-cancel cause and actual control receipt provenance;
inert-selector helper probes prove typed no-effect refusal. Neither substitutes
for raw remote expiry.

Historical finite-parent PR14 receipts at `60d0b318` and raw local prepublication
refusals remain preserved, along with the earlier derived Node deadline=false
failure. New reports must identify their actual host head/dirty state and reuse
source-keyed build receipts without overwriting historical reports. Raw repeated
and absent-context Load in forward/credits remains PENDING/incompatible with
unchanged finite lifecycle policy. Other mandatory queue/fairness/control and
0163 application/security/hook obligations remain named pending. There is no
full0189/0172 acceptance, unqualified profile claim, release or live activation
from raw-case repair or these isolated receipts.

**Private actual-Conn fairness witness:** the selected authored
`host-writer-four-frame-fairness` scenario uses a stream decorator installed
before Conn starts. The first ordinary frame blocks while nine actual Health
calls and eight genuine reverse replies contend in the real bounded lanes.
Each whole control write admits the next pre-owned refill intent, with at most
31 unknown-target `rpc/cancel` refills. The source's `>=32` control writes and
four-frame burst bound remain the assertions; 39 is the finite intent ceiling.
No standalone SDK FrameWriter trace or queue admission is publication proof.

The per-run identity fixes the exact manifest/corpus/selector, selected recipe,
runtime and unique run UUID before launch. Metadata, snapshots and physical
receipts are bounded. Control admission uses actual Conn capacity and owns no
invented parent or terminal credit. Underlying byte count/error, subsequent ACK
error, actual writer completion and genuine reverse retirement are distinct.
Full bytes followed by a failed ACK remain full physical publication with
uncertain outcome and harness failure. EOF accepts the authored unknown controls
only through their complete pre-owned receipts. This selected fairness scenario
authors no additional host cancellation: even a live or completed published
target cannot authorize a control without its per-run intent and physical
receipt. Other scenarios keep their caller/observer/descendant cancellation
guards. Snapshot `HighWater` describes the base/refusal stream, not the
negotiated reverse stream. Other notifications, leftovers and observer/control
failures retain the strict guards. Natural
worker/bridge cleanup and reap remain required. This test-only seam expands no
public API or lifecycle policy; raw forward/credits and the original remaining
mandatory ledger stay open.

Reports and run logs stay in `TMPDIR/plugin-host-interop-<source>/`:
`report.json` (inventory), `control-report.json`, `replay-report.json`, and
`received-budget-report.json`. SDK draft merge/tag/release, broader adoption,
profile activation and live actions remain outside this work.

**Selected writer queue cases:** `clip`, `queue`, and `queue-frames` use the
unchanged authored worker profiles. Each launch resolves its exact source,
manifest, selector, selected recipe, runtime and random run identity. A sequenced
arm acknowledgment followed by the actual absent-context Health input ties the
SDK's bytes-only `writer_waiting` event to that Health terminal's whole physical
bytes. The finite host observer does not supply a wire context. Helper events
name the actual command ID and result index; they are not reverse request IDs.

Clip keeps its finite 5000ms authority parent and authored 250ms writer hold,
then requires a positive reverse wire budget no greater than 4800ms. The byte
profile retains the SDK-local 32768-byte queue and refuses its 65536-byte put
before publication or backend entry. The frame profile retains its three-frame
queue: three real reads and one local `rate_limited/not_started` result. These
fixture-local policies do not change negotiated host floors or lifecycle slots.

Only a host-assembled per-run proof may reconcile the byte case's one helper
to zero publications, or the frame case's four helpers to three. Actual method,
arguments, grant, binding, parent, unique helper indices, blocked snapshots,
typed command results, backend authority and complete directional wire evidence
must agree. Missing, foreign, inconsistent or duplicate evidence fails. Default
helper accounting, notification rejection, F1/F2, terminal credit retirement,
control/observer EOF, once cleanup and natural worker/bridge exit remain strict.
Adversarial trace copies are guard evidence, not additional runtime passes.
Actual reports determine results; forward/credits remain incompatible and all
other mandatory families and operational/release boundaries remain open.
