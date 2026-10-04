# Local interop inventory

Run from this plugin-host worktree:

```sh
INTEROP_SDK_REPO=/path/to/plugin-sdk ./scripts/interop-scaffold.sh
```

`TMPDIR` must point to the agent's scratch directory. The script archives SDK
head `90adf1f02ddffde708fa3784f0063bab5171462b` without changing its checkout,
records base `d04ab2149506a96e8c54b58829f58ee480e0de41`, and builds the Go fixture
test binary once per source/toolchain receipt. All Go test/build commands run
through heavytest with GOMAXPROCS=4 and GOFLAGS=-p=2, without race instrumentation.
Source, binary, receipt and report stay under TMPDIR.

The inventory reads `protocol/v2/fixtures/duplex-child.json` and its selected
source files. Case names, profiles, scenarios and authored vectors come from
that manifest and corpus, rather than an adapter-owned copy of the cases.
Unknown manifest groups remain pending. The negotiated expanded selection is
currently prose, so it also remains pending instead of inventing a filter.
Proposed/unavailable cases retain the SDK's owner and reason. Each runtime's
version or unavailability is recorded along with the exact SDK and host heads,
host dirty state, corpus version, manifest hash and child build receipt.

This is scaffolding only: every replay is pending or unavailable. Compiling the
fixture and passing the inventory tests proves no Conn/spawn/cancel/dispose,
authorization, binding, backend or durable receipt behavior. Node/Deno workers
are not built or run here. Real host replay waits for PR #8 and integrated
slices 2–3; negotiated replay additionally waits for the activation slice.
SDK fake-host replay cannot satisfy those gates. The source contract is in
`docs/protocol/v2/children.md` and `reverse.md` at the pinned SDK head.
