# Changelog

All notable changes to plugin-host are documented here. The format
follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Write the entry for a release here BEFORE cutting its tag: the release workflow
refuses a tag whose CHANGELOG has no heading for it.

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
