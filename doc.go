// Package pluginhost is the host side of plugin-sdk's stdio JSON-RPC
// protocol: it spawns a plugin process, performs the handshake, correlates
// calls by id, watches health, restarts a crashed plugin, and stops it within
// bounded time. plugin-sdk ships the plugin half (subprocess.Serve) and the
// wire types; this package is the half that was hand-written five times.
//
// # Layers
//
// [Conn] is framing and id correlation over any reader and writer pair.
// [Client] is a thin typed layer over it, one method per protocol method,
// passing plugin-sdk's structs through. [Process] owns one OS child: [Spawn]
// starts it without a handshake, [Start] adds [Process.Handshake], and
// [Process.Stop] ends it. [Supervise] keeps a plugin running across crashes;
// [HealthGate] is an on-demand cached health verdict for hosts that would
// rather not probe in the background. [Tail] and [Redact] bound and scrub a
// plugin's stderr.
// [Lifecycle] supplies ordered host planning, compatibility gates, generation
// ownership, enable/disable/reload and context-aware scope disposal callbacks.
// It owns one classified retry loop over processes, never a nested Supervisor.
//
// # What the host still decides
//
// Trust, capability grants, secrets, manifests and where plugins come from
// are not here. [Spec] passes Env, Init.Config and Init.Granted through
// untouched. Spec.BeforeSpawn carries a host validation hook through every
// spawn and supervised restart; a refusal leaves no child. A host that keeps secrets in the environment (and sends an
// empty Config) and a host that resolves them into Config (and keeps the
// environment bare) are both expressible; the library chooses neither.
//
// # Contracts worth knowing
//
//   - The child's environment is exactly Spec.Env. Nothing is inherited
//     unless the host says so with [InheritEnv].
//   - The plugin never dies with its start context: a process is never built
//     with exec.CommandContext.
//   - A call's context bounds that call only. Giving up costs one call; the
//     late reply is dropped.
//   - When the pipe ends, every waiter fails at once with [ErrGone], and a
//     final frame written before the exit is delivered first.
//   - A request over the frame cap (8 MiB, the limit plugin-sdk's Serve
//     reads with) is refused with [ErrFrameTooLarge] before anything is
//     written, because Serve would end its read loop on it and the plugin
//     would exit.
//   - Stop takes at most UnloadTimeout + ReapTimeout, and the final kill
//     reaches the whole process group.
//
// Test hosts against the same requirements with package pluginhosttest.
package pluginhost
