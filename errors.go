package pluginhost

import "errors"

// Sentinel errors. Every one is reported wrapped, so test with [errors.Is].
var (
	// ErrRequestIDExhausted means the positive JS-safe ID space is consumed.
	// No request is published and IDs are never wrapped or reused.
	ErrRequestIDExhausted = errors.New("pluginhost: request ID space exhausted")

	// ErrGone reports that the plugin's pipe closed (it exited, crashed or was
	// killed) or that the [Conn] was closed, while a call was outstanding or
	// before one was sent. Callers get it at once when the pipe ends; nothing
	// waits out a timeout to learn that the process is gone.
	ErrGone = errors.New("pluginhost: plugin is gone")

	// ErrFrameTooLarge reports an outbound request whose encoded line exceeds
	// the connection's frame cap. Nothing was written, so the connection and
	// the plugin are unaffected. plugin-sdk's Serve reads lines with an 8 MiB
	// limit and ends its read loop (the plugin exits) on a longer one, which
	// is why the host refuses instead of sending.
	ErrFrameTooLarge = errors.New("pluginhost: request frame exceeds the frame cap")

	// ErrProtocolMismatch reports a plugin whose plugin/init answer names a
	// wire protocol other than the one this library speaks. The handshake is
	// exact, not ranged.
	ErrProtocolMismatch = errors.New("pluginhost: plugin speaks a different wire protocol")

	// ErrNoPluginID reports a plugin/init answer with an empty plugin id.
	ErrNoPluginID = errors.New("pluginhost: plugin returned no id from init")

	// ErrUnhealthy reports the last cached health verdict was bad. See
	// [HealthGate.Check].
	ErrUnhealthy = errors.New("pluginhost: plugin is unhealthy")
)
