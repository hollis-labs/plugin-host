// Package guard runs one call under an isolation contract: a panic becomes an
// error, the call is bounded by a budget, the caller's own cancellation and a
// host shutdown each release the caller with their own distinct error.
//
// It is for plugins that run inside the host's process, where a handler that
// panics or wedges would otherwise take the whole host with it. It does not
// depend on plugin-host or plugin-sdk and does not stop the call it
// abandons: an abandoned call's goroutine keeps running until it returns.
package guard
