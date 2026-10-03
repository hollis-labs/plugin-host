// Package pluginhosttest is the conformance suite for hosts of plugin-sdk
// subprocess plugins, and the fixture plugin it drives.
//
// A host of the stdio protocol writes a small adapter over its own client
// (a [Harness] returning [Instance]s) and calls [Run]. The suite spawns real
// child processes over the real wire and checks requirements R01-R18: the
// handshake, id correlation, timeouts and cancellation, crash and garbage
// handling, oversized frames, graceful and forced stop, process-group kill,
// context lifetime, stderr redaction, and error codes. A requirement a host
// cannot meet is named with [Waive]; nothing is skipped silently.
//
// The fixture is the test binary re-executing itself: the adapter's package
// calls [MaybeRunFixture] as the first line of TestMain, and [FixtureCommand]
// builds the command that selects a behavior. Compliant behavior runs the
// real subprocess.Serve; hostile behavior (hang, crash, garbage, wedge,
// stderr flood) uses a raw stdio loop.
//
// [RunLifecycle] is the separate generation/scope contract (R19–R27), using
// real children and synthetic host policy/resource adapters without waivers.
package pluginhosttest
