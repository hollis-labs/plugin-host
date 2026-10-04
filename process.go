package pluginhost

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
)

// drainGrace is how long after a plugin process exits its stdout reader keeps
// draining what the plugin already wrote. Anything written before the exit is
// in the kernel pipe buffer by then, so this only has to outlast decoding the
// last frame; it bounds the case where a grandchild still holds the pipe.
const drainGrace = 250 * time.Millisecond

// Process is one running plugin: its OS process, its [Conn] and its [Client].
//
// # Termination
//
// [Process.Stop] is the graceful path: plugin/unload, then close the child's
// stdin, then wait, then SIGKILL the whole process group. Closing stdin is
// what actually ends a plugin-sdk plugin: Serve's loop ends on stdin EOF (or
// SIGTERM), waits for in-flight requests, and calls the plugin's Unload. An
// unload over RPC alone does not end it. The kernel is the backstop: if the
// host dies, the plugin's pipes close and the plugin reaps itself.
//
// # What a Process never does
//
// It never uses exec.CommandContext. That would tie the plugin's lifetime to
// whatever context started it, which is usually a boot context with a startup
// budget: a healthy plugin would be killed when that context ends.
type Process struct {
	spec   Spec
	cmd    *exec.Cmd
	conn   *Conn
	client *Client
	tail   *Tail

	stdin   *os.File
	stdoutR *os.File

	exited chan struct{}

	mu     sync.Mutex
	exit   ExitInfo
	done   bool
	info   subprocess.InitResult
	loaded subprocess.LoadResult

	stopOnce  sync.Once
	stopErr   error
	unloadErr error
}

// Spawn starts the plugin process and connects to it, without a handshake.
// Use it when the host builds its own plugin/init (for example with resolved
// secrets in Config); otherwise use [Start].
//
// The child gets exactly Spec.Env, a fresh process group, and pipes for
// stdin, stdout and stderr. Stdout is an os.Pipe whose write end is closed in
// this process right after the start, so a concurrent cmd.Wait can never
// close the read side under a frame still in flight.
func Spawn(ctx context.Context, s Spec) (*Process, error) {
	if strings.TrimSpace(s.Command) == "" {
		return nil, fmt.Errorf("pluginhost: plugin %q names no command to spawn", s.ID)
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("pluginhost: %s: spawn: %w", s.label(), err)
	}
	s = snapshotSpec(s.normalized())
	if err := validateInit(s); err != nil {
		return nil, err
	}
	if s.BeforeSpawn != nil {
		if err := s.BeforeSpawn(ctx); err != nil {
			return nil, fmt.Errorf("pluginhost: %s: before spawn: %w", s.label(), err)
		}
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("pluginhost: %s: spawn: %w", s.label(), err)
		}
	}

	// Deliberately not exec.CommandContext; see the Process doc.
	cmd := exec.Command(s.Command, s.Args...) // #nosec G204 -- the command is host configuration, not caller input.
	cmd.Dir = s.Dir
	cmd.Env = s.Env
	cmd.WaitDelay = s.ReapTimeout
	configureProcessGroup(cmd)

	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("pluginhost: %s: stdin pipe: %w", s.label(), err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		return nil, fmt.Errorf("pluginhost: %s: stdout pipe: %w", s.label(), err)
	}
	tail := &Tail{Bytes: s.StderrBytes, Secrets: s.Secrets}
	cmd.Stdin, cmd.Stdout, cmd.Stderr = stdinR, stdoutW, tail

	startErr := cmd.Start()
	// The child has its own copies now; ours must go or EOF never arrives.
	_ = stdinR.Close()
	_ = stdoutW.Close()
	if startErr != nil {
		_ = stdinW.Close()
		_ = stdoutR.Close()
		return nil, fmt.Errorf("pluginhost: %s: spawn %s: %w", s.label(), s.Command, startErr)
	}

	conn := NewConn(stdoutR, stdinW, s.ConnOptions...)
	p := &Process{
		spec: s, cmd: cmd, conn: conn, client: NewClient(conn), tail: tail,
		stdin: stdinW, stdoutR: stdoutR,
		exited: make(chan struct{}),
	}
	go p.wait()
	return p, nil
}

// Start is [Spawn] plus [Process.Handshake]. On any failure the child has
// been killed and reaped: there is no half-started state to reason about.
//
// ctx bounds the handshake only. Canceling it after Start returns does
// nothing to the plugin.
func Start(ctx context.Context, s Spec) (*Process, error) {
	p, err := Spawn(ctx, s)
	if err != nil {
		var failure *Failure
		if errors.As(err, &failure) {
			return nil, err
		}
		return nil, processFailure(s, "spawn", err)
	}
	if _, _, err := p.Handshake(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

// wait reaps the process, records how it ended, and then retires the Conn.
func (p *Process) wait() {
	err := p.cmd.Wait()
	info := classifyExit(p.cmd.ProcessState, err)
	p.mu.Lock()
	p.exit, p.done = info, true
	p.mu.Unlock()
	close(p.exited)

	// Let the reader finish what the plugin wrote before it died (a plugin
	// that answers and exits must have its answer delivered), then fail the
	// rest with ErrGone. The read deadline keeps a grandchild that still
	// holds the pipe from stretching this.
	_ = p.stdoutR.SetReadDeadline(time.Now().Add(drainGrace))
	timer := time.NewTimer(p.spec.ReapTimeout + drainGrace)
	defer timer.Stop()
	select {
	case <-p.conn.Done():
	case <-timer.C:
	}
	_ = p.conn.Close()
}

// Handshake runs plugin/init, checks the answer, then runs plugin/load.
//
// The answer must name protocol 2 and capability contract 1 exactly (no range negotiation) and a
// non-empty plugin id. The whole handshake is bounded by
// Spec.HandshakeTimeout even when ctx has no deadline. On failure the child
// is killed and reaped and the error carries the plugin's redacted stderr
// tail; a Process whose Handshake failed is finished.
func (p *Process) Handshake(ctx context.Context) (subprocess.InitResult, subprocess.LoadResult, error) {
	ctx, cancel := context.WithTimeout(ctx, p.spec.HandshakeTimeout)
	defer cancel()

	initResult, loadResult, err := p.handshake(ctx)
	if err != nil {
		_ = p.Kill() // also lets the stderr copier drain, so the tail is complete
		return subprocess.InitResult{}, subprocess.LoadResult{}, fmt.Errorf("%w%s", err, p.diagnosticsText())
	}
	return initResult, loadResult, nil
}

// processFailure retains classification and cause without echoing raw config.
func processFailure(s Spec, step string, err error) *Failure {
	return &Failure{PluginID: s.ID, Diagnostic: safeDiagnostic(s, err.Error()), Stage: StageLoad, Step: step, Code: step + "_failed", Retryable: IsTransient(err), Cause: err}
}

func (p *Process) handshake(ctx context.Context) (subprocess.InitResult, subprocess.LoadResult, error) {
	result, err := p.initialize(ctx)
	if err != nil {
		return result, subprocess.LoadResult{}, err
	}
	if err = p.verify(result); err != nil {
		return result, subprocess.LoadResult{}, err
	}
	loaded, err := p.load(ctx)
	return result, loaded, err
}

func (p *Process) initialize(ctx context.Context) (subprocess.InitResult, error) {
	result, err := p.client.Init(ctx, p.spec.Init)
	if err != nil {
		return result, initFailure(p.spec, err)
	}
	return result, nil
}

func (p *Process) verify(result subprocess.InitResult) error {
	if err := verifyInitResult(p.spec.Init, result); err != nil {
		return initFailure(p.spec, err)
	}
	if result.Protocol != subprocess.ProtocolVersion {
		return processFailure(p.spec, "protocol", fmt.Errorf("%w: plugin speaks %d, host speaks %d", ErrProtocolMismatch, result.Protocol, subprocess.ProtocolVersion))
	}
	if result.ID == "" {
		return processFailure(p.spec, "identity", ErrNoPluginID)
	}
	if p.spec.ExpectedID != "" && result.ID != p.spec.ExpectedID {
		return processFailure(p.spec, "identity", mismatch("id", p.spec.ExpectedID, result.ID))
	}
	if p.spec.ExpectedVersion != "" {
		_, err := CompareVersions(result.Version, p.spec.ExpectedVersion)
		if err != nil || result.Version != p.spec.ExpectedVersion {
			return processFailure(p.spec, "version", errors.Join(mismatch("version", p.spec.ExpectedVersion, result.Version), err))
		}
	}
	p.mu.Lock()
	p.info = result
	p.mu.Unlock()
	return nil
}

func (p *Process) load(ctx context.Context) (subprocess.LoadResult, error) {
	loaded, err := p.client.Load(ctx)
	if err != nil {
		return loaded, processFailure(p.spec, "load", err)
	}
	p.mu.Lock()
	p.loaded = loaded
	p.loaded.SkippedRegistrations = slices.Clone(loaded.SkippedRegistrations)
	p.mu.Unlock()
	return loaded, nil
}

// Client returns the typed client over this process's connection.
func (p *Process) Client() *Client { return p.client }

// Info returns what the plugin said it was at init (zero before Handshake).
func (p *Process) Info() subprocess.InitResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.info
}

// LoadInfo returns the acknowledgment from the last successful handshake.
// Hosts can inspect skipped declarations after Start or a supervised restart
// without invoking plugin/load a second time. The returned slice is a copy.
func (p *Process) LoadInfo() subprocess.LoadResult {
	p.mu.Lock()
	defer p.mu.Unlock()
	result := p.loaded
	result.SkippedRegistrations = slices.Clone(result.SkippedRegistrations)
	return result
}

// Pid returns the plugin's OS process id (also its process group id on unix).
func (p *Process) Pid() int { return p.cmd.Process.Pid }

// Exited is closed once the process has exited and been reaped.
func (p *Process) Exited() <-chan struct{} { return p.exited }

// ExitInfo reports how the process ended; ok is false while it still runs.
func (p *Process) ExitInfo() (ExitInfo, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exit, p.done
}

// Diagnostics returns the plugin's recent stderr: bounded by
// Spec.StderrBytes, scrubbed of Spec.Secrets, then passed through
// Spec.Redact.
func (p *Process) Diagnostics() string {
	text := p.tail.String()
	if p.spec.Redact != nil {
		text = p.spec.Redact(text)
	}
	return text
}

func (p *Process) diagnosticsText() string {
	text := strings.TrimSpace(p.Diagnostics())
	if text == "" {
		return ""
	}
	return "\nplugin stderr:\n" + text
}

// Kill SIGKILLs the plugin's whole process group and waits up to
// Spec.ReapTimeout for the process to be reaped. It does not ask first; see
// [Process.Stop] for that. It is safe to call repeatedly and after exit.
func (p *Process) Kill() error {
	err := p.signalGroup()
	select {
	case <-p.exited:
	case <-time.After(p.spec.ReapTimeout):
		err = errors.Join(err, fmt.Errorf(
			"pluginhost: %s did not exit %s after SIGKILL; a process outside its group is probably holding a pipe%s",
			p.spec.label(), p.spec.ReapTimeout, p.diagnosticsText()))
	}
	_ = p.conn.Close()
	return err
}

// signalGroup kills the group unless the leader was already reaped: after
// that its pid (and so the group id) could belong to something else.
func (p *Process) signalGroup() error {
	select {
	case <-p.exited:
		return nil
	default:
	}
	return killProcessGroup(p.cmd.Process.Pid)
}

// Stop ends the plugin: ask, then close its stdin, then wait, then kill.
//
//  1. plugin/unload, then close stdin. Spec.UnloadTimeout is one budget for
//     the unload call and the wait for exit together.
//  2. Still running: SIGKILL the process group, wait up to
//     Spec.ReapTimeout.
//
// Total time is at most UnloadTimeout + ReapTimeout whatever ctx says; a ctx
// that ends earlier shortens the graceful phase and never lengthens
// anything. Stop is idempotent: later calls return the first call's result.
// A plugin that exits within the graceful budget returns nil, as does one
// killed after it; the error is for a process that could not be reaped.
func (p *Process) Stop(ctx context.Context) error {
	p.stopOnce.Do(func() { p.stopErr = p.stop(ctx) })
	return p.stopErr
}

func (p *Process) stop(ctx context.Context) error {
	deadline := time.Now().Add(p.spec.UnloadTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	graceful, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	select {
	case <-p.exited:
	default:
		// An unload failure is retained for lifecycle reports: a plugin that refuses or cannot
		// answer plugin/unload is exactly the one the stronger steps exist
		// for, and it is stopped correctly anyway.
		p.unloadErr = p.client.Unload(graceful)
		// Closing stdin is the SDK's own shutdown path. It is done whether or
		// not the unload call worked.
		_ = p.stdin.Close()
		select {
		case <-p.exited:
		case <-graceful.Done():
		}
	}

	var err error
	select {
	case <-p.exited:
	default:
		err = p.Kill()
	}
	_ = p.conn.Close()
	return err
}

// stopWithReport retains the unload error for lifecycle disposal while Stop's
// process-only contract continues to report failure to reap.
func (p *Process) stopWithReport(ctx context.Context) (error, error) {
	err := p.Stop(ctx)
	return p.unloadErr, err
}
