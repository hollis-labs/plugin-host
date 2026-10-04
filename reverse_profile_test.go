package pluginhost

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/capability"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

func reverseTestSpec(t *testing.T, services HostServices) Spec {
	t.Helper()
	if services.Log == nil {
		services.Log = func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error) {
			return subprocess.LogResult{Accepted: true}, nil
		}
	}
	if services.StorageGet == nil {
		services.StorageGet = func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
			return subprocess.StorageGetResult{Found: false}, nil
		}
	}
	identity := capability.RuntimeIdentity{HostInstance: "reverse-test", OwnerID: "fixture", OwnerGeneration: 1}
	grants := capability.GrantSet{}
	for _, g := range []struct{ id, name string }{{"g-Log", "log.write"}, {"g-StorageGet", "storage.read"}} {
		grants = append(grants, capability.Grant{GrantID: g.id, Name: g.name, SchemaVersion: 1, Scope: json.RawMessage(`{}`), HostInstance: identity.HostInstance, OwnerID: identity.OwnerID, OwnerGeneration: identity.OwnerGeneration, Audience: "host.private-stdio", IssuedAt: time.Now().Add(-time.Minute).UTC().Format(time.RFC3339Nano), ExpiresAt: time.Now().Add(time.Minute).UTC().Format(time.RFC3339Nano), PolicyRevision: "1"})
	}
	params := subprocess.InitParams{PluginDir: "/fixture", DataDir: "/fixture/data", CacheDir: "/fixture/cache", Config: map[string]string{}, LogLevel: "info", HostInfo: subprocess.HostInfo{Version: "test", Protocol: 2}, CapabilityContract: 1, Incarnation: identity, Grants: grants}
	raw := `{"reverse_rpc_version":1,"incarnation":{"host_instance":"reverse-test","owner_id":"fixture","owner_generation":1},"methods":["host/log","host/storage/get"],"limits":{"host_to_plugin_inflight":16,"plugin_to_host_inflight":8,"host_global_inflight":64,"control_slots":2,"max_frame_bytes":8388608,"max_queued_write_bytes":8388608,"write_timeout_ms":1000,"max_depth":8,"method_timeout_ms":{"host/log":1000,"host/storage/get":1000}}}`
	params.HostServices = &subprocess.HostServices{}
	if err := json.Unmarshal([]byte(raw), params.HostServices); err != nil {
		t.Fatal(err)
	}
	return Spec{ExpectedID: "fixture", ExpectedVersion: "1.0.0", Init: params, Reverse: &ReverseProfile{Runtime: NewHostServiceRuntime(services, func(context.Context, HostAuthority) error { return nil }), Required: true, LifecycleBinding: &HostBinding{GrantID: "g-Log", Scope: json.RawMessage(`{}`)}}, HandshakeTimeout: 2 * time.Second, UnloadTimeout: time.Second, ReapTimeout: time.Second}
}

const reverseInitResult = `{"id":"fixture","name":"Fixture","version":"1.0.0","description":"test","protocol":2,"capability_contract":1,"reverse_rpc_version":1}`

func reverseReadyPeer(t *testing.T, s Spec) *peer {
	t.Helper()
	p := reversePeer(t, WithReverseProfile(*s.Reverse, s.Init))
	initDone := make(chan error, 1)
	go func() { _, err := NewClient(p.conn).Init(context.Background(), s.Init); initDone <- err }()
	req := p.request()
	p.reply(req.ID, reverseInitResult)
	if err := <-initDone; err != nil {
		t.Fatal(err)
	}
	load := callAsync(context.Background(), p.conn, subprocess.MethodLoad)
	req = p.request()
	p.reply(req.ID, `{}`)
	if got := await(t, load); got.err != nil {
		t.Fatal(got.err)
	}
	if err := p.conn.ActivateHostServices(); err != nil {
		t.Fatal(err)
	}
	return p
}
func reverseParams(t *testing.T, r subprocess.RPCRequest) subprocess.ForwardContext {
	t.Helper()
	raw, _ := json.Marshal(r.Params)
	var v struct {
		Context subprocess.ForwardContext `json:"context"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v.Context
}
func reverseRequest(t *testing.T, p *peer, id uint64, method HostMethod, parent subprocess.RPCRequest) {
	t.Helper()
	fc := reverseParams(t, parent)
	if fc.BindingID == nil {
		t.Fatal("no prepared binding")
	}
	n, ok := parent.ID.Integer()
	if !ok || n <= 0 {
		t.Fatal("nonpositive parent ID")
		return
	}
	params := map[string]any{"grant_id": "g-Log", "context": subprocess.ReverseContext{BindingID: *fc.BindingID, TimeoutMS: 500, ParentCall: subprocess.ParentCall{RequestOwner: subprocess.HostRPCOwnerHost, ID: uint64(n)}}, "level": "info", "message": "test"}
	if method == HostStorageGet {
		params["grant_id"] = "g-StorageGet"
		delete(params, "level")
		delete(params, "message")
		params["key"] = "read"
	}
	raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params})
	p.raw(string(raw))
}
func TestReverseProvisionalLogHasSelectedParentBeforeReply(t *testing.T) {
	var calls atomic.Int64
	s := reverseTestSpec(t, HostServices{Log: func(_ context.Context, call *HostCall, _ subprocess.LogParams) (subprocess.LogResult, error) {
		if call.Authority().Parent.ID != 1 {
			t.Error("wrong selected Init parent")
		}
		if err := call.CheckCommit(); err != nil {
			t.Error(err)
		}
		calls.Add(1)
		return subprocess.LogResult{Accepted: true}, nil
	}})
	p := reversePeer(t, WithReverseProfile(*s.Reverse, s.Init))
	done := make(chan error, 1)
	go func() { _, err := NewClient(p.conn).Init(context.Background(), s.Init); done <- err }()
	req := p.request()
	reverseRequest(t, p, 1, HostLog, req)
	terminal := p.frame()
	if terminal.ID != subprocess.NumberID(1) || terminal.Method != "" {
		t.Fatal(terminal)
	}
	select {
	case err := <-done:
		t.Fatal("colliding reverse reply completed Init", err)
	default:
	}
	p.reply(req.ID, reverseInitResult)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("log not executed")
	}
	p.conn.reverse.business.mu.Lock()
	parents := len(p.conn.reverse.business.parents)
	terminalBinding := p.conn.reverse.business.bindings[*reverseParams(t, req).BindingID].parent.terminal
	p.conn.reverse.business.mu.Unlock()
	if parents != 0 || !terminalBinding {
		t.Fatal("reply did not retire parent")
	}
}
func TestReverseBusinessCancelAndUnloadUseDifferentAuthority(t *testing.T) {
	started := make(chan *HostCall, 1)
	stopped := make(chan struct{})
	services := HostServices{StorageGet: func(ctx context.Context, call *HostCall, _ subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		started <- call
		<-ctx.Done()
		close(stopped)
		return subprocess.StorageGetResult{}, ctx.Err()
	}, Log: func(_ context.Context, call *HostCall, _ subprocess.LogParams) (subprocess.LogResult, error) {
		if call.Authority().Parent.ID != 4 {
			t.Error("wrong Unload parent")
		}
		return subprocess.LogResult{Accepted: true}, nil
	}}
	s := reverseTestSpec(t, services)
	p := reverseReadyPeer(t, s)
	ctx, cancel := context.WithCancel(WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)}))
	result := callAsync(ctx, p.conn, subprocess.MethodHealth)
	req := p.request()
	reverseRequest(t, p, 1, HostStorageGet, req)
	call := <-started
	cancel()
	if got := await(t, result); !errors.Is(got.err, context.Canceled) {
		t.Fatal(got.err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("descendant survived parent cancel")
	}
	if err := call.CheckCommit(); err == nil {
		t.Fatal("canceled parent still authorizes commit")
	}
	p.conn.RevokeHostServices()
	if err := p.conn.ActivateHostServices(); err == nil {
		t.Fatal("reopened business")
	}
	unload := callAsync(context.Background(), p.conn, subprocess.MethodUnload)
	var u subprocess.RPCRequest
	for {
		u = p.frame()
		if u.Method == subprocess.MethodUnload {
			break
		}
	}
	reverseRequest(t, p, 2, HostLog, u)
	terminal := p.frame()
	if terminal.Method != "" {
		t.Fatal(terminal)
	}
	p.reply(u.ID, `{"ok":true}`)
	if got := await(t, unload); got.err != nil {
		t.Fatal(got.err)
	}
	if p.conn.reverse.cleanup == p.conn.reverse.business {
		t.Fatal("cleanup reused business session")
	}
	p.conn.reverse.cleanup.mu.Lock()
	closed := p.conn.reverse.cleanup.closed
	p.conn.reverse.cleanup.mu.Unlock()
	if !closed {
		t.Fatal("cleanup lease survived Unload reply")
	}
}
func TestReverseDeclineKeepsBaseOptionalAndRequiredRefuses(t *testing.T) {
	for _, required := range []bool{false, true} {
		t.Run(map[bool]string{false: "optional", true: "required"}[required], func(t *testing.T) {
			s := reverseTestSpec(t, HostServices{})
			s.Reverse.Required = required
			p := reversePeer(t, WithReverseProfile(*s.Reverse, s.Init))
			done := make(chan error, 1)
			go func() { _, err := NewClient(p.conn).Init(context.Background(), s.Init); done <- err }()
			req := p.request()
			p.reply(req.ID, strings.Replace(reverseInitResult, `,"reverse_rpc_version":1`, "", 1))
			err := <-done
			if required {
				if err == nil {
					t.Fatal("required decline accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			load := callAsync(context.Background(), p.conn, subprocess.MethodLoad)
			req = p.request()
			p.reply(req.ID, `{}`)
			if got := await(t, load); got.err != nil {
				t.Fatal(got.err)
			}
			health := callAsync(context.Background(), p.conn, subprocess.MethodHealth)
			req = p.request()
			p.reply(req.ID, `{"ok":true}`)
			if got := await(t, health); got.err != nil {
				t.Fatal(got.err)
			}
		})
	}
}
func TestReverseUnsupportedMinimaAndDefaultOfferRefusal(t *testing.T) {
	s := reverseTestSpec(t, HostServices{})
	if err := validateInit(s); err != nil {
		t.Fatal(err)
	}
	defaultSpec := snapshotSpec(s)
	defaultSpec.Reverse = nil
	if err := validateInit(defaultSpec); err == nil {
		t.Fatal("default Spec accepted reverse offer")
	}
	if err := validateInitParams(s.Init); err == nil {
		t.Fatal("default accepted offer")
	}
	for _, mutate := range []func(*subprocess.HostServiceLimits){
		func(l *subprocess.HostServiceLimits) { l.PluginToHostInflight = 1 }, func(l *subprocess.HostServiceLimits) { l.HostGlobalInflight = 1 }, func(l *subprocess.HostServiceLimits) { l.HostToPluginInflight = 1 }, func(l *subprocess.HostServiceLimits) { l.MaxDepth = 1 }, func(l *subprocess.HostServiceLimits) { l.MaxFrameBytes = 4096; l.MaxQueuedWriteBytes = 4096 },
	} {
		v := snapshotSpec(s)
		mutate(&v.Init.HostServices.Limits)
		if err := validateInit(v); err == nil {
			t.Fatal("unsupported minimum accepted")
		}
	}
	p := reversePeer(t, WithReverseProfile(*s.Reverse, s.Init))
	if p.conn.writeTimeout != time.Second {
		t.Fatal("write timeout not narrowed")
	}
}

type reverseFailWriter struct{ check func() }

func (w reverseFailWriter) Write([]byte) (int, error) { w.check(); return 0, io.ErrClosedPipe }
func TestReverseFailedPublicationRetiresAttachedParent(t *testing.T) {
	s := reverseTestSpec(t, HostServices{})
	r, w := io.Pipe()
	defer w.Close()
	var conn *Conn
	writer := reverseFailWriter{check: func() {
		conn.reverse.business.mu.Lock()
		defer conn.reverse.business.mu.Unlock()
		if len(conn.reverse.business.parents) != 1 {
			t.Error("first write before parent registration")
		}
	}}
	conn = NewConn(r, writer, WithReverseProfile(*s.Reverse, s.Init))
	defer conn.Close()
	_, err := NewClient(conn).Init(context.Background(), s.Init)
	if !errors.Is(err, ErrGone) {
		t.Fatal(err)
	}
	conn.reverse.business.mu.Lock()
	defer conn.reverse.business.mu.Unlock()
	if len(conn.reverse.business.parents) != 0 || !conn.reverse.business.closed {
		t.Fatal("failed write kept authority")
	}
}

// Unlike the forward-only peer, this wire observer records terminal replies.
func reversePeer(t *testing.T, opts ...ConnOption) *peer {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	in, out := io.Pipe()
	p := &peer{t: t, reqs: make(chan subprocess.RPCRequest, 64), toConn: out}
	p.conn = NewConn(in, w, opts...)
	go func() {
		sc := bufio.NewScanner(r)
		sc.Buffer(nil, 8<<20)
		for sc.Scan() {
			frame, err := decodeWireEnvelope(sc.Bytes())
			if err != nil {
				continue
			}
			p.reqs <- subprocess.RPCRequest{JSONRPC: "2.0", ID: frame.id, Method: frame.method, Params: frame.params}
		}
	}()
	t.Cleanup(func() { _ = p.conn.Close(); _ = out.Close(); _ = r.Close() })
	return p
}

type reverseBlockedWriter struct {
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
	closeOnce sync.Once
	closed    atomic.Bool
}

func (w *reverseBlockedWriter) Write(wire []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	if w.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	return len(wire), nil
}
func (w *reverseBlockedWriter) Close() error {
	w.closed.Store(true)
	w.closeOnce.Do(func() { close(w.release) })
	return nil
}
func TestReverseTerminalReceiptOwnedThroughPhysicalOutcome(t *testing.T) {
	for _, fence := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "fence"}[fence], func(t *testing.T) {
			in, out := io.Pipe()
			defer out.Close()
			writer := &reverseBlockedWriter{started: make(chan struct{}), release: make(chan struct{})}
			c := NewConn(in, writer)
			defer c.Close()
			credit, err := c.queue.reserveTerminal()
			if err != nil {
				t.Fatal(err)
			}
			id := subprocess.NumberID(1)
			receipt := &reverseReceipt{id: id, credit: credit}
			c.mu.Lock()
			c.inboundActive[id] = true
			err = c.publishReverseLocked(receipt, hostWireReply(1, subprocess.LogResult{Accepted: true}, nil), hostWireReply(1, nil, hostRefusal(capability.InternalError, "")))
			c.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-writer.started:
			case <-time.After(time.Second):
				t.Fatal("terminal write not selected")
			}
			c.mu.Lock()
			active := c.inboundActive[id] && c.activeReceipt == receipt
			c.mu.Unlock()
			if !active {
				t.Fatal("receipt retired before physical outcome")
			}
			if fence {
				_ = c.Close()
			} else {
				writer.closeOnce.Do(func() { close(writer.release) })
			}
			deadline := time.Now().Add(time.Second)
			for {
				c.mu.Lock()
				held := c.inboundActive[id]
				c.mu.Unlock()
				if !held {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("receipt not retired")
				}
				time.Sleep(time.Millisecond)
			}
			c.mu.Lock()
			receipt.finishLocked(c)
			receipt.finishLocked(c)
			c.mu.Unlock()
			c.queue.mu.Lock()
			reserved := c.queue.reserved
			c.queue.mu.Unlock()
			if reserved != 0 {
				t.Fatal("credit release not balanced", reserved)
			}
		})
	}
}

func TestReverseWrongOwnerCancelAndNotificationsHaveNoEffect(t *testing.T) {
	started := make(chan *HostCall, 1)
	release := make(chan struct{})
	canceled := make(chan struct{})
	s := reverseTestSpec(t, HostServices{StorageGet: func(ctx context.Context, call *HostCall, _ subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		started <- call
		select {
		case <-ctx.Done():
			close(canceled)
			return subprocess.StorageGetResult{}, ctx.Err()
		case <-release:
			return subprocess.StorageGetResult{Found: false}, nil
		}
	}})
	p := reverseReadyPeer(t, s)
	ctx := WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)})
	forward := callAsync(ctx, p.conn, subprocess.MethodHealth)
	req := p.request()
	reverseRequest(t, p, 1, HostStorageGet, req)
	call := <-started
	p.raw(`{"jsonrpc":"2.0","method":"rpc/cancel","params":{"request_owner":"host","id":1,"reason":"caller_cancelled"}}`) //nolint:misspell // SDK wire spelling.
	p.raw(`{"jsonrpc":"2.0","method":"host/storage/get","params":{}}`)
	// A barrier reply proves the earlier control/notification was processed,
	// without completing the parent whose descendants we are checking.
	barrier := callAsync(context.Background(), p.conn, "probe")
	other := p.request()
	p.reply(other.ID, `true`)
	if got := await(t, barrier); got.err != nil {
		t.Fatal(got.err)
	}
	if err := call.ctx.Err(); err != nil {
		t.Fatal("wrong-direction cancel affected plugin call", err)
	}
	p.reply(req.ID, `{"ok":true}`)
	if got := await(t, forward); got.err != nil {
		t.Fatal(got.err)
	}
	// Parent completion must now cancel descendants (independently of the invalid
	// control); retirement is observable before Call returns.
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("completed parent kept descendant")
	}
	close(release)
}

func TestReverseTerminalAndCancelRetireBeforeCallRelease(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "reply", true: "cancel"}[canceled], func(t *testing.T) {
			s := reverseTestSpec(t, HostServices{})
			p := reverseReadyPeer(t, s)
			ctx, end := context.WithTimeout(WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)}), time.Second)
			defer end()
			call, err := p.conn.queueCall(ctx, subprocess.MethodHealth, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer p.conn.releaseCall(call)
			req := p.request()
			id, ok := req.ID.Integer()
			if !ok || id <= 0 {
				t.Fatal("nonpositive selected ID")
				return
			}
			if canceled {
				p.conn.cancelQueuedCall(call, context.Canceled)
			} else {
				p.reply(req.ID, `{"ok":true}`)
				select {
				case <-call.reply:
				case <-time.After(time.Second):
					t.Fatal("no terminal")
				}
			}
			p.conn.reverse.business.mu.Lock()
			parent := p.conn.reverse.business.parents[uint64(id)]
			record := p.conn.reverse.business.bindings[call.prepared.id]
			terminal := record.parent.terminal
			p.conn.reverse.business.mu.Unlock()
			if parent != nil || !terminal {
				t.Fatal("authority remained until releaseCall")
			}
			p.conn.mu.Lock()
			admitted := p.conn.ordinaryCalls
			p.conn.mu.Unlock()
			if admitted != 1 {
				t.Fatal("test released the call before checking retirement")
			}
		})
	}
}

func TestReverseRevokeFencesInFlightCommitWithoutParentCompletion(t *testing.T) {
	started := make(chan *HostCall, 1)
	stopped := make(chan struct{})
	release := make(chan struct{})
	s := reverseTestSpec(t, HostServices{StorageGet: func(ctx context.Context, call *HostCall, _ subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		started <- call
		select {
		case <-ctx.Done():
			close(stopped)
		case <-release:
		}
		return subprocess.StorageGetResult{Found: false}, nil
	}})
	p := reverseReadyPeer(t, s)
	forward := callAsync(WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)}), p.conn, subprocess.MethodHealth)
	req := p.request()
	reverseRequest(t, p, 1, HostStorageGet, req)
	call := <-started
	p.conn.RevokeHostServices()
	if err := call.CheckCommit(); err == nil {
		t.Fatal("revoked business still allows in-flight commit")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("revoked backend not canceled")
	}
	close(release)
	p.reply(req.ID, `{"ok":true}`)
	_ = await(t, forward)
}
func TestReverseCompletedPluginIDCannotReplayUnderLiveParent(t *testing.T) {
	var calls atomic.Int64
	s := reverseTestSpec(t, HostServices{StorageGet: func(context.Context, *HostCall, subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		calls.Add(1)
		return subprocess.StorageGetResult{Found: false}, nil
	}})
	p := reverseReadyPeer(t, s)
	forward := callAsync(WithHostBinding(context.Background(), HostBinding{GrantID: "g-StorageGet", Scope: json.RawMessage(`{}`)}), p.conn, subprocess.MethodHealth)
	req := p.request()
	reverseRequest(t, p, 1, HostStorageGet, req)
	_ = p.frame()
	reverseRequest(t, p, 1, HostStorageGet, req)
	_ = p.frame()
	if calls.Load() != 1 {
		t.Fatal("terminal plugin ID replay executed backend", calls.Load())
	}
	p.reply(req.ID, `{"ok":true}`)
	if got := await(t, forward); got.err != nil {
		t.Fatal(got.err)
	}
}

func TestReverseNarrowedWholeWriteTimeoutRetiresStream(t *testing.T) {
	s := reverseTestSpec(t, HostServices{})
	s.Init.HostServices.Limits.WriteTimeoutMS = 30
	in, out := io.Pipe()
	defer out.Close()
	writer := &reverseBlockedWriter{started: make(chan struct{}), release: make(chan struct{})}
	c := NewConn(in, writer, WithReverseProfile(*s.Reverse, s.Init))
	defer c.Close()
	result := make(chan error, 1)
	go func() { result <- c.Notify("probe", nil) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrGone) {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		_ = c.Close()
		t.Fatal("physical write ignored offered 30ms ceiling")
	}
}
