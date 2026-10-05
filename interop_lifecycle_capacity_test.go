//go:build unix

package pluginhost

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-host/internal/interopfixture"
	"github.com/hollis-labs/plugin-sdk/subprocess"
)

type interopLifecycleInput struct {
	ID                          uint64
	Method, Name, Args, Binding string
	Finite                      bool
	Credit                      interopChildCredit
}
type interopLifecycleProof struct {
	Ref, expected                               interopQueueRef
	Deadline                                    time.Time
	Inputs                                      map[uint64]interopLifecycleInput
	InitParams                                  json.RawMessage
	Writes                                      []interopDisconnectWrite
	Overflow                                    bool
	Startup, Saturated, Released                map[string]int
	AfterOverflow                               map[string]int
	Authorities                                 []HostAuthority
	BackendEntries, BackendReturns              int
	LocalOrdinaryRefused, LocalLifecycleRefused bool
	CustodyRetired, Halfclosed                  bool
	ControlTrace                                []string
	BeforeRefusal, AfterRefusal                 interopLifecycleLocal
	BackendPermitBefore, BackendPermitAfter     int
	AuthorityBefore, AuthorityAfter             interopLifecycleAuthority
}

// Terminal records and retired bindings are bounded history, not live custody.
// Report them explicitly rather than projecting map lengths to zero.
type interopLifecycleAuthority struct {
	Parents, LiveBindings, RetainedBindings, Active, TerminalHistory int
}

func interopLifecycleAuthoritySnapshot(conn *Conn) interopLifecycleAuthority {
	conn.mu.Lock()
	session := conn.reverse.business
	conn.mu.Unlock()
	session.mu.Lock()
	defer session.mu.Unlock()
	v := interopLifecycleAuthority{Parents: len(session.parents), RetainedBindings: len(session.bindings), Active: len(session.active), TerminalHistory: len(session.terminal)}
	for _, binding := range session.bindings {
		if !binding.terminal {
			v.LiveBindings++
		}
	}
	return v
}

type interopLifecycleLocal struct {
	NextID                                                                                 int64
	Ordinary, Lifecycle, Pending, Correlations, Inbound, Workers, Reserved, PhysicalWrites int
}

func interopLifecycleLocalSnapshot(conn *Conn, tap *interopLifecycleWriter) interopLifecycleLocal {
	conn.mu.Lock()
	conn.queue.mu.Lock()
	v := interopLifecycleLocal{NextID: conn.nextID.Load(), Ordinary: conn.ordinaryCalls, Lifecycle: conn.lifecycleCalls, Pending: len(conn.pending), Correlations: len(conn.correlations), Inbound: len(conn.inboundActive), Workers: conn.reverse.workers, Reserved: conn.queue.reserved}
	conn.queue.mu.Unlock()
	conn.mu.Unlock()
	writes, _ := tap.snapshot()
	v.PhysicalWrites = len(writes)
	return v
}

// Preserve the real pipe's write deadlines and closure. This selected observer
// records underlying whole writes, never generates or normalizes protocol data.
type interopLifecycleWriter struct {
	file     *os.File
	mu       sync.Mutex
	writes   []interopDisconnectWrite
	overflow bool
}

func (w *interopLifecycleWriter) Write(raw []byte) (int, error) {
	n, err := w.file.Write(raw)
	v := interopDisconnectWrite{Raw: string(raw), Bytes: n, At: time.Now()}
	if err != nil {
		v.Error = err.Error()
	}
	w.mu.Lock()
	if len(w.writes) >= 64 || len(raw) > 64<<10 {
		w.overflow = true
	} else {
		w.writes = append(w.writes, v)
	}
	w.mu.Unlock()
	return n, err
}
func (w *interopLifecycleWriter) Close() error { return w.file.Close() }
func (w *interopLifecycleWriter) SetWriteDeadline(d time.Time) error {
	return w.file.SetWriteDeadline(d)
}
func (w *interopLifecycleWriter) snapshot() ([]interopDisconnectWrite, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]interopDisconnectWrite(nil), w.writes...), w.overflow
}

func interopLifecycleSnapshot(t *testing.T, c *interopControls) map[string]int {
	t.Helper()
	if err := c.release("snapshot"); err != nil {
		t.Fatal(err)
	}
	e, err := c.event("snapshot")
	if err != nil {
		t.Fatal(err)
	}
	var effects map[string]int
	if err = json.Unmarshal(e["effects"], &effects); err != nil {
		t.Fatal(err)
	}
	return effects
}

func interopLifecycleCustody(conn *Conn) bool {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	conn.queue.mu.Lock()
	defer conn.queue.mu.Unlock()
	return len(conn.pending) == 0 && len(conn.correlations) == 0 && conn.ordinaryCalls == 0 && conn.lifecycleCalls == 0 && len(conn.inboundActive) == 0 && conn.reverse.workers == 0 && len(conn.reverse.active) == 0 && conn.activeReceipt == nil && conn.queue.reserved == 0 && conn.queue.active == nil && len(conn.outbound) == 0
}

func interopLifecycleRetire(t *testing.T, conn *Conn, tap *interopLifecycleWriter, call *pendingCall, id uint64) interopChildCredit {
	t.Helper()
	for tries := 0; tries < 40; tries++ {
		conn.mu.Lock()
		conn.queue.mu.Lock()
		v := interopChildCredit{ID: id, InputBytes: call.publication.bytes, WholeInput: call.publication.complete && call.publication.err == nil && call.publication.bytes > 0, LocalOK: true, Released: call.released, CreditRetired: !call.credit.held && call.frame == nil}
		conn.queue.mu.Unlock()
		conn.mu.Unlock()
		// Finite Client.Load uses the ordinary Call path, whose publication
		// field is intentionally correlation-only. Its actual native whole
		// input is attested by the selected tap, never synthesized from return.
		writes, overflow := tap.snapshot()
		if overflow {
			t.Fatal("lifecycle tap overflow")
		}
		matches := 0
		for _, write := range writes {
			var frame struct {
				ID     uint64
				Method string
			}
			if json.Unmarshal([]byte(write.Raw), &frame) == nil && frame.ID == id && frame.Method == call.method {
				matches++
				if !call.correlation {
					v.InputBytes = write.Bytes
					v.WholeInput = write.Error == "" && write.Bytes == len(write.Raw) && write.Bytes > 0
				}
			}
		}
		if matches != 1 {
			v.WholeInput = false
		}
		if v.WholeInput && v.Released && v.CreditRetired {
			return v
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("lifecycle physical call custody not retired", id)
	return interopChildCredit{}
}

// Capture the selected pending object while the actual callback remains held.
// IDs come solely from writer publication; neither events nor the test mint IDs.
func interopLifecycleEntered(t *testing.T, p *Process, c *interopControls, method, name string) (uint64, *pendingCall) {
	t.Helper()
	e, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool {
		return rawEventString(e, "direction") == "host-to-worker" && rawEventString(e, "method") == method
	})
	var id uint64
	if err != nil || json.Unmarshal(e["id"], &id) != nil || id == 0 {
		t.Fatal("lifecycle actual input", err, e)
	}
	entered, err := c.eventWhere("entered", func(e map[string]json.RawMessage) bool { var n uint64; _ = json.Unmarshal(e["id"], &n); return n == id })
	var finite bool
	if err != nil || rawEventString(entered, "name") != name || json.Unmarshal(entered["deadline"], &finite) != nil || finite != (name != "hold") {
		t.Fatal("lifecycle entered identity/deadline", err, entered)
	}
	p.conn.mu.Lock()
	call := p.conn.pending[subprocess.NumberID(int64(id))]
	p.conn.mu.Unlock()
	if call == nil {
		t.Fatal("lifecycle selected pending object absent", id)
	}
	return id, call
}

func TestSDKLifecycleCompatiblePublicReplay(t *testing.T) {
	if os.Getenv("INTEROP_LIFECYCLE_REPLAY") != "1" {
		t.Skip("isolated lifecycle candidate validation opt-in")
	}
	source := os.Getenv("INTEROP_SDK_SOURCE")
	raw, err := os.ReadFile(filepath.Join(source, "protocol/v2/fixtures/duplex-child-lifecycle-compatible-v2.json")) //nolint:gosec // Explicit test-owned immutable source.
	if err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(source, ".interop-source-commit")) //nolint:gosec // Explicit test-owned immutable source.
	if err != nil || string(marker) != os.Getenv("INTEROP_LIFECYCLE_SDK_HEAD")+"\n" || len(marker) != 41 {
		t.Fatal("lifecycle immutable source receipt", err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != os.Getenv("INTEROP_LIFECYCLE_MANIFEST_SHA256") {
		t.Fatal("lifecycle manifest receipt")
	}
	selected, err := interopfixture.SelectLifecycle(raw, interopfixture.LifecyclePublicMode, nil)
	if err != nil {
		t.Fatal(err)
	}
	var reports []map[string]any
	for _, recipeRaw := range selected {
		var recipe interopRecipeRow
		if err = json.Unmarshal(recipeRaw, &recipe); err != nil {
			t.Fatal(err)
		}
		if recipe.Profile != "expanded" || (recipe.Scenario != "forward-v2" && recipe.Scenario != "credits-v2") {
			t.Fatalf("unsupported lifecycle candidate recipe %s/%s", recipe.Scenario, recipe.Profile)
		}
		for _, runtime := range []string{"go", "node", "deno"} {
			t.Run(recipe.Name+"/"+runtime, func(t *testing.T) {
				row := replayInteropLifecycle(t, runtime, recipe, raw, string(marker[:40]))
				row["selected_source_row"] = append(json.RawMessage(nil), recipeRaw...)
				reports = append(reports, row)
			})
		}
	}
	if path := os.Getenv("INTEROP_LIFECYCLE_REPORT"); path != "" {
		body, err := json.MarshalIndent(map[string]any{"scope": "isolated candidate validation; source status retained; historical raw forward/credits pending incompatible", "sdk_source": string(marker[:40]), "manifest_sha256": fmt.Sprintf("%x", sha256.Sum256(raw)), "manifest": json.RawMessage(raw), "host_base": "8cec622c4538d8d4737188f52ab5b3f6da8ef8a8", "host_commit": os.Getenv("INTEROP_HOST_COMMIT"), "host_dirty": os.Getenv("INTEROP_HOST_DIRTY"), "assets": interopLifecycleAssets(t, source), "rows": reports}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, append(body, '\n'), 0600); err != nil { //nolint:gosec // Explicit test-owned report path.
			t.Fatal(err)
		} //nolint:gosec // Explicit test-owned report.
	}
}

func interopLifecycleAssets(t *testing.T, source string) map[string]any {
	t.Helper()
	hashes := map[string]string{}
	paths := map[string]string{"go_child": os.Getenv("INTEROP_GO_CHILD")}
	for _, path := range []string{"protocol/v2/fixtures/duplex-child-lifecycle-compatible-v2.json", "ts/packages/plugin-sdk/test/lifecycle-compatible-selection.js", "ts/packages/plugin-sdk/test/lifecycle-compatible-selection.test.js", "ts/packages/plugin-sdk/test/lifecycle-compatible-replay.js", "ts/packages/plugin-sdk/test/negotiated-worker.js", "ts/packages/plugin-sdk/test/child-cases-worker.js", "ts/packages/plugin-sdk/test/child-control.js", "ts/packages/plugin-sdk/dist/index.js"} {
		paths[path] = filepath.Join(source, path)
	}
	for name, path := range paths {
		file, err := os.Open(path) //nolint:gosec // Explicit immutable test-owned assets.
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil {
			t.Fatal(copyErr, closeErr)
		}
		hashes[name] = fmt.Sprintf("%x", hash.Sum(nil))
	}
	buildRoot := filepath.Dir(os.Getenv("INTEROP_GO_CHILD"))
	goReceipt, err := os.ReadFile(filepath.Join(buildRoot, "build-receipt")) //nolint:gosec // Explicit test-owned build receipt.
	if err != nil {
		t.Fatal(err)
	}
	tsReceipt, err := os.ReadFile(filepath.Join(buildRoot, "ts-build-receipt")) //nolint:gosec // Explicit test-owned build receipt.
	if err != nil {
		t.Fatal(err)
	}
	return map[string]any{"sha256": hashes, "source_root": source, "go_build_receipt": string(goReceipt), "ts_build_receipt": string(tsReceipt), "sdk_module": "5c663e7ce74c40ceceb95133c439396316850858", "sdk_base": "d04ab2149506a96e8c54b58829f58ee480e0de41", "historical_fixture": "ea8ec0dca862d0c7284cc6a130a4b27fb812ed21", "runtime_versions": map[string]string{"go": os.Getenv("INTEROP_GO_VERSION"), "node": os.Getenv("INTEROP_NODE_VERSION"), "deno": os.Getenv("INTEROP_DENO_VERSION")}}
}

func replayInteropLifecycle(t *testing.T, runtime string, recipe interopRecipeRow, manifest []byte, head string) map[string]any {
	t.Helper()
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	ref := interopQueueRef{head, fmt.Sprintf("%x", sha256.Sum256(manifest)), recipe.Name, recipe.Scenario, recipe.Profile, runtime, fmt.Sprintf("%x", nonce), 1, 1}
	deadline := time.Now().Add(10 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	proof := &interopLifecycleProof{Ref: ref, expected: ref, Deadline: deadline, Inputs: map[uint64]interopLifecycleInput{}}
	b := &interopBackend{getGate: make(chan struct{}), getEntered: make(chan *HostCall, 8)}
	spec := interopSourceSpec(t, b)
	proof.InitParams, _ = json.Marshal(spec.Init)
	copyReverse := *spec.Reverse
	copyReverse.LifecycleBinding = nil
	spec.Reverse = &copyReverse
	services := b.services()
	// The frozen offer requires these callback seams to exist. This selected
	// recipe grants neither: refuse without effects if unexpected traffic arrives.
	services.Log = func(context.Context, *HostCall, subprocess.LogParams) (subprocess.LogResult, error) {
		return subprocess.LogResult{}, errors.New("lifecycle candidate forbids log callback")
	}
	services.StoragePut = func(context.Context, *HostCall, subprocess.StoragePutParams) (subprocess.StoragePutResult, error) {
		return subprocess.StoragePutResult{}, errors.New("lifecycle candidate forbids mutation callback")
	}
	originalGet := services.StorageGet
	var backendMu sync.Mutex
	services.StorageGet = func(ctx context.Context, call *HostCall, params subprocess.StorageGetParams) (subprocess.StorageGetResult, error) {
		callbackDeadline, bounded := ctx.Deadline()
		if !bounded || callbackDeadline.After(deadline) || params.Key != "read" {
			return subprocess.StorageGetResult{}, errors.New("lifecycle backend budget/target")
		}
		backendMu.Lock()
		proof.BackendEntries++
		backendMu.Unlock()
		v, err := originalGet(ctx, call, params)
		backendMu.Lock()
		proof.BackendReturns++
		backendMu.Unlock()
		return v, err
	}
	spec.Reverse.Runtime = NewHostServiceRuntime(services, func(ctx context.Context, _ HostAuthority) error { return ctx.Err() })
	tap := &interopLifecycleWriter{}
	spec.ConnOptions = append(spec.ConnOptions, func(conn *Conn) {
		file, ok := conn.w.(*os.File)
		if !ok {
			panic("lifecycle native pipe required")
		}
		tap.file = file
		conn.w = tap
	})
	p, c := spawnInteropChild(t, runtime, recipe.Profile, services, spec)
	c.childStream.Store(true)
	c.lifecycleStream.Store(true)
	c.lifecycleProof = proof
	c.expandedReverseLimit = 8
	if _, err := c.event("ready"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Client().Init(ctx, spec.Init); err != nil {
		t.Fatal(err)
	}
	initInput, err := c.eventWhere("wire", func(e map[string]json.RawMessage) bool { return rawEventString(e, "method") == subprocess.MethodInit })
	var initID uint64
	if err != nil || json.Unmarshal(initInput["id"], &initID) != nil {
		t.Fatal("lifecycle Init input", err)
	}
	proof.Inputs[initID] = interopLifecycleInput{ID: initID, Method: subprocess.MethodInit, Finite: true}
	startupDone := make(chan error, 1)
	go func() { _, e := p.Client().Load(ctx); startupDone <- e }()
	startupID, startupCall := interopLifecycleEntered(t, p, c, subprocess.MethodLoad, "load")
	if err := c.release(fmt.Sprintf("request-%d", startupID)); err != nil {
		t.Fatal(err)
	}
	if err := <-startupDone; err != nil {
		t.Fatal(err)
	}
	proof.Inputs[startupID] = interopLifecycleInput{ID: startupID, Method: subprocess.MethodLoad, Finite: true, Credit: interopLifecycleRetire(t, p.conn, tap, startupCall, startupID)}
	if err := p.conn.ActivateHostServices(); err != nil {
		t.Fatal(err)
	}
	proof.Startup = interopLifecycleSnapshot(t, c)
	if !interopLifecycleCustody(p.conn) {
		t.Fatal("startup local custody outstanding")
	}
	type held struct {
		id   uint64
		call *pendingCall
		done chan error
	}
	var calls []held
	holds := 16
	if recipe.Scenario == "credits-v2" {
		holds = 15
	}
	for i := 0; i < holds; i++ {
		done := make(chan error, 1)
		go func() {
			raw, e := p.conn.CallCorrelation(ctx, subprocess.MethodCommandExecute, subprocess.CommandExecParams{Name: "hold", Args: "{}"})
			if e == nil {
				var result subprocess.CommandExecResult
				e = json.Unmarshal(raw, &result)
				if e == nil && result.Action != "noop" {
					e = errors.New("hold typed outcome")
				}
			}
			done <- e
		}()
		id, call := interopLifecycleEntered(t, p, c, subprocess.MethodCommandExecute, "hold")
		proof.Inputs[id] = interopLifecycleInput{ID: id, Method: subprocess.MethodCommandExecute, Name: "hold", Args: "{}"}
		calls = append(calls, held{id, call, done})
	}
	if holds == 15 {
		done := make(chan error, 1)
		go func() {
			v, e := interopCommand(ctx, p, "get", map[string]any{"n": 8, "key": "read"}, "g-StorageGet")
			if e == nil {
				var outcomes []interopHelperOutcome
				e = json.Unmarshal([]byte(v.Content), &outcomes)
				if len(outcomes) != 8 {
					e = errors.New("get helper cardinality")
				}
				for _, v := range outcomes {
					if v.Code != "ok" {
						e = errors.New("get helper refused")
					}
				}
			}
			done <- e
		}()
		id, call := interopLifecycleEntered(t, p, c, subprocess.MethodCommandExecute, "get")
		for i := 0; i < 8; i++ {
			select {
			case hostCall := <-b.getEntered:
				proof.Authorities = append(proof.Authorities, hostCall.Authority())
			case <-ctx.Done():
				t.Fatal("lifecycle backend entry", ctx.Err())
			}
		}
		var binding string
		p.conn.mu.Lock()
		if call.prepared != nil {
			binding = string(call.prepared.id)
		}
		p.conn.mu.Unlock()
		proof.Inputs[id] = interopLifecycleInput{ID: id, Method: subprocess.MethodCommandExecute, Name: "get", Args: `{"key":"read","n":8}`, Binding: binding, Finite: true}
		calls = append(calls, held{id, call, done})
	}
	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() { _, e := p.Client().Load(ctx); done <- e }()
		id, call := interopLifecycleEntered(t, p, c, subprocess.MethodLoad, "load")
		proof.Inputs[id] = interopLifecycleInput{ID: id, Method: subprocess.MethodLoad, Finite: true}
		calls = append(calls, held{id, call, done})
	}
	proof.Saturated = interopLifecycleSnapshot(t, c)
	proof.BeforeRefusal = interopLifecycleLocalSnapshot(p.conn, tap)
	proof.AuthorityBefore = interopLifecycleAuthoritySnapshot(p.conn)
	spec.Reverse.Runtime.pool.mu.Lock()
	proof.BackendPermitBefore = spec.Reverse.Runtime.pool.active
	spec.Reverse.Runtime.pool.mu.Unlock()
	before := p.conn.nextID.Load()
	_, ordinaryErr := p.conn.CallCorrelation(ctx, subprocess.MethodCommandExecute, subprocess.CommandExecParams{Name: "hold", Args: "{}"})
	_, lifecycleErr := p.Client().Load(ctx)
	proof.LocalOrdinaryRefused = errors.Is(ordinaryErr, ErrAdmissionFull)
	proof.LocalLifecycleRefused = errors.Is(lifecycleErr, ErrAdmissionFull)
	proof.AfterRefusal = interopLifecycleLocalSnapshot(p.conn, tap)
	if proof.BeforeRefusal != proof.AfterRefusal {
		t.Fatal("local refusal changed writer/admission custody", proof.BeforeRefusal, proof.AfterRefusal)
	}
	if !proof.LocalOrdinaryRefused || !proof.LocalLifecycleRefused || p.conn.nextID.Load() != before {
		t.Fatal("lifecycle local prepublication overflow", ordinaryErr, lifecycleErr)
	}
	proof.AfterOverflow = interopLifecycleSnapshot(t, c)
	for _, call := range calls {
		if proof.Inputs[call.id].Name != "get" {
			if err := c.release(fmt.Sprintf("request-%d", call.id)); err != nil {
				t.Fatal(err)
			}
		}
	}
	close(b.getGate)
	for _, h := range calls {
		select {
		case err := <-h.done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		v := proof.Inputs[h.id]
		v.Credit = interopLifecycleRetire(t, p.conn, tap, h.call, h.id)
		proof.Inputs[h.id] = v
	}
	for tries := 0; tries < 40; tries++ {
		proof.Released = interopLifecycleSnapshot(t, c)
		if proof.Released["reserved_frames"] == 0 && proof.Released["reverse_pending"] == 0 && interopLifecycleCustody(p.conn) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	proof.CustodyRetired = interopLifecycleCustody(p.conn)
	proof.AuthorityAfter = interopLifecycleAuthoritySnapshot(p.conn)
	spec.Reverse.Runtime.pool.mu.Lock()
	proof.BackendPermitAfter = spec.Reverse.Runtime.pool.active
	spec.Reverse.Runtime.pool.mu.Unlock()
	if !proof.CustodyRetired || !time.Now().Before(deadline) {
		t.Fatal("lifecycle custody/root deadline")
	}
	backendMu.Lock()
	backendOutstanding := proof.BackendEntries != proof.BackendReturns
	backendMu.Unlock()
	if backendOutstanding {
		t.Fatal("lifecycle backend still executing")
	}
	b.mu.Lock()
	commits := b.commits
	b.mu.Unlock()
	if commits != 0 {
		t.Fatal("lifecycle unexpected effect")
	}
	proof.ControlTrace = append([]string(nil), c.trace...)
	proof.Halfclosed = true
	if err := p.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.exited:
	case <-ctx.Done():
		t.Fatal("lifecycle natural exit", ctx.Err())
	}
	exit, ok := p.ExitInfo()
	if !ok || exit.Code != 0 || exit.Signal != "" {
		t.Fatal("lifecycle natural exit", exit)
	}
	proof.Writes, proof.Overflow = tap.snapshot()
	if err := c.finish(0); err != nil {
		if root := os.Getenv("INTEROP_LIFECYCLE_FAILURE_DIR"); root != "" {
			body, _ := json.MarshalIndent(map[string]any{"proof": proof, "events": c.observed, "failure": err.Error(), "exit": exit}, "", "  ")
			if writeErr := os.WriteFile(filepath.Join(root, recipe.Scenario+"-"+runtime+"-failure.json"), append(body, '\n'), 0600); writeErr != nil { //nolint:gosec // Explicit test-owned failure evidence.
				t.Error(writeErr)
			} //nolint:gosec // Explicit test-owned failure evidence.
		}
		t.Fatal(err)
	}
	return map[string]any{"recipe": recipe, "runtime": runtime, "candidate_validation": "completed", "proof": proof, "events": c.observed, "exit": exit, "public_domain": "local ErrAdmissionFull; not SDK remote overflow"}
}
