package pluginhost

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hollis-labs/plugin-sdk/subprocess"
	"os"
)

func TestFailedStartCleanupToleratesUnloadBeforeInit(t *testing.T) {
	peer := newPeer(t)
	stdin, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	defer writer.Close()
	p := &Process{spec: Spec{UnloadTimeout: time.Second}, client: NewClient(peer.conn), conn: peer.conn, stdin: writer, exited: make(chan struct{})}
	result := make(chan [2]error, 1)
	go func() { a, b := p.stopWithReport(context.Background()); result <- [2]error{a, b} }()
	request := peer.request()
	peer.raw(`{"jsonrpc":"2.0","id":` + itoa(request.ID) + `,"error":{"code":-32600,"message":"init required"}}`)
	close(p.exited)
	got := <-result
	if got[0] != nil || got[1] != nil {
		t.Fatalf("pre-init terminal refusal became incomplete teardown: %v", got)
	}
	select {
	case <-p.Exited():
	default:
		t.Fatal("child not reaped")
	}
}

func TestDefaultUnloadBudgetAllowsSDKDrainAndMargin(t *testing.T) {
	if got := (Spec{}).normalized().UnloadTimeout; got < subprocess.DefaultShutdownTimeout+time.Second {
		t.Fatal(got)
	}
}

func TestInitializedCleanupRetainsInvalidRequestRefusal(t *testing.T) {
	peer := newPeer(t)
	stdin, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	defer writer.Close()
	p := &Process{spec: Spec{UnloadTimeout: time.Second}, client: NewClient(peer.conn), conn: peer.conn, stdin: writer, exited: make(chan struct{}), info: subprocess.InitResult{ID: "fixture"}}
	result := make(chan error, 1)
	go func() { a, _ := p.stopWithReport(context.Background()); result <- a }()
	request := peer.request()
	peer.raw(`{"jsonrpc":"2.0","id":` + itoa(request.ID) + `,"error":{"code":-32600,"message":"unexpected refusal"}}`)
	close(p.exited)
	var rpc *subprocess.RPCError
	if err := <-result; !errors.As(err, &rpc) || rpc.Code != -32600 {
		t.Fatal(err)
	}
}

func TestUnloadTimeoutNeverSendsCancellationControl(t *testing.T) {
	peer := newPeer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- NewClient(peer.conn).Unload(ctx) }()
	peer.request()
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := peer.conn.Notify("barrier", nil); err != nil {
		t.Fatal(err)
	}
	if next := peer.frame(); next.Method != "barrier" {
		t.Fatalf("terminal unload sent extra control: %+v", next)
	}
	if peer.conn.CancelDropped() != 0 {
		t.Fatal("attempted unload cancellation")
	}
}
