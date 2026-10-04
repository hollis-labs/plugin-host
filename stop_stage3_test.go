package pluginhost

import (
	"context"
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
