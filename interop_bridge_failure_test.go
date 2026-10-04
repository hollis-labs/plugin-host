//go:build unix

package pluginhost

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestIndependentPR14ExitOneWorker(t *testing.T) {
	if os.Getenv("SDK_FIXTURE_CHILD") != "review-exit1" {
		return
	}
	// Leave the bridge's control reader blocked before this natural exit.
	time.Sleep(100 * time.Millisecond)
	fmt.Fprintln(os.Stderr, `{"fixture_event":{"kind":"finished","effects":{"unload_attempts":1},"transport_error":"authored natural failure"}}`)
	os.Exit(1)
}

func TestIndependentPR14LateBridgeFailureCannotPass(t *testing.T) {
	root, err := os.MkdirTemp(os.Getenv("TMPDIR"), "rv14-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	control := filepath.Join(root, "control")
	if err = os.Mkdir(control, 0700); err != nil { //nolint:gosec // Private test-owned scratch path, never plugin input.
		t.Fatal(err)
	}
	if err = syscall.Mkfifo(control+".next", 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(root, "sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args, _ := json.Marshal([]string{"-test.run=^TestIndependentPR14ExitOneWorker$"})
	cmd := exec.Command(executable, "-test.run=^TestInteropControlBridge$") //nolint:gosec // Re-exec this test binary, never a plugin command.
	cmd.Env = []string{"INTEROP_BRIDGE=1", "INTEROP_SOCKET=" + listener.Addr().String(), "INTEROP_COMMAND=" + executable, "INTEROP_ARGS=" + string(args), "INTEROP_PROFILE=review-exit1", "INTEROP_PATH_CONTROL=" + control, "GOMAXPROCS=4"}
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdinR.Close()
	defer stdinW.Close()
	cmd.Stdin = stdinR
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()
	listener.SetDeadline(time.Now().Add(2 * time.Second))
	socket, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	socket.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err = io.WriteString(socket, "{\"seq\":1,\"op\":\"release\",\"gate\":\"snapshot\"}\n"); err != nil {
		t.Fatal(err)
	}
	c := &interopControls{events: make(chan map[string]json.RawMessage, 8), failure: make(chan error, 1)}
	reader := bufio.NewReader(socket)
	for {
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil {
			t.Fatal(readErr)
		}
		var v struct {
			Event map[string]json.RawMessage `json:"fixture_event"`
		}
		if err = json.Unmarshal(line, &v); err != nil {
			t.Fatal(err)
		}
		if err = c.enqueue(v.Event); err != nil {
			t.Fatal(err)
		}
		if rawEventString(v.Event, "kind") == "worker_exit" {
			break
		}
	}
	// worker_exit proves cmd.Wait selected the natural exit branch. Complete
	// the blocked control-file write now; rename FIFO onto directory must fail.
	fifo, err := os.Open(control + ".next") //nolint:gosec // Private test-owned FIFO, never plugin input.
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.ReadAll(fifo); err != nil {
		t.Fatal(err)
	}
	fifo.Close()
	for {
		line, readErr := reader.ReadBytes('\n')
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			t.Fatal(readErr)
		}
		var terminal struct {
			Event map[string]json.RawMessage `json:"fixture_event"`
		}
		if err := json.Unmarshal(line, &terminal); err != nil {
			t.Fatal(err)
		}
		if err := c.enqueue(terminal.Event); err != nil {
			t.Fatal(err)
		}
	}
	c.failure <- io.EOF
	waitErr := cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(waitErr, &exit) || exit.ExitCode() != 125 {
		t.Fatalf("required harness failure must have distinct bridge exit: %v", waitErr)
	}
	if err := c.finish(1); err == nil {
		t.Fatalf("late control rename failure accepted as natural worker exit: bridge stderr=%q", stderr.String())
	}
}
